//! Sequential, bounded native checkpoint transfers. Callers reserve the reported
//! memory and retain an abort owner across cancellation.
use crate::{StorageError, StorageProvider};
use bytes::Bytes;
use object_store::{
    ObjectStore, PutMode, PutOptions,
    multipart::{MultipartStore, PartId},
    path::Path,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::AsyncWriteExt;

pub const CHUNK_BYTES: usize = 64 * 1024;
const CLOUD_PART_BYTES: usize = 5 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;
const MAX_PART_ID_BYTES: usize = 256;
const MAX_JOURNAL_BYTES: u64 = 16 * 1024;
const COPY_OBJECT_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
const AZURE_COPY_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

fn invalid(message: &str) -> StorageError {
    StorageError::PathError(message.into())
}

pub struct StreamingCreate {
    store: Arc<dyn ObjectStore>,
    temporary: Path,
    destination: Path,
    upload: Option<CloudUpload>,
    journal: Option<Path>,
    local_file: Option<tokio::fs::File>,
    buffer: Vec<u8>,
    part_bytes: usize,
    parts: usize,
    local_root: Option<std::path::PathBuf>,
    destination_created: Arc<AtomicBool>,
    azure_block_blob: bool,
}

struct CloudUpload {
    provider: Arc<dyn MultipartStore>,
    id: String,
    parts: Vec<PartId>,
}

impl StorageProvider {
    fn native_upload_provider_kind(&self) -> &'static str {
        match self.config() {
            crate::BackendConfig::Local(_) => "local",
            crate::BackendConfig::S3(_) => "s3",
            crate::BackendConfig::R2(_) => "r2",
            crate::BackendConfig::GCS(_) => "gcs",
            crate::BackendConfig::Azure(_) => "azure-block-blob",
        }
    }
    fn checkpoint_part_bytes(&self) -> usize {
        if self.config().is_local() {
            CHUNK_BYTES
        } else {
            CLOUD_PART_BYTES
        }
    }

    /// One part is submitted at a time. Allow source/current/provider copies and
    /// bounded multipart identifiers, independently of the size of the SST.
    pub fn streaming_create_memory_bytes(&self) -> usize {
        if self.config().is_local() {
            CHUNK_BYTES * 4
        } else {
            2 * self.checkpoint_part_bytes()
                + CHUNK_BYTES
                + 2 * MAX_PARTS * (std::mem::size_of::<PartId>() + MAX_PART_ID_BYTES)
        }
    }

    /// Remote ranges amortize HTTP latency within a fixed admitted provider
    /// buffer. Consumers still write/hash in 64 KiB chunks.
    pub fn streaming_read_range_bytes(&self) -> usize {
        self.checkpoint_part_bytes()
    }

    pub fn validate_native_file_size(&self, size: u64) -> Result<(), StorageError> {
        if matches!(
            self.config(),
            crate::BackendConfig::S3(_) | crate::BackendConfig::R2(_)
        ) && size > COPY_OBJECT_LIMIT
        {
            return Err(invalid(
                "native SST exceeds atomic CopyObject 5 GiB limit; hidden multipart-copy fallback is forbidden",
            ));
        }
        Ok(())
    }

    fn validate_native_copy_strategy(&self) -> Result<(), StorageError> {
        let expected = match self.config() {
            crate::BackendConfig::S3(_) => "header:if-none-match:*",
            crate::BackendConfig::R2(_) => "header:cf-copy-destination-if-none-match:*",
            _ => return Ok(()),
        };
        let configured = self
            .storage_options()
            .get("aws_copy_if_not_exists")
            .map(|value| value.replace(' ', "").to_ascii_lowercase());
        if configured.as_deref() != Some(expected) {
            return Err(invalid(
                "native checkpoint storage requires an atomic destination conditional CopyObject header; opaque multipart copy is unsafe across process loss",
            ));
        }
        Ok(())
    }

    async fn ensure_native_copy_probe(&self, temporary: &Path) -> Result<(), StorageError> {
        if self.config().is_local() {
            return Ok(());
        }
        self.native_copy_probe.get_or_try_init(|| async {
            let first = Path::from(format!("{temporary}.probe-source-a"));
            let second = Path::from(format!("{temporary}.probe-source-b"));
            let destination = Path::from(format!("{temporary}.probe-destination"));
            let mut owned = Vec::with_capacity(3);
            let result = async {
                self.object_store.put_opts(&first, Bytes::from_static(b"native-probe-a").into(),
                    PutOptions {mode: PutMode::Create, ..Default::default()}).await?;
                owned.push(first.clone());
                self.object_store.put_opts(&second, Bytes::from_static(b"native-probe-b").into(),
                    PutOptions {mode: PutMode::Create, ..Default::default()}).await?;
                owned.push(second.clone());
                self.object_store.copy_if_not_exists(&first, &destination).await?;
                owned.push(destination.clone());
                let azure = matches!(self.config(), crate::BackendConfig::Azure(_));
                if azure {
                    wait_for_block_blob_copy(self.object_store.as_ref(), &destination, b"n", AZURE_COPY_READY_TIMEOUT).await?;
                }
                if self.object_store.get_range(&destination, 0..14).await?.as_ref() != b"native-probe-a" {
                    return Err(invalid("native storage conditional-copy probe produced wrong bytes"));
                }
                let collision = self.object_store.copy_if_not_exists(&second, &destination).await
                    .map_err(|error| normalize_copy_collision(error, &destination, azure));
                let unchanged = self.object_store.get_range(&destination, 0..14).await?;
                if !matches!(collision, Err(object_store::Error::AlreadyExists {..})) || unchanged.as_ref() != b"native-probe-a" {
                    return Err(invalid("native storage lacks verified atomic create-only copy; refusing payload upload"));
                }
                Ok::<_, StorageError>(())
            }.await;
            for path in owned { let _ = self.object_store.delete(&path).await; }
            result
        }).await?;
        Ok(())
    }

    pub async fn start_streaming_create(
        &self,
        destination: &str,
        unique_temporary: &str,
    ) -> Result<StreamingCreate, StorageError> {
        self.start_streaming_create_tracked(
            destination,
            unique_temporary,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    /// The shared flag records only positively acknowledged conditional creation.
    /// A caller's rollback must never delete a colliding preexisting destination.
    pub async fn start_streaming_create_tracked(
        &self,
        destination: &str,
        unique_temporary: &str,
        destination_created: Arc<AtomicBool>,
    ) -> Result<StreamingCreate, StorageError> {
        if destination == unique_temporary {
            return Err(invalid("streaming temporary and final paths must differ"));
        }
        self.validate_native_copy_strategy()?;
        if destination_created.load(Ordering::Acquire) {
            return Err(invalid(
                "native destination ownership flag must start unset",
            ));
        }
        let azure_block_blob = matches!(self.config(), crate::BackendConfig::Azure(_));
        let temporary = self
            .qualify_path(&Path::from(unique_temporary))
            .into_owned();
        let destination = self.qualify_path(&Path::from(destination)).into_owned();
        self.ensure_native_copy_probe(&temporary).await?;
        let local_root = match self.config() {
            crate::BackendConfig::Local(config) => Some(std::path::PathBuf::from(&config.path)),
            _ => None,
        };
        // object_store's local multipart writer uses hidden `#n` files that its
        // own listing intentionally omits. A killed worker would strand those
        // bytes beyond fenced GC's reach. Use a visible exclusively owned file.
        let (upload, local_file, journal) = if let Some(root) = &local_root {
            let path = root.join(temporary.as_ref());
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| invalid(&e.to_string()))?;
            }
            let file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .await
                .map_err(|e| invalid(&e.to_string()))?;
            (None, Some(file), None)
        } else {
            let provider = self.as_multipart().ok_or_else(|| {
                invalid("native checkpoint provider lacks journalable multipart uploads")
            })?;
            let id = provider.create_multipart(&temporary).await?;
            if id.is_empty() && !azure_block_blob || id.len() > 4096 {
                let _ = provider.abort_multipart(&temporary, &id).await;
                return Err(invalid("native multipart upload id exceeds journal bound"));
            }
            let journal = Path::from(format!("{temporary}.mpu"));
            let record = serde_json::to_vec(&serde_json::json!({
                "version": 1, "temporary": temporary.as_ref(), "upload_id": id,
                "provider": self.native_upload_provider_kind(),
            }))
            .map_err(|e| invalid(&e.to_string()))?;
            if record.len() as u64 > MAX_JOURNAL_BYTES {
                let _ = provider.abort_multipart(&temporary, &id).await;
                return Err(invalid("native multipart journal exceeds bound"));
            }
            if let Err(error) = self
                .object_store
                .put_opts(
                    &journal,
                    record.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                let _ = provider.abort_multipart(&temporary, &id).await;
                return Err(error.into());
            }
            // No payload part is submitted until the visible, immutable upload
            // journal positively exists. Process loss before this leaves zero
            // uploaded payload; fenced GC can abort every known payload owner.
            (
                Some(CloudUpload {
                    provider,
                    id,
                    parts: Vec::with_capacity(MAX_PARTS),
                }),
                None,
                Some(journal),
            )
        };
        Ok(StreamingCreate {
            store: self.object_store.clone(),
            temporary,
            destination,
            upload,
            journal,
            local_file,
            buffer: if local_root.is_some() {
                Vec::new()
            } else {
                Vec::with_capacity(self.checkpoint_part_bytes())
            },
            part_bytes: self.checkpoint_part_bytes(),
            parts: 0,
            local_root,
            destination_created,
            azure_block_blob,
        })
    }

    /// Range reads bound provider buffering as well as the caller's read buffer.
    pub async fn checkpoint_range(
        &self,
        path: &str,
        start: u64,
        end: u64,
    ) -> Result<Bytes, StorageError> {
        if end < start || end - start > self.streaming_read_range_bytes() as u64 {
            return Err(invalid("checkpoint range exceeds transfer chunk"));
        }
        let path = self.qualify_path(&Path::from(path)).into_owned();
        self.object_store
            .get_range(&path, start..end)
            .await
            .map_err(Into::into)
    }

    /// Called only after checkpoint GC has fenced the artifact's native owner.
    /// Known upload IDs remain visible until abort and temporary-payload cleanup
    /// succeed, so a process loss cannot strand an unlisted large upload.
    pub async fn delete_native_checkpoint_artifact(&self, path: &str) -> Result<(), StorageError> {
        let relative = Path::from(path);
        let parts = path.rsplit('/').take(2).collect::<Vec<_>>();
        if parts.len() != 2
            || !parts[1].starts_with("native-")
            || !parts[0].contains(".upload-")
            || !parts[0].ends_with(".mpu")
        {
            return self.delete_if_present(relative).await;
        }
        let journal = self.qualify_path(&relative).into_owned();
        let meta = match self.object_store.head(&journal).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if meta.size == 0 || meta.size > MAX_JOURNAL_BYTES {
            return Err(invalid(
                "native multipart journal exceeds bounded control record",
            ));
        }
        let bytes = self.object_store.get_range(&journal, 0..meta.size).await?;
        let record: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| invalid(&e.to_string()))?;
        let expected = journal
            .as_ref()
            .strip_suffix(".mpu")
            .ok_or_else(|| invalid("native multipart journal suffix"))?;
        let id = record
            .get("upload_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("native multipart journal lacks upload id"))?;
        let azure_block_blob = matches!(self.config(), crate::BackendConfig::Azure(_));
        if record.get("version").and_then(|v| v.as_u64()) != Some(1)
            || record.get("temporary").and_then(|v| v.as_str()) != Some(expected)
            || record.get("provider").and_then(|v| v.as_str())
                != Some(self.native_upload_provider_kind())
            || id.is_empty() && !azure_block_blob
            || id.len() > 4096
        {
            return Err(invalid(
                "native multipart journal does not own the exact temporary key",
            ));
        }
        let temporary = Path::from(expected);
        let provider = self
            .as_multipart()
            .ok_or_else(|| invalid("native upload journal without multipart provider"))?;
        if azure_block_blob {
            // Azure's ID-less abort is a no-op. Put Blob of an empty blob at
            // this EXACT fenced temporary key discards uncommitted blocks.
            // https://learn.microsoft.com/rest/api/storageservices/put-block
            self.object_store
                .put(&temporary, Bytes::new().into())
                .await?;
        } else {
            match provider.abort_multipart(&temporary, &id.to_owned()).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        match self.object_store.delete(&temporary).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        match self.object_store.delete(&journal).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl StreamingCreate {
    async fn flush_part(&mut self) -> Result<(), StorageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.local_root.is_none() && self.parts >= MAX_PARTS {
            return Err(invalid("checkpoint multipart part limit exceeded"));
        }
        self.parts = self
            .parts
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint part count overflow"))?;
        let bytes = std::mem::take(&mut self.buffer);
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| invalid("upload already finished"))?;
        let part = upload
            .provider
            .put_part(&self.temporary, &upload.id, self.parts - 1, bytes.into())
            .await?;
        if part.content_id.len() > MAX_PART_ID_BYTES {
            return Err(invalid(
                "native multipart part identifier exceeds admitted bound",
            ));
        }
        upload.parts.push(part);
        self.buffer = Vec::with_capacity(self.part_bytes);
        Ok(())
    }

    pub async fn write(&mut self, mut chunk: &[u8]) -> Result<(), StorageError> {
        if chunk.len() > CHUNK_BYTES {
            return Err(invalid("checkpoint write exceeds transfer chunk"));
        }
        if let Some(file) = &mut self.local_file {
            return file
                .write_all(chunk)
                .await
                .map_err(|e| invalid(&e.to_string()));
        }
        while !chunk.is_empty() {
            let count = chunk.len().min(self.part_bytes - self.buffer.len());
            self.buffer.extend_from_slice(&chunk[..count]);
            chunk = &chunk[count..];
            if self.buffer.len() == self.part_bytes {
                self.flush_part().await?;
            }
        }
        Ok(())
    }

    /// Temporary completion may overwrite only the caller's unique temporary
    /// key. Final publication uses the provider's atomic conditional copy.
    pub async fn finish(&mut self) -> Result<(), StorageError> {
        if let Some(file) = &mut self.local_file {
            file.flush().await.map_err(|e| invalid(&e.to_string()))?;
            file.sync_all().await.map_err(|e| invalid(&e.to_string()))?;
            self.local_file.take();
            self.store
                .copy_if_not_exists(&self.temporary, &self.destination)
                .await?;
            self.destination_created.store(true, Ordering::Release);
            self.sync_local().await?;
            self.store.delete(&self.temporary).await?;
            return Ok(());
        }
        self.flush_part().await?;
        if self.parts == 0 {
            let upload = self
                .upload
                .as_ref()
                .ok_or_else(|| invalid("upload already finished"))?;
            upload
                .provider
                .abort_multipart(&self.temporary, &upload.id)
                .await?;
            self.upload = None;
            self.store
                .put_opts(
                    &self.destination,
                    Bytes::new().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            self.destination_created.store(true, Ordering::Release);
            self.sync_local().await?;
            if let Some(journal) = self.journal.take() {
                self.store.delete(&journal).await?;
            }
            return Ok(());
        }
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| invalid("upload already finished"))?;
        upload
            .provider
            .complete_multipart(
                &self.temporary,
                &upload.id,
                std::mem::take(&mut upload.parts),
            )
            .await?;
        self.upload = None;
        let source_first = if self.azure_block_blob {
            Some(self.store.get_range(&self.temporary, 0..1).await?)
        } else {
            None
        };
        self.store
            .copy_if_not_exists(&self.temporary, &self.destination)
            .await
            .map_err(|error| {
                normalize_copy_collision(error, &self.destination, self.azure_block_blob)
            })?;
        self.destination_created.store(true, Ordering::Release);
        if let Some(first) = source_first {
            wait_for_block_blob_copy(
                self.store.as_ref(),
                &self.destination,
                &first,
                AZURE_COPY_READY_TIMEOUT,
            )
            .await?;
        }
        self.sync_local().await?;
        self.store.delete(&self.temporary).await?;
        if let Some(journal) = self.journal.take() {
            self.store.delete(&journal).await?;
        }
        Ok(())
    }

    async fn sync_local(&self) -> Result<(), StorageError> {
        let Some(root) = &self.local_root else {
            return Ok(());
        };
        let file = root.join(self.destination.as_ref());
        tokio::fs::File::open(&file)
            .await
            .map_err(|e| invalid(&e.to_string()))?
            .sync_all()
            .await
            .map_err(|e| invalid(&e.to_string()))?;
        let mut directory = file.parent();
        while let Some(path) = directory {
            if !path.starts_with(root) {
                break;
            }
            tokio::fs::File::open(path)
                .await
                .map_err(|e| invalid(&e.to_string()))?
                .sync_all()
                .await
                .map_err(|e| invalid(&e.to_string()))?;
            if path == root {
                break;
            }
            directory = path.parent();
        }
        Ok(())
    }

    pub async fn abort(mut self) {
        self.local_file.take();
        if let Some(upload) = &mut self.upload {
            if self.azure_block_blob {
                if let Err(error) = self.store.put(&self.temporary, Bytes::new().into()).await {
                    tracing::error!(%error, "Azure native temporary block cleanup failed; journal retained for fenced retry");
                    return;
                }
            } else {
                match upload
                    .provider
                    .abort_multipart(&self.temporary, &upload.id)
                    .await
                {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        tracing::error!(%error, "native multipart abort failed; visible journal retained for fenced retry");
                        return;
                    }
                }
            }
        }
        let _ = self.store.delete(&self.temporary).await;
        if let Some(journal) = &self.journal {
            let _ = self.store.delete(journal).await;
        }
    }
}

fn normalize_copy_collision(
    error: object_store::Error,
    destination: &Path,
    azure: bool,
) -> object_store::Error {
    if azure && matches!(error, object_store::Error::Precondition { .. }) {
        object_store::Error::AlreadyExists {
            path: destination.to_string(),
            source: Box::new(error),
        }
    } else {
        error
    }
}

async fn wait_for_block_blob_copy(
    store: &dyn ObjectStore,
    destination: &Path,
    expected_first: &[u8],
    timeout: std::time::Duration,
) -> Result<(), StorageError> {
    if expected_first.len() != 1 {
        return Err(invalid("Azure nonempty copy lacks a source byte"));
    }
    // Copy Blob may acknowledge 202/pending. Microsoft guarantees BLOCK blob
    // contents are Get-visible only after the entire copy finishes:
    // https://learn.microsoft.com/rest/api/storageservices/copy-blob#remarks
    // HEAD alone is insufficient; a bounded one-byte GET avoids a full SST read.
    tokio::time::timeout(timeout, async {
        loop {
            if let Ok(bytes) = store.get_range(destination, 0..1).await
                && bytes.len() == 1
            {
                return if bytes.as_ref() == expected_first {
                    Ok(())
                } else {
                    Err(invalid(
                        "Azure copied first byte differs from immutable source",
                    ))
                };
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        invalid(
            "Azure block-blob copy did not become readable before the bounded operation deadline",
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug)]
    struct AzureFixtureStore {
        inner: object_store::memory::InMemory,
        temporary: Path,
        canonical: Path,
        payload: Arc<std::sync::atomic::AtomicUsize>,
        staged: Arc<std::sync::Mutex<Vec<u8>>>,
        clears: std::sync::atomic::AtomicUsize,
        copy_started: tokio::sync::Notify,
        destination_reads: std::sync::atomic::AtomicUsize,
    }
    impl std::fmt::Display for AzureFixtureStore {
        fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(out, "Azure block-blob contract fixture")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for AzureFixtureStore {
        async fn put_opts(
            &self,
            path: &Path,
            data: object_store::PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if path == &self.temporary && data.content_length() == 0 {
                self.clears.fetch_add(1, Ordering::Relaxed);
                self.payload.store(0, Ordering::Relaxed);
                self.staged.lock().unwrap().clear();
            }
            self.inner.put_opts(path, data, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }
        async fn get_opts(
            &self,
            path: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if path == &self.canonical && options.range.is_some() {
                self.destination_reads.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(path, options).await
        }
        async fn delete(&self, path: &Path) -> object_store::Result<()> {
            self.inner.delete(path).await
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            let result = if to == &self.canonical {
                // Azure can return 202/pending after creating an EMPTY committed
                // destination. Only the test's explicit completion exposes data.
                self.inner
                    .put_opts(
                        to,
                        Bytes::new().into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
            } else {
                self.inner.copy_if_not_exists(from, to).await
            };
            if result.is_ok() && to == &self.canonical {
                self.copy_started.notify_one();
            }
            result.map_err(|error| match error {
                error @ object_store::Error::AlreadyExists { .. } => {
                    object_store::Error::Precondition {
                        path: to.to_string(),
                        source: Box::new(error),
                    }
                }
                error => error,
            })
        }
    }

    fn azure_fixture() -> (
        StorageProvider,
        Arc<AzureFixtureStore>,
        Arc<JournalMultipart>,
    ) {
        let payload = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let staged = Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = Arc::new(AzureFixtureStore {
            inner: object_store::memory::InMemory::new(),
            temporary: Path::from(
                "J/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst.upload-owned",
            ),
            canonical: Path::from(
                "J/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst",
            ),
            payload: payload.clone(),
            staged: staged.clone(),
            clears: Default::default(),
            copy_started: Default::default(),
            destination_reads: Default::default(),
        });
        let multipart = Arc::new(JournalMultipart {
            store: store.clone(),
            payload,
            staged,
            azure: true,
            fail_abort: AtomicBool::new(false),
            aborts: Default::default(),
        });
        let provider = StorageProvider {
            config: crate::BackendConfig::Azure(crate::AzureConfig {
                account: "fixture".into(),
                container: "fixture".into(),
                key: None,
            }),
            object_store: store.clone(),
            multipart_store: Some(multipart.clone()),
            canonical_url: "memory://azure-fixture".into(),
            storage_options: Default::default(),
            native_copy_probe: Default::default(),
        };
        (provider, store, multipart)
    }

    #[tokio::test]
    async fn azure_empty_upload_id_preserves_native_transfer_and_clears_only_owned_blocks() {
        let (storage, store, multipart) = azure_fixture();
        store
            .inner
            .put(
                &store.canonical,
                Bytes::from_static(b"retained canonical").into(),
            )
            .await
            .unwrap();
        let mut upload = storage
            .start_streaming_create(store.canonical.as_ref(), store.temporary.as_ref())
            .await
            .unwrap();
        for _ in 0..CLOUD_PART_BYTES / CHUNK_BYTES {
            upload.write(&vec![9; CHUNK_BYTES]).await.unwrap();
        }
        assert_eq!(multipart.payload.load(Ordering::Relaxed), CLOUD_PART_BYTES);
        drop(upload); // Simulated loss of the process-local abort owner.
        let journal = format!("{}.mpu", store.temporary);
        let bytes = storage.get(journal.as_str()).await.unwrap();
        let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record["provider"], "azure-block-blob");
        assert_eq!(record["upload_id"], "");
        storage
            .delete_native_checkpoint_artifact(&journal)
            .await
            .unwrap();
        assert_eq!(multipart.payload.load(Ordering::Relaxed), 0);
        assert_eq!(
            multipart.aborts.load(Ordering::Relaxed),
            0,
            "Azure SDK no-op abort must not be treated as reclaimed bytes"
        );
        assert_eq!(store.clears.load(Ordering::Relaxed), 1);
        assert_eq!(
            store
                .inner
                .get(&store.canonical)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"retained canonical"
        );
        assert!(!storage.exists(journal.as_str()).await.unwrap());
    }

    #[tokio::test]
    async fn azure_pending_copy_keeps_source_until_first_byte_get_proves_full_completion() {
        let (storage, store, _) = azure_fixture();
        let created = Arc::new(AtomicBool::new(false));
        let mut upload = storage
            .start_streaming_create_tracked(
                store.canonical.as_ref(),
                store.temporary.as_ref(),
                created.clone(),
            )
            .await
            .unwrap();
        upload.write(b"checkpoint native bytes").await.unwrap();
        let finish = tokio::spawn(async move {
            let result = upload.finish().await;
            (upload, result)
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            store.copy_started.notified(),
        )
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.destination_reads.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(created.load(Ordering::Acquire));
        assert!(
            !finish.is_finished(),
            "202/pending was mistaken for completed copy"
        );
        assert_eq!(store.inner.head(&store.canonical).await.unwrap().size, 0);
        assert_eq!(
            store
                .inner
                .get(&store.temporary)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"checkpoint native bytes"
        );
        assert!(
            wait_for_block_blob_copy(
                store.as_ref(),
                &store.canonical,
                b"c",
                std::time::Duration::from_millis(2)
            )
            .await
            .is_err(),
            "pending copy must respect its finite operation deadline"
        );
        store
            .inner
            .copy(&store.temporary, &store.canonical)
            .await
            .unwrap();
        let (upload, result) = tokio::time::timeout(std::time::Duration::from_secs(2), finish)
            .await
            .unwrap()
            .unwrap();
        result.unwrap();
        upload.abort().await;
        assert_eq!(
            store
                .inner
                .get(&store.canonical)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"checkpoint native bytes"
        );
        assert!(matches!(
            store.inner.head(&store.temporary).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(
            wait_for_block_blob_copy(
                store.as_ref(),
                &store.canonical,
                b"x",
                std::time::Duration::from_secs(1)
            )
            .await
            .is_err(),
            "wrong first byte must fail integrity check"
        );
    }

    struct JournalMultipart {
        store: Arc<dyn ObjectStore>,
        payload: Arc<std::sync::atomic::AtomicUsize>,
        fail_abort: AtomicBool,
        aborts: std::sync::atomic::AtomicUsize,
        azure: bool,
        staged: Arc<std::sync::Mutex<Vec<u8>>>,
    }
    #[async_trait::async_trait]
    impl MultipartStore for JournalMultipart {
        async fn create_multipart(&self, _path: &Path) -> object_store::Result<String> {
            Ok(if self.azure {
                String::new()
            } else {
                "known-upload-id".into()
            })
        }
        async fn put_part(
            &self,
            path: &Path,
            id: &String,
            part: usize,
            data: object_store::PutPayload,
        ) -> object_store::Result<PartId> {
            assert_eq!(id, if self.azure { "" } else { "known-upload-id" });
            // This is the essential crash contract: a visible ID journal exists
            // before the first large provider-owned payload allocation.
            self.store.head(&Path::from(format!("{path}.mpu"))).await?;
            self.payload
                .fetch_add(data.content_length(), Ordering::Relaxed);
            if self.azure {
                for chunk in data.iter() {
                    self.staged.lock().unwrap().extend_from_slice(chunk);
                }
            }
            Ok(PartId {
                content_id: format!("part-{part}"),
            })
        }
        async fn complete_multipart(
            &self,
            path: &Path,
            _id: &String,
            _parts: Vec<PartId>,
        ) -> object_store::Result<object_store::PutResult> {
            if self.azure {
                let payload = std::mem::take(&mut *self.staged.lock().unwrap());
                self.store.put(path, payload.into()).await
            } else {
                Err(object_store::Error::NotImplemented)
            }
        }
        async fn abort_multipart(&self, _path: &Path, id: &String) -> object_store::Result<()> {
            assert_eq!(id, if self.azure { "" } else { "known-upload-id" });
            self.aborts.fetch_add(1, Ordering::Relaxed);
            if self.azure {
                return Ok(());
            } // Matches Azure SDK's no-op abort.
            if self.fail_abort.load(Ordering::Relaxed) {
                return Err(object_store::Error::Generic {
                    store: "native multipart fixture",
                    source: std::io::Error::other("injected abort failure").into(),
                });
            }
            self.payload.store(0, Ordering::Relaxed);
            Ok(())
        }
    }
    #[tokio::test]
    async fn journal_precedes_payload_and_fenced_abort_survives_lost_owner_and_retry() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let multipart = Arc::new(JournalMultipart {
            store: objects.clone(),
            payload: Default::default(),
            fail_abort: AtomicBool::new(true),
            aborts: Default::default(),
            azure: false,
            staged: Default::default(),
        });
        let mut storage = StorageProvider {
            config: crate::BackendConfig::GCS(crate::GCSConfig {
                bucket: "fixture".into(),
                key: None,
            }),
            object_store: objects,
            multipart_store: Some(multipart.clone()),
            canonical_url: "memory://fixture".into(),
            storage_options: Default::default(),
            native_copy_probe: Default::default(),
        };
        let temporary =
            "J/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst.upload-owned";
        let journal = format!("{temporary}.mpu");
        let mut upload = storage
            .start_streaming_create(
                "J/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst",
                temporary,
            )
            .await
            .unwrap();
        for _ in 0..CLOUD_PART_BYTES / CHUNK_BYTES {
            upload.write(&vec![3; CHUNK_BYTES]).await.unwrap();
        }
        assert_eq!(multipart.payload.load(Ordering::Relaxed), CLOUD_PART_BYTES);
        // Dropping just the low-level session does not run the native owner's
        // abort guard, modeling loss of all process-local ownership metadata.
        drop(upload);
        assert!(
            storage
                .delete_native_checkpoint_artifact(&journal)
                .await
                .is_err()
        );
        assert!(storage.exists(journal.as_str()).await.unwrap());
        multipart.fail_abort.store(false, Ordering::Relaxed);
        storage
            .delete_native_checkpoint_artifact(&journal)
            .await
            .unwrap();
        assert_eq!(multipart.payload.load(Ordering::Relaxed), 0);
        assert!(!storage.exists(journal.as_str()).await.unwrap());
        storage
            .delete_native_checkpoint_artifact(&journal)
            .await
            .unwrap();
        let before = multipart.aborts.load(Ordering::Relaxed);
        storage
            .put_if_not_exists(
                journal.as_str(),
                br#"{"version":1,"temporary":"foreign-key","upload_id":"known-upload-id"}"#
                    .to_vec(),
            )
            .await
            .unwrap();
        assert!(
            storage
                .delete_native_checkpoint_artifact(&journal)
                .await
                .is_err()
        );
        assert_eq!(
            multipart.aborts.load(Ordering::Relaxed),
            before,
            "foreign journal must not authorize abort"
        );
        storage.config = crate::BackendConfig::S3(crate::S3Config {
            endpoint: None,
            region: None,
            bucket: "fixture".into(),
            key: None,
        });
        storage
            .storage_options
            .insert("aws_copy_if_not_exists".into(), "multipart".into());
        assert!(
            storage
                .start_streaming_create("unused", "unused.upload-owned")
                .await
                .is_err(),
            "opaque second multipart copy must fail before payload upload"
        );
        assert!(
            storage
                .validate_native_file_size(COPY_OBJECT_LIMIT + 1)
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_process_exit_keeps_partial_payload_visible_for_fenced_cleanup() {
        const CHILD: &str = "STREAMR_NATIVE_STREAMING_EXIT_CHILD";
        if let Some(root) = std::env::var_os(CHILD) {
            let storage = StorageProvider::for_url(std::path::Path::new(&root).to_str().unwrap())
                .await
                .unwrap();
            let mut upload = storage.start_streaming_create(
                "job/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst",
                "job/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst.upload-owned",
            ).await.unwrap();
            for _ in 0..8 {
                upload.write(&vec![7; CHUNK_BYTES]).await.unwrap();
            }
            upload.local_file.as_mut().unwrap().flush().await.unwrap();
            // Abrupt process exit deliberately skips all Rust Drop/abort owners.
            std::process::exit(0);
        }
        let root = std::env::temp_dir().join(format!(
            "native-stream-exit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child.args(["--exact", "streaming::tests::local_process_exit_keeps_partial_payload_visible_for_fenced_cleanup", "--nocapture"])
            .env(CHILD, &root).kill_on_drop(true);
        let output = tokio::time::timeout(std::time::Duration::from_secs(15), child.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let storage = StorageProvider::for_url(root.to_str().unwrap())
            .await
            .unwrap();
        use futures::TryStreamExt;
        let paths = storage
            .list_prefix("job/checkpoints/".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            paths.len(),
            1,
            "partial payload must be visible without a destructor or hidden multipart listing"
        );
        assert!(paths[0].as_ref().ends_with("000001.sst.upload-owned"));
        assert_eq!(
            storage.head(paths[0].clone()).await.unwrap().size,
            (8 * CHUNK_BYTES) as u64
        );
        storage.delete_if_present(paths[0].clone()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn streaming_create_is_bounded_create_only_and_abort_removes_only_temporary() {
        let root = std::env::temp_dir().join(format!(
            "streamr-streaming-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let storage = StorageProvider::for_url(root.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(storage.streaming_create_memory_bytes(), CHUNK_BYTES * 4);
        let payload = vec![17; CHUNK_BYTES * 3 + 9];
        let mut upload = storage
            .start_streaming_create("prefix/final", "prefix/temporary-1")
            .await
            .unwrap();
        for chunk in payload.chunks(CHUNK_BYTES) {
            upload.write(chunk).await.unwrap();
        }
        upload.finish().await.unwrap();
        upload.abort().await;
        assert_eq!(storage.get("prefix/final").await.unwrap().as_ref(), payload);
        assert!(!storage.exists("prefix/temporary-1").await.unwrap());
        let mut collision = storage
            .start_streaming_create("prefix/final", "prefix/temporary-2")
            .await
            .unwrap();
        collision.write(b"must not replace").await.unwrap();
        assert!(collision.finish().await.is_err());
        collision.abort().await;
        assert_eq!(storage.get("prefix/final").await.unwrap().as_ref(), payload);
        let mut aborted = storage
            .start_streaming_create("prefix/aborted", "prefix/temporary-3")
            .await
            .unwrap();
        aborted.write(&payload[..CHUNK_BYTES]).await.unwrap();
        assert!(aborted.write(&payload[..CHUNK_BYTES + 1]).await.is_err());
        aborted.abort().await;
        assert!(!storage.exists("prefix/aborted").await.unwrap());
        assert!(!storage.exists("prefix/temporary-3").await.unwrap());
        assert!(
            storage
                .checkpoint_range("prefix/final", 0, CHUNK_BYTES as u64 + 1)
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
