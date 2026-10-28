//! Complete native database checkpoints. The barrier pins native files, while
//! asynchronous transfers reuse only an authoritative completed file inventory.
use super::{
    LiveStateBackend, LiveStateError, Result,
    resources::{CleanupPermit, ResourcePermit, WorkerStateResources},
};
use arroyo_rpc::grpc::rpc::{
    NativeCheckpointFile, NativeCheckpointTable, NativeDatabaseCheckpointMetadata,
};
use arroyo_state_protocol::native_checkpoint as protocol;
use arroyo_storage::{
    StorageProviderRef,
    streaming::{CHUNK_BYTES, StreamingCreate},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) fn error(error: impl std::fmt::Display) -> LiveStateError {
    LiveStateError::Backend(error.to_string())
}

pub fn compatibility_identity() -> String {
    // Cargo.lock pins librocksdb-sys 0.17.3+10.4.2; the maintained build selector
    // verifies the matching native archive. A native dependency/format change
    // must update this identity rather than silently accepting older files.
    "streamr-native-v1/rocksdb-10.4.2/default-bytewise/key-v1/value-v1/lz4".to_owned()
}

#[derive(Clone)]
pub struct NativeRestoreRequest {
    pub metadata: NativeDatabaseCheckpointMetadata,
    pub storage: StorageProviderRef,
}

/// Exact restore binding; a DB lineage alone identifies many different epochs.
pub fn checkpoint_fingerprint(metadata: &NativeDatabaseCheckpointMetadata) -> Vec<u8> {
    Sha256::digest(metadata.encode_to_vec()).to_vec()
}

pub struct NativeUploadContext {
    pub job_id: String,
    pub operator_id: String,
    pub pipeline_id: Option<String>,
    pub generation: u64,
    pub epoch: u32,
    pub subtask_index: u32,
    pub tables: Vec<NativeCheckpointTable>,
}

#[derive(Default)]
pub(crate) struct NativeReuseState {
    pub(crate) confirmed: Option<NativeDatabaseCheckpointMetadata>,
    pending: Option<NativeDatabaseCheckpointMetadata>,
}
impl NativeReuseState {
    pub(crate) fn restored(metadata: NativeDatabaseCheckpointMetadata) -> Self {
        Self {
            confirmed: Some(metadata),
            pending: None,
        }
    }
    fn base(&mut self, completed: Option<u32>) -> Option<NativeDatabaseCheckpointMetadata> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| Some(p.epoch) == completed)
        {
            self.confirmed = self.pending.take();
        }
        self.confirmed
            .as_ref()
            .filter(|p| Some(p.epoch) == completed)
            .cloned()
    }
}

/// Cleanup admission and the capture permit follow the physical directory until
/// deletion completes, including cancellation of an uncancellable native call.
pub(crate) struct StagingDirectory {
    pub(crate) path: PathBuf,
    cleanup: Option<CleanupPermit>,
    permit: Option<ResourcePermit>,
    remote_cleanup: Option<RemoteArtifactCleanup>,
}
type OwnedRemoteFile = (String, Arc<AtomicBool>);
type RemoteCleanupFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
struct RemoteArtifactCleanup {
    paths: Vec<OwnedRemoteFile>,
    remove: Box<dyn FnOnce(Vec<OwnedRemoteFile>) -> RemoteCleanupFuture + Send>,
    runtime: tokio::runtime::Handle,
}
impl RemoteArtifactCleanup {
    fn new(storage: StorageProviderRef) -> Self {
        Self {
            paths: Vec::new(),
            runtime: tokio::runtime::Handle::current(),
            remove: Box::new(move |paths| {
                Box::pin(async move {
                    for (path, created) in paths {
                        if created.load(Ordering::Acquire) {
                            let _ = storage.delete_if_present(path).await;
                        }
                    }
                })
            }),
        }
    }
}
const REMOTE_CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
fn best_effort_remote_cleanup(
    runtime: tokio::runtime::Handle,
    operation: impl std::future::Future<Output = ()>,
) {
    runtime.block_on(async {
        if tokio::time::timeout(REMOTE_CLEANUP_BUDGET, operation).await.is_err() {
            tracing::warn!("native best-effort remote cleanup reached aggregate 5-second budget; fenced GC retains responsibility");
        }
    });
}
impl StagingDirectory {
    pub(crate) fn new(path: PathBuf, cleanup: CleanupPermit, permit: ResourcePermit) -> Self {
        Self {
            path,
            cleanup: Some(cleanup),
            permit: Some(permit),
            remote_cleanup: None,
        }
    }
    pub(crate) fn restoring(path: PathBuf, cleanup: CleanupPermit) -> Self {
        Self {
            path,
            cleanup: Some(cleanup),
            permit: None,
            remote_cleanup: None,
        }
    }
    pub(crate) fn release_to_database(mut self) -> CleanupPermit {
        self.cleanup.take().expect("attempt cleanup reserved")
    }
}
impl Drop for StagingDirectory {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        let path = self.path.clone();
        let temporary = PathBuf::from(format!("{}.tmp", path.display()));
        let permit = self.permit.take();
        let remote = self.remote_cleanup.take();
        cleanup.submit(move || {
            for directory in [temporary, path] {
                if let Err(error) = std::fs::remove_dir_all(&directory)
                    && error.kind() != std::io::ErrorKind::NotFound {
                    tracing::error!(%error, path=%directory.display(), "native checkpoint staging cleanup failed");
                }
            }
            drop(permit);
            // Native files and local capture admission must not remain pinned
            // behind a slow cloud deletion. The whole remote batch has ONE
            // deadline; uncompleted exclusive paths remain fenced GC candidates.
            if let Some(remote) = remote {
                best_effort_remote_cleanup(remote.runtime, (remote.remove)(remote.paths));
            }
        });
    }
}

pub struct NativeCheckpointCapture {
    pub(crate) staging: StagingDirectory,
    pub(crate) resources: WorkerStateResources,
    pub(crate) reuse: Arc<Mutex<NativeReuseState>>,
    pub(crate) database_id: Vec<u8>,
    pub(crate) files: Vec<(String, u64)>,
    pub(crate) sync_capture_seconds: f64,
    pub(crate) cfstats: Option<String>,
}

struct UploadOwner {
    upload: Option<StreamingCreate>,
    cleanup: Option<CleanupPermit>,
    runtime: tokio::runtime::Handle,
    memory: Option<ResourcePermit>,
}
impl Drop for UploadOwner {
    fn drop(&mut self) {
        if let Some(upload) = self.upload.take() {
            let runtime = self.runtime.clone();
            let memory = self.memory.take();
            self.cleanup
                .take()
                .expect("transfer cleanup reserved")
                .submit(move || {
                    let _memory = memory;
                    best_effort_remote_cleanup(runtime, upload.abort());
                });
        }
    }
}

impl NativeCheckpointCapture {
    pub async fn upload(
        mut self,
        context: NativeUploadContext,
        storage: &StorageProviderRef,
        completed_base_epoch: Option<u32>,
    ) -> Result<NativeDatabaseCheckpointMetadata> {
        let started = std::time::Instant::now();
        let base = self.reuse.lock().map_err(error)?.base(completed_base_epoch);
        let previous = base
            .as_ref()
            .filter(|base| base.database_id == self.database_id)
            .map(|base| {
                base.files
                    .iter()
                    .filter(|file| file.shared)
                    .map(|file| (file.local_name.clone(), file.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let directory = format!(
            "{}native-{}/",
            protocol::current_prefix(
                &context.job_id,
                &context.operator_id,
                context.pipeline_id.as_deref(),
                context.generation,
                context.epoch
            ),
            uuid::Uuid::new_v4().simple()
        );
        let mut metadata = NativeDatabaseCheckpointMetadata {
            format_version: protocol::FORMAT_VERSION,
            generation: context.generation,
            epoch: context.epoch,
            subtask_index: context.subtask_index,
            database_id: self.database_id.clone(),
            compatibility_identity: compatibility_identity(),
            tables: context.tables,
            files: Vec::with_capacity(self.files.len()),
            completed_base_epoch: None,
        };
        // Reserve the optional u32 completed-base field before incremental file
        // accounting. Avoid repeatedly encoding an ever-growing inventory.
        let mut wire_bytes = metadata.encoded_len() + 6;
        self.staging.remote_cleanup = Some(RemoteArtifactCleanup::new(storage.clone()));
        for (name, size) in &self.files {
            if let Some(file) = previous.get(name).filter(|file| file.size_bytes == *size) {
                let head = storage.head(file.path.clone()).await.map_err(error)?;
                if head.size != *size {
                    return Err(error("reusable native SST size differs"));
                }
                metadata.files.push(file.clone());
                metadata.completed_base_epoch = base.as_ref().map(|base| base.epoch);
            } else {
                let path = format!("{directory}{name}");
                let created = Arc::new(AtomicBool::new(false));
                self.staging
                    .remote_cleanup
                    .as_mut()
                    .expect("attempt cleanup")
                    .paths
                    .push((path.clone(), created.clone()));
                let checksum = upload_file(
                    &self.staging.path.join(name),
                    *size,
                    &path,
                    storage,
                    &self.resources,
                    created,
                )
                .await?;
                metadata.files.push(NativeCheckpointFile {
                    local_name: name.clone(),
                    path,
                    size_bytes: *size,
                    checksum,
                    shared: name.ends_with(".sst"),
                    birth_generation: context.generation,
                    birth_epoch: context.epoch,
                });
            }
            let file_bytes = metadata
                .files
                .last()
                .expect("added native file")
                .encoded_len();
            let mut length_bytes = 1;
            let mut length = file_bytes;
            while length >= 128 {
                length >>= 7;
                length_bytes += 1;
            }
            wire_bytes += 1 + length_bytes + file_bytes;
            if wire_bytes > protocol::MAX_METADATA_BYTES {
                return Err(error(
                    "native checkpoint metadata exceeds bounded RPC budget",
                ));
            }
        }
        let configs = metadata
            .tables
            .iter()
            .map(|table| {
                Ok((
                    table.name.clone(),
                    table
                        .config
                        .clone()
                        .ok_or_else(|| error("native table config absent"))?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        protocol::validate(
            &metadata,
            &configs,
            &context.job_id,
            &context.operator_id,
            context.pipeline_id.as_deref(),
        )
        .map_err(error)?;
        protocol::validate_reuse(&metadata, base.as_ref()).map_err(error)?;
        // The next barrier's authoritative acknowledgement, never local upload
        // success, promotes this bounded candidate for inheritance.
        self.reuse.lock().map_err(error)?.pending = Some(metadata.clone());
        self.staging.remote_cleanup = None;
        if std::env::var_os("STREAMR_NATIVE_CHECKPOINT_RECEIPTS").is_some() {
            let reused = metadata.files.iter().filter(|file| {
                file.birth_epoch != context.epoch || file.birth_generation != context.generation
            });
            let reused_bytes = reused.clone().map(|file| file.size_bytes).sum::<u64>();
            let uploaded = metadata.files.iter().filter(|file| {
                file.birth_epoch == context.epoch && file.birth_generation == context.generation
            });
            eprintln!(
                "NATIVE_CHECKPOINT_RECEIPT {}",
                serde_json::json!({
                    "job_id": context.job_id, "operator_id": context.operator_id,
                    "epoch": context.epoch, "generation": context.generation,
                    "sync_capture_seconds": self.sync_capture_seconds,
                    "async_export_seconds": started.elapsed().as_secs_f64(),
                    "uploaded_bytes": uploaded.clone().map(|file| file.size_bytes).sum::<u64>(),
                    "reused_bytes": reused_bytes,
                    "new_sst_bytes": uploaded.filter(|file| file.shared).map(|file| file.size_bytes).sum::<u64>(),
                    "reused_sst_bytes": reused_bytes,
                    "file_count": metadata.files.len(), "cfstats_no_file_histogram": self.cfstats,
                })
            );
        }
        Ok(metadata)
    }
}

async fn upload_file(
    source: &Path,
    size: u64,
    destination: &str,
    storage: &StorageProviderRef,
    resources: &WorkerStateResources,
    created: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    storage.validate_native_file_size(size).map_err(error)?;
    let memory = resources
        .try_decoded_value(storage.streaming_create_memory_bytes())
        .map_err(error)?;
    let cleanup = resources.cleanup().await.map_err(error)?;
    let temporary = format!("{destination}.upload-{}", uuid::Uuid::new_v4().simple());
    let transfer_storage = storage.clone();
    let destination = destination.to_owned();
    let runtime = tokio::runtime::Handle::current();
    let mut owner = resources
        .run_blocking(move || {
            let upload = runtime
                .block_on(transfer_storage.start_streaming_create_tracked(
                    &destination,
                    &temporary,
                    created,
                ))
                .map_err(error)?;
            Ok::<_, LiveStateError>(UploadOwner {
                upload: Some(upload),
                cleanup: Some(cleanup),
                runtime,
                memory: Some(memory),
            })
        })
        .await
        .map_err(error)??;
    let mut source = tokio::fs::File::open(source).await.map_err(error)?;
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut remaining = size;
    let mut checksum = Sha256::new();
    while remaining > 0 {
        let limit = (remaining.min(CHUNK_BYTES as u64)) as usize;
        let count = source.read(&mut buffer[..limit]).await.map_err(error)?;
        if count == 0 {
            return Err(error("native checkpoint source shorter than inventory"));
        }
        remaining -= count as u64;
        checksum.update(&buffer[..count]);
        owner
            .upload
            .as_mut()
            .expect("active upload")
            .write(&buffer[..count])
            .await
            .map_err(error)?;
    }
    if source.read(&mut buffer[..1]).await.map_err(error)? != 0 {
        return Err(error("native checkpoint source exceeds inventory"));
    }
    // Conditional copy may itself involve multipart completion. Keep its owner
    // and admission in an uncancellable admitted closure until it finishes, so
    // cancellation cannot abandon a hidden provider upload without cleanup.
    let runtime = tokio::runtime::Handle::current();
    resources
        .run_blocking(move || {
            runtime.block_on(async {
                owner
                    .upload
                    .as_mut()
                    .expect("active upload")
                    .finish()
                    .await
                    .map_err(error)?;
                owner.upload.take();
                Ok::<_, LiveStateError>(())
            })
        })
        .await
        .map_err(error)??;
    Ok(checksum.finalize().to_vec())
}

pub(crate) async fn download(
    request: &NativeRestoreRequest,
    directory: &Path,
    resources: &WorkerStateResources,
) -> Result<()> {
    let metadata = &request.metadata;
    if metadata.compatibility_identity != compatibility_identity()
        || metadata.format_version != protocol::FORMAT_VERSION
        || metadata.encoded_len() > protocol::MAX_METADATA_BYTES
        || metadata.files.len() > protocol::MAX_FILES
    {
        return Err(error(
            "native checkpoint compatibility or metadata limit mismatch",
        ));
    }
    let range_bytes = request.storage.streaming_read_range_bytes();
    let _buffers = resources
        .try_decoded_value(range_bytes * 2 + CHUNK_BYTES)
        .map_err(error)?;
    let mut names = std::collections::HashSet::new();
    for file in &metadata.files {
        if file.local_name.is_empty()
            || file.local_name.len() > 255
            || !file
                .local_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || matches!(file.local_name.as_str(), "." | ".." | "OWNERSHIP" | "LOCK")
            || !names.insert(&file.local_name)
            || file.checksum.len() != 32
        {
            return Err(error("unsafe or duplicate native recovery file"));
        }
        resources
            .ensure_disk_space(directory, file.size_bytes)
            .map_err(error)?;
        let head = request
            .storage
            .head(file.path.clone())
            .await
            .map_err(error)?;
        if head.size != file.size_bytes {
            return Err(error("native recovery object size mismatch"));
        }
        let mut output = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(&file.local_name))
            .await
            .map_err(error)?;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < file.size_bytes {
            let end = file.size_bytes.min(offset + range_bytes as u64);
            resources
                .ensure_disk_space(directory, end - offset)
                .map_err(error)?;
            let bytes = request
                .storage
                .checkpoint_range(&file.path, offset, end)
                .await
                .map_err(error)?;
            if bytes.len() as u64 != end - offset {
                return Err(error("native recovery object truncated"));
            }
            for chunk in bytes.chunks(CHUNK_BYTES) {
                hash.update(chunk);
                output.write_all(chunk).await.map_err(error)?;
            }
            offset = end;
        }
        if hash.finalize()[..] != file.checksum {
            return Err(error("native recovery object checksum mismatch"));
        }
        output.sync_all().await.map_err(error)?;
    }
    let directory = directory.to_path_buf();
    resources
        .run_blocking(move || std::fs::File::open(directory)?.sync_all())
        .await
        .map_err(error)?
        .map_err(error)?;
    Ok(())
}

/// Materialize only the actual bounded memory destination. The native reader
/// keeps the single database permit until its snapshots and cleanup finish.
pub(crate) async fn into_memory(
    source: super::rocks::RocksLiveState,
    metadata: &NativeDatabaseCheckpointMetadata,
    resources: WorkerStateResources,
    max_resident_bytes: usize,
) -> Result<super::memory::MemoryLiveState> {
    use super::{Ownership, ScanRange, ScanRequest, StateNamespace};
    use arroyo_rpc::grpc::rpc::{DiskKeyedTableConfig, TableEnum, TypedStateTableConfig};
    let mut destination = super::memory::MemoryRestoreBuilder::new(
        resources.clone(),
        max_resident_bytes,
        checkpoint_fingerprint(metadata),
    )?;
    let page_bytes = (resources.config().scan_page_bytes / 8)
        .min(resources.config().queued_write_bytes / 16)
        .min(resources.config().decoded_value_bytes / 8);
    if page_bytes == 0 {
        return Err(LiveStateError::InvalidLimit);
    }
    let snapshot = source.snapshot().await?;
    for table in &metadata.tables {
        let config = table
            .config
            .as_ref()
            .ok_or_else(|| error("native table config absent"))?;
        let identity = match config.table_type() {
            TableEnum::DiskKeyedMap => DiskKeyedTableConfig::decode(config.config.as_slice())
                .map_err(error)?
                .table_name
                .into_bytes(),
            TableEnum::TypedStateTable => {
                TypedStateTableConfig::decode(config.config.as_slice())
                    .map_err(error)?
                    .table_identity
            }
            _ => return Err(error("native memory restore includes a non-live table")),
        };
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: identity,
        };
        if super::encoding::encode_namespace(&namespace)? != table.namespace {
            return Err(error("native memory restore namespace differs from roster"));
        }
        let mut cursor = None;
        loop {
            let page = snapshot
                .scan(ScanRequest {
                    range: ScanRange {
                        namespace: namespace.clone(),
                        prefix: None,
                        start: None,
                        end: None,
                    },
                    max_entries: 16,
                    max_bytes: page_bytes,
                    cursor,
                })
                .await?;
            for entry in &page.entries {
                destination.insert(entry).await?;
            }
            cursor = page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }
    }
    drop(snapshot);
    source.close_and_remove().await?;
    destination.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{
        Ownership, ReadOptions, StateKey, StateNamespace, WriteBatch, WriteOperation,
    };
    use crate::live::{
        lifecycle::RocksStateConfig, resources::ResourceConfig, rocks::RocksLiveState,
    };
    use arroyo_rpc::grpc::rpc::{DiskKeyedTableConfig, TableConfig, TableEnum};

    fn resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 16 << 20,
            memtable_bytes: 2 << 20,
            queued_write_bytes: 4 << 20,
            decoded_value_bytes: 8 << 20,
            scan_page_bytes: 4 << 20,
            max_blocking_operations: 2,
            max_snapshots: 1,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }
    fn config(root: &Path, attempt: u32) -> RocksStateConfig {
        RocksStateConfig {
            root: root.to_owned(),
            job_id: "job".into(),
            operator_id: "operator".into(),
            subtask: 0,
            generation: 0,
            attempt,
        }
    }
    fn namespace() -> StateNamespace {
        StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"map".to_vec(),
        }
    }
    fn key(number: u8) -> StateKey {
        StateKey {
            namespace: namespace(),
            key: vec![number],
            routing_hash: None,
        }
    }
    fn context(epoch: u32) -> NativeUploadContext {
        NativeUploadContext {
            job_id: "job".into(),
            operator_id: "operator".into(),
            pipeline_id: None,
            epoch,
            generation: 0,
            subtask_index: 0,
            tables: vec![NativeCheckpointTable {
                name: "map".into(),
                namespace: crate::live::encoding::encode_namespace(&namespace()).unwrap(),
                config: Some(TableConfig {
                    table_type: TableEnum::DiskKeyedMap.into(),
                    state_version: 1,
                    config: DiskKeyedTableConfig {
                        table_name: "map".into(),
                        encoding_version: 1,
                        schema_identity: vec![1],
                    }
                    .encode_to_vec(),
                }),
            }],
        }
    }
    async fn write(source: &dyn LiveStateBackend, values: &[(u8, Option<&[u8]>)]) {
        source
            .write_batch(WriteBatch {
                max_bytes: 4096,
                operations: values
                    .iter()
                    .map(|(number, value)| match value {
                        Some(value) => WriteOperation::Put {
                            key: key(*number),
                            value: value.to_vec(),
                        },
                        None => WriteOperation::Delete { key: key(*number) },
                    })
                    .collect(),
            })
            .await
            .unwrap();
    }
    async fn get(source: &dyn LiveStateBackend, number: u8) -> Option<Vec<u8>> {
        source
            .get(&key(number), ReadOptions { max_bytes: 4096 })
            .await
            .unwrap()
    }
    async fn export(
        source: &RocksLiveState,
        epoch: u32,
        completed: Option<u32>,
        storage: &StorageProviderRef,
    ) -> NativeDatabaseCheckpointMetadata {
        source
            .capture_native()
            .await
            .unwrap()
            .unwrap()
            .upload(context(epoch), storage, completed)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn native_epochs_reuse_complete_ssts_restore_deletes_and_preserve_max_db_one_portability()
    {
        let local = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            arroyo_storage::StorageProvider::for_url(remote.path().to_str().unwrap())
                .await
                .unwrap(),
        );
        let resources = resources();
        let source =
            RocksLiveState::open_worker_with_resources(config(local.path(), 0), resources.clone())
                .await
                .unwrap();
        write(
            &source,
            &[
                (1, Some(b"one")),
                (2, Some(b"unchanged")),
                (3, Some(b"delete")),
            ],
        )
        .await;
        let first = export(&source, 1, None, &storage).await;
        let second = export(&source, 2, Some(1), &storage).await;
        assert_eq!(first.database_id, second.database_id);
        assert_ne!(
            checkpoint_fingerprint(&first),
            checkpoint_fingerprint(&second),
            "same lineage must not authorize skipping restore of a different epoch"
        );
        let ssts = first
            .files
            .iter()
            .filter(|file| file.shared)
            .collect::<Vec<_>>();
        assert!(
            !ssts.is_empty(),
            "WAL-disabled memtable must become native SST state"
        );
        for file in &ssts {
            assert!(
                second.files.contains(file),
                "unchanged SST bytes must be inherited"
            );
        }
        write(
            &source,
            &[(1, Some(b"changed")), (3, None), (4, Some(b"inserted"))],
        )
        .await;
        let capture = source.capture_native().await.unwrap().unwrap();
        write(&source, &[(1, Some(b"after barrier")), (5, Some(b"later"))]).await;
        let third = capture.upload(context(3), &storage, Some(2)).await.unwrap();
        let first_fingerprint = checkpoint_fingerprint(&first);
        let second_fingerprint = checkpoint_fingerprint(&second);
        source.close_and_remove().await.unwrap();
        for (attempt, metadata) in [(1, first), (2, second), (3, third.clone())] {
            let restored = RocksLiveState::restore_worker_with_resources(
                config(local.path(), attempt),
                resources.clone(),
                NativeRestoreRequest {
                    metadata: metadata.clone(),
                    storage: storage.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                restored.restored_native_checkpoint_fingerprint(),
                Some(checkpoint_fingerprint(&metadata).as_slice())
            );
            if attempt == 1 {
                assert_ne!(
                    restored.restored_native_checkpoint_fingerprint(),
                    Some(second_fingerprint.as_slice()),
                    "epoch1 backend must reject an epoch2 marker despite identical database lineage"
                );
            }
            assert_eq!(
                get(&restored, 1).await,
                Some(if attempt == 3 {
                    b"changed".to_vec()
                } else {
                    b"one".to_vec()
                })
            );
            assert_eq!(get(&restored, 2).await, Some(b"unchanged".to_vec()));
            assert_eq!(
                get(&restored, 3).await,
                if attempt == 3 {
                    None
                } else {
                    Some(b"delete".to_vec())
                }
            );
            assert_eq!(
                get(&restored, 5).await,
                None,
                "post-barrier mutation leaked"
            );
            if attempt == 3 {
                let fourth = export(&restored, 4, Some(3), &storage).await;
                assert!(
                    fourth
                        .files
                        .iter()
                        .filter(|file| file.shared)
                        .all(|file| third.files.contains(file)),
                    "fresh owner must seed verified immutable mapping"
                );
                write(&restored, &[(1, None), (2, None), (4, None)]).await;
                let empty = export(&restored, 5, Some(4), &storage).await;
                restored.close_and_remove().await.unwrap();
                let empty_owner = RocksLiveState::restore_worker_with_resources(
                    config(local.path(), 5),
                    resources.clone(),
                    NativeRestoreRequest {
                        metadata: empty,
                        storage: storage.clone(),
                    },
                )
                .await
                .unwrap();
                for number in 1..=5 {
                    assert_eq!(get(&empty_owner, number).await, None);
                }
                empty_owner.close_and_remove().await.unwrap();
            } else {
                restored.close_and_remove().await.unwrap();
            }
        }
        let reader = RocksLiveState::restore_worker_with_resources(
            config(local.path(), 6),
            resources.clone(),
            NativeRestoreRequest {
                metadata: third.clone(),
                storage: storage.clone(),
            },
        )
        .await
        .unwrap();
        let memory = into_memory(reader, &third, resources.clone(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(get(&memory, 1).await, Some(b"changed".to_vec()));
        assert_eq!(get(&memory, 2).await, Some(b"unchanged".to_vec()));
        assert_eq!(get(&memory, 3).await, None);
        assert_eq!(
            memory.restored_native_checkpoint_fingerprint(),
            Some(checkpoint_fingerprint(&third).as_slice())
        );
        assert_ne!(
            memory.restored_native_checkpoint_fingerprint(),
            Some(first_fingerprint.as_slice())
        );
        Arc::new(memory).close().await.unwrap();
        drop(resources.try_database().unwrap());
    }

    #[tokio::test]
    async fn native_corruption_failure_cleans_fresh_attempt_and_memory_limit_failure_releases_reader()
     {
        let local = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            arroyo_storage::StorageProvider::for_url(remote.path().to_str().unwrap())
                .await
                .unwrap(),
        );
        let resources = resources();
        let source =
            RocksLiveState::open_worker_with_resources(config(local.path(), 0), resources.clone())
                .await
                .unwrap();
        write(&source, &[(1, Some(b"retained"))]).await;
        let original = export(&source, 1, None, &storage).await;
        source.close_and_remove().await.unwrap();
        let reader = RocksLiveState::restore_worker_with_resources(
            config(local.path(), 1),
            resources.clone(),
            NativeRestoreRequest {
                metadata: original.clone(),
                storage: storage.clone(),
            },
        )
        .await
        .unwrap();
        assert!(
            into_memory(reader, &original, resources.clone(), 1)
                .await
                .is_err()
        );
        drop(resources.database().await.unwrap());
        let mut corrupted = original.clone();
        corrupted.files[0].checksum[0] ^= 1;
        let target = config(local.path(), 2);
        assert!(
            RocksLiveState::restore_worker_with_resources(
                target.clone(),
                resources.clone(),
                NativeRestoreRequest {
                    metadata: corrupted,
                    storage
                }
            )
            .await
            .is_err()
        );
        // The cleanup executor owns removal; wait for its ordered sentinel.
        let (done, wait) = tokio::sync::oneshot::channel();
        resources.cleanup().await.unwrap().submit(move || {
            let _ = done.send(());
        });
        wait.await.unwrap();
        assert!(!target.path().exists());
        drop(resources.try_database().unwrap());
    }

    #[tokio::test]
    async fn native_rollback_deletes_only_positively_created_canonical_destinations() {
        let local = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            arroyo_storage::StorageProvider::for_url(remote.path().to_str().unwrap())
                .await
                .unwrap(),
        );
        let resources = resources();
        let path = local.path().join("staged");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("source"), b"new bytes").unwrap();
        storage
            .put_if_not_exists("canonical", b"retained bytes".to_vec())
            .await
            .unwrap();
        let collision = Arc::new(AtomicBool::new(false));
        let owned = Arc::new(AtomicBool::new(false));
        let mut stage = StagingDirectory::new(
            path.clone(),
            resources.cleanup().await.unwrap(),
            resources.snapshot().await.unwrap(),
        );
        let mut remote = RemoteArtifactCleanup::new(storage.clone());
        remote.paths = vec![
            ("canonical".into(), collision.clone()),
            ("new-canonical".into(), owned.clone()),
        ];
        stage.remote_cleanup = Some(remote);
        assert!(
            upload_file(
                &path.join("source"),
                9,
                "canonical",
                &storage,
                &resources,
                collision.clone()
            )
            .await
            .is_err()
        );
        assert!(!collision.load(Ordering::Acquire));
        upload_file(
            &path.join("source"),
            9,
            "new-canonical",
            &storage,
            &resources,
            owned.clone(),
        )
        .await
        .unwrap();
        assert!(owned.load(Ordering::Acquire));
        drop(stage);
        let (done, wait) = tokio::sync::oneshot::channel();
        resources.cleanup().await.unwrap().submit(move || {
            let _ = done.send(());
        });
        wait.await.unwrap();
        assert_eq!(
            storage.get("canonical").await.unwrap().as_ref(),
            b"retained bytes"
        );
        assert!(!storage.exists("new-canonical").await.unwrap());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn blocked_remote_cleanup_releases_local_stage_and_capture_before_aggregate_deadline() {
        let local = tempfile::tempdir().unwrap();
        let resources = resources(); // max_snapshots=1
        let path = local.path().join("staged");
        let temporary = PathBuf::from(format!("{}.tmp", path.display()));
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(&temporary).unwrap();
        std::fs::write(path.join("pinned.sst"), [1; 4096]).unwrap();
        let (started, wait_started) = tokio::sync::oneshot::channel();
        let mut stage = StagingDirectory::new(
            path.clone(),
            resources.cleanup().await.unwrap(),
            resources.snapshot().await.unwrap(),
        );
        stage.remote_cleanup = Some(RemoteArtifactCleanup {
            paths: Vec::new(),
            runtime: tokio::runtime::Handle::current(),
            remove: Box::new(move |_| {
                Box::pin(async move {
                    let _ = started.send(());
                    std::future::pending::<()>().await;
                })
            }),
        });
        drop(stage);
        tokio::time::timeout(std::time::Duration::from_secs(2), wait_started)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !path.exists() && !temporary.exists(),
            "remote deletion pinned native SST files"
        );
        let snapshot =
            tokio::time::timeout(std::time::Duration::from_millis(200), resources.snapshot())
                .await
                .unwrap()
                .unwrap();
        drop(snapshot);
        let (done, wait) = tokio::sync::oneshot::channel();
        resources.cleanup().await.unwrap().submit(move || {
            let _ = done.send(());
        });
        tokio::time::timeout(
            REMOTE_CLEANUP_BUDGET + std::time::Duration::from_secs(2),
            wait,
        )
        .await
        .unwrap()
        .unwrap();
    }
}
