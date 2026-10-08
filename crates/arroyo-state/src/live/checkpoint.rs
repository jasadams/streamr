//! Full logical snapshots. Each immutable object is exclusive to one checkpoint.
use super::{
    LiveStateBackend, ScanRange, ScanRequest, StateNamespace, StateSnapshot, WriteBatch,
    WriteOperation, encoding,
};
use anyhow::{Result, bail, ensure};
use arroyo_rpc::grpc::rpc::{
    DiskCheckpointFile, DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
};
use arroyo_storage::StorageProviderRef;
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncReadExt;

pub const PAGE_BYTES: usize = 1024 * 1024;
const PAGE_ROWS: usize = 128;
pub(crate) const MAX_FILES: usize = 65536;
/// Leaves headroom for worker/job identity in tonic's default 4 MiB envelope.
pub(crate) const MAX_SUBTASK_CHECKPOINT_BYTES: usize = 3 * 1024 * 1024;
const MAGIC: &[u8] = b"STRDS001";
static UPLOAD_ID: AtomicU64 = AtomicU64::new(0);

#[allow(clippy::too_many_arguments)]
pub async fn export(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    config: &DiskKeyedTableConfig,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
) -> Result<DiskKeyedTableSubtaskCheckpointMetadata> {
    export_with_file_limit(
        snapshot, namespace, config, storage, path, epoch, generation, subtask, MAX_FILES,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn export_with_file_limit(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    config: &DiskKeyedTableConfig,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
    max_files: usize,
) -> Result<DiskKeyedTableSubtaskCheckpointMetadata> {
    ensure!(
        !config.table_name.is_empty()
            && !config.table_name.contains(['/', '\\'])
            && !matches!(config.table_name.as_str(), "." | ".."),
        "disk table name must be a safe path component"
    );
    ensure!(
        namespace.table == config.table_name.as_bytes(),
        "disk snapshot namespace/config mismatch"
    );
    ensure!(config.encoding_version == 1, "unsupported disk encoding");
    let mut metadata = DiskKeyedTableSubtaskCheckpointMetadata {
        subtask_index: subtask,
        format_version: 1,
        encoding_version: 1,
        schema_identity: config.schema_identity.clone(),
        namespace: encoding::encode_namespace(namespace)?,
        generation,
        epoch,
        empty: true,
        files: vec![],
    };
    // Track repeated-message wire bytes incrementally rather than rescanning
    // a growing full file list on every page (quadratic in checkpoint size).
    let mut metadata_wire_bytes = metadata.encoded_len();
    let upload_id = format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        UPLOAD_ID.fetch_add(1, Ordering::Relaxed)
    );
    let resources = super::worker::configured_worker_resources()?;
    let page_bytes = resources.as_ref().map_or(PAGE_BYTES, |r| {
        PAGE_BYTES
            .min(r.config().scan_page_bytes / (8 * r.config().max_open_databases.saturating_add(1)))
            .min(r.config().queued_write_bytes / 8)
            .saturating_sub(32768)
    });
    ensure!(page_bytes > 0, "checkpoint resource budgets are too small");
    let mut cursor = None;
    let result: Result<()> = async {
        loop {
            let _buffers = if let Some(resources) = &resources {
                Some(
                    resources
                        .scan_page(page_bytes * 4 + PAGE_ROWS * 128)
                        .await?,
                )
            } else {
                None
            };
            let page = snapshot
                .scan(ScanRequest {
                    range: ScanRange {
                        namespace: namespace.clone(),
                        prefix: None,
                        start: None,
                        end: None,
                    },
                    max_entries: PAGE_ROWS,
                    max_bytes: page_bytes,
                    cursor,
                })
                .await?;
            if !page.entries.is_empty() {
                ensure!(
                    metadata.files.len() < max_files,
                    "disk checkpoint exceeds maximum page-file count"
                );
                let mut bytes = MAGIC.to_vec();
                let rows = page.entries.len() as u64;
                for entry in page.entries {
                    let key = encoding::encode_key(&entry.key)?;
                    bytes.extend_from_slice(&(u32::try_from(key.len())?).to_be_bytes());
                    bytes.extend_from_slice(&(u32::try_from(entry.value.len())?).to_be_bytes());
                    bytes.extend_from_slice(&key);
                    bytes.extend_from_slice(&entry.value);
                }
                let file = DiskCheckpointFile {
                    path: format!("{path}/disk-{upload_id}-{:06}.bin", metadata.files.len()),
                    size_bytes: bytes.len() as u64,
                    checksum: Sha256::digest(&bytes).to_vec(),
                    row_count: rows,
                };
                let file_len = file.encoded_len();
                let mut prefix_len = 1usize;
                let mut remaining = file_len;
                while remaining >= 128 {
                    prefix_len += 1;
                    remaining >>= 7;
                }
                metadata_wire_bytes = metadata_wire_bytes.saturating_add(1 + prefix_len + file_len);
                ensure!(
                    metadata_wire_bytes <= MAX_SUBTASK_CHECKPOINT_BYTES,
                    "disk checkpoint file metadata exceeds 3 MiB RPC limit"
                );
                storage.put_if_not_exists(file.path.clone(), bytes).await?;
                metadata.files.push(file);
                metadata.empty = false;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        for file in &metadata.files {
            // Best effort only: crash leftovers are handled by fenced remote GC.
            let _ = storage.delete_if_present(file.path.clone()).await;
        }
        return Err(error);
    }
    Ok(metadata)
}

/// Restore into a fresh namespace only. A failed restore poisons that attempt:
/// callers must discard its database and retry in another fresh attempt directory.
pub async fn restore(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    config: &DiskKeyedTableConfig,
    metadata: &DiskKeyedTableSubtaskCheckpointMetadata,
    storage: &StorageProviderRef,
) -> Result<()> {
    ensure!(
        metadata.format_version == 1
            && metadata.encoding_version == 1
            && config.encoding_version == 1,
        "unsupported disk checkpoint encoding/version"
    );
    ensure!(
        metadata.schema_identity == config.schema_identity,
        "disk checkpoint schema mismatch"
    );
    ensure!(
        metadata.namespace == encoding::encode_namespace(namespace)?,
        "disk checkpoint ownership/parallelism mismatch"
    );
    ensure!(
        metadata.empty == metadata.files.is_empty(),
        "invalid disk checkpoint empty marker"
    );
    ensure_empty(backend, namespace).await?;
    ensure!(
        metadata.files.len() <= MAX_FILES,
        "disk checkpoint exceeds maximum page-file count"
    );
    let resources = super::worker::configured_worker_resources()?;
    let mut previous_key: Option<Vec<u8>> = None;
    let mut paths = std::collections::HashSet::new();
    for file in &metadata.files {
        ensure!(paths.insert(&file.path), "duplicate disk checkpoint file");
        // Head first and cap the streaming read; never trust remote metadata to
        // authorize an unbounded allocation, even when an object is corrupt.
        ensure!(
            file.size_bytes <= (PAGE_BYTES + PAGE_ROWS * 8 + MAGIC.len()) as u64,
            "disk checkpoint page exceeds restore limit"
        );
        ensure!(
            storage.head(file.path.clone()).await?.size as u64 == file.size_bytes,
            "disk checkpoint object size mismatch"
        );
        let _buffers = if let Some(resources) = &resources {
            Some(
                resources
                    .scan_page(
                        file.size_bytes as usize * 3
                            + PAGE_ROWS * 128
                            + previous_key.as_ref().map_or(0, Vec::len),
                    )
                    .await?,
            )
        } else {
            None
        };
        let stream = storage.get_as_stream(file.path.clone()).await?;
        let mut bytes = Vec::with_capacity(file.size_bytes as usize + 1);
        stream
            .take(file.size_bytes + 1)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(
            bytes.len() as u64 == file.size_bytes && Sha256::digest(&bytes)[..] == file.checksum,
            "disk checkpoint checksum/size mismatch"
        );
        ensure!(
            bytes.starts_with(MAGIC),
            "invalid disk checkpoint file magic"
        );
        let mut input = &bytes[MAGIC.len()..];
        let mut operations = vec![];
        while !input.is_empty() {
            ensure!(input.len() >= 8, "truncated disk checkpoint frame");
            let key_len = u32::from_be_bytes(input[..4].try_into()?) as usize;
            let value_len = u32::from_be_bytes(input[4..8].try_into()?) as usize;
            input = &input[8..];
            let total = key_len
                .checked_add(value_len)
                .ok_or_else(|| anyhow::anyhow!("invalid frame length"))?;
            ensure!(input.len() >= total, "truncated disk checkpoint payload");
            let encoded_key = &input[..key_len];
            let key = encoding::decode_key(encoded_key)?;
            ensure!(
                key.namespace == *namespace,
                "disk checkpoint namespace mismatch"
            );
            if let Some(previous) = &previous_key {
                ensure!(
                    previous.as_slice() < encoded_key,
                    "disk checkpoint keys out of order/duplicated"
                );
            }
            previous_key = Some(encoded_key.to_vec());
            operations.push(WriteOperation::Put {
                key,
                value: input[key_len..total].to_vec(),
            });
            input = &input[total..];
        }
        ensure!(
            operations.len() as u64 == file.row_count
                && !operations.is_empty()
                && operations.len() <= PAGE_ROWS,
            "disk checkpoint row count mismatch"
        );
        backend
            .write_batch(WriteBatch {
                operations,
                max_bytes: PAGE_BYTES + PAGE_ROWS,
            })
            .await?;
    }
    Ok(())
}

pub async fn ensure_empty(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
) -> Result<()> {
    let resources = super::worker::configured_worker_resources()?;
    let max_bytes = resources
        .as_ref()
        .map_or(PAGE_BYTES, |r| r.config().scan_page_bytes / 8);
    ensure!(max_bytes > 0, "checkpoint scan budget is too small");
    let page = backend
        .snapshot()
        .await?
        .scan(ScanRequest {
            range: ScanRange {
                namespace: namespace.clone(),
                prefix: None,
                start: None,
                end: None,
            },
            max_entries: 1,
            max_bytes,
            cursor: None,
        })
        .await?;
    if !page.entries.is_empty() {
        bail!("disk restore requires a fresh attempt database");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{Ownership, ReadOptions, StateKey, memory::MemoryLiveState};
    use arroyo_storage::StorageProvider;
    use std::sync::Arc;

    fn namespace() -> StateNamespace {
        StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"map".to_vec(),
        }
    }
    fn key(number: u32) -> StateKey {
        StateKey {
            namespace: namespace(),
            key: number.to_be_bytes().to_vec(),
            routing_hash: None,
        }
    }
    fn config() -> DiskKeyedTableConfig {
        DiskKeyedTableConfig {
            table_name: "map".into(),
            encoding_version: 1,
            schema_identity: b"utf8-v1".to_vec(),
        }
    }
    async fn storage(directory: &tempfile::TempDir) -> StorageProviderRef {
        Arc::new(
            StorageProvider::for_url(&format!("file://{}", directory.path().display()))
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn full_snapshot_keeps_unchanged_rows_and_excludes_post_barrier_writes() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let backend = MemoryLiveState::new();
        for i in 0..300 {
            backend
                .put(key(i), vec![i as u8; 8192], PAGE_BYTES)
                .await
                .unwrap();
        }
        let first = backend.snapshot().await.unwrap();
        backend.delete(key(1), PAGE_BYTES).await.unwrap();
        backend
            .put(key(0), b"new".to_vec(), PAGE_BYTES)
            .await
            .unwrap();
        let second = backend.snapshot().await.unwrap();
        backend
            .put(key(2), b"post-barrier".to_vec(), PAGE_BYTES)
            .await
            .unwrap();
        let first = export(
            &first,
            &namespace(),
            &config(),
            &storage,
            "checkpoint1/map",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        let second = export(
            &second,
            &namespace(),
            &config(),
            &storage,
            "checkpoint2/map",
            2,
            0,
            0,
        )
        .await
        .unwrap();
        assert!(second.files.len() > 1);
        let restored = MemoryLiveState::new();
        restore(&restored, &namespace(), &config(), &second, &storage)
            .await
            .unwrap();
        assert_eq!(
            restored
                .get(
                    &key(0),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(
            restored
                .get(
                    &key(1),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            restored
                .get(
                    &key(2),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            Some(vec![2; 8192])
        );
        assert_eq!(
            restored
                .get(
                    &key(299),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            Some(vec![299u32 as u8; 8192])
        );
        assert!(
            restore(&restored, &namespace(), &config(), &first, &storage)
                .await
                .is_err()
        );
        let original = MemoryLiveState::new();
        restore(&original, &namespace(), &config(), &first, &storage)
            .await
            .unwrap();
        assert_eq!(
            original
                .get(
                    &key(1),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            Some(vec![1; 8192])
        );
    }

    #[tokio::test]
    async fn corruption_and_schema_mismatch_are_rejected_and_empty_state_is_explicit() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let backend = MemoryLiveState::new();
        let empty = export(
            &backend.snapshot().await.unwrap(),
            &namespace(),
            &config(),
            &storage,
            "empty/map",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        assert!(empty.empty && empty.files.is_empty());
        restore(
            &MemoryLiveState::new(),
            &namespace(),
            &config(),
            &empty,
            &storage,
        )
        .await
        .unwrap();
        backend.put(key(0), vec![1; 32], PAGE_BYTES).await.unwrap();
        let metadata = export(
            &backend.snapshot().await.unwrap(),
            &namespace(),
            &config(),
            &storage,
            "full/map",
            2,
            0,
            0,
        )
        .await
        .unwrap();
        let mut wrong_schema = config();
        wrong_schema.schema_identity = b"other".to_vec();
        assert!(
            restore(
                &MemoryLiveState::new(),
                &namespace(),
                &wrong_schema,
                &metadata,
                &storage
            )
            .await
            .is_err()
        );
        let file = &metadata.files[0];
        storage
            .put(file.path.clone(), vec![0; file.size_bytes as usize])
            .await
            .unwrap();
        let fresh = MemoryLiveState::new();
        assert!(
            restore(&fresh, &namespace(), &config(), &metadata, &storage)
                .await
                .is_err()
        );
        assert_eq!(
            fresh
                .get(
                    &key(0),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            None
        );
    }
    #[tokio::test]
    async fn interrupted_multipage_restore_cannot_accept_leftovers_and_retries_fresh() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        for i in 0..300 {
            source
                .put(key(i), vec![i as u8; 8192], PAGE_BYTES)
                .await
                .unwrap();
        }
        let metadata = export(
            &source.snapshot().await.unwrap(),
            &namespace(),
            &config(),
            &storage,
            "epoch/map",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        assert!(metadata.files.len() >= 3);
        let middle = &metadata.files[1];
        let original = storage.get(middle.path.clone()).await.unwrap().to_vec();
        storage
            .delete_if_present(middle.path.clone())
            .await
            .unwrap();
        let interrupted = MemoryLiveState::new();
        assert!(
            restore(&interrupted, &namespace(), &config(), &metadata, &storage)
                .await
                .is_err()
        );
        assert!(
            interrupted
                .get(
                    &key(0),
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap()
                .is_some()
        );
        storage
            .put(middle.path.clone(), original.clone())
            .await
            .unwrap();
        assert!(
            restore(&interrupted, &namespace(), &config(), &metadata, &storage)
                .await
                .is_err()
        );
        // A same-length, checksum-invalid second page also fails after page one.
        storage
            .put(middle.path.clone(), vec![0; original.len()])
            .await
            .unwrap();
        let corrupt_attempt = MemoryLiveState::new();
        assert!(
            restore(
                &corrupt_attempt,
                &namespace(),
                &config(),
                &metadata,
                &storage
            )
            .await
            .is_err()
        );
        assert!(
            restore(
                &corrupt_attempt,
                &namespace(),
                &config(),
                &metadata,
                &storage
            )
            .await
            .is_err()
        );
        storage.put(middle.path.clone(), original).await.unwrap();
        let retry = MemoryLiveState::new();
        restore(&retry, &namespace(), &config(), &metadata, &storage)
            .await
            .unwrap();
        for i in 0..300 {
            assert_eq!(
                retry
                    .get(
                        &key(i),
                        ReadOptions {
                            max_bytes: PAGE_BYTES
                        }
                    )
                    .await
                    .unwrap(),
                Some(vec![i as u8; 8192])
            );
        }
    }
}
