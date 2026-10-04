//! Full logical snapshots. Each immutable object is exclusive to one checkpoint.
use super::{
    LiveStateBackend, ScanRange, ScanRequest, StateNamespace, StateSnapshot, WriteBatch,
    WriteOperation, encoding,
    resources::{CheckpointDirection, CheckpointObservation, WorkerStateResources},
};
use anyhow::{Result, bail, ensure};
use arroyo_rpc::grpc::rpc::{
    DiskCheckpointFile, DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
    TypedStateTableConfig, TypedStateTableSubtaskCheckpointMetadata,
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
    export_snapshot(
        snapshot,
        namespace,
        &config.table_name,
        config.table_name.as_bytes(),
        &config.schema_identity,
        config.encoding_version,
        storage,
        path,
        epoch,
        generation,
        subtask,
        max_files,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn export_snapshot(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    transport_name: &str,
    table_identity: &[u8],
    schema_identity: &[u8],
    encoding_version: u32,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
    max_files: usize,
) -> Result<DiskKeyedTableSubtaskCheckpointMetadata> {
    let resources = super::worker::configured_worker_resources()?;
    let mut observation =
        CheckpointObservation::new(resources.clone(), CheckpointDirection::Export);
    let result = export_snapshot_inner(
        snapshot,
        namespace,
        transport_name,
        table_identity,
        schema_identity,
        encoding_version,
        storage,
        path,
        epoch,
        generation,
        subtask,
        max_files,
        resources,
        &observation,
    )
    .await;
    observation.finish(&result);
    result
}

#[allow(clippy::too_many_arguments)]
async fn export_snapshot_inner(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    transport_name: &str,
    table_identity: &[u8],
    schema_identity: &[u8],
    encoding_version: u32,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
    max_files: usize,
    resources: Option<WorkerStateResources>,
    observation: &CheckpointObservation,
) -> Result<DiskKeyedTableSubtaskCheckpointMetadata> {
    ensure!(
        !transport_name.is_empty()
            && !transport_name.contains(['/', '\\'])
            && !matches!(transport_name, "." | ".."),
        "disk table name must be a safe path component"
    );
    ensure!(
        namespace.table == table_identity,
        "disk snapshot namespace/config mismatch"
    );
    ensure!(encoding_version == 1, "unsupported disk encoding");
    let mut metadata = DiskKeyedTableSubtaskCheckpointMetadata {
        subtask_index: subtask,
        format_version: 1,
        encoding_version: 1,
        schema_identity: schema_identity.to_vec(),
        namespace: encoding::encode_namespace(namespace)?,
        generation,
        epoch,
        empty: true,
        files: vec![],
    };
    // Track repeated-message wire bytes incrementally rather than rescanning
    // a growing full file list on every page (quadratic in checkpoint size).
    let mut metadata_wire_bytes = metadata.encoded_len();
    // These components retain their uniqueness in hexadecimal. Checkpoint
    // metadata stores complete immutable paths; readers and GC do not parse
    // the numeric basename, so older decimal names remain readable.
    let upload_id = format!(
        "{:x}-{:x}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        UPLOAD_ID.fetch_add(1, Ordering::Relaxed)
    );
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
                    // File replay follows metadata order, not basename order.
                    path: format!("{path}/disk-{upload_id}-{:x}.bin", metadata.files.len()),
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
                observation.page_transferred(file.size_bytes);
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

#[allow(clippy::too_many_arguments)]
pub async fn export_typed(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    config: &TypedStateTableConfig,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
    max_files: usize,
) -> Result<TypedStateTableSubtaskCheckpointMetadata> {
    let resources = super::worker::configured_worker_resources()?;
    export_typed_with_resources(
        snapshot, namespace, config, storage, path, epoch, generation, subtask, max_files,
        resources,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn export_typed_with_resources(
    snapshot: &StateSnapshot,
    namespace: &StateNamespace,
    config: &TypedStateTableConfig,
    storage: &StorageProviderRef,
    path: &str,
    epoch: u32,
    generation: u64,
    subtask: u32,
    max_files: usize,
    resources: Option<WorkerStateResources>,
) -> Result<TypedStateTableSubtaskCheckpointMetadata> {
    let mut observation =
        CheckpointObservation::new(resources.clone(), CheckpointDirection::Export);
    let result = async {
        arroyo_state_protocol::typed_checkpoint::validate_config(config)
            .map_err(|error| anyhow::anyhow!(error))?;
        let metadata = export_snapshot_inner(
            snapshot,
            namespace,
            &config.transport_name,
            &config.table_identity,
            &config.schema_identity,
            config.encoding_version,
            storage,
            path,
            epoch,
            generation,
            subtask,
            max_files,
            resources,
            &observation,
        )
        .await?;
        Ok(TypedStateTableSubtaskCheckpointMetadata {
            subtask_index: metadata.subtask_index,
            format_version: metadata.format_version,
            encoding_version: metadata.encoding_version,
            schema_identity: metadata.schema_identity,
            namespace: metadata.namespace,
            generation: metadata.generation,
            epoch: metadata.epoch,
            empty: metadata.empty,
            files: metadata.files,
        })
    }
    .await;
    observation.finish(&result);
    result
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
    restore_snapshot(
        backend,
        namespace,
        config.encoding_version,
        &config.schema_identity,
        metadata,
        storage,
    )
    .await
}

pub async fn restore_typed(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    config: &TypedStateTableConfig,
    metadata: &TypedStateTableSubtaskCheckpointMetadata,
    storage: &StorageProviderRef,
) -> Result<()> {
    let resources = super::worker::configured_worker_resources()?;
    restore_typed_with_resources(backend, namespace, config, metadata, storage, resources).await
}

async fn restore_typed_with_resources(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    config: &TypedStateTableConfig,
    metadata: &TypedStateTableSubtaskCheckpointMetadata,
    storage: &StorageProviderRef,
    resources: Option<WorkerStateResources>,
) -> Result<()> {
    let mut observation =
        CheckpointObservation::new(resources.clone(), CheckpointDirection::Restore);
    let result = async {
        arroyo_state_protocol::typed_checkpoint::validate_subtask(config, metadata)
            .map_err(|error| anyhow::anyhow!(error))?;
        let disk = DiskKeyedTableSubtaskCheckpointMetadata {
            subtask_index: metadata.subtask_index,
            format_version: metadata.format_version,
            encoding_version: metadata.encoding_version,
            schema_identity: metadata.schema_identity.clone(),
            namespace: metadata.namespace.clone(),
            generation: metadata.generation,
            epoch: metadata.epoch,
            empty: metadata.empty,
            files: metadata.files.clone(),
        };
        restore_snapshot_inner(
            backend,
            namespace,
            config.encoding_version,
            &config.schema_identity,
            &disk,
            storage,
            resources,
            &observation,
        )
        .await
    }
    .await;
    observation.finish(&result);
    result
}

async fn restore_snapshot(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    encoding_version: u32,
    schema_identity: &[u8],
    metadata: &DiskKeyedTableSubtaskCheckpointMetadata,
    storage: &StorageProviderRef,
) -> Result<()> {
    let resources = super::worker::configured_worker_resources()?;
    let mut observation =
        CheckpointObservation::new(resources.clone(), CheckpointDirection::Restore);
    let result = restore_snapshot_inner(
        backend,
        namespace,
        encoding_version,
        schema_identity,
        metadata,
        storage,
        resources,
        &observation,
    )
    .await;
    observation.finish(&result);
    result
}

#[allow(clippy::too_many_arguments)]
async fn restore_snapshot_inner(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    encoding_version: u32,
    schema_identity: &[u8],
    metadata: &DiskKeyedTableSubtaskCheckpointMetadata,
    storage: &StorageProviderRef,
    resources: Option<WorkerStateResources>,
    observation: &CheckpointObservation,
) -> Result<()> {
    ensure!(
        metadata.format_version == 1 && metadata.encoding_version == 1 && encoding_version == 1,
        "unsupported disk checkpoint encoding/version"
    );
    ensure!(
        metadata.schema_identity == schema_identity,
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
        observation.page_transferred(file.size_bytes);
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
    use crate::live::{
        Ownership, ReadOptions, StateKey, memory::MemoryLiveState, resources::ResourceConfig,
    };
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

    #[test]
    fn compact_immutable_paths_fit_large_leader_checkpoint_metadata() {
        // Reproduce the observed 14,015-page boundary with a long, valid job
        // path. Only the basename changes; paths still identify one exclusive
        // generation/epoch/operator/table and the same per-export components.
        let config = config();
        let base = format!(
            "P/{}/generations/0/checkpoints/checkpoint-0000001/operator-tumbling_window_8/table-map-000",
            "x".repeat(48)
        );
        let pid = 676_578u32;
        let nanos = 1_791_109_665_944_763_941u128;
        let counter = 0u64;
        let mut metadata = DiskKeyedTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            format_version: 1,
            encoding_version: 1,
            schema_identity: config.schema_identity.clone(),
            namespace: encoding::encode_namespace(&namespace()).unwrap(),
            generation: 0,
            epoch: 1,
            empty: false,
            files: (0..14_015)
                .map(|index| DiskCheckpointFile {
                    path: format!("{base}/disk-{pid}-{nanos}-{counter}-{index:06}.bin"),
                    size_bytes: 54_613,
                    checksum: vec![0; 32],
                    row_count: 6,
                })
                .collect(),
        };
        arroyo_state_protocol::disk::validate_subtask(&config, &metadata).unwrap();
        let previous_wire_bytes = metadata.encoded_len();
        assert!(previous_wire_bytes > MAX_SUBTASK_CHECKPOINT_BYTES);
        for (index, file) in metadata.files.iter_mut().enumerate() {
            file.path = format!("{base}/disk-{pid:x}-{nanos:x}-{counter:x}-{index:x}.bin");
        }
        arroyo_state_protocol::disk::validate_subtask(&config, &metadata).unwrap();
        let compact_wire_bytes = metadata.encoded_len();
        assert!(compact_wire_bytes <= MAX_SUBTASK_CHECKPOINT_BYTES);
        assert!(previous_wire_bytes - compact_wire_bytes > 35_922);
        assert_eq!(
            metadata.files[0].path.rsplit('/').next().unwrap(),
            format!("disk-{pid:x}-{nanos:x}-{counter:x}-0.bin")
        );
        assert_eq!(
            metadata.files[14_014].path.rsplit('/').next().unwrap(),
            format!("disk-{pid:x}-{nanos:x}-{counter:x}-36be.bin")
        );
    }

    #[tokio::test]
    async fn checkpoint_metrics_count_only_successfully_transferred_encoded_pages() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 512 * 1024,
            queued_write_bytes: 16 * 1024 * 1024,
            decoded_value_bytes: 16 * 1024 * 1024,
            scan_page_bytes: 16 * 1024 * 1024,
            max_blocking_operations: 1,
            max_snapshots: 1,
            max_open_databases: 1,
            disk_reserve_bytes: 1,
        })
        .unwrap();
        let registry = prometheus::Registry::new();
        resources.register_metrics(&registry).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        source
            .put(key(0), b"value".to_vec(), PAGE_BYTES)
            .await
            .unwrap();
        let snapshot = source.snapshot().await.unwrap();
        let config = config();
        let mut export =
            CheckpointObservation::new(Some(resources.clone()), CheckpointDirection::Export);
        let metadata = export_snapshot_inner(
            &snapshot,
            &namespace(),
            &config.table_name,
            config.table_name.as_bytes(),
            &config.schema_identity,
            config.encoding_version,
            &storage,
            "checkpoint/metrics",
            1,
            1,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &export,
        )
        .await;
        export.finish(&metadata);
        drop(export);
        let metadata = metadata.unwrap();
        assert_eq!(metadata.files.len(), 1);
        let encoded_bytes = metadata.files[0].size_bytes as f64;

        let restored = MemoryLiveState::new();
        let mut restore =
            CheckpointObservation::new(Some(resources.clone()), CheckpointDirection::Restore);
        let result = restore_snapshot_inner(
            &restored,
            &namespace(),
            config.encoding_version,
            &config.schema_identity,
            &metadata,
            &storage,
            Some(resources.clone()),
            &restore,
        )
        .await;
        restore.finish(&result);
        drop(restore);
        result.unwrap();

        storage
            .put(
                metadata.files[0].path.clone(),
                vec![0; metadata.files[0].size_bytes as usize],
            )
            .await
            .unwrap();
        let mut failed_restore =
            CheckpointObservation::new(Some(resources.clone()), CheckpointDirection::Restore);
        let result = restore_snapshot_inner(
            &MemoryLiveState::new(),
            &namespace(),
            config.encoding_version,
            &config.schema_identity,
            &metadata,
            &storage,
            Some(resources.clone()),
            &failed_restore,
        )
        .await;
        failed_restore.finish(&result);
        drop(failed_restore);
        assert!(result.is_err());

        let malformed_typed = TypedStateTableConfig {
            transport_name: String::new(),
            table_identity: vec![],
            schema_identity: vec![],
            schema_json: vec![],
            primary_key: vec![],
            partition_key: vec![],
            encoding_version: 0,
        };
        assert!(
            export_typed_with_resources(
                &snapshot,
                &namespace(),
                &malformed_typed,
                &storage,
                "checkpoint/invalid-typed",
                1,
                1,
                0,
                MAX_FILES,
                Some(resources.clone()),
            )
            .await
            .is_err()
        );
        let malformed_metadata = TypedStateTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            format_version: 0,
            encoding_version: 0,
            schema_identity: vec![],
            namespace: vec![],
            generation: 0,
            epoch: 0,
            empty: true,
            files: vec![],
        };
        assert!(
            restore_typed_with_resources(
                &MemoryLiveState::new(),
                &namespace(),
                &malformed_typed,
                &malformed_metadata,
                &storage,
                Some(resources),
            )
            .await
            .is_err()
        );

        let families = registry.gather();
        let bytes = families
            .iter()
            .find(|family| family.name() == "arroyo_live_state_checkpoint_encoded_page_bytes_total")
            .unwrap();
        let value = |direction: &str| {
            bytes
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "direction" && label.value() == direction)
                })
                .unwrap()
                .get_counter()
                .as_ref()
                .unwrap()
                .value()
        };
        assert_eq!(value("export"), encoded_bytes);
        assert_eq!(value("restore"), encoded_bytes);
        let operations = families
            .iter()
            .find(|family| family.name() == "arroyo_live_state_checkpoint_operations_total")
            .unwrap();
        let errors = |direction: &str| {
            operations
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "direction" && label.value() == direction)
                        && metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "outcome" && label.value() == "error")
                })
                .unwrap()
                .get_counter()
                .as_ref()
                .unwrap()
                .value()
        };
        assert_eq!(errors("export"), 1.0);
        assert_eq!(errors("restore"), 2.0);
    }

    #[tokio::test]
    async fn typed_snapshot_restores_opaque_namespace_through_safe_transport_path() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let identity = br#"state-table-v1:["public","items"]"#.to_vec();
        let transport_name =
            arroyo_state_protocol::typed_checkpoint::transport_table_name(&identity).unwrap();
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: identity.clone(),
        };
        let config = TypedStateTableConfig {
            transport_name: transport_name.clone(),
            table_identity: identity,
            schema_identity: b"schema-v1".to_vec(),
            schema_json: b"{}".to_vec(),
            primary_key: vec![0],
            partition_key: vec![0],
            encoding_version: 1,
        };
        let key = StateKey {
            namespace: namespace.clone(),
            key: b"key".to_vec(),
            routing_hash: None,
        };
        let source = MemoryLiveState::new();
        source
            .put(key.clone(), b"value".to_vec(), PAGE_BYTES)
            .await
            .unwrap();
        let path = format!(
            "P/J/generations/2/checkpoints/checkpoint-0000003/operator-o/table-{transport_name}-000"
        );
        let metadata = export_typed(
            &source.snapshot().await.unwrap(),
            &namespace,
            &config,
            &storage,
            &path,
            3,
            2,
            0,
            MAX_FILES,
        )
        .await
        .unwrap();
        let basename = metadata.files[0].path.rsplit('/').next().unwrap();
        let components = basename
            .strip_prefix("disk-")
            .unwrap()
            .strip_suffix(".bin")
            .unwrap()
            .split('-')
            .collect::<Vec<_>>();
        assert_eq!(components.len(), 4);
        assert_eq!(components[3], "0");
        assert!(
            components.iter().all(|part| {
                !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        );
        arroyo_state_protocol::typed_checkpoint::validate_subtask(&config, &metadata).unwrap();
        let restored = MemoryLiveState::new();
        restore_typed(&restored, &namespace, &config, &metadata, &storage)
            .await
            .unwrap();
        assert_eq!(
            restored
                .get(
                    &key,
                    ReadOptions {
                        max_bytes: PAGE_BYTES
                    }
                )
                .await
                .unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn typed_checkpoints_switch_backends_with_full_and_empty_epochs() {
        use crate::live::{
            lifecycle::RocksStateConfig,
            resources::{ResourceConfig, WorkerStateResources},
            worker::{BackendConstruction, construct_backend},
        };

        async fn backend(
            rocks: bool,
            root: &std::path::Path,
        ) -> (Arc<dyn LiveStateBackend>, WorkerStateResources) {
            let resources = WorkerStateResources::new(ResourceConfig {
                block_cache_bytes: 8 * 1024 * 1024,
                memtable_bytes: 2 * 1024 * 1024,
                // Restore admits a complete bounded page as a backend batch.
                queued_write_bytes: 8 * 1024 * 1024,
                decoded_value_bytes: 4 * 1024 * 1024,
                // Restore checks namespace emptiness with a PAGE_BYTES scan;
                // admit its backend buffers and request/container overhead.
                scan_page_bytes: 8 * 1024 * 1024,
                max_blocking_operations: 2,
                max_snapshots: 4,
                max_open_databases: 2,
                disk_reserve_bytes: 0,
            })
            .unwrap();
            let construction = if rocks {
                BackendConstruction::Rocksdb(RocksStateConfig {
                    root: root.to_path_buf(),
                    job_id: "typed-checkpoint-test".into(),
                    operator_id: "state-owner".into(),
                    subtask: 0,
                    generation: 7,
                    attempt: 1,
                })
            } else {
                BackendConstruction::Memory {
                    max_resident_bytes: 32 * 1024 * 1024,
                }
            };
            (
                construct_backend(construction, resources.clone())
                    .await
                    .unwrap(),
                resources,
            )
        }

        // RocksDB and its snapshots are destroyed on the resource pool's
        // dedicated cleanup thread. The nextest test process must not exit
        // while those native destructors are still running.
        async fn drain_cleanup(resources: &WorkerStateResources) {
            let (done, completed) = tokio::sync::oneshot::channel();
            resources.cleanup().await.unwrap().submit(move || {
                let _ = done.send(());
            });
            completed.await.unwrap();
        }

        for source_rocks in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let storage = storage(&directory).await;
            let identity = br#"state-table-v1:["public","inventory"]"#.to_vec();
            let transport_name =
                arroyo_state_protocol::typed_checkpoint::transport_table_name(&identity).unwrap();
            let namespace = StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: identity.clone(),
            };
            let config = TypedStateTableConfig {
                transport_name: transport_name.clone(),
                table_identity: identity,
                schema_identity: b"typed-switch-schema-v1".to_vec(),
                schema_json: b"{}".to_vec(),
                primary_key: vec![0],
                partition_key: vec![0],
                encoding_version: 1,
            };
            let key = |number: u32| StateKey {
                namespace: namespace.clone(),
                key: number.to_be_bytes().to_vec(),
                routing_hash: None,
            };
            let (source, source_resources) =
                backend(source_rocks, &directory.path().join("source")).await;
            for number in 0..64 {
                source
                    .put(key(number), vec![number as u8; 32 * 1024], PAGE_BYTES)
                    .await
                    .unwrap();
            }
            let first = source.snapshot().await.unwrap();
            source.delete(key(1), PAGE_BYTES).await.unwrap();
            source
                .put(key(0), b"updated".to_vec(), PAGE_BYTES)
                .await
                .unwrap();
            let second = source.snapshot().await.unwrap();
            for number in 0..64 {
                source.delete(key(number), PAGE_BYTES).await.unwrap();
            }
            let empty = source.snapshot().await.unwrap();
            // Newer local mutations must not leak into any selected snapshot.
            source
                .put(key(0), b"after-barrier".to_vec(), PAGE_BYTES)
                .await
                .unwrap();

            for (epoch, snapshot) in [(1, first), (2, second), (3, empty)] {
                let path = format!(
                    "P/J/generations/7/checkpoints/checkpoint-{epoch:07}/operator-o/table-{transport_name}-000"
                );
                let metadata = export_typed(
                    &snapshot, &namespace, &config, &storage, &path, epoch, 7, 0, MAX_FILES,
                )
                .await
                .unwrap();
                if epoch != 3 {
                    assert!(metadata.files.len() > 1, "exercise paged full snapshots");
                }
                if epoch == 2 {
                    // An incomplete export to the same checkpoint prefix must
                    // not delete the already complete export's immutable files.
                    assert!(
                        export_typed(
                            &snapshot, &namespace, &config, &storage, &path, epoch, 7, 0, 1,
                        )
                        .await
                        .is_err()
                    );
                    for file in &metadata.files {
                        assert!(!storage.get(file.path.clone()).await.unwrap().is_empty());
                    }
                }
                for destination_rocks in [false, true] {
                    if epoch == 2 {
                        // Fail after at least one restored page. A partially
                        // populated attempt must be discarded before retry.
                        let missing = metadata.files.last().unwrap();
                        let saved = storage.get(missing.path.clone()).await.unwrap();
                        storage
                            .delete_if_present(missing.path.clone())
                            .await
                            .unwrap();
                        let (interrupted, interrupted_resources) = backend(
                            destination_rocks,
                            &directory
                                .path()
                                .join(format!("interrupted-{destination_rocks}")),
                        )
                        .await;
                        assert!(
                            restore_typed(
                                interrupted.as_ref(),
                                &namespace,
                                &config,
                                &metadata,
                                &storage,
                            )
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
                            .put(missing.path.clone(), saved.to_vec())
                            .await
                            .unwrap();
                        assert!(
                            restore_typed(
                                interrupted.as_ref(),
                                &namespace,
                                &config,
                                &metadata,
                                &storage,
                            )
                            .await
                            .is_err(),
                            "do not accept leftovers from a failed attempt"
                        );
                        drop(interrupted);
                        drain_cleanup(&interrupted_resources).await;
                    }
                    let (restored, restored_resources) = backend(
                        destination_rocks,
                        &directory
                            .path()
                            .join(format!("restored-{epoch}-{destination_rocks}")),
                    )
                    .await;
                    restore_typed(restored.as_ref(), &namespace, &config, &metadata, &storage)
                        .await
                        .unwrap();
                    for number in 0..64 {
                        let expected = match (epoch, number) {
                            (3, _) | (2, 1) => None,
                            (2, 0) => Some(b"updated".to_vec()),
                            _ => Some(vec![number as u8; 32 * 1024]),
                        };
                        assert_eq!(
                            restored
                                .get(
                                    &key(number),
                                    ReadOptions {
                                        max_bytes: PAGE_BYTES
                                    }
                                )
                                .await
                                .unwrap(),
                            expected,
                            "source_rocks={source_rocks} destination_rocks={destination_rocks} epoch={epoch} key={number}"
                        );
                    }
                    drop(restored);
                    drain_cleanup(&restored_resources).await;
                }
            }
            drop(source);
            drain_cleanup(&source_resources).await;
        }
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
