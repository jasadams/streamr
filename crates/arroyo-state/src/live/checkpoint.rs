//! Full logical snapshots. Each immutable object is exclusive to one checkpoint.
use super::{
    LiveStateBackend, ScanRange, ScanRequest, StateNamespace, StateSnapshot, WriteBatch,
    WriteOperation, encoding,
    resources::{CheckpointDirection, CheckpointObservation, WorkerStateResources},
};
use anyhow::{Result, bail, ensure};
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arroyo_rpc::grpc::rpc::{
    DiskCheckpointFile, DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
    TypedStateTableConfig, TypedStateTableSubtaskCheckpointMetadata,
};
use arroyo_storage::StorageProviderRef;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use futures::StreamExt;
use parquet::{
    arrow::{ArrowWriter, async_reader::ParquetRecordBatchStreamBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncReadExt;

pub const PAGE_BYTES: usize = 1024 * 1024;
const PAGE_ROWS: usize = 128;
pub(crate) const MAX_FILES: usize = 65536;
/// Leaves headroom for worker/job identity in tonic's default 4 MiB envelope.
pub(crate) const MAX_SUBTASK_CHECKPOINT_BYTES: usize = 3 * 1024 * 1024;
const MAGIC: &[u8] = b"STRDS001";
const PARQUET_FORMAT_VERSION: u32 = 2;
const PARQUET_TARGET_BYTES: usize = 512 * 1024;
// A single valid scan page can be nearly 1 MiB. The larger Parquet object
// ceiling allows that page plus writer footer without changing scan admission.
const PARQUET_MAX_BYTES: usize = 2 * PAGE_BYTES;
const PARQUET_MAX_ROWS: u64 = 4096;
const PARQUET_ROW_GROUP_ROWS: usize = 16;
const PARQUET_MAX_ROW_GROUPS: usize = 256;
const PARQUET_BUFFER_BYTES: usize = 64 * 1024;
const CHECKSUM_BUFFER_BYTES: usize = 64 * 1024;
const PARQUET_FOOTER_ALLOWANCE: usize = 256 * 1024;
static UPLOAD_ID: AtomicU64 = AtomicU64::new(0);

// Export and restore must admit the same bounded file and decoded pages. A
// per-database share is a target, not a ceiling: a valid page can require more
// than that share while still fitting the worker-wide decoded-value pool.
struct ParquetCheckpointBudget {
    page_bytes: usize,
    file_limit: usize,
    decoded_bytes: usize,
}

impl ParquetCheckpointBudget {
    fn new(resources: Option<&WorkerStateResources>) -> Result<Self> {
        let per_owner_writer_budget = resources.map_or(6 * PAGE_BYTES, |r| {
            (r.config().decoded_value_bytes / r.config().max_open_databases.saturating_add(1))
                .min(6 * PAGE_BYTES)
        });
        let page_bytes = resources.map_or(PAGE_BYTES, |r| {
            PAGE_BYTES
                .min(
                    r.config().scan_page_bytes
                        / (8 * r.config().max_open_databases.saturating_add(1)),
                )
                .min(r.config().queued_write_bytes / 8)
                .saturating_sub(32768)
        });
        ensure!(page_bytes > 0, "checkpoint resource budgets are too small");
        // Keep the scan-page limit independent of the writer's per-owner target:
        // a single value that fitted the former exporter must still fit a page.
        // If that page needs more than the target share, acquire only its actual
        // bounded allowance from the full decoded pool, fail-fast before scanning.
        let minimum_file_limit = page_bytes + PARQUET_FOOTER_ALLOWANCE + PARQUET_BUFFER_BYTES;
        let file_limit = PARQUET_MAX_BYTES.min(
            per_owner_writer_budget
                .saturating_sub(page_bytes * 3 + PARQUET_FOOTER_ALLOWANCE + PAGE_ROWS * 128)
                .max(minimum_file_limit),
        );
        ensure!(
            file_limit >= page_bytes + PARQUET_FOOTER_ALLOWANCE + PARQUET_BUFFER_BYTES,
            "checkpoint Parquet writer cannot fit configured decoded-value budget"
        );
        // A page can coexist as an Arrow batch, unencoded Parquet values and
        // encoded Parquet bytes while `write`/`flush` runs. Flush after each scan
        // page so those three page-sized copies never accumulate across pages.
        let writer_reservation =
            file_limit + page_bytes * 3 + PARQUET_FOOTER_ALLOWANCE + PAGE_ROWS * 128;
        ensure!(
            resources.is_none_or(|r| writer_reservation <= r.config().decoded_value_bytes),
            "checkpoint Parquet writer cannot fit configured decoded-value budget"
        );
        Ok(Self {
            page_bytes,
            file_limit,
            decoded_bytes: writer_reservation,
        })
    }
}

struct OpenParquetFile {
    path: String,
    writer: ArrowWriter<Vec<u8>>,
    rows: u64,
}

async fn checksum_file(storage: &StorageProviderRef, path: &str, size: u64) -> Result<Vec<u8>> {
    let input = storage.get_as_stream(path.to_owned()).await?;
    tokio::pin!(input);
    let mut hasher = Sha256::new();
    let mut remaining = size;
    let mut buffer = [0u8; CHECKSUM_BUFFER_BYTES];
    loop {
        let read = input.as_mut().read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        ensure!(
            (read as u64) <= remaining,
            "checkpoint object exceeds declared size"
        );
        remaining -= read as u64;
        hasher.update(&buffer[..read]);
    }
    ensure!(
        remaining == 0,
        "checkpoint object is shorter than declared size"
    );
    Ok(hasher.finalize().to_vec())
}

async fn finish_parquet_file(
    active: OpenParquetFile,
    storage: &StorageProviderRef,
    file_limit: usize,
) -> Result<DiskCheckpointFile> {
    ensure!(active.rows > 0, "empty checkpoint Parquet file");
    let mut writer = active.writer;
    writer.flush()?;
    let bytes = writer.into_inner()?;
    let size = bytes.len() as u64;
    ensure!(
        size > 0 && size <= file_limit as u64,
        "checkpoint Parquet file exceeds restore limit"
    );
    let checksum = Sha256::digest(&bytes).to_vec();
    storage
        .put_if_not_exists(active.path.clone(), bytes)
        .await?;
    Ok(DiskCheckpointFile {
        path: active.path,
        size_bytes: size,
        checksum,
        row_count: active.rows,
    })
}

fn record_parquet_file(
    metadata: &mut DiskKeyedTableSubtaskCheckpointMetadata,
    metadata_wire_bytes: &mut usize,
    file: DiskCheckpointFile,
    observation: &CheckpointObservation,
) -> Result<()> {
    let file_len = file.encoded_len();
    let mut prefix_len = 1usize;
    let mut remaining = file_len;
    while remaining >= 128 {
        prefix_len += 1;
        remaining >>= 7;
    }
    *metadata_wire_bytes = (*metadata_wire_bytes).saturating_add(1 + prefix_len + file_len);
    ensure!(
        *metadata_wire_bytes <= MAX_SUBTASK_CHECKPOINT_BYTES,
        "disk checkpoint file metadata exceeds 3 MiB RPC limit"
    );
    observation.page_transferred(file.size_bytes);
    metadata.files.push(file);
    metadata.empty = false;
    Ok(())
}

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
        format_version: PARQUET_FORMAT_VERSION,
        encoding_version: 1,
        schema_identity: schema_identity.to_vec(),
        namespace: encoding::encode_namespace(namespace)?,
        generation,
        epoch,
        empty: true,
        files: vec![],
    };
    // Track repeated-message wire bytes incrementally rather than rescanning
    // a growing full file list on every completed Parquet object.
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
    let budget = ParquetCheckpointBudget::new(resources.as_ref())?;
    let page_bytes = budget.page_bytes;
    let file_limit = budget.file_limit;
    let writer_reservation = budget.decoded_bytes;
    let file_target = PARQUET_TARGET_BYTES.min(file_limit / 2);
    // This permit is fail-fast. No exporter can hold a scan or queued-write
    // permit while waiting for another exporter to release its writer memory.
    let _writer_memory = resources
        .as_ref()
        .map(|r| r.try_decoded_value(writer_reservation))
        .transpose()?;
    let mut cursor = None;
    let mut active: Option<OpenParquetFile> = None;
    // Completed paths are already in bounded metadata. Keep only the current
    // path separately so failures can delete an unfinished object.
    let mut incomplete_path: Option<String> = None;
    let result: Result<()> = async {
        loop {
            let (batch, rows, next_cursor) = {
                let _scan_memory = if let Some(resources) = &resources {
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
                let rows = page.entries.len() as u64;
                let batch = if rows == 0 {
                    None
                } else {
                    let keys = page
                        .entries
                        .iter()
                        .map(|entry| encoding::encode_key(&entry.key))
                        .collect::<Result<Vec<_>, _>>()?;
                    let key_array = BinaryArray::from_iter_values(keys.iter().map(Vec::as_slice));
                    let value_array = BinaryArray::from_iter_values(
                        page.entries.iter().map(|entry| entry.value.as_slice()),
                    );
                    Some(RecordBatch::try_new(
                        crate::tables::global_keyed_map::GLOBAL_KEY_VALUE_SCHEMA.clone(),
                        vec![Arc::new(key_array) as ArrayRef, Arc::new(value_array)],
                    )?)
                };
                (batch, rows, page.next_cursor)
            };
            if let Some(batch) = batch {
                if active.as_ref().is_some_and(|file| {
                    file.rows + rows > PARQUET_MAX_ROWS
                        || file.writer.flushed_row_groups().len()
                            + batch.num_rows().div_ceil(PARQUET_ROW_GROUP_ROWS)
                            > PARQUET_MAX_ROW_GROUPS
                        || file.writer.bytes_written()
                            + file.writer.in_progress_size()
                            + batch.get_array_memory_size()
                            + PARQUET_FOOTER_ALLOWANCE
                            >= file_target
                }) {
                    let file =
                        finish_parquet_file(active.take().unwrap(), storage, file_limit).await?;
                    record_parquet_file(
                        &mut metadata,
                        &mut metadata_wire_bytes,
                        file,
                        observation,
                    )?;
                    incomplete_path = None;
                }
                if active.is_none() {
                    ensure!(
                        metadata.files.len() < max_files,
                        "disk checkpoint exceeds maximum Parquet file count"
                    );
                    let path =
                        format!("{path}/disk-{upload_id}-{:x}.parquet", metadata.files.len());
                    ensure!(
                        !storage.exists(path.clone()).await?,
                        "checkpoint Parquet destination already exists"
                    );
                    incomplete_path = Some(path.clone());
                    let properties = WriterProperties::builder()
                        .set_compression(Compression::ZSTD(ZstdLevel::default()))
                        .set_dictionary_enabled(false)
                        .set_statistics_enabled(EnabledStatistics::None)
                        .set_max_row_group_size(PARQUET_ROW_GROUP_ROWS)
                        .set_write_batch_size(PARQUET_ROW_GROUP_ROWS)
                        .set_data_page_size_limit(PARQUET_BUFFER_BYTES)
                        .build();
                    active = Some(OpenParquetFile {
                        path,
                        writer: ArrowWriter::try_new(
                            Vec::with_capacity(file_limit),
                            crate::tables::global_keyed_map::GLOBAL_KEY_VALUE_SCHEMA.clone(),
                            Some(properties),
                        )?,
                        rows: 0,
                    });
                }
                let writer = active.as_mut().expect("created for nonempty page");
                ensure!(
                    writer.writer.bytes_written()
                        + writer.writer.in_progress_size()
                        + batch.get_array_memory_size()
                        + PARQUET_FOOTER_ALLOWANCE
                        <= file_limit,
                    "checkpoint Parquet page exceeds bounded file allowance"
                );
                ensure!(
                    writer.writer.memory_size()
                        + writer.writer.inner().capacity()
                        + batch.get_array_memory_size()
                        + PARQUET_FOOTER_ALLOWANCE
                        <= writer_reservation,
                    "checkpoint Parquet writer exceeds admitted memory before page write"
                );
                // A scan page may contain fewer than sixteen large rows.
                // Carrying that partial row group into the next scan page
                // retained the previous values and exceeded the allowance.
                if writer.writer.in_progress_rows() > 0 {
                    writer.writer.flush()?;
                }
                writer.writer.write(&batch)?;
                writer.writer.flush()?;
                writer.rows += rows;
                ensure!(
                    writer.writer.memory_size()
                        + writer.writer.inner().capacity()
                        + PARQUET_FOOTER_ALLOWANCE
                        <= writer_reservation
                        && writer.writer.inner().capacity() <= file_limit,
                    "checkpoint Parquet writer exceeds admitted memory after page flush"
                );
                if writer.rows >= PARQUET_MAX_ROWS
                    || writer.writer.flushed_row_groups().len() >= PARQUET_MAX_ROW_GROUPS
                    || writer.writer.bytes_written() + writer.writer.in_progress_size()
                        >= file_target
                {
                    let file =
                        finish_parquet_file(active.take().unwrap(), storage, file_limit).await?;
                    record_parquet_file(
                        &mut metadata,
                        &mut metadata_wire_bytes,
                        file,
                        observation,
                    )?;
                    incomplete_path = None;
                }
            }
            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if let Some(writer) = active.take() {
            let file = finish_parquet_file(writer, storage, file_limit).await?;
            record_parquet_file(&mut metadata, &mut metadata_wire_bytes, file, observation)?;
            incomplete_path = None;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        drop(active);
        for file in &metadata.files {
            // Best effort only: crash leftovers are handled by fenced remote GC.
            let _ = storage.delete_if_present(file.path.clone()).await;
        }
        if let Some(path) = incomplete_path {
            let _ = storage.delete_if_present(path).await;
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
async fn restore_parquet_file(
    backend: &dyn LiveStateBackend,
    namespace: &StateNamespace,
    file: &DiskCheckpointFile,
    storage: &StorageProviderRef,
    resources: Option<&WorkerStateResources>,
    previous_key: &mut Option<Vec<u8>>,
    observation: &CheckpointObservation,
) -> Result<()> {
    ensure!(
        file.size_bytes > 0
            && file.size_bytes <= PARQUET_MAX_BYTES as u64
            && file.row_count > 0
            && file.row_count <= PARQUET_MAX_ROWS,
        "checkpoint Parquet file exceeds restore limit"
    );
    let object = storage.head(file.path.clone()).await?;
    ensure!(
        object.size as u64 == file.size_bytes,
        "checkpoint Parquet object size mismatch"
    );
    {
        // The checksum pass is complete before the decoded reader or backend
        // write permits are held, so it cannot invert their acquisition order.
        let _checksum_buffer = resources
            .map(|r| r.try_scan_page(CHECKSUM_BUFFER_BYTES))
            .transpose()?;
        ensure!(
            checksum_file(storage, &file.path, file.size_bytes).await? == file.checksum,
            "checkpoint Parquet checksum mismatch"
        );
    }

    // Reserve the same file/page headroom as the exporter before parsing the
    // footer. In particular, compressed file size alone cannot bound decoded
    // row groups, and a per-database target can be smaller than the footer.
    let decoded_budget = ParquetCheckpointBudget::new(resources)?.decoded_bytes;
    ensure!(
        decoded_budget >= PARQUET_BUFFER_BYTES + PAGE_ROWS * 128
            && file.size_bytes as usize + PARQUET_FOOTER_ALLOWANCE <= decoded_budget,
        "checkpoint Parquet reader cannot fit configured decoded-value budget"
    );
    let _decoded = resources
        .map(|r| r.try_decoded_value(decoded_budget))
        .transpose()?;
    // Footer/schema parsing is admitted too. Row groups are fetched lazily,
    // and the full-object checksum pass released its scan permit above.
    let reader = ParquetObjectReader::new(storage.get_backing_store(), object.location)
        .with_file_size(object.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    ensure!(
        builder.schema().as_ref()
            == crate::tables::global_keyed_map::GLOBAL_KEY_VALUE_SCHEMA.as_ref(),
        "checkpoint Parquet key/value schema mismatch"
    );
    ensure!(
        builder.metadata().file_metadata().num_rows() == file.row_count as i64,
        "checkpoint Parquet declared row count mismatch"
    );
    ensure!(
        builder.metadata().row_groups().len() <= PARQUET_MAX_ROW_GROUPS,
        "checkpoint Parquet has too many row groups"
    );
    for group in builder.metadata().row_groups() {
        ensure!(
            group.num_rows() > 0 && group.num_rows() <= PARQUET_ROW_GROUP_ROWS as i64,
            "checkpoint Parquet row group exceeds restore row limit"
        );
        ensure!(
            group.total_byte_size() >= 0
                && (group.total_byte_size() as u64)
                    <= (decoded_budget / 2).saturating_sub(PARQUET_BUFFER_BYTES) as u64,
            "checkpoint Parquet row group exceeds decoded-value budget"
        );
    }
    let mut stream = builder.with_batch_size(PARQUET_ROW_GROUP_ROWS).build()?;
    let mut restored_rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        ensure!(
            batch.num_rows() <= PARQUET_ROW_GROUP_ROWS
                && batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(PAGE_ROWS * 128)
                    <= decoded_budget,
            "checkpoint Parquet decoded batch exceeds restore budget"
        );
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("checkpoint Parquet key is not Binary"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("checkpoint Parquet value is not Binary"))?;
        ensure!(
            keys.null_count() == 0 && values.null_count() == 0,
            "checkpoint Parquet contains null key or value"
        );
        let mut operations = Vec::with_capacity(batch.num_rows());
        let mut encoded_bytes = 0usize;
        for index in 0..batch.num_rows() {
            let encoded_key = keys.value(index);
            if let Some(previous) = previous_key {
                ensure!(
                    previous.as_slice() < encoded_key,
                    "checkpoint Parquet keys out of order/duplicated"
                );
            }
            let key = encoding::decode_key(encoded_key)?;
            ensure!(
                key.namespace == *namespace,
                "checkpoint Parquet namespace mismatch"
            );
            *previous_key = Some(encoded_key.to_vec());
            let value = values.value(index).to_vec();
            let needed = encoding::encoded_key_size(&key)?
                .checked_add(encoding::encoded_value_size(&value)?)
                .ok_or_else(|| anyhow::anyhow!("checkpoint Parquet row size overflow"))?;
            ensure!(
                needed <= PAGE_BYTES + PAGE_ROWS,
                "checkpoint Parquet row exceeds restore write limit"
            );
            if !operations.is_empty() && encoded_bytes + needed > PAGE_BYTES + PAGE_ROWS {
                backend
                    .write_batch(WriteBatch {
                        operations: std::mem::take(&mut operations),
                        max_bytes: PAGE_BYTES + PAGE_ROWS,
                    })
                    .await?;
                encoded_bytes = 0;
            }
            encoded_bytes += needed;
            operations.push(WriteOperation::Put { key, value });
        }
        restored_rows += batch.num_rows() as u64;
        if !operations.is_empty() {
            backend
                .write_batch(WriteBatch {
                    operations,
                    max_bytes: PAGE_BYTES + PAGE_ROWS,
                })
                .await?;
        }
    }
    ensure!(
        restored_rows == file.row_count && restored_rows > 0,
        "checkpoint Parquet row count mismatch"
    );
    observation.page_transferred(file.size_bytes);
    Ok(())
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
        matches!(metadata.format_version, 1 | PARQUET_FORMAT_VERSION)
            && metadata.encoding_version == 1
            && encoding_version == 1,
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
        if metadata.format_version == PARQUET_FORMAT_VERSION {
            restore_parquet_file(
                backend,
                namespace,
                file,
                storage,
                resources.as_ref(),
                &mut previous_key,
                observation,
            )
            .await?;
            continue;
        }
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
    fn incompressible_payload(number: u32) -> Vec<u8> {
        let mut seed = number as u64 + 1;
        let mut value = vec![0; 8192];
        for byte in &mut value {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *byte = seed as u8;
        }
        value
    }
    fn window_sized_key(number: u32) -> StateKey {
        let mut state_key = key(number);
        state_key.key = [b"window-partial".as_slice(), &number.to_be_bytes()].concat();
        state_key
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

    fn resource_usage(registry: &prometheus::Registry, resource: &str, measurement: &str) -> usize {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "arroyo_live_state_resources")
            .unwrap()
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "resource" && label.value() == resource)
                    && metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "measurement" && label.value() == measurement)
            })
            .unwrap()
            .get_gauge()
            .as_ref()
            .unwrap()
            .value() as usize
    }

    #[tokio::test]
    async fn checkpoint_cancelled_admission_releases_buffers_and_fresh_retry_succeeds() {
        use crate::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};

        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 * PAGE_BYTES,
            memtable_bytes: 2 * PAGE_BYTES,
            queued_write_bytes: 16 * PAGE_BYTES,
            decoded_value_bytes: 16 * PAGE_BYTES,
            scan_page_bytes: 16 * PAGE_BYTES,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 2,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let registry = prometheus::Registry::new();
        resources.register_metrics(&registry).unwrap();
        let usage =
            |resource: &str, measurement: &str| resource_usage(&registry, resource, measurement);
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        source
            .put(key(1), b"checkpoint-owner".to_vec(), 1024)
            .await
            .unwrap();
        let snapshot = source.snapshot().await.unwrap();
        let namespace = namespace();
        let config = config();
        let export_observation = CheckpointObservation::new(None, CheckpointDirection::Export);
        let path = "J/checkpoints/checkpoint-0000001/operator-o/table-map-000";
        let held_scan = resources
            .try_scan_page(resources.config().scan_page_bytes)
            .unwrap();
        {
            let mut export = Box::pin(export_snapshot_inner(
                &snapshot,
                &namespace,
                &config.table_name,
                config.table_name.as_bytes(),
                &config.schema_identity,
                1,
                &storage,
                path,
                1,
                0,
                0,
                MAX_FILES,
                Some(resources.clone()),
                &export_observation,
            ));
            assert!(futures::poll!(&mut export).is_pending());
            assert_eq!(
                usage("decoded_value_bytes", "used"),
                ParquetCheckpointBudget::new(Some(&resources))
                    .unwrap()
                    .decoded_bytes
            );
            assert_eq!(usage("scan_page_bytes", "waiting"), 1);
            assert_eq!(usage("queued_write_bytes", "used"), 0);
        }
        assert_eq!(usage("decoded_value_bytes", "used"), 0);
        assert_eq!(usage("scan_page_bytes", "waiting"), 0);
        assert_eq!(
            usage("scan_page_bytes", "used"),
            resources.config().scan_page_bytes
        );
        drop(held_scan);
        assert_eq!(usage("scan_page_bytes", "used"), 0);
        let metadata = export_snapshot_inner(
            &snapshot,
            &namespace,
            &config.table_name,
            config.table_name.as_bytes(),
            &config.schema_identity,
            1,
            &storage,
            path,
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &export_observation,
        )
        .await
        .unwrap();
        for resource in [
            "decoded_value_bytes",
            "scan_page_bytes",
            "queued_write_bytes",
        ] {
            assert_eq!(usage(resource, "used"), 0);
        }
        let state_config = RocksStateConfig {
            root: directory.path().join("live"),
            job_id: "cancel-retry".into(),
            operator_id: "owner".into(),
            subtask: 0,
            generation: 0,
            attempt: 1,
        };
        let destination = RocksLiveState::open(state_config.clone(), resources.clone())
            .await
            .unwrap();
        let restore_observation = CheckpointObservation::new(None, CheckpointDirection::Restore);
        let held_write = resources
            .try_queued_write(resources.config().queued_write_bytes)
            .unwrap();
        {
            let mut restore = Box::pin(restore_snapshot_inner(
                &destination,
                &namespace,
                1,
                &config.schema_identity,
                &metadata,
                &storage,
                Some(resources.clone()),
                &restore_observation,
            ));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut restore => panic!("restore unexpectedly completed: {result:?}"),
                    _ = async {
                        while usage("queued_write_bytes", "waiting") == 0 {
                            tokio::task::yield_now().await;
                        }
                    } => {}
                }
            })
            .await
            .unwrap();
            assert_eq!(usage("queued_write_bytes", "waiting"), 1);
            assert_eq!(
                usage("decoded_value_bytes", "used"),
                ParquetCheckpointBudget::new(Some(&resources))
                    .unwrap()
                    .decoded_bytes
            );
            assert_eq!(usage("scan_page_bytes", "used"), 0);
        }
        assert_eq!(usage("queued_write_bytes", "waiting"), 0);
        assert_eq!(usage("decoded_value_bytes", "used"), 0);
        assert_eq!(usage("scan_page_bytes", "used"), 0);
        assert_eq!(
            usage("queued_write_bytes", "used"),
            resources.config().queued_write_bytes
        );
        drop(held_write);
        destination.close_and_remove().await.unwrap();
        let destination = RocksLiveState::open(
            RocksStateConfig {
                attempt: 2,
                ..state_config
            },
            resources.clone(),
        )
        .await
        .unwrap();
        restore_snapshot_inner(
            &destination,
            &namespace,
            1,
            &config.schema_identity,
            &metadata,
            &storage,
            Some(resources.clone()),
            &restore_observation,
        )
        .await
        .unwrap();
        assert_eq!(
            destination
                .get(&key(1), ReadOptions { max_bytes: 1024 })
                .await
                .unwrap(),
            Some(b"checkpoint-owner".to_vec())
        );
        for resource in [
            "decoded_value_bytes",
            "scan_page_bytes",
            "queued_write_bytes",
        ] {
            assert_eq!(usage(resource, "used"), 0);
            assert_eq!(usage(resource, "waiting"), 0);
        }
        destination.close_and_remove().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_binary_checkpoint_remains_readable() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let path = "J/checkpoints/checkpoint-0000001/operator-o/table-map-000/disk-legacy.bin";
        let mut bytes = MAGIC.to_vec();
        for (number, value) in [(1, b"first".as_slice()), (2, b"second".as_slice())] {
            let encoded = encoding::encode_key(&key(number)).unwrap();
            bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&encoded);
            bytes.extend_from_slice(value);
        }
        let metadata = DiskKeyedTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            format_version: 1,
            encoding_version: 1,
            schema_identity: config().schema_identity,
            namespace: encoding::encode_namespace(&namespace()).unwrap(),
            generation: 0,
            epoch: 1,
            empty: false,
            files: vec![DiskCheckpointFile {
                path: path.into(),
                size_bytes: bytes.len() as u64,
                checksum: Sha256::digest(&bytes).to_vec(),
                row_count: 2,
            }],
        };
        arroyo_state_protocol::disk::validate_subtask(&config(), &metadata).unwrap();
        storage.put(path, bytes).await.unwrap();
        let restored = MemoryLiveState::new();
        restore(&restored, &namespace(), &config(), &metadata, &storage)
            .await
            .unwrap();
        for (number, value) in [(1, b"first".as_slice()), (2, b"second".as_slice())] {
            assert_eq!(
                restored
                    .get(&key(number), ReadOptions { max_bytes: 32 })
                    .await
                    .unwrap(),
                Some(value.to_vec())
            );
        }
    }

    #[tokio::test]
    async fn parquet_export_streams_incompressible_pages_and_restores_all_rows() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 512 * 1024,
            queued_write_bytes: 16 * 1024 * 1024,
            decoded_value_bytes: 16 * 1024 * 1024,
            scan_page_bytes: 2 * 1024 * 1024,
            max_blocking_operations: 1,
            max_snapshots: 1,
            max_open_databases: 2,
            disk_reserve_bytes: 1,
        })
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        let mut expected = Vec::new();
        for number in 0..300 {
            let value = incompressible_payload(number);
            source
                .put(key(number), value.clone(), PAGE_BYTES)
                .await
                .unwrap();
            expected.push(value);
        }
        let observation = CheckpointObservation::new(None, CheckpointDirection::Export);
        let metadata = export_snapshot_inner(
            &source.snapshot().await.unwrap(),
            &namespace(),
            &config().table_name,
            config().table_name.as_bytes(),
            &config().schema_identity,
            1,
            &storage,
            "J/checkpoints/checkpoint-0000001/operator-o/table-map-000",
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &observation,
        )
        .await
        .unwrap();
        assert_eq!(metadata.format_version, PARQUET_FORMAT_VERSION);
        assert!(metadata.files.len() >= 3);
        assert_eq!(metadata.files.iter().map(|f| f.row_count).sum::<u64>(), 300);
        assert!(metadata.files.iter().all(|f| {
            f.size_bytes > 0
                && f.size_bytes <= PARQUET_MAX_BYTES as u64
                && f.path.ends_with(".parquet")
        }));
        arroyo_state_protocol::disk::validate_subtask(&config(), &metadata).unwrap();
        let restored = MemoryLiveState::new();
        let restore_observation = CheckpointObservation::new(None, CheckpointDirection::Restore);
        restore_snapshot_inner(
            &restored,
            &namespace(),
            1,
            &config().schema_identity,
            &metadata,
            &storage,
            Some(resources),
            &restore_observation,
        )
        .await
        .unwrap();
        for (number, value) in expected.into_iter().enumerate() {
            assert_eq!(
                restored
                    .get(&key(number as u32), ReadOptions { max_bytes: 8192 })
                    .await
                    .unwrap(),
                Some(value)
            );
        }
    }

    #[tokio::test]
    async fn rocks_checkpoint_restores_with_decoded_share_smaller_than_footer() {
        use crate::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};

        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 * 1024 * 1024,
            memtable_bytes: 2 * 1024 * 1024,
            queued_write_bytes: 8 * 1024 * 1024,
            decoded_value_bytes: 4 * 1024 * 1024,
            scan_page_bytes: 8 * 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 16,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        assert!(
            resources.config().decoded_value_bytes / (resources.config().max_open_databases + 1)
                < PARQUET_FOOTER_ALLOWANCE
        );
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let state_config = RocksStateConfig {
            root: directory.path().join("live"),
            job_id: "checkpoint-admission".into(),
            operator_id: "state-owner".into(),
            subtask: 0,
            generation: 0,
            attempt: 1,
        };
        let source = RocksLiveState::open(state_config.clone(), resources.clone())
            .await
            .unwrap();
        let mut expected = Vec::new();
        for number in 0..160 {
            // Exercise both compressed and incompressible row groups, with
            // each row fitting the reported 1 KiB caller write limit.
            let value = if number % 2 == 0 {
                vec![number as u8; 896]
            } else {
                incompressible_payload(number)[..896].to_vec()
            };
            source.put(key(number), value.clone(), 1024).await.unwrap();
            expected.push(value);
        }
        let snapshot = source.snapshot().await.unwrap();
        let observation = CheckpointObservation::new(None, CheckpointDirection::Export);
        let metadata = export_snapshot_inner(
            &snapshot,
            &namespace(),
            &config().table_name,
            config().table_name.as_bytes(),
            &config().schema_identity,
            1,
            &storage,
            "J/checkpoints/checkpoint-0000001/operator-o/table-map-000",
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &observation,
        )
        .await
        .unwrap();
        assert!(metadata.files.len() > 1);
        assert_eq!(
            metadata
                .files
                .iter()
                .map(|file| file.row_count)
                .sum::<u64>(),
            160
        );
        arroyo_state_protocol::disk::validate_subtask(&config(), &metadata).unwrap();
        drop(snapshot);
        source.close_and_remove().await.unwrap();

        let destination = RocksLiveState::open(
            RocksStateConfig {
                attempt: 2,
                ..state_config.clone()
            },
            resources.clone(),
        )
        .await
        .unwrap();
        let observation = CheckpointObservation::new(None, CheckpointDirection::Restore);
        // Admission remains worker-wide and fail-fast. Recovery retries use a
        // fresh attempt after releasing contention, with unchanged budgets.
        let held = resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .unwrap();
        let error = restore_snapshot_inner(
            &destination,
            &namespace(),
            1,
            &config().schema_identity,
            &metadata,
            &storage,
            Some(resources.clone()),
            &observation,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decoded_value_bytes budget exhausted")
        );
        drop(held);
        destination.close_and_remove().await.unwrap();
        let destination = RocksLiveState::open(
            RocksStateConfig {
                attempt: 3,
                ..state_config
            },
            resources.clone(),
        )
        .await
        .unwrap();
        restore_snapshot_inner(
            &destination,
            &namespace(),
            1,
            &config().schema_identity,
            &metadata,
            &storage,
            Some(resources.clone()),
            &observation,
        )
        .await
        .unwrap();
        for (number, value) in expected.into_iter().enumerate() {
            assert_eq!(
                destination
                    .get(&key(number as u32), ReadOptions { max_bytes: 1024 })
                    .await
                    .unwrap(),
                Some(value)
            );
        }
        destination.close_and_remove().await.unwrap();
        // All decoded permits, including failed admission, were released.
        let _all_decoded = resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .unwrap();
    }

    #[tokio::test]
    async fn compact_rows_share_files_across_scan_pages() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        for number in 0..512 {
            source
                .put(key(number), number.to_be_bytes().to_vec(), PAGE_BYTES)
                .await
                .unwrap();
        }
        let metadata = export(
            &source.snapshot().await.unwrap(),
            &namespace(),
            &config(),
            &storage,
            "checkpoint/compact-rows",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.files[0].row_count, 512);
        let restored = MemoryLiveState::new();
        restore(&restored, &namespace(), &config(), &metadata, &storage)
            .await
            .unwrap();
        for number in 0..512 {
            assert_eq!(
                restored
                    .get(&key(number), ReadOptions { max_bytes: 4 })
                    .await
                    .unwrap(),
                Some(number.to_be_bytes().to_vec())
            );
        }
    }

    #[tokio::test]
    async fn two_exporters_preserve_admitted_large_rows_with_tight_decoded_pool() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 512 * 1024,
            queued_write_bytes: 8 * 1024 * 1024,
            decoded_value_bytes: 4 * 1024 * 1024,
            scan_page_bytes: 8 * 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 2,
            disk_reserve_bytes: 1,
        })
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        let mut seed = 1u64;
        let mut value = vec![0; 300_000];
        for byte in &mut value {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *byte = seed as u8;
        }
        source.put(key(1), value.clone(), PAGE_BYTES).await.unwrap();
        let snapshot = source.snapshot().await.unwrap();
        let namespace = namespace();
        let config = config();
        let first = CheckpointObservation::new(None, CheckpointDirection::Export);
        let second = CheckpointObservation::new(None, CheckpointDirection::Export);
        let export_one = export_snapshot_inner(
            &snapshot,
            &namespace,
            &config.table_name,
            config.table_name.as_bytes(),
            &config.schema_identity,
            1,
            &storage,
            "checkpoint/large-one",
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &first,
        );
        let export_two = export_snapshot_inner(
            &snapshot,
            &namespace,
            &config.table_name,
            config.table_name.as_bytes(),
            &config.schema_identity,
            1,
            &storage,
            "checkpoint/large-two",
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &second,
        );
        let (one, two) = tokio::join!(export_one, export_two);
        let one = one.unwrap();
        let two = two.unwrap();
        assert_eq!(one.files[0].row_count, 1);
        assert_eq!(two.files[0].row_count, 1);
        let restored = MemoryLiveState::new();
        let restore_observation = CheckpointObservation::new(None, CheckpointDirection::Restore);
        restore_snapshot_inner(
            &restored,
            &namespace,
            1,
            &config.schema_identity,
            &one,
            &storage,
            Some(resources),
            &restore_observation,
        )
        .await
        .unwrap();
        assert_eq!(
            restored
                .get(&key(1), ReadOptions { max_bytes: 300_000 })
                .await
                .unwrap(),
            Some(value)
        );
    }

    #[tokio::test]
    async fn window_sized_values_cross_scan_pages_without_retained_row_groups() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 * 1024 * 1024,
            memtable_bytes: 4 * 1024 * 1024,
            queued_write_bytes: 4 * 1024 * 1024,
            decoded_value_bytes: 16 * 1024 * 1024,
            scan_page_bytes: 2 * 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 2,
            disk_reserve_bytes: 1,
        })
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        for number in 0..16 {
            source
                .put(
                    window_sized_key(number),
                    incompressible_payload(number),
                    PAGE_BYTES,
                )
                .await
                .unwrap();
        }
        let snapshot = source.snapshot().await.unwrap();
        let observation = CheckpointObservation::new(None, CheckpointDirection::Export);
        let config = config();
        let namespace = namespace();
        let metadata = export_snapshot_inner(
            &snapshot,
            &namespace,
            &config.table_name,
            config.table_name.as_bytes(),
            &config.schema_identity,
            1,
            &storage,
            "checkpoint/window-values",
            1,
            0,
            0,
            MAX_FILES,
            Some(resources.clone()),
            &observation,
        )
        .await
        .unwrap();
        assert_eq!(
            metadata
                .files
                .iter()
                .map(|file| file.row_count)
                .sum::<u64>(),
            16
        );
        let restored = MemoryLiveState::new();
        let restore_observation = CheckpointObservation::new(None, CheckpointDirection::Restore);
        restore_snapshot_inner(
            &restored,
            &namespace,
            1,
            &config.schema_identity,
            &metadata,
            &storage,
            Some(resources),
            &restore_observation,
        )
        .await
        .unwrap();
        for number in 0..16 {
            assert_eq!(
                restored
                    .get(&window_sized_key(number), ReadOptions { max_bytes: 8192 })
                    .await
                    .unwrap(),
                Some(incompressible_payload(number))
            );
        }
    }

    #[tokio::test]
    async fn file_limit_failure_cleans_completed_immutable_objects() {
        let directory = tempfile::tempdir().unwrap();
        let storage = storage(&directory).await;
        let source = MemoryLiveState::new();
        for number in 0..300 {
            source
                .put(key(number), incompressible_payload(number), PAGE_BYTES)
                .await
                .unwrap();
        }
        let result = export_with_file_limit(
            &source.snapshot().await.unwrap(),
            &namespace(),
            &config(),
            &storage,
            "checkpoint/limit-cleanup",
            1,
            0,
            0,
            1,
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("maximum Parquet file count")
        );
        let objects = storage.list(true).await.unwrap().collect::<Vec<_>>().await;
        assert!(
            objects.is_empty(),
            "failed export left immutable objects: {objects:?}"
        );
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
            .strip_suffix(".parquet")
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
                    assert!(!metadata.files.is_empty(), "exercise full snapshots");
                }
                if epoch == 2 {
                    // An incomplete export to the same checkpoint prefix must
                    // not delete the already complete export's immutable files.
                    assert!(
                        export_typed(
                            &snapshot, &namespace, &config, &storage, &path, epoch, 7, 0, 0,
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
        assert!(!second.files.is_empty());
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
        let mut valid_checksum_but_not_parquet = metadata.clone();
        valid_checksum_but_not_parquet.files[0].checksum =
            Sha256::digest(vec![0; file.size_bytes as usize]).to_vec();
        let fresh = MemoryLiveState::new();
        assert!(
            restore(
                &fresh,
                &namespace(),
                &config(),
                &valid_checksum_but_not_parquet,
                &storage
            )
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
                .put(key(i), incompressible_payload(i), PAGE_BYTES)
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
                Some(incompressible_payload(i))
            );
        }
    }
}
