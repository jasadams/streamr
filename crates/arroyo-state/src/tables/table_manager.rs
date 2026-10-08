// Owned live handles are separate from the legacy Parquet checkpoint manager.
pub use crate::live::table::{LiveTable, LiveTableManager};

use crate::live::{LiveStateBackend, Ownership, StateNamespace, StateSnapshot};
use arroyo_rpc::grpc::rpc::{DiskKeyedTableConfig, DiskKeyedTableTaskCheckpointMetadata};
use prost::Message;
use std::any::Any;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

struct RegisteredLiveTable {
    backend: Arc<dyn LiveStateBackend>,
    namespace: StateNamespace,
    config: DiskKeyedTableConfig,
}
struct CapturedLiveTables {
    tables: Vec<(String, StateSnapshot, StateNamespace, DiskKeyedTableConfig)>,
    _permit: OwnedSemaphorePermit,
}
type LiveCaptures = Arc<Mutex<HashMap<u32, CapturedLiveTables>>>;

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use crate::StorageProviderFor;
use anyhow::{Result, anyhow, bail};
use arroyo_rpc::CompactionResult;
use arroyo_rpc::{
    CheckpointCompleted, ControlResp,
    grpc::rpc::{
        SubtaskCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    },
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{CheckpointBarrier, Data, Key, TaskInfo, from_micros, to_micros};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

use super::expiring_time_key_map::{
    ExpiringTimeKeyTable, ExpiringTimeKeyView, KeyTimeView, UncachedKeyValueView,
};
use super::global_keyed_map::GlobalKeyedView;
use super::{ErasedCheckpointer, ErasedTable, MigratableState};
use crate::{
    BackingStore, StateBackend, StateMessage, get_storage_provider,
    tables::global_keyed_map::GlobalKeyedTable,
};
use crate::{CheckpointMessage, TableData};
use arroyo_rpc::MetadataOrManifest;
use arroyo_rpc::errors::{DataflowResult, StateError};
use arroyo_rpc::grpc::rpc::OperatorCheckpointMetadata;
use tracing::{debug, error, info, warn};

#[allow(unused)]
pub struct TableManager {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    writer: BackendWriter,
    task_info: Arc<TaskInfo>,
    storage: StorageProviderRef,
    caches: HashMap<String, Box<dyn Any + Send>>,
    control_tx: Sender<ControlResp>,
    checkpoint_failed: bool,
    restoring: bool,
    restore_layout: Option<arroyo_types::CheckpointFilePathLayout>,
    live_configs: HashMap<String, TableConfig>,
    live_restore: HashMap<String, arroyo_rpc::grpc::rpc::TableCheckpointMetadata>,
    live_tables: HashMap<String, RegisteredLiveTable>,
    live_captures: LiveCaptures,
    live_capture_limit: Arc<Semaphore>,
}

pub struct BackendWriter {
    sender: Sender<StateMessage>,
    finish_rx: Option<oneshot::Receiver<()>>,
    flusher: tokio::task::AbortHandle,
    // TODO: compaction
}

#[allow(unused)]
pub struct BackendFlusher {
    queue: Receiver<StateMessage>,
    storage: StorageProviderRef,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: Arc<TaskInfo>,
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    table_configs: HashMap<String, TableConfig>,
    table_checkpointers: HashMap<String, Box<dyn ErasedCheckpointer>>,
    current_epoch: u32,
    last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
    live_captures: LiveCaptures,
}

impl BackendFlusher {
    fn start(mut self) -> tokio::task::AbortHandle {
        tokio::spawn(async move {
            loop {
                match self.flush_iteration().await {
                    Ok(continue_flushing) => {
                        if !continue_flushing {
                            return;
                        }
                    }
                    Err(err) => {
                        error!("Failed to flush state file: {:?}", err);
                        self.control_tx
                            .send(ControlResp::TaskFailed {
                                task_id: self.task_info.operator_idx,
                                subtask_idx: self.task_info.task_index,
                                error: err.with_operator(self.task_info.operator_id.clone()).into(),
                            })
                            .await
                            .expect("control queue closed");
                        return;
                    }
                }
            }
        })
        .abort_handle()
    }

    async fn flush_iteration(&mut self) -> DataflowResult<bool> {
        let mut checkpoint_epoch = None;

        for (table_name, checkpointer) in &self.tables {
            let epoch_checkpointer = checkpointer.epoch_checkpointer(
                self.current_epoch,
                self.last_epoch_checkpoints.remove(table_name),
            )?;
            self.table_checkpointers
                .insert(table_name.clone(), epoch_checkpointer);
        }
        self.last_epoch_checkpoints.clear();
        let mut compacted_tables = None;

        // accumulate writes in the RecordBatchBuilders until we get a checkpoint
        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(StateMessage::Checkpoint(checkpoint)) => {
                            checkpoint_epoch = Some(checkpoint);
                        }
                        Some(StateMessage::Compaction(compacted_tables_message)) => {
                            compacted_tables = Some(compacted_tables_message);
                        }
                        Some(StateMessage::TableData { table, data }) => {
                            self.table_checkpointers
                                .get_mut(&table).expect("checkpointer should be there")
                                .insert_data(data).await?
                        },
                        None => {
                            debug!("Parquet flusher closed");
                            return Ok(false);
                        }
                    }
                }
            }
        }
        let Some(cp) = checkpoint_epoch else {
            unreachable!("somehow exited loop without checkpoint_epoch being set");
        };

        let mut metadatas = HashMap::new();
        let has_disk = self
            .table_configs
            .values()
            .any(|config| config.table_type() == TableEnum::DiskKeyedMap);
        if has_disk {
            validate_disk_tables_wire_budget(&self.table_configs, &metadatas)?;
        }
        let mut bytes = 0;
        for (table_name, checkpointer) in self.table_checkpointers.drain() {
            if let Some((subtask_checkpoint_data, size)) = checkpointer.finish(&cp).await? {
                metadatas.insert(table_name.clone(), subtask_checkpoint_data);
                if has_disk {
                    validate_disk_tables_wire_budget(&self.table_configs, &metadatas)?;
                }
                bytes += size;
            }
        }

        if let Some(captured) = self.live_captures.lock().await.remove(&cp.epoch) {
            for (name, snapshot, namespace, config) in captured.tables {
                let path = super::table_checkpoint_path(
                    &self.task_info,
                    &self.task_info.operator_id,
                    &name,
                    self.task_info.task_index as usize,
                    cp.epoch,
                    false,
                );
                let generation = match &self.task_info.checkpoint_file_path_layout {
                    arroyo_types::CheckpointFilePathLayout::Legacy => 0,
                    arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => {
                        *generation
                    }
                };
                let metadata = crate::live::checkpoint::export_with_file_limit(
                    &snapshot,
                    &namespace,
                    &config,
                    &self.storage,
                    &path,
                    cp.epoch,
                    generation,
                    self.task_info.task_index,
                    crate::live::checkpoint::MAX_FILES
                        / self
                            .table_configs
                            .values()
                            .filter(|config| config.table_type() == TableEnum::DiskKeyedMap)
                            .count()
                            .max(1),
                )
                .await
                .map_err(|error| StateError::Other {
                    table: name.clone(),
                    error: error.to_string(),
                })?;
                bytes += metadata
                    .files
                    .iter()
                    .map(|f| f.size_bytes as usize)
                    .sum::<usize>();
                metadatas.insert(
                    name,
                    TableSubtaskCheckpointMetadata {
                        subtask_index: self.task_info.task_index,
                        table_type: TableEnum::DiskKeyedMap as i32,
                        data: metadata.encode_to_vec(),
                    },
                );
                if has_disk {
                    validate_disk_tables_wire_budget(&self.table_configs, &metadatas)?;
                }
            }
        }

        if let Some(compaction_metas) = compacted_tables {
            for (table_name, compacted_metadata) in compaction_metas {
                let table = self.tables.get(&table_name).unwrap();
                let Some(compacted_metadata) =
                    table.subtask_metadata_from_table(compacted_metadata)?
                else {
                    continue;
                };
                if let Some(current_metadata) = metadatas.get(&table_name) {
                    let new_metadata = table.apply_compacted_checkpoint(
                        self.current_epoch,
                        compacted_metadata,
                        current_metadata.clone(),
                    )?;
                    metadatas.insert(table_name, new_metadata);
                } else {
                    warn!(
                        "received compaction map for operator {} table {} but no metadata. no checkpoint emitted, as we trust the subtask. map is {:?}",
                        self.task_info.operator_id, table_name, compacted_metadata
                    );
                }
            }
        }
        self.last_epoch_checkpoints = metadatas.clone();
        self.current_epoch += 1;

        // send controller the subtask metadata
        let subtask_metadata = SubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index,
            start_time: to_micros(cp.time),
            finish_time: to_micros(SystemTime::now()),
            watermark: cp.watermark.map(to_micros),
            table_metadata: metadatas,
            table_configs: self.table_configs.clone(),
            bytes: bytes as u64,
        };
        if self
            .table_configs
            .values()
            .any(|config| config.table_type() == TableEnum::DiskKeyedMap)
        {
            validate_disk_subtask_metadata_size(&subtask_metadata)?;
        }
        self.control_tx
            .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                checkpoint_epoch: cp.epoch as u64,
                operator_idx: self.task_info.operator_idx,
                operator_id: self.task_info.operator_id.clone(),
                subtask_metadata,
            }))
            .await
            .expect("control queue closed");
        if cp.then_stop {
            self.finish_tx
                .take()
                .unwrap()
                .send(())
                .map_err(|_| anyhow::anyhow!("can't send finish"))?;
            return Ok(false);
        }
        Ok(true)
    }
}

impl BackendWriter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_info: Arc<TaskInfo>,
        control_tx: Sender<ControlResp>,
        table_configs: HashMap<String, TableConfig>,
        tables: HashMap<String, Arc<dyn ErasedTable>>,
        storage: StorageProviderRef,
        current_epoch: u32,
        last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
        live_captures: LiveCaptures,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        let flusher = (BackendFlusher {
            queue: rx,
            storage,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info,
            tables,
            table_configs,
            current_epoch,
            table_checkpointers: HashMap::new(),
            last_epoch_checkpoints,
            live_captures,
        })
        .start();

        Self {
            sender: tx,
            finish_rx: Some(finish_rx),
            flusher,
        }
    }
}

impl Drop for BackendWriter {
    fn drop(&mut self) {
        // Operator cancellation also cancels its exporter. Otherwise a detached
        // flusher could publish a checkpoint and retain snapshots after worker loss.
        self.flusher.abort();
    }
}

fn protobuf_blob_wire_bytes(bytes: usize) -> usize {
    let mut prefix = 1usize;
    let mut remaining = bytes;
    while remaining >= 128 {
        prefix += 1;
        remaining >>= 7;
    }
    1 + prefix + bytes
}

fn protobuf_map_wire_bytes<M: Message>(entries: &HashMap<String, M>) -> usize {
    entries
        .iter()
        .map(|(key, value)| {
            protobuf_blob_wire_bytes(
                protobuf_blob_wire_bytes(key.len()) + protobuf_blob_wire_bytes(value.encoded_len()),
            )
        })
        .sum()
}

fn validate_disk_tables_wire_budget(
    configs: &HashMap<String, TableConfig>,
    tables: &HashMap<String, TableSubtaskCheckpointMetadata>,
) -> Result<(), StateError> {
    if !configs
        .values()
        .any(|config| config.table_type() == TableEnum::DiskKeyedMap)
    {
        return Ok(());
    }
    // Use worst-case timestamp/count varints before final metadata construction;
    // this bounds assembly across maps as well as the final serialized message.
    let envelope = SubtaskCheckpointMetadata {
        subtask_index: 0,
        start_time: u64::MAX,
        finish_time: u64::MAX,
        watermark: Some(u64::MAX),
        bytes: u64::MAX,
        table_configs: HashMap::new(),
        table_metadata: HashMap::new(),
    }
    .encoded_len();
    let bytes = envelope + protobuf_map_wire_bytes(configs) + protobuf_map_wire_bytes(tables);
    if bytes > crate::live::checkpoint::MAX_SUBTASK_CHECKPOINT_BYTES {
        return Err(StateError::Other {
            table: "disk SQL checkpoint".into(),
            error: format!(
                "serialized subtask checkpoint metadata exceeds 3 MiB RPC limit during table assembly: {bytes} bytes"
            ),
        });
    }
    Ok(())
}

fn validate_disk_subtask_metadata_size(
    metadata: &SubtaskCheckpointMetadata,
) -> Result<(), StateError> {
    if !metadata
        .table_configs
        .values()
        .any(|config| config.table_type() == TableEnum::DiskKeyedMap)
    {
        return Ok(());
    }
    let bytes = metadata.encoded_len();
    if bytes > crate::live::checkpoint::MAX_SUBTASK_CHECKPOINT_BYTES {
        return Err(StateError::Other {
            table: "disk SQL checkpoint".into(),
            error: format!(
                "serialized subtask checkpoint metadata exceeds 3 MiB RPC limit: {bytes} bytes (includes every table, config and protobuf envelope)"
            ),
        });
    }
    Ok(())
}

fn validate_selected_disk_checkpoint(
    config: &DiskKeyedTableConfig,
    metadata: &DiskKeyedTableTaskCheckpointMetadata,
    task: &TaskInfo,
    layout: &arroyo_types::CheckpointFilePathLayout,
    epoch: u32,
) -> Result<()> {
    arroyo_state_protocol::disk::validate_table(config, metadata)
        .map_err(|error| anyhow!(error))?;
    let generation = match layout {
        arroyo_types::CheckpointFilePathLayout::Legacy => 0,
        arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => *generation,
    };
    for subtask in metadata.subtasks.values() {
        if subtask.epoch != epoch
            || subtask.generation != generation
            || subtask.subtask_index != task.task_index
        {
            bail!("disk checkpoint differs from selected epoch/generation/owner");
        }
        let prefix = format!(
            "{}/disk-",
            layout.table_checkpoint_path(
                &task.job_id,
                &task.operator_id,
                &config.table_name,
                task.task_index as usize,
                epoch,
                false
            )
        );
        for file in &subtask.files {
            let suffix = file.path.strip_prefix(&prefix).ok_or_else(|| {
                anyhow!(
                    "disk checkpoint file belongs to another job/operator/table/epoch/generation"
                )
            })?;
            if suffix.is_empty() || suffix.contains(['/', '\\']) || !suffix.ends_with(".bin") {
                bail!("disk checkpoint file is not an exclusive logical page");
            }
        }
    }
    Ok(())
}

async fn load_operator_metadata(
    m: &MetadataOrManifest,
    operator_id: &str,
) -> Result<OperatorCheckpointMetadata, anyhow::Error> {
    match m {
        MetadataOrManifest::Metadata(m) => StateBackend::load_operator_metadata(
            &StorageProviderFor::Worker,
            &m.job_id,
            operator_id,
            m.epoch,
        )
        .await?
        .ok_or_else(|| anyhow!("missing metadata field in checkpoint; invalid protobuf")),
        MetadataOrManifest::Manifest(manifest) => manifest
            .operators
            .iter()
            .find(|op| {
                op.operator_metadata
                    .as_ref()
                    .map(|op| op.operator_id == operator_id)
                    .unwrap_or(false)
            })
            .cloned()
            .ok_or_else(|| anyhow!("operator is missing from checkpoint metadata")),
    }
}

impl TableManager {
    pub async fn load(
        task_info: Arc<TaskInfo>,
        table_configs: HashMap<String, TableConfig>,
        tx: Sender<ControlResp>,
        restore_from: Option<&MetadataOrManifest>,
    ) -> Result<(Self, Option<SystemTime>)> {
        let restore_identity = restore_from
            .map(|metadata| -> Result<_> {
                match metadata {
                    MetadataOrManifest::Metadata(metadata) => {
                        if metadata.job_id != task_info.job_id {
                            bail!("selected checkpoint belongs to another job");
                        }
                        Ok((
                            arroyo_types::CheckpointFilePathLayout::Legacy,
                            metadata.epoch,
                        ))
                    }
                    MetadataOrManifest::Manifest(manifest) => {
                        if manifest.job_id != task_info.job_id {
                            bail!("selected manifest belongs to another job");
                        }
                        Ok((
                            arroyo_types::CheckpointFilePathLayout::Protocol {
                                pipeline_id: arroyo_types::PipelineId(Arc::new(
                                    manifest.pipeline_id.clone(),
                                )),
                                generation: manifest.generation,
                            },
                            u32::try_from(manifest.epoch)?,
                        ))
                    }
                }
            })
            .transpose()?;
        let (watermark, checkpoint_metadata) = if let Some(metadata) = restore_from {
            let operator_metadata =
                load_operator_metadata(metadata, &task_info.operator_id).await?;
            if operator_metadata
                .operator_metadata
                .as_ref()
                .map(|metadata| metadata.epoch)
                != restore_identity.as_ref().map(|(_, epoch)| *epoch)
            {
                bail!("operator checkpoint epoch differs from selected checkpoint");
            }
            let watermark = operator_metadata
                .operator_metadata
                .as_ref()
                .unwrap()
                .min_watermark
                .map(from_micros);

            (watermark, Some(operator_metadata))
        } else {
            (None, None)
        };

        let storage = get_storage_provider(&StorageProviderFor::Worker).await?;

        let live_configs: HashMap<_, _> = table_configs
            .iter()
            .filter(|(_, c)| c.table_type() == TableEnum::DiskKeyedMap)
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect();
        if live_configs.len() > 32 {
            bail!("disk SQL supports at most 32 named maps per operator");
        }
        let live_restore: HashMap<String, arroyo_rpc::grpc::rpc::TableCheckpointMetadata> =
            checkpoint_metadata
                .as_ref()
                .map(|m| {
                    m.table_checkpoint_metadata
                        .iter()
                        .filter(|(n, _)| live_configs.contains_key(*n))
                        .map(|(n, m)| (n.clone(), m.clone()))
                        .collect()
                })
                .unwrap_or_default();
        let mut restore_files = 0usize;
        for metadata in live_restore.values() {
            if metadata.table_type() != TableEnum::DiskKeyedMap {
                bail!("legacy state cannot be restored as disk SQL state");
            }
            let metadata = DiskKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())?;
            for subtask in metadata.subtasks.values() {
                restore_files = restore_files
                    .checked_add(subtask.files.len())
                    .ok_or_else(|| anyhow!("disk checkpoint file count overflow"))?;
            }
            if restore_files > crate::live::checkpoint::MAX_FILES {
                bail!("disk checkpoint exceeds 65536 page files per operator");
            }
        }
        let live_captures = Arc::new(Mutex::new(HashMap::new()));
        let tables = table_configs
            .iter()
            .filter(|(_, config)| config.table_type() != TableEnum::DiskKeyedMap)
            .map(|(table_name, table_config)| {
                let table_restore_from = checkpoint_metadata.as_ref().and_then(|metadata| {
                    metadata.table_checkpoint_metadata.get(table_name).cloned()
                });
                let erased_table = match table_config.table_type() {
                    TableEnum::DiskKeyedMap => unreachable!("filtered disk tables"),
                    TableEnum::MissingTableType => bail!("should have table type"),
                    TableEnum::GlobalKeyValue => {
                        Arc::new(<GlobalKeyedTable as ErasedTable>::from_config(
                            table_config.clone(),
                            task_info.clone(),
                            storage.clone(),
                            table_restore_from,
                        )?) as Arc<dyn ErasedTable>
                    }
                    TableEnum::ExpiringKeyedTimeTable => {
                        Arc::new(<ExpiringTimeKeyTable as ErasedTable>::from_config(
                            table_config.clone(),
                            task_info.clone(),
                            storage.clone(),
                            table_restore_from,
                        )?) as Arc<dyn ErasedTable>
                    }
                };
                Ok((table_name.to_string(), erased_table))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let epoch;
        let min_epoch;
        let mut last_epoch_checkpoints = HashMap::new();
        match checkpoint_metadata {
            Some(metadata) => {
                // TODO: validate this logic.
                let Some(operator_metadata) = metadata.operator_metadata else {
                    bail!("missing operator metadata");
                };
                epoch = operator_metadata.epoch + 1;
                min_epoch = operator_metadata.epoch;
                for (table, table_metadata) in metadata.table_checkpoint_metadata.clone() {
                    if live_configs.contains_key(&table) {
                        continue;
                    }
                    let table_implementation = tables
                        .get(&table)
                        .ok_or_else(|| anyhow!("missing table {}", table))?;
                    if let Some(metadata) =
                        table_implementation.subtask_metadata_from_table(table_metadata)?
                    {
                        last_epoch_checkpoints.insert(table.clone(), metadata);
                    }
                }
            }
            None => {
                epoch = 1;
                min_epoch = 1;
            }
        }

        let writer = BackendWriter::new(
            task_info.clone(),
            tx.clone(),
            table_configs,
            tables.clone(),
            storage.clone(),
            epoch,
            last_epoch_checkpoints,
            live_captures.clone(),
        );
        Ok((
            Self {
                epoch,
                min_epoch,
                tables,
                writer,
                task_info,
                storage: Arc::clone(&storage),
                caches: HashMap::new(),
                control_tx: tx,
                checkpoint_failed: false,
                restoring: restore_from.is_some(),
                restore_layout: restore_identity.map(|(layout, _)| layout),
                live_configs,
                live_restore,
                live_tables: HashMap::new(),
                live_captures,
                live_capture_limit: Arc::new(Semaphore::new(1)),
            },
            watermark,
        ))
    }

    /// Register a fresh backend before processing input. The protocol-selected
    /// checkpoint is restored here; local WAL contents are never recovery state.
    pub async fn register_live_table(
        &mut self,
        name: &str,
        backend: Arc<dyn LiveStateBackend>,
    ) -> Result<LiveTable> {
        if self.live_tables.contains_key(name) {
            bail!("duplicate live table registration {name}");
        }
        if self
            .live_tables
            .values()
            .any(|table| !Arc::ptr_eq(&table.backend, &backend))
        {
            bail!("all disk tables in an operator must share one attempt backend");
        }
        if self.task_info.parallelism != 1 || self.task_info.task_index != 0 {
            bail!("disk SQL supports singleton execution without rescaling");
        }
        if self.restoring && !self.live_restore.contains_key(name) {
            bail!("selected checkpoint missing disk table {name}");
        }
        let wrapped = self
            .live_configs
            .get(name)
            .ok_or_else(|| anyhow!("unregistered disk table {name}"))?;
        let config = DiskKeyedTableConfig::decode(wrapped.config.as_slice())?;
        if config.table_name.contains(['/', '\\'])
            || matches!(config.table_name.as_str(), "." | "..")
        {
            bail!("disk-map table names must be a single safe checkpoint path component");
        }
        if config.table_name != name
            || config.encoding_version != 1
            || config.schema_identity.is_empty()
        {
            bail!("invalid disk table config {name}");
        }
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: self.task_info.task_index,
                parallelism: self.task_info.parallelism,
            },
            table: name.as_bytes().to_vec(),
        };
        if crate::live::encoding::encoded_namespace_size(&namespace)? > 512
            || config.schema_identity.len() > 1024
        {
            bail!("disk table namespace exceeds 512 bytes or schema identity exceeds 1024 bytes");
        }
        crate::live::checkpoint::ensure_empty(backend.as_ref(), &namespace).await?;
        if let Some(metadata) = self.live_restore.get(name) {
            if metadata.table_type() != TableEnum::DiskKeyedMap {
                bail!("legacy state cannot be restored as disk SQL state");
            }
            let metadata = DiskKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())?;
            validate_selected_disk_checkpoint(
                &config,
                &metadata,
                &self.task_info,
                self.restore_layout
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing selected checkpoint layout"))?,
                self.min_epoch,
            )?;
            let subtask = metadata
                .subtasks
                .get(&self.task_info.task_index)
                .ok_or_else(|| anyhow!("missing disk subtask checkpoint"))?;
            if subtask.epoch != self.min_epoch || subtask.subtask_index != self.task_info.task_index
            {
                bail!("disk checkpoint epoch/owner mismatch");
            }
            crate::live::checkpoint::restore(
                backend.as_ref(),
                &namespace,
                &config,
                subtask,
                &self.storage,
            )
            .await?;
        }
        let mut handles = crate::live::table::LiveTableManager::new(
            backend.clone(),
            namespace.ownership.clone(),
        )?;
        let handle = handles.register(name)?;
        self.live_tables.insert(
            name.to_string(),
            RegisteredLiveTable {
                backend,
                namespace,
                config,
            },
        );
        Ok(handle)
    }

    pub async fn checkpoint(&mut self, barrier: CheckpointBarrier, watermark: Option<SystemTime>) {
        if self.checkpoint_failed {
            return;
        }
        if !self.live_configs.is_empty() {
            assert_eq!(
                self.live_tables.len(),
                self.live_configs.len(),
                "disk tables must register before checkpoint"
            );
            let permit = self
                .live_capture_limit
                .clone()
                .acquire_owned()
                .await
                .expect("capture semaphore open");
            let mut tables = Vec::with_capacity(self.live_tables.len());
            let mut snapshots: Vec<(Arc<dyn LiveStateBackend>, StateSnapshot)> = vec![];
            for (name, table) in &self.live_tables {
                // Operator processing is suspended here: all pre-barrier writes
                // finished, and post-barrier processing cannot begin until capture.
                let snapshot = if let Some((_, snapshot)) = snapshots
                    .iter()
                    .find(|(backend, _)| Arc::ptr_eq(backend, &table.backend))
                {
                    snapshot.clone()
                } else {
                    let snapshot = match table.backend.snapshot().await {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            self.checkpoint_failed = true;
                            let error =
                                arroyo_rpc::errors::DataflowError::from(StateError::Other {
                                    table: name.clone(),
                                    error: error.to_string(),
                                });
                            let _ = self
                                .control_tx
                                .send(ControlResp::TaskFailed {
                                    task_id: self.task_info.operator_idx,
                                    subtask_idx: self.task_info.task_index,
                                    error: error
                                        .with_operator(self.task_info.operator_id.clone())
                                        .into(),
                                })
                                .await;
                            return;
                        }
                    };
                    snapshots.push((table.backend.clone(), snapshot.clone()));
                    snapshot
                };
                tables.push((
                    name.clone(),
                    snapshot,
                    table.namespace.clone(),
                    table.config.clone(),
                ));
            }
            self.live_captures.lock().await.insert(
                barrier.epoch,
                CapturedLiveTables {
                    tables,
                    _permit: permit,
                },
            );
        }
        self.writer
            .sender
            .send(StateMessage::Checkpoint(CheckpointMessage {
                epoch: barrier.epoch,
                time: barrier.timestamp,
                watermark,
                then_stop: barrier.then_stop,
            }))
            .await
            .expect("should be able to send checkpoint");

        if barrier.then_stop {
            match self.writer.finish_rx.take().unwrap().await {
                Ok(_) => info!("finished stopping checkpoint"),
                Err(err) => warn!("error waiting for stopping checkpoint {:?}", err),
            }
        }
    }

    pub async fn load_compacted(&mut self, compacted: &CompactionResult) {
        assert_eq!(
            compacted.operator_id, self.task_info.operator_id,
            "shouldn't be loading compaction for other operator"
        );

        self.writer
            .sender
            .send(StateMessage::Compaction(compacted.compacted_tables.clone()))
            .await
            .expect("queue closed");
    }

    pub async fn insert_committing_data(&mut self, table: &str, data: Vec<u8>) {
        self.writer
            .sender
            .send(StateMessage::TableData {
                table: table.to_string(),
                data: TableData::CommitData { data },
            })
            .await
            .expect("checkpoint queue closed");
    }

    pub async fn get_global_keyed_state<K: Key, V: Data>(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>, StateError> {
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let global_keyed_table = table_implementation
                .as_any()
                .downcast_ref::<GlobalKeyedTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;

            let saved_data = global_keyed_table
                .memory_view::<K, V>(self.writer.sender.clone())
                .await?;

            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut GlobalKeyedView<K, V> =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;
        Ok(cache)
    }

    pub async fn get_global_keyed_state_migratable<K: Key, V: MigratableState>(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>, StateError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let global_keyed_table = table_implementation
                .as_any()
                .downcast_ref::<GlobalKeyedTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;

            let saved_data = global_keyed_table
                .memory_view_migratable::<K, V>(self.writer.sender.clone())
                .await?;

            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut GlobalKeyedView<K, V> =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;
        Ok(cache)
    }

    pub async fn get_expiring_time_key_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut ExpiringTimeKeyView, StateError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "expiring_time_key_table",
                })?;
            let saved_data = expiring_time_key_table
                .get_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut ExpiringTimeKeyView =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "expiring_time_key_table",
                })?;
        Ok(cache)
    }

    pub async fn get_key_time_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut KeyTimeView, StateError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "key_time_table",
                })?;
            let saved_data = expiring_time_key_table
                .get_key_time_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut KeyTimeView =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "key_time_table",
                })?;
        Ok(cache)
    }

    pub async fn get_uncached_key_value_view(
        &mut self,
        table_name: &str,
    ) -> Result<&mut UncachedKeyValueView, StateError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "uncached_key_value",
                })?;

            let view = expiring_time_key_table
                .get_uncached_key_value_view(self.writer.sender.clone())
                .await?;

            let cache: Box<dyn Any + Send> = Box::new(view);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut UncachedKeyValueView =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "uncached_key_value",
                })?;

        Ok(cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{ReadOptions, ScanPage, ScanRequest, SnapshotReader, StateKey};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    struct BlockingSnapshot {
        scanning: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for BlockingSnapshot {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl SnapshotReader for BlockingSnapshot {
        async fn get(&self, _: &StateKey, _: ReadOptions) -> crate::live::Result<Option<Vec<u8>>> {
            unreachable!()
        }
        async fn multi_get(
            &self,
            _: &[StateKey],
            _: ReadOptions,
        ) -> crate::live::Result<Vec<Option<Vec<u8>>>> {
            unreachable!()
        }
        async fn scan(&self, _: ScanRequest) -> crate::live::Result<ScanPage> {
            self.scanning.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn operator_drop_cancels_export_and_releases_captured_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            arroyo_storage::StorageProvider::for_url(&format!(
                "file://{}",
                directory.path().display()
            ))
            .await
            .unwrap(),
        );
        let task_info = Arc::new(TaskInfo {
            job_id: "job".into(),
            operator_idx: 0,
            operator_name: "stateful".into(),
            operator_id: "operator".into(),
            task_index: 0,
            parallelism: 1,
            key_range: crate::FULL_KEY_RANGE,
            checkpoint_file_path_layout: arroyo_types::CheckpointFilePathLayout::Legacy,
        });
        let scanning = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let snapshot = StateSnapshot(Arc::new(BlockingSnapshot {
            scanning: scanning.clone(),
            dropped: dropped.clone(),
        }));
        let captures = Arc::new(Mutex::new(HashMap::new()));
        captures.lock().await.insert(
            1,
            CapturedLiveTables {
                tables: vec![(
                    "map".into(),
                    snapshot,
                    StateNamespace {
                        ownership: Ownership::PartitionLocal {
                            subtask: 0,
                            parallelism: 1,
                        },
                        table: b"map".to_vec(),
                    },
                    DiskKeyedTableConfig {
                        table_name: "map".into(),
                        encoding_version: 1,
                        schema_identity: vec![1],
                    },
                )],
                _permit: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
            },
        );
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let writer = BackendWriter::new(
            task_info,
            control_tx,
            HashMap::new(),
            HashMap::new(),
            storage,
            1,
            HashMap::new(),
            captures.clone(),
        );
        writer
            .sender
            .send(StateMessage::Checkpoint(CheckpointMessage {
                epoch: 1,
                time: SystemTime::now(),
                watermark: None,
                then_stop: false,
            }))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scanning.notified())
            .await
            .unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(writer);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(captures.lock().await.is_empty());
        assert!(
            control_rx.try_recv().is_err(),
            "aborted operator must not report checkpoint completion"
        );
    }
    #[test]
    fn selected_checkpoint_rejects_foreign_pages_and_wrong_generation() {
        let task = TaskInfo {
            job_id: "job".into(),
            operator_idx: 0,
            operator_name: "stateful".into(),
            operator_id: "operator".into(),
            task_index: 0,
            parallelism: 1,
            key_range: crate::FULL_KEY_RANGE,
            checkpoint_file_path_layout: arroyo_types::CheckpointFilePathLayout::Protocol {
                pipeline_id: arroyo_types::PipelineId(Arc::new("pipeline".into())),
                generation: 9,
            },
        };
        let config = DiskKeyedTableConfig {
            table_name: "map".into(),
            encoding_version: 1,
            schema_identity: vec![1],
        };
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"map".to_vec(),
        };
        let selected = arroyo_types::CheckpointFilePathLayout::Protocol {
            pipeline_id: arroyo_types::PipelineId(Arc::new("pipeline".into())),
            generation: 7,
        };
        let path = format!(
            "{}/disk-page.bin",
            selected.table_checkpoint_path("job", "operator", "map", 0, 2, false)
        );
        let subtask = arroyo_rpc::grpc::rpc::DiskKeyedTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            format_version: 1,
            encoding_version: 1,
            schema_identity: vec![1],
            namespace: crate::live::encoding::encode_namespace(&namespace).unwrap(),
            generation: 7,
            epoch: 2,
            empty: false,
            files: vec![arroyo_rpc::grpc::rpc::DiskCheckpointFile {
                path,
                size_bytes: 1,
                checksum: vec![0; 32],
                row_count: 1,
            }],
        };
        let mut metadata = DiskKeyedTableTaskCheckpointMetadata {
            format_version: 1,
            subtasks: HashMap::from([(0, subtask)]),
        };
        // Current attempt generation nine can recover selected generation seven.
        validate_selected_disk_checkpoint(&config, &metadata, &task, &selected, 2).unwrap();
        assert!(
            validate_selected_disk_checkpoint(
                &config,
                &metadata,
                &task,
                &task.checkpoint_file_path_layout,
                2
            )
            .is_err()
        );
        metadata.subtasks.get_mut(&0).unwrap().files[0].path = format!(
            "{}/disk-page.bin",
            selected.table_checkpoint_path("foreign-job", "operator", "map", 0, 2, false)
        );
        assert!(
            validate_selected_disk_checkpoint(&config, &metadata, &task, &selected, 2).is_err()
        );
        metadata.subtasks.get_mut(&0).unwrap().files[0].path = format!(
            "{}/disk-page.bin",
            selected.table_checkpoint_path("job", "foreign-operator", "map", 0, 2, false)
        );
        assert!(
            validate_selected_disk_checkpoint(&config, &metadata, &task, &selected, 2).is_err()
        );
        let subtask = metadata.subtasks.get_mut(&0).unwrap();
        subtask.empty = true;
        subtask.files.clear();
        subtask.generation = 8;
        assert!(
            validate_selected_disk_checkpoint(&config, &metadata, &task, &selected, 2).is_err()
        );
        assert!(
            validate_selected_disk_checkpoint(
                &config,
                &metadata,
                &task,
                &arroyo_types::CheckpointFilePathLayout::Legacy,
                2
            )
            .is_err()
        );
    }
    #[test]
    fn metadata_wire_cap_includes_all_tables_configs_and_protobuf_overhead() {
        use arroyo_rpc::grpc::rpc::{DiskCheckpointFile, DiskKeyedTableSubtaskCheckpointMetadata};
        let config = DiskKeyedTableConfig {
            table_name: "map".into(),
            encoding_version: 1,
            schema_identity: b"sql-utf8-v1".to_vec(),
        };
        let disk = DiskKeyedTableSubtaskCheckpointMetadata {
            subtask_index: 0, format_version: 1, encoding_version: 1, schema_identity: config.schema_identity.clone(), namespace: vec![1], generation: 1, epoch: 2, empty: false,
            files: (0..8000).map(|page| DiskCheckpointFile {
                path: format!("pipeline/job/generations/1/checkpoints/checkpoint-0000002/operator-{}/table-map-000/disk-{page:06}.bin", "o".repeat(160)),
                size_bytes: 55000, row_count: 128, checksum: vec![0;32],
            }).collect(),
        };
        let mut metadata = SubtaskCheckpointMetadata {
            subtask_index: 0,
            start_time: u64::MAX,
            finish_time: u64::MAX,
            watermark: Some(u64::MAX),
            bytes: u64::MAX,
            table_metadata: HashMap::from([
                (
                    "map".into(),
                    TableSubtaskCheckpointMetadata {
                        subtask_index: 0,
                        table_type: TableEnum::DiskKeyedMap as i32,
                        data: disk.encode_to_vec(),
                    },
                ),
                (
                    "connector".into(),
                    TableSubtaskCheckpointMetadata {
                        subtask_index: 0,
                        table_type: TableEnum::GlobalKeyValue as i32,
                        data: vec![],
                    },
                ),
            ]),
            table_configs: HashMap::from([
                (
                    "map".into(),
                    TableConfig {
                        table_type: TableEnum::DiskKeyedMap as i32,
                        state_version: 1,
                        config: config.encode_to_vec(),
                    },
                ),
                (
                    "connector".into(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        state_version: 1,
                        config: vec![1; 1024],
                    },
                ),
            ]),
        };
        let cap = crate::live::checkpoint::MAX_SUBTASK_CHECKPOINT_BYTES;
        assert!(metadata.encoded_len() < cap);
        // Binary-search opaque connector bytes to land on the exact whole-message
        // boundary; disk file-list bytes alone omit table/config framing.
        let mut low = 0;
        let mut high = cap;
        while low < high {
            let size = low + (high - low) / 2;
            metadata
                .table_metadata
                .get_mut("connector")
                .unwrap()
                .data
                .resize(size, 0);
            if metadata.encoded_len() < cap {
                low = size + 1;
            } else {
                high = size;
            }
        }
        metadata
            .table_metadata
            .get_mut("connector")
            .unwrap()
            .data
            .resize(low, 0);
        assert_eq!(metadata.encoded_len(), cap);
        validate_disk_subtask_metadata_size(&metadata).unwrap();
        validate_disk_tables_wire_budget(&metadata.table_configs, &metadata.table_metadata)
            .unwrap();
        metadata
            .table_metadata
            .get_mut("connector")
            .unwrap()
            .data
            .push(0);
        assert_eq!(metadata.encoded_len(), cap + 1);
        assert!(
            validate_disk_tables_wire_budget(&metadata.table_configs, &metadata.table_metadata)
                .is_err()
        );
        let error = validate_disk_subtask_metadata_size(&metadata).unwrap_err();
        assert!(error.to_string().contains("3 MiB RPC limit"));
    }

    #[test]
    fn disk_metadata_cap_preserves_memory_only_checkpoint_sizes() {
        // This legacy checkpoint remains below the transport's 4 MiB limit,
        // but exceeds the extra allowance reserved for disk checkpoint RPCs.
        let mut metadata = SubtaskCheckpointMetadata {
            table_metadata: HashMap::from([(
                "connector".into(),
                TableSubtaskCheckpointMetadata {
                    subtask_index: 0,
                    table_type: TableEnum::GlobalKeyValue as i32,
                    data: vec![0; 7 * 1024 * 1024 / 2],
                },
            )]),
            table_configs: HashMap::from([(
                "connector".into(),
                TableConfig {
                    table_type: TableEnum::GlobalKeyValue as i32,
                    state_version: 1,
                    config: vec![0; 1024],
                },
            )]),
            ..Default::default()
        };
        assert!(metadata.encoded_len() > crate::live::checkpoint::MAX_SUBTASK_CHECKPOINT_BYTES);
        assert!(metadata.encoded_len() < 4 * 1024 * 1024);
        validate_disk_tables_wire_budget(&metadata.table_configs, &metadata.table_metadata)
            .unwrap();
        validate_disk_subtask_metadata_size(&metadata).unwrap();

        // A disk-enabled operator caps the entire message, including unchanged
        // legacy table metadata and configs, even before disk pages are added.
        metadata.table_configs.insert(
            "map".into(),
            TableConfig {
                table_type: TableEnum::DiskKeyedMap as i32,
                state_version: 1,
                config: vec![],
            },
        );
        assert!(
            validate_disk_tables_wire_budget(&metadata.table_configs, &metadata.table_metadata)
                .is_err()
        );
        assert!(validate_disk_subtask_metadata_size(&metadata).is_err());
    }
}
