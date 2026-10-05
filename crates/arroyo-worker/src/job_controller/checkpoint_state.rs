use crate::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};
use anyhow::{anyhow, bail};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::grpc::api::OperatorCheckpointDetail;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
    OperatorCheckpointMetadata, OperatorMetadata, SubtaskCheckpointMetadata,
    TableCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    TaskCheckpointCompletedReq, TaskCheckpointEventReq, TypedStateTableConfig,
    TypedStateTableSubtaskCheckpointMetadata, TypedStateTableTaskCheckpointMetadata,
};
use arroyo_rpc::grpc::{api, rpc};
use arroyo_rpc::{TaskEventSpans, get_event_spans, grpc, log_trace_event};
use arroyo_state::tables::ErasedTable;
use arroyo_state::tables::disk_keyed_map::DiskKeyedTable;
use arroyo_state::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state_protocol::types::Epoch;
use arroyo_types::{from_micros, to_micros};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

pub type CommitData = HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>;

fn merge_typed_checkpoint_metadata(
    config: TableConfig,
    subtasks: HashMap<u32, TableSubtaskCheckpointMetadata>,
) -> anyhow::Result<Option<TableCheckpointMetadata>> {
    if config.table_type() != TableEnum::TypedStateTable {
        bail!("typed state-table type mismatch");
    }
    let config = TypedStateTableConfig::decode(config.config.as_slice())?;
    let mut result = TypedStateTableTaskCheckpointMetadata {
        format_version: arroyo_state_protocol::typed_checkpoint::TYPED_CHECKPOINT_VERSION,
        subtasks: HashMap::new(),
    };
    for (index, wrapped) in subtasks {
        if wrapped.table_type() != TableEnum::TypedStateTable || index != wrapped.subtask_index {
            bail!("typed state-table subtask type or ownership mismatch");
        }
        let subtask = TypedStateTableSubtaskCheckpointMetadata::decode(wrapped.data.as_slice())?;
        if index != subtask.subtask_index {
            bail!("typed state-table subtask ownership mismatch");
        }
        arroyo_state_protocol::typed_checkpoint::validate_subtask(&config, &subtask)
            .map_err(|e| anyhow!(e))?;
        result.format_version = subtask.format_version;
        result.subtasks.insert(index, subtask);
    }
    arroyo_state_protocol::typed_checkpoint::validate_table(&config, &result)
        .map_err(|e| anyhow!(e))?;
    Ok(Some(TableCheckpointMetadata {
        table_type: TableEnum::TypedStateTable.into(),
        data: result.encode_to_vec(),
    }))
}

#[derive(Debug, Clone)]
pub struct CheckpointState {
    job_id: Arc<String>,
    pub checkpoint_id: String,
    epoch: Epoch,
    min_epoch: Epoch,
    start_time: SystemTime,
    operators: usize,
    operators_checkpointed: usize,
    bytes: u64,
    operator_state: HashMap<String, OperatorState>,
    subtasks_to_commit: HashSet<(String, u32)>,
    // map of operator_id -> table_name -> subtask_index -> Data
    pub commit_data: CommitData,

    // Used for the web ui -- eventually should be replaced with some other way of tracking / reporting
    // this data
    pub operator_details: HashMap<String, OperatorCheckpointDetail>,
    operator_names: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct OperatorState {
    subtasks: usize,
    subtasks_checkpointed: usize,
    bytes: usize,
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
    table_state: HashMap<String, TableState>,
    watermarks: Vec<Option<SystemTime>>,
}

impl OperatorState {
    fn new(subtasks: usize) -> Self {
        OperatorState {
            subtasks,
            subtasks_checkpointed: 0,
            bytes: 0,
            start_time: None,
            finish_time: None,
            table_state: HashMap::new(),
            watermarks: vec![],
        }
    }

    fn finish_subtask(
        &mut self,
        c: SubtaskCheckpointMetadata,
    ) -> anyhow::Result<
        Option<(
            HashMap<String, TableConfig>,
            HashMap<String, TableCheckpointMetadata>,
        )>,
    > {
        self.subtasks_checkpointed += 1;
        self.watermarks.push(c.watermark.map(from_micros));
        self.start_time = match self.start_time {
            Some(existing_start_time) => Some(existing_start_time.min(from_micros(c.start_time))),
            None => Some(from_micros(c.start_time)),
        };
        self.finish_time = match self.finish_time {
            Some(existing_finish_time) => {
                Some(existing_finish_time.max(from_micros(c.finish_time)))
            }
            None => Some(from_micros(c.finish_time)),
        };
        self.bytes += c.bytes as usize;
        for (table, table_metadata) in c.table_metadata {
            self.table_state
                .entry(table)
                .or_insert_with_key(|key| TableState {
                    table_config: c
                        .table_configs
                        .get(key)
                        .expect("should have metadata")
                        .clone(),
                    subtask_tables: HashMap::new(),
                })
                .subtask_tables
                .insert(table_metadata.subtask_index, table_metadata);
        }

        if self.subtasks == self.subtasks_checkpointed {
            let mut table_configs = HashMap::new();
            let mut table_metadatas = HashMap::new();
            for (table_name, table_state) in self.table_state.drain() {
                if let Some((config, metadata)) = table_state.into_table_metadata()? {
                    table_configs.insert(table_name.clone(), config);
                    table_metadatas.insert(table_name, metadata);
                }
            }
            Ok(Some((table_configs, table_metadatas)))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableState {
    table_config: TableConfig,
    subtask_tables: HashMap<u32, TableSubtaskCheckpointMetadata>,
}

impl TableState {
    fn into_table_metadata(self) -> anyhow::Result<Option<(TableConfig, TableCheckpointMetadata)>> {
        let metadata = match self.table_config.table_type() {
            TableEnum::MissingTableType => bail!("missing checkpoint table type"),
            TableEnum::DiskKeyedMap => DiskKeyedTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )?,
            TableEnum::TypedStateTable => {
                merge_typed_checkpoint_metadata(self.table_config.clone(), self.subtask_tables)?
            }
            TableEnum::GlobalKeyValue => GlobalKeyedTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )?,
            TableEnum::ExpiringKeyedTimeTable => ExpiringTimeKeyTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )?,
        };
        Ok(metadata.map(|metadata| (self.table_config, metadata)))
    }
}

impl CheckpointState {
    pub fn new(
        job_id: Arc<String>,
        checkpoint_id: String,
        epoch: Epoch,
        min_epoch: Epoch,
        program: Arc<LogicalProgram>,
    ) -> Self {
        Self {
            job_id,
            checkpoint_id,
            epoch,
            min_epoch,
            bytes: 0,
            start_time: SystemTime::now(),
            operators: program.tasks_per_operator().len(),
            operators_checkpointed: 0,
            operator_state: program
                .tasks_per_operator()
                .into_iter()
                .map(|(operator_id, subtasks)| (operator_id, OperatorState::new(subtasks)))
                .collect(),
            subtasks_to_commit: HashSet::new(),
            commit_data: HashMap::new(),
            operator_details: HashMap::new(),
            operator_names: program.operator_names_by_id(),
        }
    }

    fn log_checkpoint_event(
        operator_names: &HashMap<String, String>,
        operator_id: Option<&str>,
        subtask_idx: Option<u64>,
        epoch: Epoch,
        size: u64,
        total_time: Duration,
        spans: TaskEventSpans,
    ) {
        fn to_duration(s: Option<(u64, u64)>) -> f64 {
            match s {
                Some((start, end)) => from_micros(end)
                    .duration_since(from_micros(start))
                    .unwrap_or_default()
                    .as_millis() as f64,
                None => 0.0,
            }
        }

        let name = operator_id.and_then(|id| operator_names.get(id));
        log_trace_event!("checkpoint", {
            "operator_id": operator_id,
            "operator_name": name,
            "subtask_idx": subtask_idx.map(|i| i.to_string()).unwrap_or_default(),
            "epoch": epoch.to_string(),
        }, [
            "size_bytes" => size as f64,
            "total_time" => total_time.as_millis() as f64,
            "alignment_time" => to_duration(spans.alignment),
            "sync_time" => to_duration(spans.sync),
            "async_time" => to_duration(spans.r#async),
            "commit_time" => to_duration(spans.committing),
        ]);
    }

    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    pub fn checkpoint_event(&mut self, c: TaskCheckpointEventReq) -> anyhow::Result<()> {
        debug!(message = "Checkpoint event", checkpoint_id = self.checkpoint_id, event_type = ?c.event_type(), subtask_idx = c.subtask_idx, operator_id = ?c.operator_id);

        if grpc::rpc::TaskCheckpointEventType::FinishedCommit == c.event_type() {
            bail!(
                "shouldn't receive finished commit {:?} while checkpointing",
                c
            );
        }

        // This is all for the UI
        self.operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: c.time,
                finish_time: None,
                has_state: false,
                started_metadata_write: None,
                tasks: HashMap::new(),
            })
            .tasks
            .entry(c.subtask_idx)
            .or_insert_with(|| api::TaskCheckpointDetail {
                subtask_index: c.subtask_idx,
                start_time: c.time,
                finish_time: None,
                bytes: None,
                events: vec![],
            })
            .events
            .push(api::TaskCheckpointEvent {
                time: c.time,
                event_type: match c.event_type() {
                    rpc::TaskCheckpointEventType::StartedAlignment => {
                        api::TaskCheckpointEventType::AlignmentStarted
                    }
                    rpc::TaskCheckpointEventType::StartedCheckpointing => {
                        api::TaskCheckpointEventType::CheckpointStarted
                    }
                    rpc::TaskCheckpointEventType::FinishedOperatorSetup => {
                        api::TaskCheckpointEventType::CheckpointOperatorSetupFinished
                    }
                    rpc::TaskCheckpointEventType::FinishedSync => {
                        api::TaskCheckpointEventType::CheckpointSyncFinished
                    }
                    rpc::TaskCheckpointEventType::FinishedCommit => {
                        api::TaskCheckpointEventType::CheckpointPreCommit
                    }
                } as i32,
            });
        Ok(())
    }

    pub fn checkpoint_finished(
        &mut self,
        c: TaskCheckpointCompletedReq,
    ) -> anyhow::Result<Option<OperatorCheckpointMetadata>> {
        // TODO: UI management
        let metadata = c
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?;

        if c.epoch != *self.epoch {
            bail!("checkpoint completion epoch mismatch");
        }
        for (name, wrapped) in &metadata.table_metadata {
            if wrapped.table_type() == TableEnum::TypedStateTable {
                let config = metadata
                    .table_configs
                    .get(name)
                    .ok_or_else(|| anyhow!("missing typed state-table configuration"))?;
                if config.table_type() != TableEnum::TypedStateTable {
                    bail!("typed state-table configuration type mismatch");
                }
                let config = TypedStateTableConfig::decode(config.config.as_slice())?;
                let typed =
                    TypedStateTableSubtaskCheckpointMetadata::decode(wrapped.data.as_slice())?;
                arroyo_state_protocol::typed_checkpoint::validate_subtask(&config, &typed)
                    .map_err(|e| anyhow!(e))?;
                if config.transport_name != *name
                    || u64::from(typed.epoch) != c.epoch
                    || typed.subtask_index != metadata.subtask_index
                    || wrapped.subtask_index != metadata.subtask_index
                {
                    bail!(
                        "typed state-table checkpoint table, epoch or subtask ownership mismatch"
                    );
                }
                let context = c.worker_context.as_ref().ok_or_else(|| {
                    anyhow!("typed state-table checkpoint missing worker ownership")
                })?;
                if context.job_id != *self.job_id {
                    bail!("typed state-table checkpoint job ownership mismatch");
                }
                let legacy_prefix = format!(
                    "{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000/",
                    self.job_id, c.epoch, c.operator_id, name
                );
                let protocol_prefix = format!(
                    "{}/{}/generations/{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000/",
                    context.pipeline_id,
                    self.job_id,
                    typed.generation,
                    c.epoch,
                    c.operator_id,
                    name
                );
                for file in &typed.files {
                    if !(typed.generation == 0 && file.path.starts_with(&legacy_prefix)
                        || typed.generation == context.generation
                            && file.path.starts_with(&protocol_prefix))
                    {
                        bail!(
                            "typed state-table checkpoint file is outside exclusive worker table owner"
                        );
                    }
                }
                continue;
            }
            if wrapped.table_type() != TableEnum::DiskKeyedMap {
                continue;
            }
            let config = metadata
                .table_configs
                .get(name)
                .ok_or_else(|| anyhow!("missing disk-map checkpoint configuration"))?;
            if config.table_type() != TableEnum::DiskKeyedMap {
                bail!("disk-map checkpoint configuration type mismatch");
            }
            let config = DiskKeyedTableConfig::decode(config.config.as_slice())?;
            let disk = DiskKeyedTableSubtaskCheckpointMetadata::decode(wrapped.data.as_slice())?;
            arroyo_state_protocol::disk::validate_subtask(&config, &disk)
                .map_err(|error| anyhow!(error))?;
            if config.table_name != *name
                || u64::from(disk.epoch) != c.epoch
                || disk.subtask_index != metadata.subtask_index
                || wrapped.subtask_index != metadata.subtask_index
            {
                bail!("disk-map checkpoint table, epoch or subtask ownership mismatch");
            }
            let context = c
                .worker_context
                .as_ref()
                .ok_or_else(|| anyhow!("disk-map checkpoint missing worker ownership"))?;
            if context.job_id != *self.job_id {
                bail!("disk-map checkpoint job ownership mismatch");
            }
            let legacy_prefix = format!(
                "{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000/",
                self.job_id, c.epoch, c.operator_id, name
            );
            let protocol_prefix = format!(
                "{}/{}/generations/{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000/",
                context.pipeline_id, self.job_id, disk.generation, c.epoch, c.operator_id, name
            );
            for file in &disk.files {
                if !(disk.generation == 0 && file.path.starts_with(&legacy_prefix)
                    || disk.generation == context.generation
                        && file.path.starts_with(&protocol_prefix))
                {
                    bail!("disk-map checkpoint file is outside its exclusive worker table owner");
                }
            }
        }

        debug!(
            message = "Checkpoint finished",
            checkpoint_id = self.checkpoint_id,
            job_id = *self.job_id,
            epoch = *self.epoch,
            min_epoch = *self.min_epoch,
            operator_id = %c.operator_id,
            subtask_index = metadata.subtask_index,
            time = c.time);

        let operator_detail = self
            .operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: metadata.start_time,
                finish_time: None,
                has_state: false,
                started_metadata_write: None,
                tasks: HashMap::new(),
            });

        self.bytes += metadata.bytes;

        let detail = operator_detail
            .tasks
            .entry(metadata.subtask_index)
            .or_insert_with(|| {
                warn!(
                    "Received checkpoint completion but no start event {:?}",
                    metadata
                );
                api::TaskCheckpointDetail {
                    subtask_index: metadata.subtask_index,
                    start_time: metadata.start_time,
                    finish_time: None,
                    bytes: None,
                    events: vec![],
                }
            });
        detail.bytes = Some(metadata.bytes);
        detail.finish_time = Some(c.time);

        let operator_state = self
            .operator_state
            .get_mut(&c.operator_id)
            .ok_or_else(|| anyhow!("unexpected operator checkpoint {}", c.operator_id))?;
        if let Some((table_configs, table_checkpoint_metadata)) = operator_state.finish_subtask(
            c.metadata
                .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?,
        )? {
            self.operators_checkpointed += 1;

            Self::log_checkpoint_event(
                &self.operator_names,
                Some(&c.operator_id),
                None,
                self.epoch,
                operator_state.bytes as u64,
                operator_state
                    .finish_time
                    .unwrap()
                    .duration_since(operator_state.start_time.unwrap())
                    .unwrap_or_default(),
                Default::default(),
            );

            // watermarks are None if any subtasks are None.
            let (min_watermark, max_watermark) =
                if operator_state.watermarks.iter().any(|w| w.is_none()) {
                    (None, None)
                } else {
                    (
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .min(),
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .max(),
                    )
                };
            for (table, checkpoint_metadata) in table_checkpoint_metadata.iter() {
                let config = table_configs
                    .get(table)
                    .expect("should have a config for the table");
                if let Some(committing_data) = match config.table_type() {
                    TableEnum::MissingTableType => bail!("missing table type"),
                    TableEnum::DiskKeyedMap => None,
                    TableEnum::TypedStateTable => None,
                    TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                    TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                } {
                    for i in 0..operator_state.subtasks_checkpointed {
                        self.subtasks_to_commit
                            .insert((c.operator_id.clone(), i as u32));
                    }
                    self.commit_data
                        .entry(c.operator_id.clone())
                        .or_default()
                        .insert(table.clone(), committing_data);
                }
            }

            operator_detail.started_metadata_write = Some(to_micros(SystemTime::now()));
            let metadata = OperatorCheckpointMetadata {
                start_time: to_micros(operator_state.start_time.unwrap()),
                finish_time: to_micros(operator_state.finish_time.unwrap()),
                table_checkpoint_metadata,
                table_configs,
                operator_metadata: Some(OperatorMetadata {
                    job_id: self.job_id.to_string(),
                    operator_id: c.operator_id,
                    epoch: *self.epoch as u32,
                    min_watermark,
                    max_watermark,
                    parallelism: operator_state.subtasks_checkpointed as u64,
                }),
            };

            operator_detail.finish_time = Some(to_micros(SystemTime::now()));
            return Ok(Some(metadata));
        }
        Ok(None)
    }

    pub fn done(&self) -> bool {
        self.operators == self.operators_checkpointed
    }

    pub fn build_metadata(&mut self) -> CheckpointMetadata {
        let finish_time = SystemTime::now();

        for (op, details) in &self.operator_details {
            for (subtask_index, task_details) in &details.tasks {
                let spans = self
                    .operator_details
                    .get(op)
                    .and_then(|c| c.tasks.get(subtask_index))
                    .map(get_event_spans)
                    .unwrap_or_default();

                Self::log_checkpoint_event(
                    &self.operator_names,
                    Some(op),
                    Some(*subtask_index as u64),
                    self.epoch,
                    task_details.bytes.unwrap_or_default(),
                    Duration::from_micros(
                        task_details
                            .finish_time
                            .unwrap_or_default()
                            .saturating_sub(task_details.start_time),
                    ),
                    spans,
                );
            }
        }

        Self::log_checkpoint_event(
            &self.operator_names,
            None,
            None,
            self.epoch,
            self.bytes,
            finish_time
                .duration_since(self.start_time)
                .unwrap_or_default(),
            Default::default(),
        );

        CheckpointMetadata {
            job_id: self.job_id.to_string(),
            epoch: *self.epoch as u32,
            min_epoch: *self.min_epoch as u32,
            start_time: to_micros(self.start_time),
            finish_time: to_micros(finish_time),
            operator_ids: self
                .operator_state
                .keys()
                .map(|key| key.to_string())
                .collect(),
        }
    }

    pub fn needs_commit(&self) -> bool {
        !self.subtasks_to_commit.is_empty()
    }

    pub fn into_commit(self, checkpoint_id: CheckpointIdOrRef) -> CommittingState {
        CommittingState::new(checkpoint_id, self.subtasks_to_commit, self.commit_data)
    }
}
