use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::types::public::StopMode as SqlStopMode;
use anyhow::bail;
use arroyo_rpc::checkpoints::CheckpointMetadataStore;
use arroyo_rpc::grpc::rpc::{CommitReq, StopExecutionReq, StopMode};
use arroyo_rpc::identity::WorkerClient;
use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
use arroyo_types::{JobId, PipelineId, WorkerId};
use rand::{Rng, rng};

use crate::{JobConfig, JobMessage};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::worker_types::{RunningMessage, TaskFailedEvent};
use arroyo_state_protocol::ProtocolPaths;
use arroyo_state_protocol::types::{Epoch, Generation};
use arroyo_worker::job_controller::committing_state::CommittingState;
use arroyo_worker::job_controller::job_metrics;
use arroyo_worker::job_controller::job_metrics::JobMetrics;
use arroyo_worker::job_controller::model::{
    CheckpointingOrCommittingState, JobState, RunningJobModel, TaskState, TaskStatus, WorkerState,
    WorkerStatus,
};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tracing::{error, info};

pub mod checkpoint_store;
pub mod leader_manager;

const CHECKPOINT_ROWS_TO_KEEP: u64 = 100;
const CHECKPOINTS_TO_KEEP: u64 = 10;
const COMPACT_EVERY: u64 = 4;

pub struct JobController {
    checkpoint_store: Arc<dyn CheckpointMetadataStore>,
    config: JobConfig,
    model: RunningJobModel,
    cleanup_task: Option<JoinHandle<anyhow::Result<Epoch>>>,
}

impl std::fmt::Debug for JobController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobController")
            .field("pipeline_id", &self.model.pipeline_id)
            .field("config", &self.config)
            .field("model", &self.model)
            .field("cleaning", &self.cleanup_task.is_some())
            .finish()
    }
}

pub enum ControllerProgress {
    Continue,
    Finishing,
    TaskFailed(TaskFailedEvent),
}

impl JobController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        checkpoint_store: Arc<dyn CheckpointMetadataStore>,
        config: JobConfig,
        pipeline_id: PipelineId,
        state_url: Option<String>,
        generation: u64,
        program: Arc<LogicalProgram>,
        epoch: u64,
        min_epoch: u64,
        completed_base_epoch: Option<u32>,
        worker_connects: HashMap<WorkerId, WorkerClient>,
        commit_state: Option<CommittingState>,
        job_metrics: Option<JobMetrics>,
    ) -> Self {
        let last_completed_epoch = completed_base_epoch.filter(|_| commit_state.is_none());
        Self {
            checkpoint_store,
            model: RunningJobModel {
                protocol_paths: ProtocolPaths::new(pipeline_id.clone(), JobId(config.id.clone())),
                pipeline_id,
                job_id: JobId(config.id.clone()),
                generation: Generation(generation),
                state: JobState::Running,
                checkpoint_state: commit_state.map(CheckpointingOrCommittingState::Committing),
                epoch: Epoch(epoch),
                last_completed_epoch,
                min_epoch: Epoch(min_epoch),
                // delay the initial checkpoint by a random amount so that on controller restart,
                // checkpoint times are staggered across jobs
                last_checkpoint: Instant::now()
                    + Duration::from_millis(
                        rng().random_range(0..config.checkpoint_interval.as_millis() as u64),
                    ),
                workers: worker_connects
                    .into_iter()
                    .map(|(id, connect)| {
                        (
                            id,
                            WorkerStatus {
                                id,
                                connect,
                                last_heartbeat: Instant::now(),
                                state: WorkerState::Running,
                            },
                        )
                    })
                    .collect(),
                tasks: program
                    .graph
                    .node_weights()
                    .flat_map(|node| {
                        (0..node.parallelism).map(|idx| {
                            (
                                (node.node_id, idx as u32),
                                TaskStatus {
                                    state: TaskState::Running,
                                },
                            )
                        })
                    })
                    .collect(),
                operator_parallelism: program.tasks_per_node(),
                metric_update_task: None,
                last_updated_metrics: Instant::now(),
                checkpoint_parent_ref: None,
                program,
                checkpoint_spans: vec![],
                worker_leader_mode: false,
                storage_role: StorageProviderFor::Controller {
                    storage_url: state_url,
                },
                finished_operators: vec![],
                generation_manifest: None,
                job_metrics,
            },
            config,
            cleanup_task: None,
        }
    }

    pub fn update_config(&mut self, config: JobConfig) {
        self.config = config;
    }

    pub async fn handle_message(&mut self, msg: RunningMessage) -> anyhow::Result<()> {
        self.model
            .handle_message(msg, &*self.checkpoint_store)
            .await
    }

    pub fn cleanup_needed(&self) -> Option<Epoch> {
        if *self.model.epoch - *self.model.min_epoch > CHECKPOINTS_TO_KEEP
            && self.model.epoch.is_multiple_of(COMPACT_EVERY)
        {
            Some(Epoch(*self.model.epoch - CHECKPOINTS_TO_KEEP))
        } else {
            None
        }
    }

    pub async fn progress(&mut self) -> anyhow::Result<ControllerProgress> {
        // have any of our workers failed?
        if self.model.worker_timedout() {
            bail!("worker failed");
        }

        // have any tasks failed?
        if let Some(event) = self.model.task_failed() {
            return Ok(ControllerProgress::TaskFailed(event));
        }

        // have any of our tasks finished?
        if self.model.any_finished_sources() {
            return Ok(ControllerProgress::Finishing);
        }

        // check on compaction
        if self.cleanup_task.is_some() && self.cleanup_task.as_ref().unwrap().is_finished() {
            let task = self.cleanup_task.take().unwrap();

            match task.await {
                Ok(Ok(min_epoch)) => {
                    info!(
                        message = "setting new min epoch",
                        min_epoch = *min_epoch,
                        job_id = *self.config.id
                    );
                    self.model.min_epoch = min_epoch;
                }
                Ok(Err(e)) => {
                    error!(
                        message = "cleanup failed",
                        job_id = *self.config.id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!(
                        message = "cleanup panicked",
                        job_id = *self.config.id,
                        error = format!("{:?}", e)
                    );

                    // wait a bit before trying again
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        if let Some(new_epoch) = self.cleanup_needed()
            && self.cleanup_task.is_none()
            && self.model.checkpoint_state.is_none()
        {
            self.cleanup_task = Some(self.start_cleanup(new_epoch));
        }

        // check on checkpointing
        if self.model.checkpoint_state.is_some() {
            self.model
                .finish_checkpoint_if_done(&*self.checkpoint_store)
                .await?;
        } else if self.model.last_checkpoint.elapsed() > self.config.checkpoint_interval
            && self.cleanup_task.is_none()
        {
            // or do we need to start checkpointing?
            self.checkpoint(false).await?;
        }

        // update metrics
        if self.model.last_updated_metrics.elapsed() > job_metrics::COLLECTION_RATE {
            self.model.update_metrics().await;
            self.model.last_updated_metrics = Instant::now();
        }

        Ok(ControllerProgress::Continue)
    }

    pub async fn stop_job(&mut self, stop_mode: StopMode) -> anyhow::Result<()> {
        for c in self.model.workers.values_mut() {
            c.connect
                .stop_execution(StopExecutionReq {
                    stop_mode: stop_mode as i32,
                })
                .await?;
        }

        Ok(())
    }

    pub async fn checkpoint(&mut self, then_stop: bool) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_none() {
            self.model
                .start_checkpoint(&*self.checkpoint_store, then_stop)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn finished(&self) -> bool {
        self.model.all_tasks_finished()
    }

    pub async fn checkpoint_finished(&mut self) -> anyhow::Result<bool> {
        if self.model.checkpoint_state.is_some() {
            self.model
                .finish_checkpoint_if_done(&*self.checkpoint_store)
                .await?;
        }
        Ok(self.model.checkpoint_state.is_none())
    }

    pub async fn send_commit_messages(&mut self) -> anyhow::Result<()> {
        let Some(CheckpointingOrCommittingState::Committing(committing)) =
            &self.model.checkpoint_state
        else {
            bail!("should be committing")
        };
        for worker in self.model.workers.values_mut() {
            worker
                .connect
                .commit(CommitReq {
                    epoch: *self.model.epoch,
                    committing_data: committing.committing_data(),
                })
                .await?;
        }
        Ok(())
    }

    pub async fn wait_for_finish(&mut self, rx: &mut Receiver<JobMessage>) -> anyhow::Result<()> {
        loop {
            if self.model.all_tasks_finished() {
                return Ok(());
            }

            match rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("channel closed while receiving"))?
            {
                JobMessage::RunningMessage(msg) => {
                    self.model
                        .handle_message(msg, &*self.checkpoint_store)
                        .await?;
                }
                JobMessage::ConfigUpdate(c) if c.stop_mode == SqlStopMode::immediate => {
                    info!(
                        message = "stopping job immediately",
                        job_id = *self.config.id
                    );
                    self.stop_job(StopMode::Immediate).await?;
                }
                _ => {
                    // ignore other messages
                }
            }
        }
    }

    pub fn operator_parallelism(&self, node_id: u32) -> Option<usize> {
        self.model.operator_parallelism.get(&node_id).cloned()
    }

    fn start_cleanup(&mut self, new_min: Epoch) -> JoinHandle<anyhow::Result<Epoch>> {
        let min_epoch = Epoch((*self.model.min_epoch).max(1));
        let job_id = self.config.id.clone();
        let store = self.checkpoint_store.clone();
        let storage_role = self.model.storage_role.clone();

        info!(
            message = "Starting cleaning",
            job_id = *job_id,
            min_epoch = *min_epoch,
            new_min = *new_min
        );
        let start = Instant::now();
        let cur_epoch = self.model.epoch;

        let runtime = tokio::runtime::Handle::current();
        // The existing per-job cleanup gate bounds one blocking closure per
        // pass; no sort or fsync executes on a Tokio worker thread. The closure
        // keeps the owned spool alive even if its join handle is cancelled.
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let checkpoint = StateBackend::load_checkpoint_metadata(
                    &storage_role,
                    &job_id,
                    *cur_epoch as u32,
                )
                .await?;

                store
                    .mark_compacting(&job_id, *min_epoch as u32, *new_min as u32)
                    .await?;

                StateBackend::cleanup_checkpoint(
                    &storage_role,
                    checkpoint,
                    *min_epoch as u32,
                    *new_min as u32,
                )
                .await?;

                store
                    .mark_checkpoints_compacted(&job_id, *new_min as u32)
                    .await?;

                if let Some(epoch_to_filter_before) = min_epoch.checked_sub(CHECKPOINT_ROWS_TO_KEEP)
                {
                    store
                        .drop_old_checkpoint_rows(&job_id, epoch_to_filter_before as u32)
                        .await?;
                }

                info!(
                    message = "Finished cleaning",
                    job_id = *job_id,
                    min_epoch = *min_epoch,
                    new_min = *new_min,
                    duration = start.elapsed().as_secs_f32()
                );

                Ok(new_min)
            })
        })
    }
}

#[cfg(test)]
mod native_completed_base_tests {
    use super::*;
    use crate::types::public::{RestartMode, StopMode as ConfigStopMode};
    use arroyo_worker::job_controller::committing_state::CheckpointIdOrRef;
    use arroyo_worker::job_controller::{CheckpointHistory, JobControllerStatus};
    use std::sync::Mutex;
    fn make(completed: Option<u32>, pending: bool) -> JobController {
        let store = Arc::new(JobControllerStatus {
            job_status: Arc::new(Mutex::new(arroyo_rpc::grpc::rpc::JobStatus::default())),
            checkpoint_history: Arc::new(Mutex::new(CheckpointHistory::default())),
        });
        let config = JobConfig {
            id: Arc::new("J".into()),
            organization_id: "org".into(),
            pipeline_name: "pipeline".into(),
            pipeline_id: 1,
            stop_mode: ConfigStopMode::none,
            checkpoint_interval: Duration::from_secs(1),
            ttl: None,
            parallelism_overrides: HashMap::new(),
            restart_nonce: 0,
            restart_mode: RestartMode::safe,
            ignore_state_before_epoch: Some(100),
            env_vars: serde_json::json!({}),
            scheduler_config: serde_json::json!({}),
        };
        let commit = pending.then(|| {
            CommittingState::new(
                CheckpointIdOrRef::CheckpointId("selected".into()),
                Default::default(),
                Default::default(),
            )
        });
        JobController::new(
            store,
            config,
            PipelineId::new("P"),
            None,
            1,
            Arc::new(LogicalProgram::default()),
            99,
            90,
            completed,
            HashMap::new(),
            commit,
            None,
        )
    }
    #[test]
    fn fresh_epoch_threshold_does_not_advertise_a_completed_checkpoint() {
        assert_eq!(make(None, false).model.last_completed_epoch, None);
        assert_eq!(make(Some(99), false).model.last_completed_epoch, Some(99));
        assert_eq!(make(Some(99), true).model.last_completed_epoch, None);
    }
}
