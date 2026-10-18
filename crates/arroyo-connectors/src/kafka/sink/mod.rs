use anyhow::Result;
use std::borrow::Cow;

use anyhow::anyhow;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum};
use arroyo_rpc::{CheckpointEvent, ControlResp};
use arroyo_types::*;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use tracing::warn;
pub(crate) mod recovery;
use recovery::{PendingCommit, RecoveryState};

use rdkafka::producer::{DeliveryFuture, FutureRecord, Producer};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, AsDisplayable, DisplayableOperator};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use prost::Message;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::time::{Duration, SystemTime};

use super::{Context, FutureProducer, SinkCommitMode};

#[cfg(test)]
mod test;

pub struct KafkaSinkFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub consistency_mode: ConsistencyMode,
    pub timestamp_field: Option<String>,
    pub timestamp_col: Option<usize>,
    pub key_field: Option<String>,
    pub key_col: Option<usize>,
    pub producer: Option<FutureProducer>,
    pub write_futures: Vec<DeliveryFuture>,
    pub client_config: HashMap<String, String>,
    pub context: Context,
    pub serializer: ArrowSerializer,
    pub recovery_topic: Option<String>,
    pub recovery_state: Option<RecoveryState>,
}

pub enum ConsistencyMode {
    AtLeastOnce,
    ExactlyOnce {
        next_transaction_index: usize,
        producer_to_complete: Option<FutureProducer>,
    },
}

impl Display for ConsistencyMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyMode::AtLeastOnce => write!(f, "AtLeastOnce"),
            ConsistencyMode::ExactlyOnce { .. } => write!(f, "ExactlyOnce"),
        }
    }
}

impl From<SinkCommitMode> for ConsistencyMode {
    fn from(commit_mode: SinkCommitMode) -> Self {
        match commit_mode {
            SinkCommitMode::AtLeastOnce => ConsistencyMode::AtLeastOnce,
            SinkCommitMode::ExactlyOnce => ConsistencyMode::ExactlyOnce {
                next_transaction_index: 0,
                producer_to_complete: None,
            },
        }
    }
}

impl KafkaSinkFunc {
    fn set_timestamp_col(&mut self, schema: &ArroyoSchema) {
        if let Some(f) = &self.timestamp_field {
            if let Ok(f) = schema.schema.field_with_name(f) {
                match f.data_type() {
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        self.timestamp_col = Some(schema.schema.index_of(f.name()).unwrap());
                        return;
                    }
                    _ => {
                        warn!(
                            "Kafka sink configured with timestamp_field '{f}', but it has type \
                        {}, not TIMESTAMP... ignoring",
                            f.data_type()
                        );
                    }
                }
            } else {
                warn!(
                    "Kafka sink configured with timestamp_field '{f}', but that \
                does not appear in the schema... ignoring"
                );
            }
        }

        self.timestamp_col = Some(schema.timestamp_index);
    }

    fn set_key_col(&mut self, schema: &ArroyoSchema) {
        if let Some(f) = &self.key_field {
            if let Ok(f) = schema.schema.field_with_name(f) {
                if matches!(f.data_type(), DataType::Utf8) {
                    self.key_col = Some(schema.schema.index_of(f.name()).unwrap());
                } else {
                    warn!(
                        "Kafka sink configured with key_field '{f}', but it has type \
                {}, not TEXT... ignoring",
                        f.data_type()
                    );
                }
            } else {
                warn!(
                    "Kafka sink configured with key_field '{f}', but that \
                does not appear in the schema... ignoring"
                );
            }
        }
    }

    fn producer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set(
                "queue.buffering.max.kbytes",
                recovery::DEFAULT_QUEUE_KBYTES.to_string(),
            );
        for (key, value) in &self.client_config {
            config.set(key, value);
        }
        config
    }
    fn transactional_id(&self, task: &TaskInfo, index: usize) -> String {
        format!(
            "streamr-v4-id-{}-{}-{}-{}-{}",
            task.job_id,
            task.operator_id,
            self.topic,
            task.task_index,
            index % 2
        )
    }
    fn transaction_client(&self, task: &TaskInfo, index: usize) -> Result<FutureProducer> {
        let mut config = self.producer_config();
        config
            .set("enable.idempotence", "true")
            .set("transactional.id", self.transactional_id(task, index));
        let producer: FutureProducer = config.create_with_context(self.context.clone())?;
        Ok(producer)
    }
    fn transaction_producer(&self, task: &TaskInfo, index: usize) -> Result<FutureProducer> {
        let producer = self.transaction_client(task, index)?;
        recovery::retry_transaction(|| {
            producer.init_transactions(Timeout::After(Duration::from_secs(10)))
        })?;
        Ok(producer)
    }
    fn marker_key(&self, task: &TaskInfo) -> String {
        format!(
            "streamr-v4/{}/{}/{}/{}",
            task.job_id, task.operator_id, self.topic, task.task_index
        )
    }
    fn init_producer(&mut self, task: &TaskInfo) -> Result<()> {
        if let ConsistencyMode::ExactlyOnce {
            next_transaction_index,
            ..
        } = &self.consistency_mode
        {
            let index = *next_transaction_index;
            let producer = self.transaction_producer(task, index)?;
            producer.begin_transaction()?;
            if let ConsistencyMode::ExactlyOnce {
                next_transaction_index,
                ..
            } = &mut self.consistency_mode
            {
                *next_transaction_index = next_transaction_index
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("Kafka transaction index exhausted"))?;
            }
            self.producer = Some(producer);
        } else {
            self.producer = Some(
                self.producer_config()
                    .create_with_context(self.context.clone())?,
            );
        }
        Ok(())
    }

    async fn flush(&mut self, ctx: &mut OperatorContext) {
        self.producer
            .as_ref()
            .unwrap()
            // FutureProducer has a thread polling every 100ms,
            // but better to send a signal immediately
            // Duration 0 timeouts are non-blocking,
            .poll(Timeout::After(Duration::ZERO));

        // ensure all messages were delivered before finishing the checkpoint
        for future in self.write_futures.drain(..) {
            let delivery = future.await.unwrap();
            if let Err((e, _)) = delivery {
                ctx.error_reporter
                    .report_error("Kafka producer shut down", e.to_string())
                    .await;
                panic!("Kafka producer shut down: {e:?}");
            }
        }
    }

    async fn publish(
        &mut self,
        ts: Option<i64>,
        k: Option<Vec<u8>>,
        v: Vec<u8>,
        ctx: &mut OperatorContext,
    ) -> Result<()> {
        // A record larger than the producer's entire byte queue cannot ever
        // become sendable: fail rather than retrying QueueFull forever.
        let queue_kbytes = self
            .client_config
            .get("queue.buffering.max.kbytes")
            .map(|s| s.parse::<usize>())
            .transpose()?
            .unwrap_or(recovery::DEFAULT_QUEUE_KBYTES);
        recovery::require(
            v.len()
                .checked_add(k.as_ref().map_or(0, Vec::len))
                .is_some_and(|n| n <= queue_kbytes.saturating_mul(1024)),
            "Kafka output record exceeds configured producer byte queue",
        )?;
        if self.write_futures.len() >= recovery::MAX_DELIVERY_FUTURES {
            self.flush(ctx).await;
        }
        let mut rec = {
            let mut rec = FutureRecord::<Vec<u8>, Vec<u8>>::to(&self.topic);
            if let Some(ts) = ts {
                rec = rec.timestamp(ts);
            }
            if let Some(k) = k.as_ref() {
                rec = rec.key(k);
            }

            rec.payload(&v)
        };

        loop {
            match self.producer.as_mut().unwrap().send_result(rec) {
                Ok(future) => {
                    self.write_futures.push(future);
                    return Ok(());
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), f)) => {
                    rec = f;
                }
                Err((e, _)) => {
                    ctx.error_reporter
                        .report_error("Could not write to Kafka", format!("{e:?}"))
                        .await;

                    panic!("Failed to write to kafka: {e:?}");
                }
            }

            // back off and retry
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[async_trait]
impl ArrowOperator for KafkaSinkFunc {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("KafkaSinkFunc"),
            fields: vec![
                ("topic", self.topic.as_str().into()),
                ("bootstrap_servers", self.bootstrap_servers.as_str().into()),
                (
                    "consistency_mode",
                    AsDisplayable::Display(&self.consistency_mode),
                ),
                (
                    "timestamp_field",
                    AsDisplayable::Debug(&self.timestamp_field),
                ),
                ("key_field", AsDisplayable::Debug(&self.key_field)),
                ("client_config", AsDisplayable::Debug(&self.client_config)),
            ],
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.is_committing() {
            ["i"]
                .into_iter()
                .map(|name| {
                    (
                        name.to_string(),
                        TableConfig {
                            table_type: TableEnum::GlobalKeyValue.into(),
                            config: GlobalKeyedTableConfig {
                                table_name: name.to_string(),
                                description: "compact prepared Kafka transaction".to_string(),
                                uses_two_phase_commit: name == "i",
                            }
                            .encode_to_vec(),
                            state_version: 0,
                        },
                    )
                })
                .collect()
        } else {
            HashMap::new()
        }
    }

    fn is_committing(&self) -> bool {
        matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce { .. })
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.set_timestamp_col(&ctx.in_schemas[0]);
        self.set_key_col(&ctx.in_schemas[0]);

        if self.is_committing() {
            let topic = self
                .recovery_topic
                .clone()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("exactly_once requires explicit sink.recovery_topic"))?;
            recovery::require(topic != self.topic, "Invalid Kafka recovery topic")?;
            recovery::validate_topic(&self.producer_config(), self.context.clone(), &topic).await?;
            let saved = ctx
                .table_manager
                .get_global_keyed_state::<u32, Vec<u8>>("i")
                .await.map_err(|e| anyhow!("Cannot restore Kafka recovery state; unsupported legacy or corrupt checkpoint: {e}"))?
                .get(&ctx.task_info.task_index)
                .cloned();
            recovery::require(
                ctx.restore_commit_epoch.is_none() || saved.is_some(),
                "Kafka selected pending checkpoint is missing transaction state",
            )?;
            let state = if let Some(bytes) = saved {
                let state = recovery::decode_state(&bytes)?;
                recovery::require(
                    ctx.restore_commit_epoch
                        .is_none_or(|epoch| epoch == state.checkpoint_epoch),
                    "Kafka selected pending checkpoint epoch differs from transaction state",
                )?;
                // Preserve the selected checkpoint's pending producer identity.
                // Only fence the alternate slot containing abandoned later work.
                let _ = self.transaction_producer(&ctx.task_info, state.next_transaction_index)?;
                tracing::info!(job_id = %ctx.task_info.job_id, epoch = state.checkpoint_epoch,
                    generation = %state.generation, operator_id = %ctx.task_info.operator_id,
                    subtask_index = ctx.task_info.task_index,
                    transactional_id = %self.transactional_id(&ctx.task_info, state.next_transaction_index),
                    transaction_index = state.next_transaction_index,
                    "Kafka alternate transaction fenced");
                if ctx.restore_commit_epoch.is_none() {
                    let marker_epoch = recovery::scan_marker(
                        &self.producer_config(),
                        self.context.clone(),
                        &topic,
                        &self.marker_key(&ctx.task_info),
                        &state.generation,
                    )
                    .await?;
                    recovery::require(
                        marker_epoch == state.checkpoint_epoch,
                        "Kafka restored checkpoint marker differs; refusing missing commit or rollback",
                    )?;
                }
                state
            } else {
                let state = RecoveryState {
                    version: recovery::PROTOCOL_VERSION,
                    generation: uuid::Uuid::now_v7().to_string(),
                    next_transaction_index: 1,
                    checkpoint_epoch: 0,
                };
                let producer = self.transaction_producer(&ctx.task_info, 0)?;
                producer.begin_transaction().map_err(anyhow::Error::from)?;
                recovery::send_marker(
                    &producer,
                    &topic,
                    &self.marker_key(&ctx.task_info),
                    &state.generation,
                    0,
                )
                .await?;
                recovery::retry_transaction(|| {
                    producer.commit_transaction(Timeout::After(Duration::from_secs(10)))
                })?;
                state
            };
            if let ConsistencyMode::ExactlyOnce {
                next_transaction_index,
                ..
            } = &mut self.consistency_mode
            {
                *next_transaction_index = state.next_transaction_index;
            }
            self.recovery_state = Some(state);
        }
        self.init_producer(&ctx.task_info)?;
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let values = self.serializer.serialize(&batch);
        let timestamps = batch
            .column(
                self.timestamp_col
                    .expect("timestamp column not initialized!"),
            )
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>();

        let keys = self.key_col.map(|i| batch.column(i).as_string::<i32>());

        for (i, v) in values.enumerate() {
            // kafka timestamp as unix millis
            let timestamp = timestamps.map(|ts| {
                if ts.is_null(i) {
                    0
                } else {
                    ts.value(i) / 1_000_000
                }
            });
            // TODO: this copy should be unnecessary but likely needs a custom trait impl
            let key = keys.map(|k| k.value(i).as_bytes().to_vec());
            self.publish(timestamp, key, v, ctx).await?;
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // Do not replace the prepared transaction until its commit has been
        // acknowledged. The controller also permits only one pending checkpoint.
        if let ConsistencyMode::ExactlyOnce {
            producer_to_complete,
            ..
        } = &self.consistency_mode
        {
            recovery::require(
                producer_to_complete.is_none(),
                "Kafka checkpoint overlaps pending commit",
            )?;
        }
        self.flush(ctx).await;
        if self.is_committing() {
            let ConsistencyMode::ExactlyOnce {
                next_transaction_index,
                ..
            } = &self.consistency_mode
            else {
                unreachable!()
            };
            let index = *next_transaction_index;
            let state = RecoveryState {
                next_transaction_index: index,
                checkpoint_epoch: barrier.epoch,
                ..self
                    .recovery_state
                    .clone()
                    .ok_or_else(|| anyhow!("Kafka recovery state missing"))?
            };
            let topic = self.recovery_topic.as_ref().unwrap();
            recovery::send_marker(
                self.producer.as_ref().unwrap(),
                topic,
                &self.marker_key(&ctx.task_info),
                &state.generation,
                barrier.epoch,
            )
            .await?;
            // The delivered marker proves a registered broker transaction even
            // when this epoch contains no application records.
            let (producer_id, producer_epoch) = recovery::prepare(self.producer.as_ref().unwrap())?;
            let pending = PendingCommit {
                version: recovery::PROTOCOL_VERSION,
                generation: state.generation.clone(),
                epoch: barrier.epoch,
                transaction_index: index - 1,
                producer_id,
                producer_epoch,
            };
            tracing::info!(job_id = %ctx.task_info.job_id, epoch = barrier.epoch,
                operator_id = %ctx.task_info.operator_id,
                transactional_id = %self.transactional_id(&ctx.task_info, pending.transaction_index),
                transaction_index = pending.transaction_index, producer_id, producer_epoch,
                "Kafka compact transaction prepared");
            let pending_bytes = recovery::encode_pending(&pending)?;
            ctx.table_manager
                .insert_committing_data("i", pending_bytes)
                .await;
            ctx.table_manager
                .get_global_keyed_state::<u32, Vec<u8>>("i")
                .await?
                .insert(
                    ctx.task_info.task_index,
                    serde_json::to_vec(&state).map_err(anyhow::Error::from)?,
                )
                .await;
            if let ConsistencyMode::ExactlyOnce {
                producer_to_complete,
                ..
            } = &mut self.consistency_mode
            {
                *producer_to_complete = self.producer.take();
            }
            self.recovery_state = Some(state);
            self.init_producer(&ctx.task_info)?;
        }
        Ok(())
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        if !self.is_committing() {
            warn!("received commit for nontransactional sink");
            return Ok(());
        }
        let bytes = _commit_data
            .get("i")
            .and_then(|m| m.get(&ctx.task_info.task_index))
            .ok_or_else(|| anyhow!("Missing compact Kafka prepared transaction metadata"))?;
        let pending = recovery::decode_pending(bytes, epoch)?;
        let state = self
            .recovery_state
            .as_ref()
            .ok_or_else(|| anyhow!("Kafka recovery state missing"))?;
        recovery::require(
            state.checkpoint_epoch == epoch
                && state.generation == pending.generation
                && pending.transaction_index.checked_add(1) == Some(state.next_transaction_index),
            "Kafka pending transaction/state mismatch",
        )?;
        let topic = self.recovery_topic.as_ref().unwrap();
        let key = self.marker_key(&ctx.task_info);
        let committing = if let ConsistencyMode::ExactlyOnce {
            producer_to_complete,
            ..
        } = &mut self.consistency_mode
        {
            producer_to_complete.take()
        } else {
            None
        };
        recovery::fault_pause(&ctx.task_info.job_id, epoch, "before", true).await;
        if let Some(producer) = committing {
            recovery::retry_transaction(|| {
                producer.commit_transaction(Timeout::After(Duration::from_secs(10)))
            })?;
        } else {
            // Reattach without InitProducerId: fencing here would abort the
            // transaction whose durable checkpoint we have been asked to commit.
            let producer = self.transaction_client(&ctx.task_info, pending.transaction_index)?;
            tracing::info!(job_id = %ctx.task_info.job_id, epoch,
                operator_id = %ctx.task_info.operator_id,
                transactional_id = %self.transactional_id(&ctx.task_info, pending.transaction_index),
                transaction_index = pending.transaction_index, producer_id = pending.producer_id,
                producer_epoch = pending.producer_epoch, "Kafka compact transaction resume");
            recovery::resume(&producer, &pending)?;
            let commit_result = recovery::retry_transaction(|| {
                producer.commit_transaction(Timeout::After(Duration::from_secs(10)))
            });
            let marker_epoch = recovery::scan_marker(
                &self.producer_config(),
                self.context.clone(),
                topic,
                &key,
                &pending.generation,
            )
            .await?;
            recovery::validate_marker_epoch(marker_epoch, epoch)?;
            if marker_epoch != epoch {
                return Err(anyhow!("Kafka prepared transaction recovery failed: {}; no exact committed marker (observed {marker_epoch}, expected {epoch})",
                    commit_result.err().map_or_else(|| "EndTxn acknowledged".to_string(), |e| e.to_string())).into());
            }
            tracing::info!(job_id = %ctx.task_info.job_id, epoch,
                operator_id = %ctx.task_info.operator_id,
                transactional_id = %self.transactional_id(&ctx.task_info, pending.transaction_index),
                transaction_index = pending.transaction_index,
                producer_id = pending.producer_id, producer_epoch = pending.producer_epoch,
                "Kafka prepared transaction recovered");
        }
        recovery::fault_pause(&ctx.task_info.job_id, epoch, "after", true).await;
        let checkpoint_event = ControlResp::CheckpointEvent(CheckpointEvent {
            checkpoint_epoch: epoch as u64,
            operator_idx: ctx.task_info.operator_idx,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_idx: ctx.task_info.task_index,
            time: SystemTime::now(),
            event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
        Ok(())
    }

    async fn on_close(
        &mut self,
        _: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush(ctx).await;
        Ok(())
    }
}
