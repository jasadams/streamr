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
use recovery::{PendingCommit, RecoveryState, ReplayRecord};

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
    pub recovery_max_bytes: usize,
    pub recovery_state: Option<RecoveryState>,
    pub journal: Vec<ReplayRecord>,
    pub journal_bytes: usize,
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
        config.set("bootstrap.servers", &self.bootstrap_servers);
        for (key, value) in &self.client_config {
            config.set(key, value);
        }
        config
    }
    fn transaction_producer(&self, task: &TaskInfo, index: usize) -> Result<FutureProducer> {
        let mut config = self.producer_config();
        config.set("enable.idempotence", "true").set(
            "transactional.id",
            format!(
                "streamr-v2-id-{}-{}-{}-{}-{}",
                task.job_id,
                task.operator_id,
                self.topic,
                task.task_index,
                index % 2
            ),
        );
        let producer: FutureProducer = config.create_with_context(self.context.clone())?;
        recovery::retry_transaction(|| {
            producer.init_transactions(Timeout::After(Duration::from_secs(10)))
        })?;
        Ok(producer)
    }
    fn marker_key(&self, task: &TaskInfo) -> String {
        format!(
            "streamr-v2/{}/{}/{}/{}",
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
        let transactional = self.is_committing();
        for (index, future) in self.write_futures.drain(..).enumerate() {
            let delivery = future.await.unwrap();
            if let Ok((partition, _)) = &delivery
                && transactional
            {
                self.journal[index].partition = *partition;
            }
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
        if self.is_committing() {
            let next_bytes = recovery::check_record_budget(
                self.journal_bytes,
                v.len(),
                k.as_ref().map_or(0, Vec::len),
                self.recovery_max_bytes,
            )?;
            let record = ReplayRecord {
                timestamp: ts,
                key: k.clone(),
                payload: v.clone(),
                partition: -1,
            };
            self.journal_bytes = next_bytes;
            self.journal.push(record);
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
            single_item_hash_map(
                "i".to_string(),
                TableConfig {
                    table_type: TableEnum::GlobalKeyValue.into(),
                    config: GlobalKeyedTableConfig {
                        table_name: "i".to_string(),
                        description: "index for transactional ids".to_string(),
                        uses_two_phase_commit: true,
                    }
                    .encode_to_vec(),
                    state_version: 0,
                },
            )
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
            recovery::require(
                topic != self.topic && self.recovery_max_bytes > 0,
                "Invalid Kafka recovery topic/budget",
            )?;
            recovery::validate_topic(&self.producer_config(), self.context.clone(), &topic).await?;
            let saved = ctx
                .table_manager
                .get_global_keyed_state::<u32, Vec<u8>>("i")
                .await.map_err(|e| anyhow!("Cannot restore Kafka recovery state; unsupported legacy or corrupt checkpoint: {e}"))?
                .get(&ctx.task_info.task_index)
                .cloned();
            let state = if let Some(bytes) = saved {
                let state = recovery::decode_state(&bytes)?;
                // Fence both the checkpoint's pending transaction and subsequent abandoned work.
                let _ =
                    self.transaction_producer(&ctx.task_info, state.next_transaction_index - 1)?;
                let _ = self.transaction_producer(&ctx.task_info, state.next_transaction_index)?;
                let marker_epoch = recovery::scan_marker(
                    &self.producer_config(),
                    self.context.clone(),
                    &topic,
                    &self.marker_key(&ctx.task_info),
                    &state.generation,
                )
                .await?;
                recovery::validate_marker_epoch(marker_epoch, state.checkpoint_epoch)?;
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
            let pending = PendingCommit {
                version: recovery::PROTOCOL_VERSION,
                generation: state.generation.clone(),
                epoch: barrier.epoch,
                transaction_index: index - 1,
                records: std::mem::take(&mut self.journal),
            };
            self.journal_bytes = 0;
            let pending_bytes = recovery::encode_pending(&pending)?;
            let topic = self.recovery_topic.as_ref().unwrap();
            recovery::send_marker(
                self.producer.as_ref().unwrap(),
                topic,
                &self.marker_key(&ctx.task_info),
                &state.generation,
                barrier.epoch,
            )
            .await?;
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
                recovery::require(
                    producer_to_complete.is_none(),
                    "Kafka checkpoint overlaps pending commit",
                )?;
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
        let bytes = _commit_data.get("i").and_then(|m|m.get(&ctx.task_info.task_index)).ok_or_else(|| anyhow!("Missing Kafka replay metadata; legacy committing checkpoints cannot recover safely"))?;
        let pending = recovery::decode_pending(bytes, epoch, self.recovery_max_bytes)?;
        let state = self
            .recovery_state
            .as_ref()
            .ok_or_else(|| anyhow!("Kafka recovery state missing"))?;
        recovery::require(
            state.generation == pending.generation
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
        recovery::fault_pause(
            &ctx.task_info.job_id,
            epoch,
            "before",
            !pending.records.is_empty(),
        )
        .await;
        if let Some(producer) = committing {
            recovery::retry_transaction(|| {
                producer.commit_transaction(Timeout::After(Duration::from_secs(10)))
            })?;
        } else {
            let producer = self.transaction_producer(&ctx.task_info, pending.transaction_index)?;
            let committed_epoch = recovery::scan_marker(
                &self.producer_config(),
                self.context.clone(),
                topic,
                &key,
                &pending.generation,
            )
            .await?;
            recovery::validate_marker_epoch(committed_epoch, epoch)?;
            tracing::info!(job_id = %ctx.task_info.job_id, epoch, records = pending.records.len(), action = if committed_epoch < epoch { "replay" } else { "skipped" }, "Kafka commit recovery");
            if committed_epoch < epoch {
                recovery::replay(&producer, &self.topic, &pending).await?;
                recovery::send_marker(&producer, topic, &key, &pending.generation, epoch).await?;
                recovery::retry_transaction(|| {
                    producer.commit_transaction(Timeout::After(Duration::from_secs(10)))
                })?;
            }
        }
        recovery::fault_pause(
            &ctx.task_info.job_id,
            epoch,
            "after",
            !pending.records.is_empty(),
        )
        .await;
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
