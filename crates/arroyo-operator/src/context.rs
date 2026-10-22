use crate::{RateLimiter, server_for_hash_array};
use arrow::array::{Array, PrimitiveArray, RecordBatch};
use arrow::compute::{partition, sort_to_indices, take};
use arrow::datatypes::UInt64Type;
use arroyo_formats::de::{ArrowDeserializer, FieldValueType};
use arroyo_metrics::{QueueGauges, TaskCounters, register_queue_gauge};
use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult, TaskError};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{TableConfig, TaskCheckpointEventType};
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::{
    CompactionResult, ControlMessage, ControlResp, MetadataField, MetadataOrManifest, get_hasher,
};
use arroyo_state::tables::table_manager::TableManager;
use arroyo_types::{
    ArrowMessage, ChainInfo, CheckpointBarrier, SignalMessage, TaskInfo, Watermark,
};
use async_trait::async_trait;
use datafusion::common::hash_utils;
use rand::Rng;
use std::collections::HashMap;
use std::mem::size_of_val;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{trace, warn};

use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::EventResponse;

pub type QueueItem = ArrowMessage;

pub struct WatermarkHolder {
    // This is the last watermark with an actual value; this helps us keep track of the watermark we're at even
    // if we're currently idle
    last_present_watermark: Option<SystemTime>,
    cur_watermark: Option<Watermark>,
    watermarks: Vec<Option<Watermark>>,
}

impl WatermarkHolder {
    pub fn new(watermarks: Vec<Option<Watermark>>) -> Self {
        let mut s = Self {
            last_present_watermark: None,
            cur_watermark: None,
            watermarks,
        };
        s.update_watermark();

        s
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.cur_watermark
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.last_present_watermark
    }

    fn update_watermark(&mut self) {
        self.cur_watermark =
            self.watermarks
                .iter()
                .try_fold(Watermark::Idle, |current, next| match (current, (*next)?) {
                    (Watermark::EventTime(cur), Watermark::EventTime(next)) => {
                        Some(Watermark::EventTime(cur.min(next)))
                    }
                    (Watermark::Idle, Watermark::EventTime(t))
                    | (Watermark::EventTime(t), Watermark::Idle) => Some(Watermark::EventTime(t)),
                    (Watermark::Idle, Watermark::Idle) => Some(Watermark::Idle),
                });

        if let Some(Watermark::EventTime(t)) = self.cur_watermark {
            self.last_present_watermark = Some(t);
        }
    }

    pub fn set(&mut self, idx: usize, watermark: Watermark) -> Option<Option<Watermark>> {
        *(self.watermarks.get_mut(idx)?) = Some(watermark);
        self.update_watermark();
        Some(self.cur_watermark)
    }
}

/// A wrapper for an UnboundedSender<QueueItem> that bounds by the number of rows within
/// a batch rather than the number of batches
#[derive(Clone)]
pub struct BatchSender {
    size: u32,
    tx: UnboundedSender<QueueItem>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[inline]
fn message_count(item: &QueueItem, size: u32) -> u32 {
    match item {
        QueueItem::Data(d) => (d.num_rows() as u32).min(size),
        QueueItem::Signal(_) => 1,
        QueueItem::Event(response) => response.queue_cost(size),
    }
}

#[inline]
fn message_bytes(item: &QueueItem) -> u64 {
    match item {
        QueueItem::Data(d) => d.get_array_memory_size() as u64,
        QueueItem::Signal(s) => size_of_val(s) as u64,
        QueueItem::Event(response) => response.retained_bytes() as u64,
    }
}

impl BatchSender {
    pub async fn send(&self, item: QueueItem) -> Result<(), SendError<QueueItem>> {
        if matches!(&item, QueueItem::Event(_)) && self.size == 0 {
            return Err(SendError(item));
        }
        // Ensure that every message is sendable, even if it's bigger than our max size
        let count = message_count(&item, self.size);
        loop {
            if self.tx.is_closed() {
                return Err(SendError(item));
            }

            let cur = self.queued_messages.load(Ordering::Acquire);
            if cur as usize + count as usize <= self.size as usize {
                match self.queued_messages.compare_exchange(
                    cur,
                    cur + count,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        self.queued_bytes
                            .fetch_add(message_bytes(&item), Ordering::AcqRel);
                        let bytes = message_bytes(&item);
                        return self.tx.send(item).inspect_err(|_| {
                            // No receiver accepted this item; undo its diagnostic counters.
                            self.queued_messages.fetch_sub(count, Ordering::SeqCst);
                            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                        });
                    }
                    Err(_) => {
                        // try again
                        continue;
                    }
                }
            } else {
                // first register the notify listener
                let notified = self.notify.notified();

                // then recheck -- there may now be space since we checked
                let cur = self.queued_messages.load(Ordering::Acquire);
                if cur as usize + count as usize <= self.size as usize {
                    // space is now available, retry immediately
                    continue;
                }

                // if not, we're now guaranteed to receive the notification if space is made
                // available
                tokio::select! {
                    _ = notified => {},
                    _ = self.tx.closed() => return Err(SendError(item)),
                }
            }
        }
    }

    pub fn capacity(&self) -> u32 {
        self.size
            .saturating_sub(self.queued_messages.load(Ordering::Relaxed))
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

pub struct BatchReceiver {
    size: u32,
    rx: UnboundedReceiver<QueueItem>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl BatchReceiver {
    pub fn size(&self) -> u32 {
        self.size
    }

    pub async fn recv(&mut self) -> Option<QueueItem> {
        let item = self.rx.recv().await;
        if let Some(item) = &item {
            let count = message_count(item, self.size);
            self.queued_messages.fetch_sub(count, Ordering::SeqCst);
            self.queued_bytes
                .fetch_sub(message_bytes(item), Ordering::AcqRel);
            self.notify.notify_waiters();
        }
        item
    }
}

pub fn batch_bounded(size: u32) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = unbounded_channel();
    let notify = Arc::new(Notify::new());
    let queued_messages = Arc::new(AtomicU32::new(0));
    let queued_bytes = Arc::new(AtomicU64::new(0));
    (
        BatchSender {
            size,
            tx,
            queued_messages: queued_messages.clone(),
            queued_bytes: queued_bytes.clone(),
            notify: notify.clone(),
        },
        BatchReceiver {
            size,
            rx,
            notify,
            queued_bytes,
            queued_messages,
        },
    )
}

fn queue_closed_error() -> DataflowError {
    DataflowError::InternalOperatorError {
        error: "downstream graph queue closed",
        message: "could not deliver message".into(),
    }
}

pub struct SourceContext {
    pub out_schema: Arc<ArroyoSchema>,
    pub control_tx: Sender<ControlResp>,
    pub control_rx: Receiver<ControlMessage>,
    pub chain_info: Arc<ChainInfo>,
    pub task_info: Arc<TaskInfo>,
    pub table_manager: TableManager,
    pub watermarks: WatermarkHolder,
}

impl SourceContext {
    pub fn from_operator(
        ctx: OperatorContext,
        chain_info: Arc<ChainInfo>,
        control_rx: Receiver<ControlMessage>,
    ) -> Self {
        Self {
            out_schema: ctx.out_schema.expect("sources must have downstream nodes"),
            control_tx: ctx.control_tx,
            control_rx,
            chain_info,
            task_info: ctx.task_info,
            table_manager: ctx.table_manager,
            watermarks: ctx.watermarks,
        }
    }

    pub async fn load_compacted(&mut self, compaction: CompactionResult) {
        //TODO: support compaction in the table manager
        self.table_manager.load_compacted(&compaction).await;
    }

    pub async fn report_nonfatal_error(&mut self, error: DataflowError) {
        self.control_tx
            .send(ControlResp::Error {
                task_id: self.task_info.operator_idx,
                subtask_idx: self.task_info.task_index,
                operator_id: self.task_info.operator_id.clone(),
                message: "".to_string(),
                details: error.to_string(),
            })
            .await
            .unwrap();
    }
}

pub struct SourceCollector {
    deserializer: Option<ArrowDeserializer>,
    buffered_error: Option<TaskError>,
    error_rate_limiter: RateLimiter,
    pub out_schema: Arc<ArroyoSchema>,
    pub(crate) collector: ArrowCollector,
    control_tx: Sender<ControlResp>,
    task_info: Arc<TaskInfo>,
    connection_id: Option<String>,
}

impl SourceCollector {
    pub fn new(
        out_schema: Arc<ArroyoSchema>,
        collector: ArrowCollector,
        control_tx: Sender<ControlResp>,
        task_info: &Arc<TaskInfo>,
    ) -> Self {
        Self {
            out_schema,
            collector,
            control_tx,
            task_info: task_info.clone(),
            deserializer: None,
            buffered_error: None,
            error_rate_limiter: RateLimiter::new(),
            connection_id: None,
        }
    }

    pub fn set_connection_id(&mut self, connection_id: String) {
        self.connection_id = Some(connection_id);
    }

    pub async fn collect(&mut self, record: RecordBatch) -> DataflowResult<()> {
        self.collector.collect(record).await
    }

    pub fn initialize_deserializer_with_resolver(
        &mut self,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
        metadata_fields: &[MetadataField],
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) {
        self.deserializer = Some(ArrowDeserializer::with_schema_resolver(
            format,
            framing,
            self.out_schema.clone(),
            metadata_fields,
            bad_data.unwrap_or_default(),
            schema_resolver,
        ));
    }

    pub fn initialize_deserializer(
        &mut self,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
        metadata_fields: &[MetadataField],
    ) {
        if self.deserializer.is_some() {
            panic!("Deserialize already initialized");
        }

        self.deserializer = Some(ArrowDeserializer::new(
            format,
            self.out_schema.clone(),
            metadata_fields,
            framing,
            bad_data.unwrap_or_default(),
        ));
    }

    pub fn should_flush(&self) -> bool {
        self.deserializer
            .as_ref()
            .map(|d| d.should_flush())
            .unwrap_or(false)
    }

    pub async fn deserialize_slice(
        &mut self,
        msg: &[u8],
        time: SystemTime,
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) -> DataflowResult<()> {
        let deserializer = self
            .deserializer
            .as_mut()
            .expect("deserializer not initialized!");

        let errors = deserializer
            .deserialize_slice(msg, time, additional_fields)
            .await;
        self.collect_source_errors(errors).await?;

        Ok(())
    }

    /// Handling errors and rate limiting error reporting.
    /// Considers the `bad_data` option to determine whether to drop or fail on bad data.
    async fn collect_source_errors(&mut self, errors: Vec<DataflowError>) -> DataflowResult<()> {
        let bad_data = self
            .deserializer
            .as_ref()
            .expect("deserializer not initialized")
            .bad_data();

        for error in errors {
            match (bad_data, error) {
                (BadData::Drop { .. }, DataflowError::DataError { count, details }) => {
                    if config().pipeline.store_deserialization_errors {
                        self.error_rate_limiter
                            .rate_limit(|| async {
                                warn!("Dropping invalid data ({count}): {details}");
                                self.control_tx
                                    .send(ControlResp::Error {
                                        task_id: self.task_info.operator_idx,
                                        operator_id: self.task_info.operator_id.clone(),
                                        subtask_idx: self.task_info.task_index,
                                        message: format!("Dropping invalid data ({count})"),
                                        details,
                                    })
                                    .await
                                    .unwrap();
                            })
                            .await;
                    }

                    TaskCounters::DeserializationErrors.for_connection(
                        &self.collector.chain_info,
                        self.connection_id.as_deref().unwrap_or_default(),
                        |c| c.inc_by(count as u64),
                    );
                }
                (_, e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    // Completion must inspect a sticky failure without flushing buffered rows:
    // Immediate shutdown deliberately does not deliver or wait on downstreams.
    pub(crate) fn check_delivery_error(&self) -> Result<(), TaskError> {
        match &self.buffered_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn delivery_error(&self, error: DataflowError) -> TaskError {
        self.buffered_error.clone().unwrap_or_else(|| error.into())
    }

    async fn report_delivery_error(&mut self, error: &DataflowError) {
        if self.buffered_error.is_none() {
            let error = TaskError::from(error);
            self.buffered_error = Some(error.clone());
            // Report before returning: some connectors ignore a flush error
            // and advance their local source checkpoint immediately afterward.
            self.control_tx
                .send(ControlResp::TaskFailed {
                    task_id: self.task_info.operator_idx,
                    subtask_idx: self.task_info.task_index,
                    error,
                })
                .await
                .ok();
        }
    }

    pub async fn flush_buffer(&mut self) -> DataflowResult<()> {
        self.check_delivery_error()
            .map_err(|error| DataflowError::InternalOperatorError {
                error: "source delivery previously failed",
                message: error.message,
            })?;
        if let Some(deserializer) = self.deserializer.as_mut() {
            let (batch, errors) = deserializer.flush_buffer();
            if !errors.is_empty() {
                self.collect_source_errors(errors).await?;
            }

            if let Some(batch) = batch
                && let Err(error) = self.collector.collect(batch).await
            {
                // The deserializer has consumed this batch. Remember the
                // first failure even if a connector ignores the returned Err,
                // so a later barrier/completion cannot certify lost rows.
                self.report_delivery_error(&error).await;
                return Err(error);
            }
        }

        Ok(())
    }

    pub(crate) async fn broadcast_checked(&mut self, message: SignalMessage) -> DataflowResult<()> {
        self.flush_buffer().await?;
        self.collector.broadcast_checked(message).await
    }

    pub async fn broadcast(&mut self, message: SignalMessage) {
        if let Err(error) = self.broadcast_checked(message).await {
            // This compatibility API cannot return an error. Preserve/report
            // the first typed failure and make completion/next flush fail too.
            self.report_delivery_error(&error).await;
        }
    }
}

pub async fn send_checkpoint_event(
    tx: &Sender<ControlResp>,
    info: &TaskInfo,
    barrier: CheckpointBarrier,
    event_type: TaskCheckpointEventType,
) {
    // These messages are received by the engine control thread,
    // which then sends a TaskCheckpointEventReq to the controller.
    tx.send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
        checkpoint_epoch: barrier.epoch as u64,
        operator_idx: info.operator_idx,
        operator_id: info.operator_id.clone(),
        subtask_idx: info.task_index,
        time: SystemTime::now(),
        event_type,
    }))
    .await
    .unwrap();
}

pub struct OperatorContext {
    /// Selected checkpoint still awaits coordinator-authorized external commits.
    pub restore_commit_epoch: Option<u32>,
    pub task_info: Arc<TaskInfo>,
    pub control_tx: Sender<ControlResp>,
    pub watermarks: WatermarkHolder,
    pub in_schemas: Vec<Arc<ArroyoSchema>>,
    pub out_schema: Option<Arc<ArroyoSchema>>,
    pub table_manager: TableManager,
    pub error_reporter: ErrorReporter,
}

#[derive(Clone)]
pub struct ErrorReporter {
    pub tx: Sender<ControlResp>,
    pub task_info: Arc<TaskInfo>,
}

impl ErrorReporter {
    pub async fn report_error(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.tx
            .send(ControlResp::Error {
                task_id: self.task_info.operator_idx,
                operator_id: self.task_info.operator_id.clone(),
                subtask_idx: self.task_info.task_index,
                message: message.into(),
                details: details.into(),
            })
            .await
            .unwrap();
    }
}

#[async_trait]
pub trait Collector: Send {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()>;
    async fn collect_response(&mut self, _response: EventResponse) -> DataflowResult<()> {
        Err(DataflowError::ArgumentError(
            "collector does not accept event responses".into(),
        ))
    }
    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()>;
}

#[derive(Clone)]
pub struct ArrowCollector {
    pub chain_info: Arc<ChainInfo>,
    out_schema: Option<Arc<ArroyoSchema>>,
    event_spec: Option<Arc<EventEdgeSpec>>,
    out_qs: Vec<Vec<BatchSender>>,
    tx_queue_rem_gauges: QueueGauges,
    tx_queue_size_gauges: QueueGauges,
    tx_queue_bytes_gauges: QueueGauges,
}

fn repartition<'a>(
    record: &'a RecordBatch,
    keys: Option<&'a Vec<usize>>,
    qs: usize,
) -> impl Iterator<Item = (usize, RecordBatch)> + 'a {
    // One destination is an identity operation: keep the original order and
    // complete logical input batch without sorting equal destination IDs.
    if qs == 1 {
        return (record.num_rows() != 0)
            .then(|| (0, record.clone()))
            .into_iter()
            .collect::<Vec<_>>()
            .into_iter();
    }
    let mut buf = vec![0; record.num_rows()];

    if let Some(keys) = keys {
        let keys: Vec<_> = keys.iter().map(|i| record.column(*i).clone()).collect();

        hash_utils::create_hashes(&keys[..], &get_hasher(), &mut buf).unwrap();
        let buf_array = PrimitiveArray::from(buf);

        let servers = server_for_hash_array(&buf_array, qs).unwrap();

        let indices = sort_to_indices(&servers, None, None).unwrap();
        let columns = record
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(record.schema(), columns).unwrap();
        let sorted_keys = take(&servers, &indices, None).unwrap();

        let partition: arrow::compute::Partitions =
            partition(vec![sorted_keys.clone()].as_slice()).unwrap();
        let typed_keys: &PrimitiveArray<UInt64Type> = sorted_keys.as_any().downcast_ref().unwrap();
        let result: Vec<_> = partition
            .ranges()
            .into_iter()
            .map(|range| {
                let server_batch = sorted.slice(range.start, range.end - range.start);
                let server_id = typed_keys.value(range.start) as usize;
                (server_id, server_batch)
            })
            .collect();
        result.into_iter()
    } else {
        let range_size = record.num_rows() / qs + 1;
        let rotation = rand::rng().random_range(0..qs);
        let result: Vec<_> = (0..qs)
            .filter_map(|i| {
                let start = i * range_size;
                let end = (i + 1) * range_size;
                if start >= record.num_rows() {
                    None
                } else {
                    let server_batch = record.slice(start, end.min(record.num_rows()) - start);
                    Some(((i + rotation) % qs, server_batch))
                }
            })
            .collect();
        result.into_iter()
    }
}

#[async_trait]
impl Collector for ArrowCollector {
    async fn collect(&mut self, record: RecordBatch) -> DataflowResult<()> {
        if let Some(spec) = &self.event_spec {
            if !spec.maintenance
                || record.schema() != spec.schema
                || !(1..=2).contains(&record.num_rows())
            {
                return Err(DataflowError::ArgumentError(
                    "ordinary Data on response edge without a complete native maintenance unit"
                        .into(),
                ));
            }
        }
        TaskCounters::MessagesSent
            .for_task(&self.chain_info, |c| c.inc_by(record.num_rows() as u64));
        TaskCounters::BatchesSent.for_task(&self.chain_info, |c| c.inc());
        TaskCounters::BytesSent.for_task(&self.chain_info, |c| {
            c.inc_by(record.get_array_memory_size() as u64)
        });

        let out_schema = self
            .out_schema
            .as_ref()
            .unwrap_or_else(|| panic!("No out-schema in {}!", self.chain_info));

        let record = RecordBatch::try_new(out_schema.schema.clone(), record.columns().to_vec())
            .unwrap_or_else(|e| {
                panic!(
                    "Data does not match expected schema for {}: {:?}. expected schema:\n{:#?}\n, actual schema:\n{:#?}",
                    self.chain_info, e, out_schema.schema, record.schema()
                );
            });

        for (i, out_q) in self.out_qs.iter_mut().enumerate() {
            let partitions = repartition(&record, out_schema.routing_keys(), out_q.len());

            for (partition, batch) in partitions {
                out_q[partition]
                    .send(ArrowMessage::Data(batch))
                    .await
                    .map_err(|_| queue_closed_error())?;

                self.tx_queue_rem_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].capacity() as i64));

                self.tx_queue_size_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].size() as i64));

                self.tx_queue_bytes_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].queued_bytes() as i64));
            }
        }

        Ok(())
    }

    async fn collect_response(&mut self, mut response: EventResponse) -> DataflowResult<()> {
        let spec = self
            .event_spec
            .as_ref()
            .ok_or_else(|| DataflowError::ArgumentError("unregistered response output".into()))?;
        response.discard_invalid_body_proof(spec);
        spec.validate(&response)
            .map_err(DataflowError::ArgumentError)?;
        if self
            .out_qs
            .iter()
            .any(|group| group.len() != 1 || group[0].size() == 0)
        {
            return Err(DataflowError::ArgumentError(
                "response route requires singleton forward destinations".into(),
            ));
        }
        // Await delivery of this ONE response to every branch before returning
        // to the root caller; even an empty response is never repartitioned away.
        for (i, group) in self.out_qs.iter_mut().enumerate() {
            group[0]
                .send(ArrowMessage::Event(response.clone()))
                .await
                .map_err(|_| queue_closed_error())?;
            self.tx_queue_rem_gauges[i][0]
                .iter()
                .for_each(|g| g.set(group[0].capacity() as i64));
            self.tx_queue_size_gauges[i][0]
                .iter()
                .for_each(|g| g.set(group[0].size() as i64));
            self.tx_queue_bytes_gauges[i][0]
                .iter()
                .for_each(|g| g.set(group[0].queued_bytes() as i64));
        }
        Ok(())
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.broadcast_checked(SignalMessage::Watermark(watermark))
            .await
    }
}

impl ArrowCollector {
    /// Engine registration only; no operator is implicitly switched to events.
    pub fn register_event_output(&mut self, spec: Arc<EventEdgeSpec>) -> DataflowResult<()> {
        if self.event_spec.is_some()
            || self
                .out_schema
                .as_ref()
                .is_none_or(|schema| schema.schema != spec.schema)
            || self
                .out_qs
                .iter()
                .any(|group| group.len() != 1 || group[0].size() == 0)
        {
            return Err(DataflowError::ArgumentError(
                "invalid or duplicate response edge registration".into(),
            ));
        }
        self.event_spec = Some(spec);
        Ok(())
    }

    pub fn new(
        chain_info: Arc<ChainInfo>,
        out_schema: Option<Arc<ArroyoSchema>>,
        out_qs: Vec<Vec<BatchSender>>,
    ) -> Self {
        let tx_queue_size_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_size",
            "Size of a tx queue",
            &chain_info,
            &out_qs,
            config().worker.queue_size as i64,
        );

        let tx_queue_rem_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_rem",
            "Remaining space in a tx queue",
            &chain_info,
            &out_qs,
            config().worker.queue_size as i64,
        );

        let tx_queue_bytes_gauges = register_queue_gauge(
            "arroyo_worker_tx_bytes",
            "Number of bytes queued in a tx queue",
            &chain_info,
            &out_qs,
            0,
        );

        // initialize counters so that tasks that never produce data still report 0
        for m in TaskCounters::variants() {
            m.for_task(&chain_info, |_| {});
        }

        Self {
            chain_info,
            out_schema,
            event_spec: None,
            out_qs,
            tx_queue_rem_gauges,
            tx_queue_size_gauges,
            tx_queue_bytes_gauges,
        }
    }

    pub(crate) async fn broadcast_checked(&mut self, message: SignalMessage) -> DataflowResult<()> {
        trace!("[{}] Broadcast {:?}", self.chain_info, message);
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(ArrowMessage::Signal(message.clone()))
                    .await
                    .map_err(|_| queue_closed_error())?;
            }
        }
        Ok(())
    }

    pub async fn broadcast(&mut self, message: SignalMessage) {
        self.broadcast_checked(message)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "failed to broadcast message for operator {}: {}",
                    self.chain_info, error
                )
            });
    }
}

impl OperatorContext {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        task_info: Arc<TaskInfo>,
        restore_from: Option<&MetadataOrManifest>,
        control_tx: Sender<ControlResp>,
        input_partitions: usize,
        in_schemas: Vec<Arc<ArroyoSchema>>,
        out_schema: Option<Arc<ArroyoSchema>>,
        tables: HashMap<String, TableConfig>,
    ) -> Self {
        let (table_manager, watermark) =
            TableManager::load(task_info.clone(), tables, control_tx.clone(), restore_from)
                .await
                .expect("should be able to create TableManager");

        Self {
            restore_commit_epoch: None,
            task_info: task_info.clone(),
            control_tx: control_tx.clone(),
            watermarks: WatermarkHolder::new(vec![
                watermark.map(Watermark::EventTime);
                input_partitions
            ]),
            in_schemas,
            out_schema: out_schema.clone(),
            table_manager,
            error_reporter: ErrorReporter {
                tx: control_tx,
                task_info,
            },
        }
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.watermarks.watermark()
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.watermarks.last_present_watermark()
    }

    pub async fn load_compacted(&mut self, compaction: &CompactionResult) {
        //TODO: support compaction in the table manager
        self.table_manager.load_compacted(compaction).await;
    }

    pub async fn report_error(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.error_reporter.report_error(message, details).await;
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arroyo_types::to_nanos;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;

    #[test]
    fn single_destination_preserves_one_batch_order_and_backing_ownership() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![8, 1, 7, 1, 3, 0, 3, 9]))],
        )
        .unwrap();
        for keys in [None, Some(vec![0])] {
            let output = repartition(&input, keys.as_ref(), 1).collect::<Vec<_>>();
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].0, 0);
            assert_eq!(output[0].1, input);
            assert!(Arc::ptr_eq(output[0].1.column(0), input.column(0)));
            assert!(Arc::ptr_eq(&output[0].1.schema(), &input.schema()));
        }
    }

    #[test]
    fn test_watermark_holder() {
        let t1 = SystemTime::UNIX_EPOCH;
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t2 + Duration::from_secs(1);

        let mut w = WatermarkHolder::new(vec![None, None, None]);

        assert!(w.watermark().is_none());

        w.set(0, Watermark::EventTime(t1));
        w.set(1, Watermark::EventTime(t2));

        assert!(w.watermark().is_none());

        w.set(2, Watermark::EventTime(t3));

        assert_eq!(w.watermark(), Some(Watermark::EventTime(t1)));

        w.set(0, Watermark::Idle);
        assert_eq!(w.watermark(), Some(Watermark::EventTime(t2)));

        w.set(1, Watermark::Idle);
        w.set(2, Watermark::Idle);
        assert_eq!(w.watermark(), Some(Watermark::Idle));
    }

    #[tokio::test]
    async fn test_shuffles() {
        let timestamp = SystemTime::now();

        let data = vec![0, 101, 0, 101, 0, 101, 0, 0];

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(data.clone())),
            Arc::new(TimestampNanosecondArray::from(
                data.iter()
                    .map(|_| to_nanos(timestamp) as i64)
                    .collect::<Vec<_>>(),
            )),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let (tx1, mut rx1) = batch_bounded(8);
        let (tx2, mut rx2) = batch_bounded(8);

        let record = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let chain_info = Arc::new(ChainInfo {
            job_id: "test-job".to_string(),
            task_id: 1,
            description: "test-operator".to_string(),
            task_index: 0,
        });

        let out_qs = vec![vec![tx1, tx2]];

        let mut collector = ArrowCollector::new(
            chain_info,
            Some(Arc::new(ArroyoSchema::new_keyed(schema, 1, vec![0]))),
            out_qs,
        );

        collector.collect(record).await.unwrap();

        drop(collector);

        // pull all messages out of the two queues
        let mut q1 = vec![];
        while let Some(m) = rx1.recv().await {
            q1.push(m);
        }

        let mut q2 = vec![];
        while let Some(m) = rx2.recv().await {
            q2.push(m);
        }

        let v1 = &q1[0];
        for v in &q1[1..] {
            assert_eq!(v1, v);
        }

        let v2 = &q2[0];
        for v in &q2[1..] {
            assert_eq!(v2, v);
        }
    }

    #[tokio::test]
    async fn test_batch_queues() {
        let (tx, mut rx) = batch_bounded(8);
        let msg = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        tx.send(ArrowMessage::Data(msg.clone())).await.unwrap();
        tx.send(ArrowMessage::Data(msg.clone())).await.unwrap();

        assert_eq!(tx.capacity(), 0);

        rx.recv().await.unwrap();
        rx.recv().await.unwrap();

        assert_eq!(tx.capacity(), 8);
    }

    fn queue_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap()
    }

    type GateTestFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn std::any::Any + Send>> + Send>>;

    struct RestoreGateSink {
        fail_commit: bool,
        observed: tokio::sync::mpsc::UnboundedSender<&'static str>,
    }
    #[async_trait]
    impl crate::operator::ArrowOperator for RestoreGateSink {
        fn name(&self) -> String {
            "restore-gate-test".into()
        }
        fn is_committing(&self) -> bool {
            true
        }
        async fn process_batch(
            &mut self,
            _: RecordBatch,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<()> {
            self.observed.send("batch").unwrap();
            Ok(())
        }
        async fn handle_commit(
            &mut self,
            _: u32,
            _: &HashMap<String, HashMap<u32, Vec<u8>>>,
            _: &mut OperatorContext,
        ) -> DataflowResult<()> {
            self.observed.send("commit").unwrap();
            if self.fail_commit {
                return Err(anyhow::anyhow!("injected restored commit failure").into());
            }
            Ok(())
        }
        fn future_to_poll(&mut self) -> Option<GateTestFuture> {
            self.observed.send("getter").unwrap();
            None
        }
        async fn handle_tick(
            &mut self,
            _: u64,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<()> {
            self.observed.send("tick").unwrap();
            Ok(())
        }
    }
    #[tokio::test]
    async fn restore_commit_gate_blocks_data_and_ticks_until_authorized_commit() {
        use crate::operator::{ChainedOperator, OperatorNode};
        for (received, fail_commit) in [
            (Some(7), false),
            (Some(8), false),
            (Some(7), true),
            (Some(0), false),
            (None, false),
        ] {
            let success = received == Some(7) && !fail_commit;
            let (control, mut events) = tokio::sync::mpsc::channel(32);
            let (commands, command_rx) = tokio::sync::mpsc::channel(8);
            let (observed, mut callbacks) = tokio::sync::mpsc::unbounded_channel();
            let task = Arc::new(TaskInfo {
                job_id: format!(
                    "restore-gate-{}-{received:?}-{fail_commit}",
                    std::process::id()
                ),
                operator_idx: 95,
                operator_name: "restore-gate-test".into(),
                operator_id: "sink".into(),
                task_index: 0,
                parallelism: 1,
                key_range: 0..=u64::MAX,
                checkpoint_file_path_layout: Default::default(),
            });
            let mut context =
                OperatorContext::new(task, None, control.clone(), 1, vec![], None, HashMap::new())
                    .await;
            context.restore_commit_epoch = Some(7);
            let node = Box::new(OperatorNode::Chained(ChainedOperator::new(
                Box::new(RestoreGateSink {
                    observed,
                    fail_commit,
                }),
                context,
            )));
            let (input, receiver) = batch_bounded(8);
            input.send(ArrowMessage::Data(queue_batch())).await.unwrap();
            let run = tokio::spawn(node.start(
                control,
                command_rx,
                vec![receiver],
                vec![],
                None,
                Arc::new(tokio::sync::Barrier::new(1)),
            ));
            assert!(matches!(
                events.recv().await,
                Some(ControlResp::TaskStarted { .. })
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), callbacks.recv())
                    .await
                    .is_err()
            );
            let Some(epoch) = received else {
                run.abort();
                assert!(run.await.err().expect("cancelled task").is_cancelled());
                assert!(callbacks.try_recv().is_err());
                continue;
            };
            if epoch == 0 {
                drop(commands);
                tokio::time::timeout(Duration::from_secs(2), run)
                    .await
                    .unwrap()
                    .unwrap();
                let mut closed_failure = false;
                while let Ok(event) = events.try_recv() {
                    if let ControlResp::TaskFailed { error, .. } = event {
                        closed_failure |= error
                            .message
                            .contains("control channel closed before restored commit");
                    }
                }
                assert!(closed_failure);
                assert!(callbacks.try_recv().is_err());
                continue;
            }
            commands
                .send(ControlMessage::Commit {
                    epoch,
                    commit_data: HashMap::new(),
                })
                .await
                .unwrap();
            if success {
                assert_eq!(callbacks.recv().await, Some("commit"));
                loop {
                    if callbacks.recv().await == Some("batch") {
                        break;
                    }
                }
                input
                    .send(ArrowMessage::Signal(SignalMessage::EndOfData))
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), run)
                .await
                .unwrap()
                .unwrap();
            let mut failed = false;
            while let Ok(event) = events.try_recv() {
                failed |= matches!(event, ControlResp::TaskFailed { .. });
            }
            assert_eq!(failed, !success);
            if !success {
                while let Ok(callback) = callbacks.try_recv() {
                    assert_ne!(callback, "batch");
                    assert_ne!(callback, "tick");
                    assert_ne!(callback, "getter");
                }
            }
        }
    }

    struct BufferedFinishSource {
        finish: Option<crate::SourceFinishType>,
    }

    #[async_trait]
    impl crate::operator::SourceOperator for BufferedFinishSource {
        fn name(&self) -> String {
            "buffered-finish-test".into()
        }

        async fn run(
            &mut self,
            _ctx: &mut SourceContext,
            collector: &mut SourceCollector,
        ) -> DataflowResult<crate::SourceFinishType> {
            collector.initialize_deserializer(Format::Json(Default::default()), None, None, &[]);
            let input =
                serde_json::to_vec(&serde_json::json!({"value": "x".repeat(8192)})).unwrap();
            collector
                .deserialize_slice(&input, SystemTime::UNIX_EPOCH, None)
                .await?;
            Ok(self.finish.take().unwrap())
        }
    }

    fn source_test_schema() -> Arc<ArroyoSchema> {
        Arc::new(ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),
            1,
        ))
    }

    async fn start_buffered_test_source(
        source: BufferedFinishSource,
        tx: BatchSender,
    ) -> Vec<ControlResp> {
        use crate::operator::{OperatorNode, SourceNode};
        static NEXT_JOB: AtomicUsize = AtomicUsize::new(0);
        let task = Arc::new(TaskInfo {
            job_id: format!(
                "source-finish-{}-{}",
                std::process::id(),
                NEXT_JOB.fetch_add(1, Ordering::Relaxed)
            ),
            operator_idx: 94,
            operator_name: "buffered-finish-test".into(),
            operator_id: "source".into(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        });
        let schema = source_test_schema();
        let (control, mut events) = tokio::sync::mpsc::channel(32);
        let context = OperatorContext::new(
            task,
            None,
            control.clone(),
            0,
            vec![],
            Some(schema.clone()),
            HashMap::new(),
        )
        .await;
        let node = Box::new(OperatorNode::Source(SourceNode {
            operator: Box::new(source),
            context,
        }));
        let (_commands, command_rx) = tokio::sync::mpsc::channel(1);
        tokio::time::timeout(
            Duration::from_secs(2),
            node.start(
                control,
                command_rx,
                vec![],
                vec![vec![tx]],
                Some(schema),
                Arc::new(tokio::sync::Barrier::new(1)),
            ),
        )
        .await
        .expect("source completion must not wait for a full downstream on Immediate");
        let mut observed = Vec::new();
        while let Ok(event) = events.try_recv() {
            observed.push(event);
        }
        observed
    }

    #[tokio::test]
    async fn immediate_source_completion_does_not_flush_buffered_rows_or_wait_for_downstream() {
        for full in [false, true] {
            let (tx, mut rx) = batch_bounded(1);
            let sentinel = ArrowMessage::Signal(SignalMessage::Watermark(Watermark::Idle));
            if full {
                tx.send(sentinel.clone()).await.unwrap();
            }
            let bytes_before = tx.queued_bytes();
            let events = start_buffered_test_source(
                BufferedFinishSource {
                    finish: Some(crate::SourceFinishType::Immediate),
                },
                tx.clone(),
            )
            .await;
            assert_eq!(tx.queued_bytes(), bytes_before);
            assert!(matches!(
                events.first(),
                Some(ControlResp::TaskStarted { .. })
            ));
            assert!(matches!(
                events.last(),
                Some(ControlResp::TaskFinished { .. })
            ));
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, ControlResp::TaskFailed { .. }))
            );
            if full {
                assert_eq!(rx.recv().await, Some(sentinel));
            }
            assert!(futures::poll!(Box::pin(rx.recv())).is_pending());
        }
    }

    #[tokio::test]
    async fn graceful_and_final_source_completion_flush_rows_before_terminal_signal() {
        for (finish, expected_signal) in [
            (crate::SourceFinishType::Graceful, SignalMessage::Stop),
            (crate::SourceFinishType::Final, SignalMessage::EndOfData),
        ] {
            let (tx, mut rx) = batch_bounded(8);
            let events = start_buffered_test_source(
                BufferedFinishSource {
                    finish: Some(finish),
                },
                tx.clone(),
            )
            .await;
            assert!(matches!(
                events.last(),
                Some(ControlResp::TaskFinished { .. })
            ));
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, ControlResp::TaskFailed { .. }))
            );
            let Some(ArrowMessage::Data(batch)) = rx.recv().await else {
                panic!("buffered rows must precede the terminal signal");
            };
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema(), source_test_schema().schema);
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap()
                    .value(0),
                "x".repeat(8192)
            );
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
                0
            );
            assert_eq!(rx.recv().await, Some(ArrowMessage::Signal(expected_signal)));
            assert!(futures::poll!(Box::pin(rx.recv())).is_pending());
        }
    }

    #[tokio::test]
    async fn row_queue_backpressure_preserves_bytes_and_accepts_large_batch() {
        // Byte size is observed, not an admission limit. A batch larger than
        // row capacity still fits alone, as in upstream Arroyo.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                "x".repeat(
                    128 * 1024
                );
                4
            ]))],
        )
        .unwrap();
        let bytes = batch.get_array_memory_size() as u64;
        let (tx, mut rx) = batch_bounded(2);
        tx.send(ArrowMessage::Data(batch.clone())).await.unwrap();
        assert_eq!(tx.capacity(), 0);
        assert_eq!(tx.queued_bytes(), bytes);
        let mut pending = Box::pin(tx.send(ArrowMessage::Signal(SignalMessage::Stop)));
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(tx.queued_bytes(), bytes);
        assert_eq!(rx.recv().await, Some(ArrowMessage::Data(batch)));
        tokio::time::timeout(Duration::from_secs(1), &mut pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tx.capacity(), 1);
        assert_eq!(
            rx.recv().await,
            Some(ArrowMessage::Signal(SignalMessage::Stop))
        );
        assert_eq!(tx.capacity(), 2);
        assert_eq!(tx.queued_bytes(), 0);
    }

    #[tokio::test]
    async fn row_queue_cancelled_send_and_closed_receiver_release_arrays() {
        let (tx, rx) = batch_bounded(4);
        let batch = queue_batch();
        let queued_array = Arc::downgrade(batch.column(0));
        tx.send(ArrowMessage::Data(batch)).await.unwrap();
        let unsent = queue_batch();
        let unsent_array = Arc::downgrade(unsent.column(0));
        let mut cancelled = Box::pin(tx.send(ArrowMessage::Data(unsent)));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        assert!(unsent_array.upgrade().is_none());
        assert!(queued_array.upgrade().is_some());
        let mut blocked = Box::pin(tx.send(ArrowMessage::Signal(SignalMessage::Stop)));
        assert!(futures::poll!(&mut blocked).is_pending());
        drop(rx);
        assert!(queued_array.upgrade().is_none());
        let failed = tokio::time::timeout(Duration::from_secs(1), &mut blocked)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(failed.0, ArrowMessage::Signal(SignalMessage::Stop));
    }

    #[tokio::test]
    async fn test_panic_propagation() {
        let (tx, mut rx) = batch_bounded(8);

        let msg = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        tokio::task::spawn(async move {
            let _f = rx.recv();
            panic!("at the disco");
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(tx.send(ArrowMessage::Data(msg)).await.is_err());
    }
}
