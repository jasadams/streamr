use crate::{RateLimiter, server_for_hash_array};
use arrow::array::{Array, PrimitiveArray, RecordBatch};
use arrow::compute::{partition, sort_to_indices, take};
use arrow::datatypes::UInt64Type;
use arroyo_formats::de::{ArrowDeserializer, FieldValueType};
use arroyo_metrics::{QueueGauge, TaskCounters, register_queue_gauge};
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
use datafusion::common::DataFusionError;
use datafusion::common::hash_utils;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use rand::Rng;
use std::collections::HashMap;
use std::mem::size_of_val;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{trace, warn};

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
    tx: UnboundedSender<QueuedItem>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
    enqueue: Arc<Mutex<()>>,
    budget: Option<Arc<QueueBudget>>,
    telemetry: Arc<Mutex<QueueTelemetry>>,
    #[cfg(test)]
    enqueue_pause: Option<Arc<EnqueuePause>>,
}

/// Gauge sampling and publication share one short critical section. A sender
/// cannot publish an old occupancy sample after the final envelope is dropped.
/// This lock never acquires the enqueue/budget locks or spans an await.
#[derive(Default)]
struct QueueTelemetry {
    remaining: QueueGauge,
    size: QueueGauge,
    bytes: QueueGauge,
}

impl QueueTelemetry {
    fn refresh(&self, size: u32, messages: &AtomicU32, bytes: &AtomicU64) {
        if let Some(gauge) = &self.remaining {
            gauge.set(size.saturating_sub(messages.load(Ordering::Acquire)) as i64);
        }
        if let Some(gauge) = &self.size {
            gauge.set(size as i64);
        }
        if let Some(gauge) = &self.bytes {
            gauge.set(bytes.load(Ordering::Acquire) as i64);
        }
    }
}

#[cfg(test)]
struct EnqueuePause {
    entered: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

struct QueueBudget {
    runtime: Arc<RuntimeEnv>,
    max_batch_bytes: usize,
    messages: AtomicUsize,
    metadata: Mutex<MemoryReservation>,
}

// Tokio 1.47.1's mpsc list uses 32-slot blocks on 64-bit targets (16 on
// 32-bit), with a four-word header. Six spare blocks cover partial head/tail,
// its three recycled blocks, and one transient grow allocation. Accounted
// enqueue is serialized below so producer tail publication cannot delay
// reclamation while other producers extend the list indefinitely.
fn queue_metadata_bytes(messages: usize) -> DataflowResult<usize> {
    let slots = if usize::BITS == 64 { 32 } else { 16 };
    let block = std::mem::size_of::<QueuedItem>()
        .checked_mul(slots)
        .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<usize>()));
    // Chan's two cache-padded fields, Notify, counters and receiver state fit
    // within 1024 bytes in the pinned Tokio version. Include our own controls.
    let controls = 1024
        + std::mem::size_of::<QueueBudget>()
        + 3 * std::mem::size_of::<Notify>()
        + std::mem::size_of::<Mutex<()>>()
        + std::mem::size_of::<Mutex<QueueTelemetry>>()
        + 4 * std::mem::size_of::<usize>()
        + 16 * std::mem::size_of::<usize>();
    messages
        .div_ceil(slots)
        .checked_add(6)
        .and_then(|blocks| block.and_then(|bytes| bytes.checked_mul(blocks)))
        .and_then(|bytes| bytes.checked_add(controls))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("graph queue metadata size overflow".into()).into()
        })
}

struct QueueAdmission {
    _reservation: MemoryReservation,
    budget: Arc<QueueBudget>,
}

fn queue_message_charge(bytes: usize) -> Option<usize> {
    // DF48 SharedRegistration owns MemoryConsumer, its name, a fat pool Arc
    // and its own Arc counters. FairSpillPool adds no per-consumer map entry.
    bytes.checked_add(
        std::mem::size_of::<QueuedItem>()
            + std::mem::size_of::<MemoryConsumer>()
            + "Streamr graph queue message".len()
            + 4 * std::mem::size_of::<usize>(),
    )
}

/// Own the queue charge for exactly as long as the channel owns the message.
/// This also releases charges for a failed send or receiver teardown racing
/// with a send, without depending on the receiver draining each item.
struct QueuedItem {
    item: Option<QueueItem>,
    count: u32,
    bytes: u64,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
    admission: Option<QueueAdmission>,
    size: u32,
    telemetry: Arc<Mutex<QueueTelemetry>>,
}

impl Drop for QueuedItem {
    fn drop(&mut self) {
        self.queued_messages.fetch_sub(self.count, Ordering::SeqCst);
        self.queued_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(admission) = &self.admission {
            admission.budget.messages.fetch_sub(1, Ordering::AcqRel);
        }
        self.telemetry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .refresh(self.size, &self.queued_messages, &self.queued_bytes);
        self.notify.notify_waiters();
    }
}

#[inline]
fn message_count(item: &QueueItem, size: u32) -> u32 {
    match item {
        QueueItem::Data(d) => (d.num_rows() as u32).min(size),
        QueueItem::Signal(_) => 1,
    }
}

#[inline]
fn message_bytes(item: &QueueItem) -> u64 {
    match item {
        QueueItem::Data(d) => d.get_array_memory_size() as u64,
        QueueItem::Signal(s) => size_of_val(s) as u64,
    }
}

impl BatchSender {
    pub async fn send(&self, item: QueueItem) -> Result<(), SendError<QueueItem>> {
        self.send_inner(item)
            .await
            .map_err(|(item, _)| SendError(item))
    }

    /// Engine delivery preserving precise configured-resource failures.
    pub async fn send_checked(&self, item: QueueItem) -> DataflowResult<()> {
        self.send_inner(item).await.map_err(|(_, error)| error)
    }

    async fn send_inner(&self, item: QueueItem) -> Result<(), (QueueItem, DataflowError)> {
        let admission = if let Some(budget) = &self.budget {
            let bytes = message_bytes(&item) as usize;
            if matches!(&item, QueueItem::Data(_)) && bytes > budget.max_batch_bytes {
                return Err((
                    item,
                    DataFusionError::ResourcesExhausted(format!(
                        "graph queue batch requires {bytes} bytes; max-batch-bytes is {}",
                        budget.max_batch_bytes,
                    ))
                    .into(),
                ));
            }
            let mut reservation = MemoryConsumer::new("Streamr graph queue message")
                .register(&budget.runtime.memory_pool);
            // Include the pending-send envelope before awaiting row capacity.
            let Some(bytes) = queue_message_charge(bytes) else {
                return Err((
                    item,
                    DataFusionError::ResourcesExhausted("graph queue message size overflow".into())
                        .into(),
                ));
            };
            if let Err(error) = reservation.try_grow(bytes) {
                return Err((item, error.into()));
            }
            Some(QueueAdmission {
                _reservation: reservation,
                budget: budget.clone(),
            })
        } else {
            None
        };
        // Ensure that every message is sendable, even if it's bigger than our max size
        let count = if self.budget.is_some() {
            match &item {
                QueueItem::Data(batch) => u32::try_from(batch.num_rows())
                    .unwrap_or(u32::MAX)
                    .max(1)
                    .min(self.size),
                QueueItem::Signal(_) => 1,
            }
        } else {
            message_count(&item, self.size)
        };
        loop {
            if self.tx.is_closed() {
                return Err((item, queue_closed_error()));
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
                        let bytes = message_bytes(&item);
                        self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
                        if let Some(admission) = &admission {
                            admission.budget.messages.fetch_add(1, Ordering::AcqRel);
                        }
                        let mut queued = QueuedItem {
                            item: Some(item),
                            count,
                            bytes,
                            queued_messages: self.queued_messages.clone(),
                            queued_bytes: self.queued_bytes.clone(),
                            notify: self.notify.clone(),
                            admission,
                            size: self.size,
                            telemetry: self.telemetry.clone(),
                        };
                        self.telemetry
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .refresh(self.size, &self.queued_messages, &self.queued_bytes);
                        // Tokio 1.47.1 increments its unbounded message count
                        // before publishing into the list. Receiver drop can
                        // otherwise finish draining before that publication,
                        // retaining the item until the last sender drops.
                        // Serialize publication with our receiver close/drain.
                        let _enqueue = match self.enqueue.lock() {
                            Ok(guard) => guard,
                            Err(_) => {
                                return Err((
                                    queued.item.take().unwrap(),
                                    DataflowError::InternalOperatorError {
                                        error: "graph queue enqueue mutex poisoned",
                                        message: "message was not delivered".into(),
                                    },
                                ));
                            }
                        };
                        #[cfg(test)]
                        if let Some(pause) = &self.enqueue_pause {
                            pause.entered.send(()).unwrap();
                            // Dropping the test's release sender also resumes
                            // this thread if an assertion unwinds the test.
                            let _ = pause.resume.lock().unwrap().recv();
                        }
                        if let Some(budget) = &self.budget {
                            let result = (|| {
                                let mut metadata = budget.metadata.lock().map_err(|_| {
                                    DataflowError::InternalOperatorError {
                                        error: "graph queue budget mutex poisoned",
                                        message: "cannot admit another message".into(),
                                    }
                                })?;
                                let bytes =
                                    queue_metadata_bytes(budget.messages.load(Ordering::Acquire))?;
                                // Tokio may retain recycled blocks. Keep the
                                // high-water charge until this queue is dropped.
                                if bytes > metadata.size() {
                                    metadata.try_resize(bytes)?;
                                }
                                Ok::<_, DataflowError>(metadata)
                            })();
                            let _metadata = match result {
                                Ok(metadata) => metadata,
                                Err(error) => return Err((queued.item.take().unwrap(), error)),
                            };
                            return self.tx.send(queued).map_err(|mut error| {
                                (error.0.item.take().unwrap(), queue_closed_error())
                            });
                        }
                        return self.tx.send(queued).map_err(|mut error| {
                            (error.0.item.take().unwrap(), queue_closed_error())
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
                    _ = self.tx.closed() => return Err((item, queue_closed_error())),
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
    rx: UnboundedReceiver<QueuedItem>,
    enqueue: Arc<Mutex<()>>,
    // Retain both the metadata charge and runtime even after all senders exit.
    _budget: Option<Arc<QueueBudget>>,
    #[cfg(test)]
    close_contended: Option<std::sync::mpsc::Sender<bool>>,
}

impl Drop for BatchReceiver {
    fn drop(&mut self) {
        // No publisher can be between Tokio's admission and list push while
        // this guard is held. Closing prevents all later sends; every earlier
        // publication is now available to try_recv, without a Busy list slot.
        // QueuedItem::drop refreshes telemetry but never takes this mutex or
        // the metadata mutex. Recover poison to finish teardown too.
        #[cfg(test)]
        if let Some(observation) = &self.close_contended {
            let contended = matches!(
                self.enqueue.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            let _ = observation.send(contended);
        }
        let _enqueue = self
            .enqueue
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.rx.close();
        while let Ok(item) = self.rx.try_recv() {
            drop(item);
        }
    }
}

impl BatchReceiver {
    pub async fn recv(&mut self) -> Option<QueueItem> {
        self.rx.recv().await?.item.take()
    }
}

pub fn batch_bounded(size: u32) -> (BatchSender, BatchReceiver) {
    batch_bounded_inner(size, None)
}

/// Construct engine graph channels using an existing shared execution runtime.
pub fn batch_bounded_accounted(
    size: u32,
    runtime: Arc<RuntimeEnv>,
    max_batch_bytes: usize,
) -> DataflowResult<(BatchSender, BatchReceiver)> {
    if size == 0 || max_batch_bytes == 0 {
        return Err(DataflowError::ArgumentError(
            "accounted graph queues require positive row and batch limits".into(),
        ));
    }
    let mut metadata =
        MemoryConsumer::new("Streamr graph queue metadata").register(&runtime.memory_pool);
    metadata.try_grow(queue_metadata_bytes(0)?)?;
    let budget = Arc::new(QueueBudget {
        runtime,
        max_batch_bytes,
        messages: AtomicUsize::new(0),
        metadata: Mutex::new(metadata),
    });
    Ok(batch_bounded_inner(size, Some(budget)))
}

fn queue_closed_error() -> DataflowError {
    DataflowError::InternalOperatorError {
        error: "downstream graph queue closed",
        message: "message was not delivered".into(),
    }
}

fn batch_bounded_inner(
    size: u32,
    budget: Option<Arc<QueueBudget>>,
) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = unbounded_channel();
    let notify = Arc::new(Notify::new());
    let queued_messages = Arc::new(AtomicU32::new(0));
    let queued_bytes = Arc::new(AtomicU64::new(0));
    let enqueue = Arc::new(Mutex::new(()));
    (
        BatchSender {
            size,
            tx,
            queued_messages: queued_messages.clone(),
            queued_bytes: queued_bytes.clone(),
            notify: notify.clone(),
            enqueue: enqueue.clone(),
            budget: budget.clone(),
            telemetry: Arc::new(Mutex::new(QueueTelemetry::default())),
            #[cfg(test)]
            enqueue_pause: None,
        },
        BatchReceiver {
            rx,
            enqueue,
            _budget: budget,
            #[cfg(test)]
            close_contended: None,
        },
    )
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
    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()>;
}

#[derive(Clone)]
pub struct ArrowCollector {
    pub chain_info: Arc<ChainInfo>,
    out_schema: Option<Arc<ArroyoSchema>>,
    out_qs: Vec<Vec<BatchSender>>,
}

fn repartition<'a>(
    record: &'a RecordBatch,
    keys: Option<&'a Vec<usize>>,
    qs: usize,
) -> impl Iterator<Item = (usize, RecordBatch)> + 'a {
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

        for out_q in &mut self.out_qs {
            let partitions = repartition(&record, out_schema.routing_keys(), out_q.len());

            for (partition, batch) in partitions {
                out_q[partition]
                    .send_checked(ArrowMessage::Data(batch))
                    .await?;
            }
        }

        Ok(())
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.broadcast_checked(SignalMessage::Watermark(watermark))
            .await
    }
}

impl ArrowCollector {
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

        for (i, queues) in out_qs.iter().enumerate() {
            for (partition, queue) in queues.iter().enumerate() {
                let mut telemetry = queue
                    .telemetry
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                *telemetry = QueueTelemetry {
                    remaining: tx_queue_rem_gauges[i][partition].clone(),
                    size: tx_queue_size_gauges[i][partition].clone(),
                    bytes: tx_queue_bytes_gauges[i][partition].clone(),
                };
                // Registration may follow enqueue or race with a drain. Share
                // the publication lock and sample the current queue state.
                telemetry.refresh(queue.size, &queue.queued_messages, &queue.queued_bytes);
            }
        }

        // initialize counters so that tasks that never produce data still report 0
        for m in TaskCounters::variants() {
            m.for_task(&chain_info, |_| {});
        }

        Self {
            chain_info,
            out_schema,
            out_qs,
        }
    }

    pub(crate) async fn broadcast_checked(&mut self, message: SignalMessage) -> DataflowResult<()> {
        trace!("[{}] Broadcast {:?}", self.chain_info, message);
        for out_node in &self.out_qs {
            for q in out_node {
                q.send_checked(ArrowMessage::Signal(message.clone()))
                    .await?;
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
    use std::time::Duration;

    use super::*;

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

    fn queue_collector(tx: &BatchSender) -> ArrowCollector {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        ArrowCollector::new(
            Arc::new(ChainInfo {
                job_id: "queue-telemetry".into(),
                task_id: 1,
                description: format!("queue telemetry {id}"),
                task_index: 0,
            }),
            Some(Arc::new(ArroyoSchema::new_unkeyed(
                queue_batch().schema(),
                0,
            ))),
            vec![vec![tx.clone()]],
        )
    }

    fn assert_queue_metrics(tx: &BatchSender, remaining: u32, bytes: u64) {
        let telemetry = tx.telemetry.lock().unwrap();
        assert_eq!(
            telemetry.remaining.as_ref().unwrap().get(),
            remaining as i64
        );
        assert_eq!(telemetry.size.as_ref().unwrap().get(), tx.size() as i64);
        assert_eq!(telemetry.bytes.as_ref().unwrap().get(), bytes as i64);
    }

    #[tokio::test]
    async fn queue_metrics_follow_drain_without_another_send_and_collector_drop() {
        for accounted in [false, true] {
            let (tx, mut rx) = if accounted {
                batch_bounded_accounted(4, queue_runtime(1024 * 1024), 1024).unwrap()
            } else {
                batch_bounded(4)
            };
            // The envelope must find handles registered after it was enqueued.
            let batch = queue_batch();
            let bytes = batch.get_array_memory_size() as u64;
            tx.send_checked(ArrowMessage::Data(batch)).await.unwrap();
            let mut collector = queue_collector(&tx);
            assert_queue_metrics(&tx, 0, bytes);
            drop(rx.recv().await.unwrap());
            assert_queue_metrics(&tx, 4, 0);

            collector.collect(queue_batch()).await.unwrap();
            assert_queue_metrics(&tx, 0, bytes);
            drop(rx.recv().await.unwrap());
            assert_queue_metrics(&tx, 4, 0);

            collector
                .broadcast_watermark(Watermark::Idle)
                .await
                .unwrap();
            assert_queue_metrics(
                &tx,
                3,
                message_bytes(&ArrowMessage::Signal(SignalMessage::Watermark(
                    Watermark::Idle,
                ))),
            );
            drop(collector);
            drop(rx.recv().await.unwrap());
            assert_queue_metrics(&tx, 4, 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_queue_metrics_finish_empty_after_last_drain() {
        for accounted in [false, true] {
            let (tx, mut rx) = if accounted {
                batch_bounded_accounted(8, queue_runtime(1024 * 1024), 1024).unwrap()
            } else {
                batch_bounded(8)
            };
            let collector = queue_collector(&tx);
            let mut producers = Vec::new();
            for _ in 0..4 {
                let sender = tx.clone();
                producers.push(tokio::spawn(async move {
                    for _ in 0..64 {
                        sender
                            .send_checked(ArrowMessage::Data(queue_batch()))
                            .await
                            .unwrap();
                    }
                }));
            }
            for _ in 0..256 {
                drop(rx.recv().await.unwrap());
            }
            for producer in producers {
                producer.await.unwrap();
            }
            // Joining every producer ensures a delayed enqueue refresh cannot
            // overwrite the last drain's empty state.
            assert_queue_metrics(&tx, 8, 0);
            drop(collector);
        }
    }

    fn queue_runtime(bytes: usize) -> Arc<RuntimeEnv> {
        use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(bytes)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .unwrap()
    }

    #[tokio::test]
    async fn accounted_queues_share_memory_and_failed_admission_rolls_back() {
        let bytes = queue_batch().get_array_memory_size();
        let charge = queue_message_charge(bytes).unwrap();
        let empty_metadata = queue_metadata_bytes(0).unwrap();
        let used_metadata = queue_metadata_bytes(1).unwrap();
        let runtime = queue_runtime(2 * used_metadata + charge);
        let pool = runtime.memory_pool.clone();
        let (first, mut first_rx) = batch_bounded_accounted(4, runtime.clone(), bytes).unwrap();
        let (second, mut second_rx) = batch_bounded_accounted(4, runtime.clone(), bytes).unwrap();
        first
            .send_checked(ArrowMessage::Data(queue_batch()))
            .await
            .unwrap();
        let error = second
            .send_checked(ArrowMessage::Data(queue_batch()))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowError::DataFusionError(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(second.capacity(), 4);
        assert_eq!(second.queued_bytes(), 0);
        assert_eq!(pool.reserved(), used_metadata + empty_metadata + charge);
        drop(first_rx.recv().await.unwrap());
        assert_eq!(pool.reserved(), used_metadata + empty_metadata);
        second
            .send_checked(ArrowMessage::Data(queue_batch()))
            .await
            .unwrap();
        assert_eq!(pool.reserved(), 2 * used_metadata + charge);
        drop(second_rx.recv().await.unwrap());
        drop((first, first_rx, second, second_rx, runtime));
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn accounted_empty_batches_backpressure_and_pending_send_remains_charged() {
        let empty = queue_batch().slice(0, 0);
        let bytes = empty.get_array_memory_size();
        let charge = queue_message_charge(bytes).unwrap();
        let runtime = queue_runtime(queue_metadata_bytes(1).unwrap() + 2 * charge);
        let pool = runtime.memory_pool.clone();
        let (tx, mut rx) = batch_bounded_accounted(1, runtime, bytes).unwrap();
        tx.send_checked(ArrowMessage::Data(empty.clone()))
            .await
            .unwrap();
        let mut blocked = Box::pin(tx.send_checked(ArrowMessage::Data(empty)));
        assert!(futures::poll!(&mut blocked).is_pending());
        assert_eq!(tx.capacity(), 0);
        assert_eq!(
            pool.reserved(),
            queue_metadata_bytes(1).unwrap() + 2 * charge
        );
        drop(rx.recv().await.unwrap());
        blocked.await.unwrap();
        assert_eq!(pool.reserved(), queue_metadata_bytes(1).unwrap() + charge);
        drop(rx);
        assert_eq!(tx.queued_bytes(), 0);
        assert_eq!(tx.capacity(), 1);
        assert_eq!(pool.reserved(), queue_metadata_bytes(1).unwrap());
        drop(tx);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn accounted_metadata_tracks_actual_peak_and_pins_runtime_until_receiver_drop() {
        let runtime = queue_runtime(1024 * 1024);
        let pool = runtime.memory_pool.clone();
        let weak_runtime = Arc::downgrade(&runtime);
        // A large row capacity does not prepay a million message envelopes.
        let (tx, mut rx) = batch_bounded_accounted(1_000_000, runtime, 1024).unwrap();
        assert_eq!(pool.reserved(), queue_metadata_bytes(0).unwrap());
        for _ in 0..3 {
            for _ in 0..70 {
                tx.send_checked(ArrowMessage::Signal(SignalMessage::Watermark(
                    Watermark::Idle,
                )))
                .await
                .unwrap();
            }
            for _ in 0..70 {
                drop(rx.recv().await.unwrap());
            }
            assert_eq!(pool.reserved(), queue_metadata_bytes(70).unwrap());
        }
        drop(tx);
        assert!(weak_runtime.upgrade().is_some());
        assert_eq!(pool.reserved(), queue_metadata_bytes(70).unwrap());
        drop(rx);
        assert!(weak_runtime.upgrade().is_none());
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn accounted_queue_construction_failure_releases_metadata_reservation() {
        let runtime = queue_runtime(queue_metadata_bytes(0).unwrap() - 1);
        assert!(matches!(
            batch_bounded_accounted(4, runtime.clone(), 1024),
            Err(DataflowError::DataFusionError(
                DataFusionError::ResourcesExhausted(_)
            ))
        ));
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn collector_preserves_accounted_queue_limit_error() {
        let input = queue_batch();
        let bytes = input.get_array_memory_size();
        let runtime = queue_runtime(1024 * 1024);
        let (tx, rx) = batch_bounded_accounted(8, runtime.clone(), bytes - 1).unwrap();
        let mut collector = ArrowCollector::new(
            Arc::new(ChainInfo {
                job_id: "accounted-queue-limit".into(),
                task_id: 1,
                description: "accounted queue limit".into(),
                task_index: 0,
            }),
            Some(Arc::new(ArroyoSchema::new_unkeyed(input.schema(), 0))),
            vec![vec![tx.clone()]],
        );
        let error = collector.collect(input).await.unwrap_err();
        assert!(matches!(
            &error,
            DataflowError::DataFusionError(DataFusionError::ResourcesExhausted(_))
        ));
        assert!(error.to_string().contains("graph queue batch requires"));
        assert_eq!(tx.queued_bytes(), 0);
        assert_queue_metrics(&tx, 8, 0);
        drop((collector, tx, rx));
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn collector_signal_metadata_exhaustion_preserves_error_and_no_barrier_delivery() {
        let message = SignalMessage::Watermark(Watermark::Idle);
        let charge = queue_message_charge(size_of_val(&message)).unwrap();
        // The payload fits exactly, but the first live block growth does not.
        let runtime = queue_runtime(queue_metadata_bytes(0).unwrap() + charge);
        let (tx, mut rx) = batch_bounded_accounted(8, runtime.clone(), 1024).unwrap();
        let mut collector = ArrowCollector::new(
            Arc::new(ChainInfo {
                job_id: "signal-admission".into(),
                task_id: 92,
                description: "signal admission".into(),
                task_index: 3,
            }),
            None,
            vec![vec![tx.clone()]],
        );
        let error = collector
            .broadcast_watermark(Watermark::Idle)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowError::DataFusionError(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(tx.capacity(), 8);
        assert_eq!(tx.queued_bytes(), 0);
        assert_queue_metrics(&tx, 8, 0);
        assert_eq!(
            runtime.memory_pool.reserved(),
            queue_metadata_bytes(0).unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), rx.recv())
                .await
                .is_err()
        );
        drop((collector, tx, rx));
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn source_barrier_admission_reports_typed_failure_and_completion_stays_failed() {
        let signal = SignalMessage::Barrier(arroyo_types::CheckpointBarrier {
            epoch: 4,
            min_epoch: 1,
            timestamp: SystemTime::UNIX_EPOCH,
            then_stop: true,
        });
        let runtime = queue_runtime(
            queue_metadata_bytes(0).unwrap() + queue_message_charge(size_of_val(&signal)).unwrap(),
        );
        let (tx, mut rx) = batch_bounded_accounted(8, runtime.clone(), 1024).unwrap();
        let collector = ArrowCollector::new(
            Arc::new(ChainInfo {
                job_id: "source-signal-admission".into(),
                task_id: 93,
                description: "source signal admission".into(),
                task_index: 2,
            }),
            None,
            vec![vec![tx.clone()]],
        );
        let task = Arc::new(TaskInfo {
            job_id: "source-signal-admission".into(),
            operator_idx: 93,
            operator_name: "test".into(),
            operator_id: "test-source".into(),
            task_index: 2,
            parallelism: 1,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        });
        let schema = Arc::new(ArroyoSchema::new_unkeyed(queue_batch().schema(), 0));
        let (control, mut events) = tokio::sync::mpsc::channel(4);
        let mut source = SourceCollector::new(schema, collector, control, &task);
        source.broadcast(signal).await;
        let ControlResp::TaskFailed {
            task_id,
            subtask_idx,
            error,
        } = events.recv().await.unwrap()
        else {
            panic!("expected task failure");
        };
        assert_eq!((task_id, subtask_idx), (93, 2));
        assert!(
            error.message.contains("Streamr graph queue metadata"),
            "{}",
            error.message
        );
        assert_eq!(error.domain, arroyo_rpc::errors::ErrorDomain::External);
        // A connector that observes/ignores one flush failure still cannot make
        // the operator's completion flush pass and report TaskFinished.
        assert!(source.flush_buffer().await.is_err());
        assert!(source.flush_buffer().await.is_err());
        assert!(events.try_recv().is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), rx.recv())
                .await
                .is_err()
        );
        drop((source, tx, rx));
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    struct BufferedFinishSource {
        finish: Option<crate::SourceFinishType>,
        failure: Option<tokio::sync::oneshot::Sender<TaskError>>,
        checkpoint_after_failure: bool,
        propagate_second_failure: bool,
    }

    #[async_trait]
    impl crate::operator::SourceOperator for BufferedFinishSource {
        fn name(&self) -> String {
            "buffered-finish-test".into()
        }

        async fn run(
            &mut self,
            ctx: &mut SourceContext,
            collector: &mut SourceCollector,
        ) -> DataflowResult<crate::SourceFinishType> {
            collector.initialize_deserializer(Format::Json(Default::default()), None, None, &[]);
            let input =
                serde_json::to_vec(&serde_json::json!({"value": "x".repeat(8192)})).unwrap();
            collector
                .deserialize_slice(&input, SystemTime::UNIX_EPOCH, None)
                .await?;
            if let Some(first_error) = self.failure.take() {
                let error = collector.flush_buffer().await.unwrap_err();
                assert!(matches!(
                    &error,
                    DataflowError::DataFusionError(DataFusionError::ResourcesExhausted(_))
                ));
                first_error.send(TaskError::from(&error)).unwrap();
                // Match existing connectors that ignore this first failure and
                // attempt their normal checkpoint/then-stop path afterward.
                if self.checkpoint_after_failure {
                    assert!(
                        self.start_checkpoint(source_test_barrier(), ctx, collector)
                            .await
                    );
                }
                if self.propagate_second_failure {
                    collector.flush_buffer().await?;
                }
            }
            Ok(self.finish.take().unwrap())
        }
    }

    fn source_test_barrier() -> CheckpointBarrier {
        CheckpointBarrier {
            epoch: 4,
            min_epoch: 1,
            timestamp: SystemTime::UNIX_EPOCH,
            then_stop: true,
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
                    failure: None,
                    checkpoint_after_failure: false,
                    propagate_second_failure: false,
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
                    failure: None,
                    checkpoint_after_failure: false,
                    propagate_second_failure: false,
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
    async fn ignored_buffered_delivery_failure_blocks_checkpoint_and_immediate_success() {
        for (checkpoint, propagate_second_failure) in [(false, false), (true, false), (false, true)]
        {
            let signal = SignalMessage::Barrier(source_test_barrier());
            // This pool can admit a barrier, including the first channel block,
            // but not the actual buffered 8192-byte string batch. The batch cap
            // itself remains permissive so this exercises shared-pool failure.
            let runtime = queue_runtime(
                queue_metadata_bytes(1).unwrap()
                    + queue_message_charge(size_of_val(&signal)).unwrap(),
            );
            let (tx, mut rx) = batch_bounded_accounted(8, runtime.clone(), 1024 * 1024).unwrap();
            tx.send_checked(ArrowMessage::Signal(signal.clone()))
                .await
                .unwrap();
            assert_eq!(rx.recv().await, Some(ArrowMessage::Signal(signal)));
            let (first_error, expected_error) = tokio::sync::oneshot::channel();
            let events = start_buffered_test_source(
                BufferedFinishSource {
                    finish: Some(crate::SourceFinishType::Immediate),
                    failure: Some(first_error),
                    checkpoint_after_failure: checkpoint,
                    propagate_second_failure,
                },
                tx.clone(),
            )
            .await;
            let expected = expected_error.await.unwrap();
            assert_eq!(expected.domain, arroyo_rpc::errors::ErrorDomain::External);
            assert_eq!(
                expected.retry_hint,
                arroyo_rpc::errors::RetryHint::WithBackoff
            );
            assert!(expected.message.contains("Streamr graph queue message"));
            assert!(matches!(
                events.first(),
                Some(ControlResp::TaskStarted { .. })
            ));
            assert!(matches!(
                events.get(1),
                Some(ControlResp::TaskFailed { .. })
            ));
            assert!(matches!(
                events.last(),
                Some(ControlResp::TaskFailed { .. })
            ));
            let failures: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    ControlResp::TaskFailed { error, .. } => Some(error),
                    _ => None,
                })
                .collect();
            assert_eq!(
                failures.len(),
                2,
                "first admission failure and terminal failure only"
            );
            for error in failures {
                assert_eq!(error.message, expected.message);
                assert_eq!(error.domain, expected.domain);
                assert_eq!(error.retry_hint, expected.retry_hint);
                assert_eq!(error.operator_id, expected.operator_id);
                assert_eq!(error.details, expected.details);
            }
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, ControlResp::TaskFinished { .. }))
            );
            if checkpoint {
                assert!(events.iter().skip(2).any(|event| matches!(
                    event,
                    ControlResp::CheckpointEvent(event)
                        if event.event_type == TaskCheckpointEventType::FinishedSync
                )));
            }
            assert_eq!(tx.queued_bytes(), 0);
            assert!(futures::poll!(Box::pin(rx.recv())).is_pending());
            drop((tx, rx));
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn receiver_cancellation_wakes_blocked_senders_and_releases_queued_arrays() {
        let (tx, rx) = batch_bounded(4);
        let _collector = queue_collector(&tx);
        let queued = queue_batch();
        let held_array = Arc::downgrade(queued.column(0));
        let bytes = queued.get_array_memory_size() as u64;
        tx.send(ArrowMessage::Data(queued)).await.unwrap();
        let other = tx.clone();
        let mut first = Box::pin(tx.send(ArrowMessage::Data(queue_batch())));
        let mut second = Box::pin(other.send(ArrowMessage::Data(queue_batch())));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(tx.queued_bytes(), bytes);
        assert_queue_metrics(&tx, 0, bytes);
        assert_eq!(tx.capacity(), 0);
        assert!(held_array.upgrade().is_some());

        drop(rx);
        let (first, second) =
            tokio::time::timeout(Duration::from_secs(1), futures::future::join(first, second))
                .await
                .expect("receiver cancellation must wake every blocked sender");
        assert!(first.is_err());
        assert!(second.is_err());
        assert!(held_array.upgrade().is_none());
        assert_eq!(tx.queued_bytes(), 0);
        assert_eq!(tx.capacity(), 4);
        assert_queue_metrics(&tx, 4, 0);
    }

    #[tokio::test]
    async fn cancelling_blocked_send_preserves_queued_owner_and_releases_unsent_array() {
        let (tx, mut rx) = batch_bounded(4);
        let _collector = queue_collector(&tx);
        let queued = queue_batch();
        let queued_array = Arc::downgrade(queued.column(0));
        let bytes = queued.get_array_memory_size() as u64;
        tx.send(ArrowMessage::Data(queued)).await.unwrap();
        let unsent = queue_batch();
        let unsent_array = Arc::downgrade(unsent.column(0));
        let mut blocked = Box::pin(tx.send(ArrowMessage::Data(unsent)));
        assert!(futures::poll!(&mut blocked).is_pending());
        drop(blocked);
        assert!(unsent_array.upgrade().is_none());
        assert!(queued_array.upgrade().is_some());
        assert_eq!(tx.queued_bytes(), bytes);
        assert_queue_metrics(&tx, 0, bytes);
        assert_eq!(tx.capacity(), 0);

        let received = rx.recv().await.unwrap();
        assert_eq!(tx.queued_bytes(), 0);
        assert_eq!(tx.capacity(), 4);
        assert_queue_metrics(&tx, 4, 0);
        assert!(queued_array.upgrade().is_some());
        drop(received);
        assert!(queued_array.upgrade().is_none());
        tx.send(ArrowMessage::Data(queue_batch())).await.unwrap();
        drop(rx);
        assert_eq!(tx.queued_bytes(), 0);
        assert_eq!(tx.capacity(), 4);
        assert_queue_metrics(&tx, 4, 0);
    }

    #[test]
    fn receiver_teardown_waits_for_paused_publication_then_releases_queued_array() {
        for accounted in [false, true] {
            let runtime = queue_runtime(1024 * 1024);
            let (mut tx, mut rx) = if accounted {
                batch_bounded_accounted(4, runtime.clone(), 1024 * 1024).unwrap()
            } else {
                batch_bounded(4)
            };
            let collector = queue_collector(&tx);
            let queued = queue_batch();
            let held_array = Arc::downgrade(queued.column(0));
            let bytes = queued.get_array_memory_size() as u64;
            let (entered, entered_rx) = std::sync::mpsc::channel();
            let (resume, resume_rx) = std::sync::mpsc::channel();
            let (contended, contended_rx) = std::sync::mpsc::channel();
            tx.enqueue_pause = Some(Arc::new(EnqueuePause {
                entered,
                resume: Mutex::new(resume_rx),
            }));
            rx.close_contended = Some(contended);
            let sender = tx.clone();
            let publication = std::thread::spawn(move || {
                drop(futures::executor::block_on(
                    sender.send(ArrowMessage::Data(queued)),
                ));
            });
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let teardown = std::thread::spawn(move || drop(rx));
            // Receiver teardown has reached the publication gate while the
            // actual production send holds it, before calling Tokio send.
            let observed_contention = contended_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let remained_open = !tx.tx.is_closed();
            let retained = held_array.upgrade().is_some();
            let queued_bytes = tx.queued_bytes();
            resume.send(()).unwrap();
            publication.join().unwrap();
            teardown.join().unwrap();
            assert!(observed_contention);
            assert!(remained_open);
            assert!(retained);
            assert_eq!(queued_bytes, bytes);
            // Both threads have finished, but the original sender is still
            // alive. Cleanup must not depend on dropping its Tokio channel.
            assert!(held_array.upgrade().is_none());
            assert_eq!(tx.queued_bytes(), 0);
            assert_eq!(tx.capacity(), 4);
            assert_queue_metrics(&tx, 4, 0);
            if let Some(budget) = &tx.budget {
                assert_eq!(budget.messages.load(Ordering::Acquire), 0);
                assert_eq!(
                    runtime.memory_pool.reserved(),
                    budget.metadata.lock().unwrap().size()
                );
            } else {
                assert_eq!(runtime.memory_pool.reserved(), 0);
            }
            drop(collector);
            drop(tx);
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_close_racing_with_delivery_does_not_leak_queue_charges() {
        for accounted in [false, true] {
            for iteration in 0..64 {
                let runtime = queue_runtime(1024 * 1024);
                let (tx, rx) = if accounted {
                    batch_bounded_accounted(4, runtime.clone(), 1024 * 1024).unwrap()
                } else {
                    batch_bounded(4)
                };
                let collector = queue_collector(&tx);
                let queued = queue_batch();
                let held_array = Arc::downgrade(queued.column(0));
                let barrier = Arc::new(tokio::sync::Barrier::new(2));
                let sender_barrier = barrier.clone();
                let sender = tx.clone();
                let delivery = tokio::spawn(async move {
                    sender_barrier.wait().await;
                    // Either delivery before closure or rejection is valid.
                    // Consume SendError's original message before task return.
                    drop(sender.send(ArrowMessage::Data(queued)).await);
                });
                barrier.wait().await;
                drop(rx);
                tokio::time::timeout(Duration::from_secs(1), delivery)
                    .await
                    .unwrap()
                    .unwrap();
                // Keep the original sender alive: Tokio's last-sender cleanup
                // must not conceal a message published after receiver teardown.
                assert!(
                    held_array.upgrade().is_none(),
                    "accounted={accounted} iteration={iteration} retained={} queued_bytes={} capacity={}",
                    held_array.strong_count(),
                    tx.queued_bytes(),
                    tx.capacity()
                );
                assert_eq!(tx.queued_bytes(), 0);
                assert_eq!(tx.capacity(), 4);
                assert_queue_metrics(&tx, 4, 0);
                if let Some(budget) = &tx.budget {
                    assert_eq!(budget.messages.load(Ordering::Acquire), 0);
                    // Only channel metadata may remain with the live sender.
                    assert_eq!(
                        runtime.memory_pool.reserved(),
                        budget.metadata.lock().unwrap().size()
                    );
                } else {
                    assert_eq!(runtime.memory_pool.reserved(), 0);
                }
                drop(collector);
                drop(tx);
                assert_eq!(runtime.memory_pool.reserved(), 0);
            }
        }
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
