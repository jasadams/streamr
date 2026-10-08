//! Opt-in native TUMBLE/HOP execution over one generic live-state owner.
//!
//! SQL plans remain DataFusion plans. This adapter changes only where partial
//! batches live and how the final plan receives one group's paged partials.
use super::{
    StatelessPhysicalExecutor,
    window_store::{WindowSnapshot, WindowStore, WindowStoreLimits},
};
use anyhow::{Context, Result, ensure};
use arrow::{
    compute::{sort_to_indices, take},
    row::{RowConverter, SortField},
};
use arrow_array::{
    Array, ListArray, PrimitiveArray, RecordBatch, StringArray, UInt32Array,
    types::TimestampNanosecondType,
};
use arrow_schema::{DataType, SchemaRef};
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::Registry,
};
use arroyo_planner::{
    physical::{ArroyoPhysicalExtensionCodec, DecodingContext},
    schemas::add_timestamp_field_arrow,
};
use arroyo_rpc::{
    config::{WindowStateConfig, config},
    df::ArroyoSchema,
    grpc::rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
};
use arroyo_state::live::worker::{
    ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend,
};
use arroyo_types::to_nanos;
use datafusion::{
    common::ScalarValue,
    execution::memory_pool::MemoryConsumer,
    physical_expr::PhysicalExpr,
    physical_plan::{ExecutionPlan, aggregates::AggregateExec},
};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::{StreamExt, try_join};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::mpsc::{Receiver, channel};

const TABLE: &str = "w";

fn is_terminal_watermark(watermark: std::time::SystemTime) -> bool {
    to_nanos(watermark) == u64::MAX as u128
}

fn hop_end(progress: Option<i64>, earliest: i64, slide: i64) -> Result<i64> {
    let first_due = earliest
        .checked_add(slide)
        .context("native window end overflow")?;
    Ok(match progress {
        Some(last) => last
            .checked_add(slide)
            .context("native window end overflow")?
            .max(first_due),
        None => first_due,
    })
}

#[derive(Default)]
struct CollectionSize {
    encoded_bytes: usize,
    elements: usize,
    array_bytes: usize,
    output_elements: usize,
    output_bytes: usize,
    partials: usize,
}

impl CollectionSize {
    fn add_partial(
        &mut self,
        batch: &RecordBatch,
        encoded: usize,
        columns: &[(usize, bool)],
    ) -> Result<()> {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(encoded)
            .context("native window collection IPC size overflow")?;
        self.partials = self
            .partials
            .checked_add(1)
            .context("native window collection partial count overflow")?;
        for (column, output_array) in columns {
            let list = batch
                .column(*column)
                .as_any()
                .downcast_ref::<ListArray>()
                .context("native window collection state changed from ListArray")?;
            ensure!(
                list.len() == 1,
                "native window collection partial must contain one row"
            );
            let values = list.value(0);
            self.elements = self
                .elements
                .checked_add(values.len())
                .context("native window collection element count overflow")?;
            self.array_bytes = self
                .array_bytes
                .checked_add(values.get_array_memory_size())
                .context("native window collection array size overflow")?;
            if *output_array {
                self.output_elements = self
                    .output_elements
                    .checked_add(values.len())
                    .context("native window output element count overflow")?;
                // The final ARRAY_AGG result compacts partial children. A
                // one-element partial can own a much larger Arrow allocation;
                // summing those capacities would reject small valid outputs.
                let payload = match values.data_type() {
                    DataType::Boolean => Some(values.len().div_ceil(8)),
                    DataType::Int8 | DataType::UInt8 => Some(values.len()),
                    DataType::Int16 | DataType::UInt16 => values.len().checked_mul(2),
                    DataType::Int32 | DataType::UInt32 => values.len().checked_mul(4),
                    DataType::Int64 | DataType::UInt64 => values.len().checked_mul(8),
                    DataType::Utf8 => {
                        let strings = values
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("native window Utf8 collection changed type")?;
                        strings
                            .iter()
                            .flatten()
                            .try_fold(0usize, |sum, value| sum.checked_add(value.len()))
                    }
                    other => anyhow::bail!("native window collection type changed to {other}"),
                }
                .context("native window output payload size overflow")?;
                let offsets = if matches!(values.data_type(), DataType::Utf8) {
                    values.len().checked_add(1).and_then(|n| n.checked_mul(4))
                } else {
                    Some(0)
                }
                .context("native window output offsets size overflow")?;
                self.output_bytes = self
                    .output_bytes
                    .checked_add(payload)
                    .and_then(|bytes| bytes.checked_add(offsets))
                    .and_then(|bytes| bytes.checked_add(values.len().div_ceil(8)))
                    .context("native window output size overflow")?;
            }
        }
        Ok(())
    }

    fn output_bound(&self, key_bytes: usize, fields: usize) -> Result<usize> {
        self.output_bytes
            .checked_add(
                self.output_elements
                    .checked_mul(4)
                    .context("collection offsets overflow")?,
            )
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .and_then(|bytes| bytes.checked_add(fields.checked_mul(64)?))
            .context("native window collection output bound overflow")
    }

    fn working_bound(&self, output_limit: usize) -> Result<usize> {
        // DF48 flat DISTINCT uses HashSet<ScalarValue>; ARRAY_AGG keeps
        // ArrayRefs and may concatenate them. Eight scalar slots per input
        // covers hash-table spare capacity, cloned evaluation vectors, and
        // transient final state. Eight child-buffer copies cover Arrow concat,
        // list output and the decoded source while the same snapshot is read.
        self.elements
            .checked_mul(std::mem::size_of::<ScalarValue>())
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(self.array_bytes.checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(self.encoded_bytes))
            .and_then(|bytes| bytes.checked_add(self.partials.checked_mul(256)?))
            .and_then(|bytes| bytes.checked_add(output_limit.checked_mul(2)?))
            .context("native window collection working bound overflow")
    }
}

pub(crate) struct NativeWindow {
    width: Duration,
    slide: Duration,
    hopping: bool,
    binning: Arc<dyn PhysicalExpr>,
    partial: StatelessPhysicalExecutor,
    finish: Arc<dyn ExecutionPlan>,
    finish_receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    projection: Option<StatelessPhysicalExecutor>,
    finish_timestamp_schema: SchemaRef,
    partial_schema: SchemaRef,
    group_converter: Option<RowConverter>,
    group_fields: usize,
    collection_columns: Vec<(usize, bool)>,
    collection_output: bool,
    limits: WindowStateConfig,
    identity: Vec<u8>,
    store: Option<WindowStore>,
}

impl NativeWindow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        width: Duration,
        slide: Duration,
        hopping: bool,
        binning: Arc<dyn PhysicalExpr>,
        binning_bytes: &[u8],
        partial_bytes: &[u8],
        finish_bytes: &[u8],
        projection_bytes: Option<&[u8]>,
        partial_schema: &ArroyoSchema,
        registry: Arc<Registry>,
        limits: WindowStateConfig,
    ) -> Result<Self> {
        limits.validate()?;
        ensure!(
            config().worker.execution_resources.is_some(),
            "native windows require worker.execution-resources"
        );
        ensure!(
            width > Duration::ZERO && slide > Duration::ZERO,
            "native window width and slide must be positive"
        );
        ensure!(
            !hopping || (width >= slide && width.as_nanos().is_multiple_of(slide.as_nanos())),
            "native HOP width must be an integer multiple of slide"
        );
        let partial = StatelessPhysicalExecutor::new(partial_bytes, registry.as_ref())?;
        let physical_partial_schema = partial.plan.schema();
        ensure!(
            physical_partial_schema.fields().len() + 1 == partial_schema.schema.fields().len(),
            "native window partial schema changed"
        );
        let key_count = partial_schema.routing_keys().map_or(0, Vec::len);
        let partial_aggregate = partial
            .plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .context("native window partial plan must be a physical aggregate")?;
        ensure!(
            partial_aggregate.group_expr().expr().len() == key_count,
            "native window group key layout changed"
        );
        let mut collection_columns = Vec::new();
        let mut collection_output = false;
        let mut variable_other_output = false;
        let mut state_column = key_count;
        for aggregate in partial_aggregate.aggr_expr() {
            let name = aggregate.fun().name().to_ascii_lowercase();
            let ordered_first_last = matches!(name.as_str(), "first_value" | "last_value")
                && aggregate.order_bys().is_some_and(|order| !order.is_empty())
                && !aggregate.is_distinct();
            let unordered_scalar = matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
                && aggregate.order_bys().is_none()
                && !aggregate.is_distinct();
            let function = aggregate.fun().inner().as_any();
            let array_agg = function.is::<datafusion::functions_aggregate::array_agg::ArrayAgg>();
            let distinct_count = aggregate.is_distinct()
                && function.is::<datafusion::functions_aggregate::count::Count>();
            let collection = (array_agg || distinct_count) && aggregate.order_bys().is_none();
            if collection {
                let args = aggregate.expressions();
                ensure!(
                    args.len() == 1
                        && matches!(
                            args[0].data_type(&partial_aggregate.input().schema())?,
                            arrow_schema::DataType::Boolean
                                | arrow_schema::DataType::Int8
                                | arrow_schema::DataType::Int16
                                | arrow_schema::DataType::Int32
                                | arrow_schema::DataType::Int64
                                | arrow_schema::DataType::UInt8
                                | arrow_schema::DataType::UInt16
                                | arrow_schema::DataType::UInt32
                                | arrow_schema::DataType::UInt64
                                | arrow_schema::DataType::Utf8
                        ),
                    "native window collection supports one flat Boolean, integer, or Utf8 argument"
                );
                collection_columns.push((state_column, array_agg));
                collection_output |= array_agg;
            } else {
                variable_other_output |= !matches!(
                    aggregate.field().data_type(),
                    arrow_schema::DataType::Boolean
                        | arrow_schema::DataType::Int8
                        | arrow_schema::DataType::Int16
                        | arrow_schema::DataType::Int32
                        | arrow_schema::DataType::Int64
                        | arrow_schema::DataType::UInt8
                        | arrow_schema::DataType::UInt16
                        | arrow_schema::DataType::UInt32
                        | arrow_schema::DataType::UInt64
                        | arrow_schema::DataType::Float32
                        | arrow_schema::DataType::Float64
                        | arrow_schema::DataType::Date32
                        | arrow_schema::DataType::Date64
                        | arrow_schema::DataType::Timestamp(_, _)
                );
            }
            let state_fields = aggregate.state_fields()?;
            if collection {
                ensure!(
                    state_fields.len() == 1
                        && matches!(state_fields[0].data_type(), arrow_schema::DataType::List(_)),
                    "native window collection partial state must be one list"
                );
            }
            state_column = state_column
                .checked_add(state_fields.len())
                .context("native window partial state column overflow")?;
            ensure!(
                unordered_scalar || ordered_first_last || collection,
                "native window state supports scalar aggregates, ordered FIRST_VALUE/LAST_VALUE, and bounded flat ARRAY_AGG or COUNT(DISTINCT)"
            );
        }
        ensure!(
            key_count <= physical_partial_schema.fields().len(),
            "native window group key count exceeds partial schema"
        );
        ensure!(
            state_column == physical_partial_schema.fields().len(),
            "native window partial state column layout changed"
        );
        ensure!(
            collection_columns.is_empty() || !variable_other_output,
            "native window collection with variable-width companion aggregate is not bounded"
        );
        let group_converter = (key_count > 0)
            .then(|| {
                RowConverter::new(
                    physical_partial_schema.fields()[..key_count]
                        .iter()
                        .map(|field| SortField::new(field.data_type().clone()))
                        .collect(),
                )
            })
            .transpose()?;
        let finish_receiver = Arc::new(RwLock::new(None));
        let finish_codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::BoundedBatchStream(finish_receiver.clone()),
        };
        let execution = super::execution::configured_execution_resources()?
            .context("native windows require worker.execution-resources")?;
        let finish = PhysicalPlanNode::decode(finish_bytes)?.try_into_physical_plan(
            registry.as_ref(),
            &execution.runtime,
            &finish_codec,
        )?;
        let projection = projection_bytes
            .map(|bytes| StatelessPhysicalExecutor::new(bytes, registry.as_ref()))
            .transpose()?;
        let finish_timestamp_schema = add_timestamp_field_arrow((*finish.schema()).clone());
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-window-partial.v1");
        identity.update(width.as_nanos().to_be_bytes());
        identity.update(slide.as_nanos().to_be_bytes());
        identity.update([u8::from(hopping)]);
        identity.update([u8::from(projection_bytes.is_some())]);
        for bytes in [
            binning_bytes,
            partial_bytes,
            finish_bytes,
            projection_bytes.unwrap_or_default(),
        ] {
            identity.update(u64::try_from(bytes.len())?.to_be_bytes());
            identity.update(bytes);
        }
        identity.update(serde_json::to_vec(physical_partial_schema.as_ref())?);
        Ok(Self {
            width,
            slide,
            hopping,
            binning,
            partial,
            finish,
            finish_receiver,
            projection,
            finish_timestamp_schema,
            partial_schema: physical_partial_schema,
            group_converter,
            group_fields: key_count,
            collection_columns,
            collection_output,
            limits,
            identity: identity.finalize().to_vec(),
            store: None,
        })
    }

    pub fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::from([(
            TABLE.into(),
            TableConfig {
                table_type: TableEnum::DiskKeyedMap.into(),
                state_version: 1,
                config: DiskKeyedTableConfig {
                    table_name: TABLE.into(),
                    encoding_version: 1,
                    schema_identity: self.identity.clone(),
                }
                .encode_to_vec(),
            },
        )])
    }

    pub async fn on_start(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        let worker = config();
        worker.worker.validate_sql_state()?;
        let resources = configured_worker_resources()?
            .context("native windows require worker.live-state-resources")?;
        let generation = match ctx.task_info.checkpoint_file_path_layout {
            arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
            _ => 0,
        };
        let backend = construct_configured_backend(
            ConfiguredBackendOwner {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask: ctx.task_info.task_index,
                generation,
            },
            self.limits.max_resident_bytes,
            resources.clone(),
        )
        .await?;
        let table = ctx
            .table_manager
            .register_live_table(TABLE, backend.clone())
            .await?;
        self.store = Some(WindowStore::new(
            backend,
            table,
            resources,
            self.partial_schema.clone(),
            WindowStoreLimits {
                key_bytes: self.limits.key_bytes,
                value_bytes: self.limits.partial_bytes,
                page_bytes: self.limits.page_bytes,
                page_entries: self.limits.page_entries,
                write_bytes: self.limits.write_bytes,
                write_operations: self.limits.write_operations,
                max_resident_bytes: self.limits.max_resident_bytes,
            },
        )?);
        Ok(())
    }

    fn store(&self) -> Result<&WindowStore> {
        self.store.as_ref().context("native window was not started")
    }

    fn bin_start(&self, time: i64) -> Result<i64> {
        let width = i64::try_from(self.slide.as_nanos())?;
        Ok(time.div_euclid(width) * width)
    }

    pub async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // The end-of-data signal follows its terminal watermark. Any late
        // batch delivered after that signal has no open window to enter.
        if ctx
            .last_present_watermark()
            .is_some_and(is_terminal_watermark)
        {
            return Ok(());
        }
        let execution = super::execution::configured_execution_resources()?
            .context("native windows require worker.execution-resources")?;
        let _input_permit = execution.reserve_batch("native window source", &batch)?;
        let scratch_bytes = batch
            .num_rows()
            .checked_mul(32)
            .context("native window source row count overflow")?;
        let mut _scratch_permit =
            datafusion::execution::memory_pool::MemoryConsumer::new("native window bin scratch")
                .register(&execution.runtime.memory_pool);
        _scratch_permit.try_grow(scratch_bytes)?;
        let bins = self
            .binning
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let typed = bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .context("native window binning result is not nanosecond timestamp")?;
        let indices = sort_to_indices(bins.as_ref(), None, None)?;
        let sorted_bins = take(&bins, &indices, None)?;
        let sorted_typed = sorted_bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .context("native window sorted bins changed type")?;
        ensure!(
            typed.null_count() == 0 && sorted_typed.null_count() == 0,
            "native window bin timestamps must be non-null"
        );
        let mut start = 0usize;
        while start < batch.num_rows() {
            let bin = sorted_typed.value(start);
            let mut end = start + 1;
            while end < batch.num_rows() && sorted_typed.value(end) == bin {
                end += 1;
            }
            let watermark_floor = ctx
                .last_present_watermark()
                .map(|wm| i64::try_from(to_nanos(wm)))
                .transpose()?
                .map(|wm| self.bin_start(wm))
                .transpose()?;
            if watermark_floor.is_none_or(|wm| bin >= wm) {
                let mut offset = start;
                let mut partial_chunks = 0usize;
                while offset < end {
                    let stop = (offset + 64).min(end);
                    let chunk_indices = UInt32Array::from(
                        (offset..stop)
                            .map(|index| Ok::<_, anyhow::Error>(indices.value(index)))
                            .collect::<Result<Vec<_>>>()?,
                    );
                    let columns = batch
                        .columns()
                        .iter()
                        .map(|column| take(column, &chunk_indices, None))
                        .collect::<arrow::error::Result<Vec<_>>>()?;
                    let chunk = RecordBatch::try_new(batch.schema(), columns)?;
                    let mut partials = self.partial.process_batch(chunk).await;
                    while let Some(partial) = partials.next().await {
                        let partial = partial?;
                        let keys = if let Some(converter) = &self.group_converter {
                            let columns = partial.columns()[..self.group_fields].to_vec();
                            converter
                                .convert_columns(&columns)?
                                .iter()
                                .map(|row| row.as_ref().to_vec())
                                .collect::<Vec<_>>()
                        } else {
                            vec![Vec::new(); partial.num_rows()]
                        };
                        self.store()?.append_rows(bin, &partial, &keys).await?;
                    }
                    partial_chunks += 1;
                    offset = stop;
                }
                tracing::debug!(
                    source_rows = batch.num_rows(),
                    bin,
                    bin_rows = end - start,
                    partial_chunks,
                    "native window partial chunks processed"
                );
            }
            start = end;
        }
        Ok(())
    }

    async fn emit_interval(
        &mut self,
        snapshot: &WindowSnapshot,
        start: i64,
        end: i64,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        // All intervals at this watermark share one stable view. Each next
        // interval starts at or beyond the prior expiry cutoff.
        let mut after_group = None;
        loop {
            let group = snapshot.next_group(after_group.as_deref()).await?;
            let Some((key, latest)) = group else {
                break;
            };
            after_group = Some(key.clone());
            if latest < start {
                continue;
            }
            let Some(first) = snapshot.next_partial(&key, start, end, None).await? else {
                continue;
            };
            // Collection preflight counts the same snapshot and reserves its
            // final-state workspace. Keep its existing lifetime and budget:
            // only scalar/ordered groups carry this decoded first partial
            // into the bounded producer.
            let first_for_producer = if self.collection_columns.is_empty() {
                Some(first)
            } else {
                drop(first);
                None
            };
            let execution = super::execution::configured_execution_resources()?
                .context("native windows require worker.execution-resources")?;
            // ARRAY_AGG and DISTINCT retain cardinality-growing final state.
            // Count the actual persisted IPC bytes in this same snapshot before
            // invoking DataFusion; this pass retains only one decoded partial.
            // Flat builtin state has one ListArray per collection expression.
            // Charge its element count and actual child buffers rather than
            // multiplying IPC framing overhead for small partials.
            let _collection_reservations = if !self.collection_columns.is_empty() {
                let mut after = None;
                let mut size = CollectionSize::default();
                while let Some(partial) = snapshot
                    .next_partial(&key, start, end, after.as_deref())
                    .await?
                {
                    size.add_partial(
                        &partial.batch,
                        partial.encoded_bytes,
                        &self.collection_columns,
                    )?;
                    after = Some(partial.key.clone());
                }
                if self.collection_output {
                    ensure!(
                        size.output_bound(
                            self.limits.key_bytes,
                            self.partial_schema.fields().len(),
                        )? <= self.limits.partial_bytes,
                        "native window collection output exceeds configured partial limit"
                    );
                }
                let expanded = size.working_bound(self.limits.partial_bytes)?;
                ensure!(
                    expanded
                        .checked_add(self.limits.partial_bytes)
                        .is_some_and(|bound| bound <= execution.limits.memory_bytes),
                    "native window collection exceeds execution-memory budget"
                );
                let decoded = self.store()?.reserve_collection_final(expanded)?;
                // DataFusion charges its own accumulator in this pool. Reserve
                // only the output allowance here so the same state is not
                // charged twice and the final plan can still make progress.
                let mut execution_reservation = MemoryConsumer::new("native window collection")
                    .register(&execution.runtime.memory_pool);
                execution_reservation.try_grow(self.limits.partial_bytes)?;
                Some((decoded, execution_reservation))
            } else {
                None
            };
            let _input_queue_permit = self.store()?.reserve_final_input_queue()?;
            let (sender, receiver) = channel(1);
            *self.finish_receiver.write().unwrap() = Some(receiver);
            self.finish.reset()?;
            let mut finish = self.finish.execute(0, execution.task_context())?;
            // Move the sender into the producer so the input stream observes
            // EOF as soon as all paged partials have been sent.
            let producer_snapshot = snapshot;
            let producer = async move {
                let mut after = None;
                if let Some(first) = first_for_producer {
                    after = Some(first.key.clone());
                    // RecordBatch::clone shares Arrow buffers. Retain the
                    // decoded permit through channel admission, then release
                    // it before reading another partial; the one-slot queue
                    // has its own reservation for the transferred batch.
                    sender.send(first.batch.clone()).await.map_err(|_| {
                        anyhow::anyhow!(
                            "native window final aggregate stopped before consuming partials"
                        )
                    })?;
                    drop(first);
                }
                while let Some(partial) = producer_snapshot
                    .next_partial(&key, start, end, after.as_deref())
                    .await?
                {
                    after = Some(partial.key.clone());
                    sender.send(partial.batch).await.map_err(|_| {
                        anyhow::anyhow!(
                            "native window final aggregate stopped before consuming partials"
                        )
                    })?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let consumer = async {
                let mut output = None;
                while let Some(batch) = finish.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    ensure!(
                        output.is_none() && batch.num_rows() == 1,
                        "native window final aggregate returned more than one row for a group"
                    );
                    output = Some(batch);
                }
                Ok::<_, anyhow::Error>(output)
            };
            let (_, output) = try_join!(producer, consumer)?;
            if let Some(output) = output {
                ensure!(
                    output.get_array_memory_size() <= self.limits.partial_bytes,
                    "native window result exceeds configured partial/output limit"
                );
                let timestamp = ScalarValue::TimestampNanosecond(Some(start), None)
                    .to_array_of_size(output.num_rows())?;
                let mut columns = output.columns().to_vec();
                columns.push(timestamp);
                let output = RecordBatch::try_new(self.finish_timestamp_schema.clone(), columns)?;
                if let Some(projection) = self.projection.as_mut() {
                    let mut output = projection.process_batch(output).await;
                    while let Some(batch) = output.next().await {
                        let batch = batch?;
                        let _output_permit =
                            execution.reserve_batch("native window output", &batch)?;
                        collector.collect(batch).await?;
                    }
                } else {
                    let _output_permit =
                        execution.reserve_batch("native window output", &output)?;
                    collector.collect(output).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn handle_watermark(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        let Some(watermark) = ctx.last_present_watermark() else {
            return Ok(());
        };
        // EndOfData uses u64::MAX nanoseconds, which intentionally lies
        // outside Arrow's signed timestamp range. Drain only windows that
        // still contain retained panes; never count slide ticks toward MAX.
        let floor = if is_terminal_watermark(watermark) {
            None
        } else {
            Some(self.bin_start(i64::try_from(to_nanos(watermark))?)?)
        };
        let width = i64::try_from(self.width.as_nanos())?;
        let slide = i64::try_from(self.slide.as_nanos())?;
        let snapshot = self.store()?.snapshot().await?;
        let mut retired_through = None;
        if self.hopping {
            let mut progress = self.store()?.progress().await?;
            loop {
                let Some(earliest) = snapshot
                    .next_expiry_time(retired_through.as_deref())
                    .await?
                else {
                    break;
                };
                // No input contributes to the empty slide intervals before
                // this pane. Skip a watermark jump without looping per slide.
                let next = hop_end(progress, earliest, slide)?;
                if floor.is_some_and(|floor| next > floor) {
                    break;
                }
                self.emit_interval(
                    &snapshot,
                    next.checked_sub(width)
                        .context("native window start overflow")?,
                    next,
                    collector,
                )
                .await?;
                self.store()?.set_progress(next).await?;
                progress = Some(next);
                let expiry = next
                    .checked_add(slide)
                    .and_then(|time| time.checked_sub(width))
                    .context("native window expiry overflow")?;
                self.store()?
                    .expire_before_snapshot(&snapshot, expiry, &mut retired_through)
                    .await?;
            }
        } else {
            while let Some(first) = snapshot
                .next_expiry_time(retired_through.as_deref())
                .await?
            {
                if floor.is_some_and(|floor| first >= floor) {
                    break;
                }
                let end = first
                    .checked_add(width)
                    .context("native window end overflow")?;
                self.emit_interval(&snapshot, first, end, collector).await?;
                self.store()?.set_progress(end).await?;
                self.store()?
                    .expire_before_snapshot(&snapshot, end, &mut retired_through)
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Int64Array,
        builder::{Int64Builder, ListBuilder},
    };
    use arrow_schema::{DataType, Field, Schema};
    use arroyo_state::live::{
        LiveStateBackend, Ownership,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
        table::LiveTableManager,
    };

    fn store() -> WindowStore {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
        let mut tables = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        WindowStore::new(
            backend,
            tables.register("window").unwrap(),
            resources,
            Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )])),
            WindowStoreLimits {
                key_bytes: 128,
                value_bytes: 8192,
                page_bytes: 32768,
                page_entries: 1,
                write_bytes: 65536,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn partial() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap()
    }

    #[test]
    fn collection_output_bound_uses_compact_values_not_partial_allocations() {
        let mut builder = ListBuilder::new(Int64Builder::with_capacity(4096));
        builder.values().append_value(7);
        builder.append(true);
        let list = Arc::new(builder.finish());
        let field = Field::new("values", list.data_type().clone(), false);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone(), field.clone(), field])),
            vec![list.clone(), list.clone(), list],
        )
        .unwrap();
        let mut small = CollectionSize::default();
        for _ in 0..78 {
            small
                .add_partial(&batch, 512, &[(0, true), (1, true), (2, false)])
                .unwrap();
        }
        assert!(small.array_bytes > 32 * 1024);
        assert!(small.output_bound(512, 5).unwrap() <= 32 * 1024);

        let mut large = CollectionSize::default();
        for _ in 0..4096 {
            large.add_partial(&batch, 512, &[(0, true)]).unwrap();
        }
        assert!(large.output_bound(512, 2).unwrap() > 32 * 1024);
    }

    #[tokio::test]
    async fn terminal_watermark_drains_only_retained_tumbling_windows() {
        assert!(is_terminal_watermark(arroyo_types::from_nanos(
            u64::MAX as u128
        )));
        let store = store();
        store.append(b"a", 0, &partial()).await.unwrap();
        store.append(b"b", 20, &partial()).await.unwrap();
        let mut ends = Vec::new();
        while let Some(first) = store.earliest_time().await.unwrap() {
            let end = first + 10;
            ends.push(end);
            assert!(
                ends.len() <= 2,
                "terminal drain did not stop at retained panes"
            );
            store.set_progress(end).await.unwrap();
            while store.expire_page(end).await.unwrap() != 0 {}
        }
        assert_eq!(ends, [10, 30]);
        assert_eq!(store.earliest_time().await.unwrap(), None);
    }

    #[tokio::test]
    async fn terminal_watermark_drains_hop_contributions_and_skips_empty_gap() {
        let store = store();
        store.append(b"a", 0, &partial()).await.unwrap();
        store.append(b"b", 20, &partial()).await.unwrap();
        let mut progress = None;
        let mut ends = Vec::new();
        while let Some(first) = store.earliest_time().await.unwrap() {
            let end = hop_end(progress, first, 2).unwrap();
            ends.push(end);
            assert!(
                ends.len() <= 6,
                "terminal drain did not stop at retained panes"
            );
            store.set_progress(end).await.unwrap();
            progress = Some(end);
            while store.expire_page(end + 2 - 6).await.unwrap() != 0 {}
        }
        assert_eq!(ends, [2, 4, 6, 22, 24, 26]);
        assert_eq!(store.progress().await.unwrap(), Some(26));
        assert_eq!(store.earliest_time().await.unwrap(), None);
    }
}
