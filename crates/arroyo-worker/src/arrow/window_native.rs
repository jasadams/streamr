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
    Array, ListArray, PrimitiveArray, RecordBatch, StringArray, StructArray, UInt32Array,
    UInt64Array, types::TimestampNanosecondType,
};
use arrow_schema::{DataType, SchemaRef};
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::Registry,
};
use arroyo_planner::{
    aggregate_capability::{
        AggregateDescriptor, aggregate_descriptor, is_extremum, logical_payload_bytes,
    },
    grouped_selector::{ORDINAL_FIELD, SelectorBudget},
    grouped_selector_udaf::{bind_window_budget, classify},
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
    execution::{TaskContext, runtime_env::RuntimeEnvBuilder},
    physical_expr::PhysicalExpr,
    physical_plan::{ExecutionPlan, aggregates::AggregateExec},
    prelude::SessionContext,
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

pub(super) fn has_grouped_selector(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any()
        .downcast_ref::<AggregateExec>()
        .is_some_and(|aggregate| {
            aggregate
                .aggr_expr()
                .iter()
                .any(|expression| classify(expression.fun()).is_some())
        })
        || plan
            .children()
            .iter()
            .any(|child| has_grouped_selector(child.as_ref()))
}

const TABLE: &str = "w";

fn partial_payload_bound(
    base: usize,
    columns: &[(usize, AggregateDescriptor)],
    batch: &RecordBatch,
    indices: &UInt32Array,
) -> Result<usize> {
    columns
        .iter()
        .try_fold(base, |total, (column, descriptor)| {
            let payload =
                indices
                    .values()
                    .iter()
                    .try_fold(0usize, |sum, index| -> Result<usize> {
                        sum.checked_add(logical_payload_bytes(
                            batch.column(*column),
                            *index as usize,
                        )?)
                        .context("native window partial payload size overflow")
                    })?;
            let with_payload = descriptor
                .retained_bound(1, payload)
                .context("native window partial aggregate size overflow")?;
            let without_payload = descriptor
                .retained_bound(1, 0)
                .context("native window partial aggregate size overflow")?;
            total
                .checked_add(
                    with_payload
                        .checked_sub(without_payload)
                        .context("native window partial aggregate size underflow")?,
                )
                .context("native window partial aggregate size overflow")
        })
}

fn partial_chunk_rows(per_row: usize, limit: usize) -> Result<usize> {
    if per_row == 0 {
        return Ok(64);
    }
    ensure!(
        per_row <= limit,
        "native window one partial aggregate row exceeds configured partial limit"
    );
    Ok((limit / per_row).min(64))
}

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
    output_elements: usize,
    output_bytes: usize,
}

impl CollectionSize {
    fn add_partial(&mut self, batch: &RecordBatch, columns: &[(usize, bool)]) -> Result<()> {
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

    fn admit(&self, output_array: bool, fields: usize, limits: WindowStateConfig) -> Result<()> {
        if output_array {
            let output = self.output_bound(limits.key_bytes, fields)?;
            ensure!(
                output <= limits.partial_bytes,
                "native window collection output exceeds configured partial limit"
            );
        }
        Ok(())
    }
}

fn validate_ordered_partial_pairs(batch: &RecordBatch, pairs: &[(usize, usize)]) -> Result<()> {
    for (value_column, ordering_column) in pairs {
        let values = batch
            .column(*value_column)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("native ordered ARRAY_AGG values must be a list")?;
        let ordering = batch
            .column(*ordering_column)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("native ordered ARRAY_AGG orderings must be a list")?;
        ensure!(
            values.len() == 1 && ordering.len() == 1,
            "native ordered ARRAY_AGG partial must contain one row"
        );
        ensure!(
            !ordering.is_null(0),
            "native ordered ARRAY_AGG ordering list cannot be null"
        );
        let value_is_null = values.is_null(0);
        let values = values.value(0);
        let ordering = ordering.value(0);
        // DF48 state() represents an empty filtered result as a null value
        // list paired with a nonnull empty ordering list. No hidden children
        // in a null list are valid input to its final merge accumulator.
        ensure!(
            !value_is_null || (values.is_empty() && ordering.is_empty()),
            "native ordered ARRAY_AGG null value list must be empty"
        );
        let ordering = ordering
            .as_any()
            .downcast_ref::<StructArray>()
            .context("native ordered ARRAY_AGG orderings must contain structs")?;
        ensure!(
            values.len() == ordering.len(),
            "native ordered ARRAY_AGG value/order lengths differ"
        );
        ensure!(
            ordering.null_count() == 0,
            "native ordered ARRAY_AGG ordering tuples cannot be null"
        );
    }
    Ok(())
}

fn flat_collection_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
    )
}

fn validate_ordered_array_state(
    fields: &[arrow_schema::FieldRef],
    value_type: &DataType,
    ordering_types: &[DataType],
) -> Result<()> {
    ensure!(
        fields.len() == 2,
        "native ordered ARRAY_AGG partial must have value and ordering lists"
    );
    let DataType::List(value) = fields[0].data_type() else {
        anyhow::bail!("native ordered ARRAY_AGG value state must be a list")
    };
    let DataType::List(ordering) = fields[1].data_type() else {
        anyhow::bail!("native ordered ARRAY_AGG ordering state must be a list")
    };
    let DataType::Struct(ordering) = ordering.data_type() else {
        anyhow::bail!("native ordered ARRAY_AGG ordering list must contain structs")
    };
    ensure!(
        value.data_type() == value_type && flat_collection_type(value_type),
        "native ordered ARRAY_AGG value state type changed"
    );
    ensure!(
        !ordering_types.is_empty()
            && ordering.len() == ordering_types.len()
            && ordering
                .iter()
                .zip(ordering_types)
                .all(|(field, expected)| field.data_type() == expected
                    && flat_collection_type(expected)),
        "native ordered ARRAY_AGG requires matching flat ordering fields"
    );
    Ok(())
}

pub(crate) struct NativeWindow {
    width: Duration,
    slide: Duration,
    hopping: bool,
    binning: Arc<dyn PhysicalExpr>,
    partial: StatelessPhysicalExecutor,
    finish: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    finish_receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    projection: Option<StatelessPhysicalExecutor>,
    finish_timestamp_schema: SchemaRef,
    partial_schema: SchemaRef,
    group_converter: Option<RowConverter>,
    group_fields: usize,
    collection_columns: Vec<(usize, bool)>,
    collection_ordering_pairs: Vec<(usize, usize)>,
    collection_output: bool,
    replay_columns: Vec<(usize, AggregateDescriptor)>,
    partial_state_per_row: usize,
    fixed_state_bytes: usize,
    partial_payload_columns: Vec<(usize, AggregateDescriptor)>,
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
            (width == Duration::ZERO && slide == Duration::ZERO && !hopping)
                || (width > Duration::ZERO && slide > Duration::ZERO),
            "native windows require positive width/slide or exact-timestamp tumbling mode"
        );
        ensure!(
            !hopping || (width >= slide && width.as_nanos().is_multiple_of(slide.as_nanos())),
            "native HOP width must be an integer multiple of slide"
        );
        let budget = SelectorBudget {
            key_bytes: limits.key_bytes,
            value_bytes: limits.partial_bytes,
            decoded_bytes: limits
                .partial_bytes
                .checked_mul(3)
                .context("window selector budget overflow")?,
        };
        let mut partial = StatelessPhysicalExecutor::new(partial_bytes, registry.as_ref())?;
        partial.plan = bind_window_budget(partial.plan, budget)?;
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
        let mut collection_ordering_pairs = Vec::new();
        let mut collection_output = false;
        let mut replay_columns = Vec::new();
        let mut partial_state_per_row = 0usize;
        let mut fixed_state_bytes = 0usize;
        let mut partial_payload_columns = Vec::new();
        let mut variable_other_output = false;
        let mut state_column = key_count;
        for aggregate in partial_aggregate.aggr_expr() {
            let grouped_selector = classify(aggregate.fun()).is_some() && !aggregate.is_distinct();
            let descriptor = aggregate_descriptor(aggregate, &partial_aggregate.input().schema())?;
            let capability = descriptor.map(|descriptor| descriptor.capability);
            if let Some(descriptor) = descriptor {
                let per_group = descriptor
                    .retained_bound(1, 0)
                    .context("native window partial aggregate size overflow")?;
                partial_state_per_row = partial_state_per_row
                    .checked_add(per_group)
                    .context("native window partial companion total overflow")?;
                if descriptor.payload_multiplier > 0 {
                    let args = aggregate.expressions();
                    let column = args[0]
                        .as_any()
                        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                        .context("native window companion projection schema mismatch")?;
                    partial_payload_columns.push((column.index(), descriptor));
                }
                if descriptor.state_list_column.is_some() || descriptor.hll_registers {
                    replay_columns.push((state_column, descriptor));
                } else {
                    fixed_state_bytes = fixed_state_bytes
                        .checked_add(
                            descriptor
                                .retained_bound(0, 0)
                                .context("native window fixed aggregate size overflow")?,
                        )
                        .context("native window fixed companion total overflow")?;
                }
            }
            let function = aggregate.fun().inner().as_any();
            let array_agg = function.is::<datafusion::functions_aggregate::array_agg::ArrayAgg>();
            let distinct_count = aggregate.is_distinct()
                && function.is::<datafusion::functions_aggregate::count::Count>();
            let ordered_array = array_agg
                && !aggregate.is_distinct()
                && aggregate.order_bys().is_some_and(|order| !order.is_empty());
            let collection = ((array_agg || (distinct_count && descriptor.is_none()))
                && aggregate.order_bys().is_none())
                || ordered_array;
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
            if ordered_array {
                let ordering = aggregate
                    .order_bys()
                    .context("native ordered ARRAY_AGG order missing")?;
                ensure!(
                    aggregate.expressions()[0]
                        .as_any()
                        .is::<datafusion::physical_expr::expressions::Column>()
                        && ordering.iter().all(|sort| {
                            sort.expr
                                .as_any()
                                .is::<datafusion::physical_expr::expressions::Column>()
                        }),
                    "native ordered ARRAY_AGG requires direct value and ordering columns"
                );
                let ordering_types = ordering
                    .iter()
                    .map(|sort| sort.expr.data_type(&partial_aggregate.input().schema()))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                validate_ordered_array_state(
                    &state_fields,
                    &aggregate.expressions()[0].data_type(&partial_aggregate.input().schema())?,
                    &ordering_types,
                )?;
                let ordering_column = state_column
                    .checked_add(1)
                    .context("native ordered ARRAY_AGG state column overflow")?;
                collection_ordering_pairs.push((state_column, ordering_column));
            } else if collection {
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
                capability.is_some() || is_extremum(aggregate) || grouped_selector || collection,
                "native window state supports scalar aggregates, shared grouped FIRST_VALUE/LAST_VALUE, and bounded flat ARRAY_AGG or COUNT(DISTINCT)"
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
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let finish = PhysicalPlanNode::decode(finish_bytes)?.try_into_physical_plan(
            registry.as_ref(),
            &runtime,
            &finish_codec,
        )?;
        let finish = bind_window_budget(finish, budget)?;
        let projection = projection_bytes
            .map(|bytes| StatelessPhysicalExecutor::new(bytes, registry.as_ref()))
            .transpose()?;
        let finish_timestamp_schema = add_timestamp_field_arrow((*finish.schema()).clone());
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-window-partial.v2");
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
            task_context: SessionContext::new_with_config_rt(Default::default(), runtime)
                .task_ctx(),
            finish_receiver,
            projection,
            finish_timestamp_schema,
            partial_schema: physical_partial_schema,
            group_converter,
            group_fields: key_count,
            collection_columns,
            collection_ordering_pairs,
            collection_output,
            replay_columns,
            partial_state_per_row,
            fixed_state_bytes,
            partial_payload_columns,
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
                state_version: 2,
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
        if self.width == Duration::ZERO {
            return Ok(time);
        }
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
        // Preserve source order across bin sorting, partial chunks and HOP merges.
        let physical_input = self
            .partial
            .plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .map(|aggregate| aggregate.input().schema());
        let batch = if let Some(schema) =
            physical_input.filter(|schema| schema.index_of(ORDINAL_FIELD).is_ok())
        {
            let arrivals = self.store()?.allocate_arrivals(batch.num_rows()).await?;
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(UInt64Array::from_iter_values(arrivals)));
            RecordBatch::try_new(schema, columns)?
        } else {
            batch
        };
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
                    // A chunk can contain a different group per source row.
                    // Bound each group's fixed metadata/initial Vec before DF
                    // constructs any partial accumulator (not merely its output).
                    let chunk_rows =
                        partial_chunk_rows(self.partial_state_per_row, self.limits.partial_bytes)?;
                    let chunk_rows = if self.partial_payload_columns.is_empty() {
                        chunk_rows
                    } else {
                        chunk_rows.min(1)
                    };
                    let stop = (offset + chunk_rows).min(end);
                    let retained = self
                        .partial_state_per_row
                        .checked_mul(stop - offset)
                        .context("native window partial retained state overflow")?;

                    let chunk_indices = UInt32Array::from(
                        (offset..stop)
                            .map(|index| Ok::<_, anyhow::Error>(indices.value(index)))
                            .collect::<Result<Vec<_>>>()?,
                    );
                    // Projected argument values are already evaluated once.
                    // Check their selected logical bytes before take can copy
                    // any source string/binary value into a partial chunk.
                    let retained = partial_payload_bound(
                        retained,
                        &self.partial_payload_columns,
                        &batch,
                        &chunk_indices,
                    )?;
                    ensure!(
                        retained <= self.limits.partial_bytes,
                        "native window partial aggregate payload exceeds configured partial limit"
                    );
                    let _partial_state = self.store()?.reserve_retained_aggregate(retained)?;
                    self.store()?.check_reader_capacity()?;
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
            let Some(first) = snapshot
                .next_partial_range(&key, start, end, self.width == Duration::ZERO, None)
                .await?
            else {
                continue;
            };
            // Collection preflight scans the same snapshot. Scalar/ordered
            // groups carry this decoded first partial into the paged producer.
            let first_for_producer =
                if self.collection_columns.is_empty() && self.replay_columns.is_empty() {
                    Some(first)
                } else {
                    drop(first);
                    None
                };
            // ARRAY_AGG and DISTINCT retain cardinality-growing final state.
            // Count collection output values in this same snapshot before
            // invoking DataFusion; this pass retains only one decoded partial.
            // Count only value lists; paired ordering state is validated separately.
            // Measure the value bound without imposing an execution pool.
            let mut replay_rows = vec![0usize; self.replay_columns.len()];
            let mut retained = self.fixed_state_bytes;
            let mut replay_payload = vec![0usize; self.replay_columns.len()];
            if !self.collection_columns.is_empty() || !self.replay_columns.is_empty() {
                let mut after = None;
                let mut size = CollectionSize::default();
                while let Some(partial) = snapshot
                    .next_partial_range(
                        &key,
                        start,
                        end,
                        self.width == Duration::ZERO,
                        after.as_deref(),
                    )
                    .await?
                {
                    validate_ordered_partial_pairs(
                        &partial.batch,
                        &self.collection_ordering_pairs,
                    )?;
                    size.add_partial(&partial.batch, &self.collection_columns)?;
                    retained = self.fixed_state_bytes;
                    for (((column, descriptor), rows), payload) in self
                        .replay_columns
                        .iter()
                        .zip(&mut replay_rows)
                        .zip(&mut replay_payload)
                    {
                        let arrays =
                            &partial.batch.columns()[*column..*column + descriptor.state_fields];
                        descriptor.validate_partial_state(arrays, 0)?;
                        if let Some(list_column) = descriptor.state_list_column {
                            let list = arrays[list_column]
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .context("native window replay companion state must be List")?;
                            ensure!(
                                list.len() == 1,
                                "native window replay partial must have one row"
                            );
                            if !list.is_null(0) {
                                *rows = rows
                                    .checked_add(list.value_length(0) as usize)
                                    .context("native window retained row count overflow")?;
                                if descriptor.payload_multiplier > 0 {
                                    let values = list.value(0);
                                    for row in 0..values.len() {
                                        *payload = payload
                                            .checked_add(logical_payload_bytes(&values, row)?)
                                            .context("native window retained payload overflow")?;
                                    }
                                }
                            }
                        }
                        retained = retained
                            .checked_add(
                                descriptor
                                    .retained_bound(*rows, *payload)
                                    .context("native window retained aggregate size overflow")?,
                            )
                            .context("native window companion total overflow")?;
                    }
                    ensure!(
                        retained <= self.limits.partial_bytes,
                        "native window retained aggregate state exceeds configured partial limit"
                    );
                    // Refuse as soon as the bounded scan proves admission
                    // impossible. Do not walk the rest of a hot group before
                    // discovering a limit that has already been exceeded.
                    size.admit(
                        self.collection_output,
                        self.partial_schema.fields().len(),
                        self.limits,
                    )?;
                    after = Some(partial.key.clone());
                }
            }
            ensure!(
                retained <= self.limits.partial_bytes,
                "native window retained aggregate state exceeds configured partial limit"
            );
            let _retained_permit = self.store()?.reserve_retained_aggregate(retained)?;
            let _input_queue_permit = self.store()?.reserve_final_input_queue()?;
            self.store()?.check_reader_capacity()?;
            let (sender, receiver) = channel(1);
            *self.finish_receiver.write().unwrap() = Some(receiver);
            self.finish.reset()?;
            let mut finish = self.finish.execute(0, self.task_context.clone())?;
            // Move the sender into the producer so the input stream observes
            // EOF as soon as all paged partials have been sent.
            let producer_snapshot = snapshot;
            let exact = self.width == Duration::ZERO;
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
                    .next_partial_range(&key, start, end, exact, after.as_deref())
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
                        collector.collect(batch).await?;
                    }
                } else {
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
                    .expire_range_snapshot(
                        &snapshot,
                        end,
                        self.width == Duration::ZERO,
                        &mut retired_through,
                    )
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

    fn store_for_schema(schema: SchemaRef) -> (WindowStore, WorkerStateResources) {
        store_for_schema_limit(schema, 8192)
    }

    fn store_for_schema_limit(
        schema: SchemaRef,
        value_bytes: usize,
    ) -> (WindowStore, WorkerStateResources) {
        let write_bytes = 65536.max(value_bytes * 8);
        let write_reservation =
            arroyo_state::live::write::AdmittedWriteBatch::reservation_bytes(write_bytes, 16)
                .unwrap();
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: (1024 * 1024).max(write_reservation),
            decoded_value_bytes: 4 * 1024 * 1024,
            scan_page_bytes: (1024 * 1024).max(value_bytes * 16),
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
            resources.clone(),
            schema,
            WindowStoreLimits {
                key_bytes: 128,
                value_bytes,
                page_bytes: 32768.max(value_bytes * 2),
                page_entries: 1,
                write_bytes,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            },
        )
        .map(|store| (store, resources))
        .unwrap()
    }

    fn store_with_resources() -> (WindowStore, WorkerStateResources) {
        store_for_schema(partial().schema())
    }

    fn store() -> WindowStore {
        store_with_resources().0
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
                .add_partial(&batch, &[(0, true), (1, true), (2, false)])
                .unwrap();
        }
        assert!(small.output_bound(512, 5).unwrap() <= 32 * 1024);

        let mut large = CollectionSize::default();
        for _ in 0..4096 {
            large.add_partial(&batch, &[(0, true)]).unwrap();
        }
        assert!(large.output_bound(512, 2).unwrap() > 32 * 1024);
    }

    #[tokio::test]
    async fn ordered_arrays_preserve_paired_values_filter_and_null_order_across_paged_partials() {
        use arrow_array::BooleanArray;
        use arroyo_planner::physical::{ArroyoMemExec, new_registry};
        use datafusion::functions_aggregate::array_agg::array_agg_udaf;
        use datafusion::physical_expr::{
            LexOrdering, PhysicalSortExpr, aggregate::AggregateExprBuilder, expressions::col,
        };
        use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
        use futures::TryStreamExt;

        for nulls_first in [true, false] {
            let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
            let registry = Arc::new(new_registry());
            let raw_schema = Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("number", DataType::Int64, false),
                Field::new("sort_key", DataType::Utf8, true),
                Field::new("tie", DataType::Int64, true),
                Field::new("selected", DataType::Boolean, false),
            ]));
            let ordering = LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    col("sort_key", &raw_schema).unwrap(),
                    arrow::compute::SortOptions {
                        descending: true,
                        nulls_first,
                    },
                ),
                PhysicalSortExpr::new(
                    col("tie", &raw_schema).unwrap(),
                    arrow::compute::SortOptions {
                        descending: false,
                        nulls_first,
                    },
                ),
            ]);
            let aggregates = ["label", "number"]
                .into_iter()
                .map(|name| {
                    Arc::new(
                        AggregateExprBuilder::new(
                            array_agg_udaf(),
                            vec![col(name, &raw_schema).unwrap()],
                        )
                        .schema(raw_schema.clone())
                        .alias(format!("{name}_items"))
                        .order_by(ordering.clone())
                        .build()
                        .unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            for aggregate in &aggregates {
                let fields = aggregate.state_fields().unwrap();
                validate_ordered_array_state(
                    &fields,
                    &aggregate.expressions()[0].data_type(&raw_schema).unwrap(),
                    &[DataType::Utf8, DataType::Int64],
                )
                .unwrap();
            }
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(ArroyoMemExec::new("input".into(), raw_schema.clone()));
            let planning: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    PhysicalGroupBy::new_single(vec![]),
                    aggregates.clone(),
                    vec![Some(col("selected", &raw_schema).unwrap()); 2],
                    input,
                    raw_schema.clone(),
                )
                .unwrap(),
            );
            let partial_schema = planning.schema();
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::Planning,
            };
            let partial_bytes = PhysicalPlanNode::try_from_physical_plan(planning, &codec)
                .unwrap()
                .encode_to_vec();
            let mut partial = StatelessPhysicalExecutor::new(&partial_bytes, &registry).unwrap();
            let (store, resources) = store_for_schema(partial_schema.clone());
            let columns = [(0, true), (2, true)];
            let pairs = [(0, 1), (2, 3)];
            // One retained page per partial; overlapping sort keys and a
            // rejected FILTER row exercise the real DF Partial state layout.
            for (label, number, key, tie, selected) in [
                ("z-later", 21, Some("z"), Some(2), true),
                ("a", 10, Some("a"), Some(1), true),
                ("filtered", 99, Some("zz"), None, false),
                ("nil", 30, None, None, true),
                ("z-first", 20, Some("z"), Some(1), true),
            ] {
                let raw = RecordBatch::try_new(
                    raw_schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![label])),
                        Arc::new(Int64Array::from(vec![number])),
                        Arc::new(StringArray::from(vec![key])),
                        Arc::new(Int64Array::from(vec![tie])),
                        Arc::new(BooleanArray::from(vec![selected])),
                    ],
                )
                .unwrap();
                let batches = partial
                    .process_batch(raw)
                    .await
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(batches.len(), 1);
                let mut size = CollectionSize::default();
                size.add_partial(&batches[0], &columns).unwrap();
                validate_ordered_partial_pairs(&batches[0], &pairs).unwrap();
                if !selected {
                    assert_eq!(size.output_elements, 0);
                }
                store.append(&[], 0, &batches[0]).await.unwrap();
            }
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(ArroyoMemExec::new("input".into(), partial_schema.clone()));
            let planning: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Final,
                    PhysicalGroupBy::new_single(vec![]),
                    aggregates,
                    vec![None, None],
                    input,
                    raw_schema,
                )
                .unwrap(),
            );
            let serialized = PhysicalPlanNode::try_from_physical_plan(planning, &codec).unwrap();
            let receiver = Arc::new(RwLock::new(None));
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::BoundedBatchStream(receiver.clone()),
            };
            let finish = serialized
                .try_into_physical_plan(registry.as_ref(), &runtime, &codec)
                .unwrap();
            let finish_timestamp_schema = add_timestamp_field_arrow((*finish.schema()).clone());
            let mut operator = NativeWindow {
                width: Duration::ZERO,
                slide: Duration::ZERO,
                hopping: false,
                binning: Arc::new(datafusion::physical_expr::expressions::Literal::new(
                    ScalarValue::TimestampNanosecond(Some(0), None),
                )),
                partial,
                finish,
                task_context: SessionContext::new_with_config_rt(
                    Default::default(),
                    runtime.clone(),
                )
                .task_ctx(),
                finish_receiver: receiver,
                projection: None,
                finish_timestamp_schema,
                partial_schema,
                group_converter: None,
                group_fields: 0,
                collection_columns: columns.to_vec(),
                collection_ordering_pairs: pairs.to_vec(),
                collection_output: true,
                replay_columns: vec![],
                partial_state_per_row: 0,
                fixed_state_bytes: 0,
                partial_payload_columns: vec![],
                limits: WindowStateConfig {
                    key_bytes: 128,
                    partial_bytes: 8192,
                    page_bytes: 32768,
                    page_entries: 1,
                    write_bytes: 65536,
                    write_operations: 16,
                    max_resident_bytes: 8 * 1024 * 1024,
                },
                identity: vec![],
                store: Some(store),
            };
            let snapshot = operator.store().unwrap().snapshot().await.unwrap();
            let mut collector = RecordedCollector::default();
            operator.limits.partial_bytes = 128;
            let refusal = operator
                .emit_interval(&snapshot, 0, 0, &mut collector)
                .await
                .unwrap_err();
            assert!(
                refusal.to_string().contains("configured partial limit"),
                "unexpected preflight refusal: {refusal:#}"
            );
            assert!(collector.0.is_empty());
            assert_eq!(
                operator.store().unwrap().earliest_time().await.unwrap(),
                Some(0)
            );
            operator.limits.partial_bytes = 8192;
            operator
                .emit_interval(&snapshot, 0, 0, &mut collector)
                .await
                .unwrap();
            assert_eq!(collector.0.len(), 1);
            let labels = collector.0[0]
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            let labels = labels
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let numbers = collector.0[0]
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            let numbers = numbers
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let expected = if nulls_first {
                vec![
                    (Some("nil"), Some(30)),
                    (Some("z-first"), Some(20)),
                    (Some("z-later"), Some(21)),
                    (Some("a"), Some(10)),
                ]
            } else {
                vec![
                    (Some("z-first"), Some(20)),
                    (Some("z-later"), Some(21)),
                    (Some("a"), Some(10)),
                    (Some("nil"), Some(30)),
                ]
            };
            assert_eq!(
                labels.into_iter().zip(numbers).collect::<Vec<_>>(),
                expected
            );
            drop(collector);
            drop(snapshot);
            drop(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .unwrap(),
            );
        }
    }

    #[test]
    fn ordered_collection_rejects_malformed_partial_pairs() {
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        let fields = vec![Arc::new(Field::new("ordering", DataType::Utf8, true))];
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a"]));
        let order: Arc<dyn Array> = Arc::new(StructArray::new(
            fields.clone().into(),
            vec![values.clone()],
            None,
        ));
        let make_list = |child: Arc<dyn Array>, null: bool| -> Arc<dyn Array> {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(child.data_type().clone(), true)),
                OffsetBuffer::new(vec![0_i32, child.len() as i32].into()),
                child,
                Some(NullBuffer::from(vec![!null])),
            ))
        };
        let make_batch = |value: Arc<dyn Array>, order: Arc<dyn Array>| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("values", value.data_type().clone(), true),
                    Field::new("orderings", order.data_type().clone(), true),
                ])),
                vec![value, order],
            )
            .unwrap()
        };
        for batch in [
            make_batch(
                make_list(values.clone(), true),
                make_list(order.clone(), false),
            ),
            make_batch(
                make_list(values.clone(), false),
                make_list(order.clone(), true),
            ),
            make_batch(
                make_list(Arc::new(StringArray::from(vec!["a", "b"])), false),
                make_list(order.clone(), false),
            ),
            make_batch(
                make_list(values.clone(), false),
                make_list(
                    Arc::new(StructArray::new(
                        fields.into(),
                        vec![values],
                        Some(NullBuffer::from(vec![false])),
                    )),
                    false,
                ),
            ),
        ] {
            assert!(validate_ordered_partial_pairs(&batch, &[(0, 1)]).is_err());
        }
    }

    #[tokio::test]
    async fn planned_count_distinct_byte_types_share_grouped_selector_window_runtime() {
        use arroyo_rpc::grpc::api::TumblingWindowAggregateOperator;
        use datafusion_proto::physical_plan::{
            DefaultPhysicalExtensionCodec, from_proto::parse_physical_expr,
        };
        use datafusion_proto::protobuf::PhysicalExprNode;
        for target in ["Utf8View", "LargeUtf8", "Binary"] {
            let sql = format!(
                "CREATE TABLE input_rows (event_time TIMESTAMP NOT NULL,value TEXT, \
                WATERMARK FOR event_time AS event_time - INTERVAL '1 second') \
                WITH(connector='single_file',path='/tmp/str60-count-distinct.jsonl',format='json',type='source'); \
                SELECT TUMBLE(INTERVAL '1 second') AS window,FIRST_VALUE(value) AS first, \
                COUNT(DISTINCT arrow_cast(value,'{target}')) AS n FROM input_rows GROUP BY window"
            );
            let compiled = arroyo_planner::parse_and_get_program(
                &sql,
                arroyo_planner::ArroyoSchemaProvider::new(),
                arroyo_planner::SqlConfig {
                    default_parallelism: 1,
                },
            )
            .await
            .unwrap();
            let operator = compiled
                .program
                .graph
                .node_weights()
                .flat_map(|node| node.operator_chain.iter())
                .map(|(operator, _)| operator)
                .find(|operator| {
                    operator.operator_name
                        == arroyo_datastream::logical::OperatorName::TumblingWindowAggregate
                })
                .unwrap();
            let declared =
                TumblingWindowAggregateOperator::decode(operator.operator_config.as_slice())
                    .unwrap();
            let input: ArroyoSchema = declared.input_schema.clone().unwrap().try_into().unwrap();
            let partial_schema: ArroyoSchema =
                declared.partial_schema.clone().unwrap().try_into().unwrap();
            let registry = Arc::new(arroyo_planner::physical::new_registry());
            let binning = parse_physical_expr(
                &PhysicalExprNode::decode(declared.binning_function.as_slice()).unwrap(),
                registry.as_ref(),
                &input.schema,
                &DefaultPhysicalExtensionCodec {},
            )
            .unwrap();
            // CountView's verified constructor floor requires real headroom;
            // derive backend page/write allowances from this explicit value cap.
            let value_bytes = 128 * 1024;
            let mut native = NativeWindow::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                false,
                binning,
                &declared.binning_function,
                &declared.partial_aggregation_plan,
                &declared.final_aggregation_plan,
                declared.final_projection.as_deref(),
                &partial_schema,
                registry,
                WindowStateConfig {
                    key_bytes: 128,
                    partial_bytes: value_bytes,
                    page_bytes: value_bytes * 2,
                    page_entries: 1,
                    write_bytes: value_bytes * 8,
                    write_operations: 16,
                    max_resident_bytes: 8 * 1024 * 1024,
                },
            )
            .unwrap_or_else(|error| panic!("{target} constructor: {error:#}"));
            assert_eq!(
                native.collection_columns.len(),
                0,
                "descriptor-covered COUNT DISTINCT entered legacy ARRAY checks"
            );
            let columns = input
                .schema
                .fields()
                .iter()
                .map(|field| -> Arc<dyn Array> {
                    match field.data_type() {
                        DataType::Utf8 => Arc::new(StringArray::from(vec!["b", "a", "b"])),
                        DataType::LargeUtf8 => {
                            Arc::new(arrow_array::LargeStringArray::from(vec!["b", "a", "b"]))
                        }
                        DataType::Utf8View => {
                            Arc::new(arrow_array::StringViewArray::from(vec!["b", "a", "b"]))
                        }
                        DataType::Binary => Arc::new(arrow_array::BinaryArray::from(vec![
                            b"b".as_slice(),
                            b"a".as_slice(),
                            b"b".as_slice(),
                        ])),
                        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                            Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
                                30, 10, 20,
                            ]))
                        }
                        other => panic!("unexpected {target} projected input type {other:?}"),
                    }
                })
                .collect();
            let batch = RecordBatch::try_new(input.schema.clone(), columns).unwrap();
            let (store, resources) =
                store_for_schema_limit(native.partial_schema.clone(), value_bytes);
            native.store = Some(store);
            let (control, _rx) = channel(16);
            let mut ctx = OperatorContext::new(
                Arc::new(arroyo_types::get_test_task_info()),
                None,
                control,
                1,
                vec![Arc::new(input)],
                None,
                HashMap::new(),
            )
            .await;
            native
                .process_batch(batch, &mut ctx)
                .await
                .unwrap_or_else(|error| panic!("{target} input: {error:#}"));
            let snapshot = native.store().unwrap().snapshot().await.unwrap();
            let mut output = RecordedCollector::default();
            native
                .emit_interval(&snapshot, 0, 1_000_000_000, &mut output)
                .await
                .unwrap_or_else(|error| panic!("{target} output: {error:#}"));
            assert_eq!(output.0.len(), 1, "{target}");
            let batch = &output.0[0];
            // NativeWindow emits the physical GROUP BY columns followed by
            // aggregate expressions; SELECT aliases belong to the downstream
            // projection. This fixture invokes the serialized window stage.
            assert_eq!(batch.num_columns(), 4);
            let first = 1;
            let count = 2;
            assert_eq!(
                batch
                    .column(first)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "a",
                "{target}"
            );
            assert_eq!(
                batch
                    .column(count)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                2,
                "{target}"
            );
            drop(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .unwrap(),
            );
        }
    }

    #[test]
    fn variable_partial_preflight_checks_selected_logical_bytes_before_take() {
        use datafusion::physical_expr::{aggregate::AggregateExprBuilder, expressions::col};
        let limit = 128 * 1024;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8View,
            true,
        )]));
        let aggregate = AggregateExprBuilder::new(
            datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf(),
            vec![col("value", &schema).unwrap()],
        )
        .schema(schema.clone())
        .alias("hll")
        .build()
        .unwrap();
        let descriptor = aggregate_descriptor(&aggregate, &schema).unwrap().unwrap();
        let huge = "x".repeat(2 * limit);
        let values = Arc::new(arrow_array::StringViewArray::from(vec![
            Some(huge.as_str()),
            Some("tiny"),
        ])) as arrow_array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        assert!(batch.get_array_memory_size() > limit);
        let base = descriptor.retained_bound(1, 0).unwrap();
        let selected = partial_payload_bound(
            base,
            &[(0, descriptor)],
            &batch,
            &UInt32Array::from(vec![1]),
        )
        .unwrap();
        assert_eq!(selected, base + 4);
        assert!(
            selected <= limit,
            "small selected value was charged for unused backing"
        );
        let oversized = partial_payload_bound(
            base,
            &[(0, descriptor)],
            &batch,
            &UInt32Array::from(vec![0]),
        )
        .unwrap();
        assert!(
            oversized > limit,
            "oversized selected payload passed pre-take admission"
        );
        // The actual production path performs this same check before invoking
        // take or starting the partial DF executor. No SQL argument is evaluated.
    }

    async fn median_fixture() -> (NativeWindow, WorkerStateResources, OperatorContext) {
        use arroyo_planner::physical::{ArroyoMemExec, new_registry};
        use datafusion::{
            functions_aggregate::median::median_udaf,
            physical_expr::{aggregate::AggregateExprBuilder, expressions::col},
            physical_plan::aggregates::{AggregateMode, PhysicalGroupBy},
        };
        let raw = partial().schema();
        let aggregates = (0..2)
            .map(|index| {
                Arc::new(
                    AggregateExprBuilder::new(median_udaf(), vec![col("count", &raw).unwrap()])
                        .schema(raw.clone())
                        .alias(format!("median_{index}"))
                        .build()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let partial_plan: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![]),
                aggregates.clone(),
                vec![None; 2],
                Arc::new(ArroyoMemExec::new("input".into(), raw.clone())),
                raw.clone(),
            )
            .unwrap(),
        );
        let state_schema = partial_plan.schema();
        let finish: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::new_single(vec![]),
                aggregates,
                vec![None; 2],
                Arc::new(ArroyoMemExec::new("input".into(), state_schema.clone())),
                raw.clone(),
            )
            .unwrap(),
        );
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let partial_bytes = PhysicalPlanNode::try_from_physical_plan(partial_plan, &codec)
            .unwrap()
            .encode_to_vec();
        let finish_bytes = PhysicalPlanNode::try_from_physical_plan(finish, &codec)
            .unwrap()
            .encode_to_vec();
        let schema =
            ArroyoSchema::from_schema_unkeyed(add_timestamp_field_arrow((*state_schema).clone()))
                .unwrap();
        let mut operator = NativeWindow::new(
            Duration::from_nanos(10),
            Duration::from_nanos(10),
            false,
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                ScalarValue::TimestampNanosecond(Some(0), None),
            )),
            &[],
            &partial_bytes,
            &finish_bytes,
            None,
            &schema,
            Arc::new(new_registry()),
            WindowStateConfig {
                key_bytes: 128,
                partial_bytes: 8192,
                page_bytes: 32768,
                page_entries: 1,
                write_bytes: 65536,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap();
        let (store, resources) = store_for_schema(state_schema);
        operator.store = Some(store);
        let (control, _rx) = channel(16);
        let ctx = OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control,
            1,
            vec![Arc::new(
                ArroyoSchema::from_schema_unkeyed(add_timestamp_field_arrow((*raw).clone()))
                    .unwrap(),
            )],
            None,
            HashMap::new(),
        )
        .await;
        (operator, resources, ctx)
    }

    #[tokio::test]
    async fn median_partial_preflight_shares_bounds_and_caps_each_chunk_before_materialization() {
        let (mut operator, resources, mut ctx) = median_fixture().await;
        let one = operator.partial_state_per_row;
        assert_eq!(
            one,
            operator
                .replay_columns
                .iter()
                .map(|(_, descriptor)| descriptor.retained_bound(1, 0).unwrap())
                .sum::<usize>()
        );
        operator.limits.partial_bytes = one - 1;
        let error = operator
            .process_batch(partial(), &mut ctx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("one partial aggregate row"));
        assert_eq!(
            operator.store().unwrap().earliest_time().await.unwrap(),
            None
        );
        // Exactly one conservative group allowance fits: 65 rows must become
        // 65 finite one-row partials rather than a 64-row unbounded accumulator.
        operator.limits.partial_bytes = one;
        let rows = RecordBatch::try_new(
            partial().schema(),
            vec![Arc::new(Int64Array::from(vec![1; 65]))],
        )
        .unwrap();
        operator.process_batch(rows, &mut ctx).await.unwrap();
        let snapshot = operator.store().unwrap().snapshot().await.unwrap();
        let mut after = None;
        let mut pages = 0;
        while let Some(partial) = snapshot
            .next_partial_range(&[], 0, 10, false, after.as_deref())
            .await
            .unwrap()
        {
            pages += 1;
            assert!(pages <= 65);
            for column in partial.batch.columns() {
                assert_eq!(
                    column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .value_length(0),
                    1
                );
            }
            after = Some(partial.key);
        }
        assert_eq!(pages, 65);
        drop(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn median_final_preflight_refusal_and_cancellation_release_state_without_retirement() {
        let (mut operator, resources, mut ctx) = median_fixture().await;
        for _ in 0..9 {
            operator.process_batch(partial(), &mut ctx).await.unwrap();
        }
        let snapshot = operator.store().unwrap().snapshot().await.unwrap();
        let mut collector = RecordedCollector::default();
        operator.limits.partial_bytes = operator
            .replay_columns
            .iter()
            .map(|(_, descriptor)| descriptor.retained_bound(9, 0).unwrap())
            .sum::<usize>()
            - 1;
        let error = operator
            .emit_interval(&snapshot, 0, 10, &mut collector)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("retained aggregate state"));
        assert!(collector.0.is_empty());
        assert_eq!(
            operator.store().unwrap().earliest_time().await.unwrap(),
            Some(0)
        );
        assert_eq!(operator.store().unwrap().progress().await.unwrap(), None);
        drop(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .unwrap(),
        );
        operator.limits.partial_bytes = 8192;
        let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
        let (_release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut blocked = BlockedCollector {
            started: Some(started_tx),
            release: release_rx,
        };
        let mut pending = Box::pin(operator.emit_interval(&snapshot, 0, 10, &mut blocked));
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! { result = &mut pending => panic!("unexpected completion {result:?}"), started = &mut started_rx => { started.unwrap(); } }
        }).await.unwrap();
        drop(pending);
        assert_eq!(
            operator.store().unwrap().earliest_time().await.unwrap(),
            Some(0)
        );
        assert_eq!(operator.store().unwrap().progress().await.unwrap(), None);
        drop(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .unwrap(),
        );
        operator
            .emit_interval(&snapshot, 0, 10, &mut collector)
            .await
            .unwrap();
        assert_eq!(collector.0.len(), 1);
        for column in &collector.0[0].columns()[..2] {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
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

    #[test]
    fn collection_preflight_refuses_backend_output_limit_before_final_execution() {
        let limits = WindowStateConfig {
            key_bytes: 128,
            partial_bytes: 8192,
            page_bytes: 32768,
            page_entries: 1,
            write_bytes: 65536,
            write_operations: 16,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        let mut builder = ListBuilder::new(Int64Builder::new());
        for value in 0..1024 {
            builder.values().append_value(value);
        }
        builder.append(true);
        let list = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "values",
                list.data_type().clone(),
                false,
            )])),
            vec![list],
        )
        .unwrap();
        let mut size = CollectionSize::default();
        size.add_partial(&batch, &[(0, true)]).unwrap();
        assert!(
            size.admit(true, 1, limits)
                .unwrap_err()
                .to_string()
                .contains("configured partial limit")
        );
        let wider = WindowStateConfig {
            partial_bytes: 32768,
            ..limits
        };
        assert!(size.admit(true, 1, wider).is_ok());
    }

    struct BlockedCollector {
        started: Option<tokio::sync::oneshot::Sender<std::sync::Weak<dyn Array>>>,
        release: tokio::sync::oneshot::Receiver<()>,
    }

    #[async_trait::async_trait]
    impl Collector for BlockedCollector {
        async fn collect(&mut self, batch: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            self.started
                .take()
                .unwrap()
                .send(Arc::downgrade(batch.column(0)))
                .unwrap();
            (&mut self.release).await.unwrap();
            drop(batch);
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct RecordedCollector(Vec<RecordBatch>);

    #[async_trait::async_trait]
    impl Collector for RecordedCollector {
        async fn collect(&mut self, batch: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            self.0.push(batch);
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn slow_tumble_cancellation_preserves_many_hot_panes() {
        slow_closure_cancellation_preserves_many_hot_panes(false, false, false).await;
    }

    #[tokio::test]
    async fn slow_hop_cancellation_preserves_many_hot_panes() {
        slow_closure_cancellation_preserves_many_hot_panes(true, false, false).await;
    }

    #[tokio::test]
    async fn collection_refusal_retry_and_cancellation_release_paged_state() {
        slow_closure_cancellation_preserves_many_hot_panes(false, true, false).await;
    }

    #[tokio::test]
    async fn slow_exact_cancellation_preserves_many_hot_bins() {
        slow_closure_cancellation_preserves_many_hot_panes(false, false, true).await;
    }

    #[tokio::test]
    async fn exact_collection_refusal_retry_and_cancellation_release_paged_state() {
        slow_closure_cancellation_preserves_many_hot_panes(false, true, true).await;
    }

    async fn slow_closure_cancellation_preserves_many_hot_panes(
        hopping: bool,
        collection: bool,
        exact: bool,
    ) {
        closure_fixture(hopping, collection, exact, false).await;
    }

    async fn closure_fixture(hopping: bool, collection: bool, exact: bool, diagnostic: bool) {
        use arroyo_planner::physical::{ArroyoMemExec, new_registry};
        use datafusion::functions_aggregate::{
            array_agg::array_agg_udaf, count::count_udaf, sum::sum_udaf,
        };
        use datafusion::physical_expr::{aggregate::AggregateExprBuilder, expressions::col};
        use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
        use std::time::UNIX_EPOCH;

        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        {
            let registry = new_registry();
            let raw_schema = partial().schema();
            let mut aggregates = vec![];
            if collection {
                for (fun, distinct, alias) in [
                    (array_agg_udaf(), false, "items"),
                    (array_agg_udaf(), true, "unique_items"),
                    (count_udaf(), true, "distinct_count"),
                ] {
                    let builder =
                        AggregateExprBuilder::new(fun, vec![col("count", &raw_schema).unwrap()])
                            .schema(raw_schema.clone())
                            .alias(alias);
                    let builder = if distinct {
                        builder.distinct()
                    } else {
                        builder
                    };
                    aggregates.push(Arc::new(builder.build().unwrap()));
                }
            } else {
                aggregates.push(Arc::new(
                    AggregateExprBuilder::new(sum_udaf(), vec![col("count", &raw_schema).unwrap()])
                        .schema(raw_schema.clone())
                        .alias("total")
                        .build()
                        .unwrap(),
                ));
            }
            let schema = if collection {
                Arc::new(Schema::new(
                    aggregates
                        .iter()
                        .flat_map(|aggregate| aggregate.state_fields().unwrap())
                        .collect::<Vec<_>>(),
                ))
            } else {
                raw_schema.clone()
            };
            let row = if collection {
                let lists = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let DataType::List(item) = field.data_type() else {
                            panic!("flat collection state required")
                        };
                        let mut builder =
                            ListBuilder::new(Int64Builder::new()).with_field(item.clone());
                        builder.values().append_value(1);
                        builder.append(true);
                        Arc::new(builder.finish()) as Arc<dyn Array>
                    })
                    .collect();
                RecordBatch::try_new(schema.clone(), lists).unwrap()
            } else {
                partial()
            };
            let (store, state) = store_for_schema(schema.clone());
            let (store, diagnostic_owner) = if diagnostic {
                drop(store);
                let root = std::env::temp_dir().join(format!(
                    "str124-operator-{}-{hopping}-{collection}",
                    std::process::id()
                ));
                std::fs::create_dir_all(&root).unwrap();
                let backend: Arc<dyn LiveStateBackend> = Arc::new(
                    arroyo_state::live::rocks::RocksLiveState::open(
                        arroyo_state::live::lifecycle::RocksStateConfig {
                            root: root.join("live"),
                            job_id: "str124".into(),
                            operator_id: "window".into(),
                            subtask: 0,
                            generation: 1,
                            attempt: 0,
                        },
                        state.clone(),
                    )
                    .await
                    .unwrap(),
                );
                let mut tables = LiveTableManager::new(
                    backend.clone(),
                    Ownership::PartitionLocal {
                        subtask: 0,
                        parallelism: 1,
                    },
                )
                .unwrap();
                let store = WindowStore::new(
                    backend.clone(),
                    tables.register("window").unwrap(),
                    state.clone(),
                    schema.clone(),
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
                .unwrap();
                drop(tables);
                (store, Some((backend, root)))
            } else {
                (store, None)
            };
            // Many panes, with a hot first pane spanning many one-entry pages.
            if !diagnostic {
                for pane in 0..48 {
                    for _ in 0..if pane == 0 { 128 } else { 1 } {
                        store
                            .append(&[], pane * if exact { 1 } else { 10 }, &row)
                            .await
                            .unwrap();
                    }
                }
            }
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(ArroyoMemExec::new("input".into(), schema.clone()));
            let filters = vec![None; aggregates.len()];
            let planning: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    if collection {
                        AggregateMode::Final
                    } else {
                        AggregateMode::Single
                    },
                    PhysicalGroupBy::new_single(vec![]),
                    aggregates,
                    filters,
                    input.clone(),
                    raw_schema,
                )
                .unwrap(),
            );
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::Planning,
            };
            let serialized = PhysicalPlanNode::try_from_physical_plan(planning, &codec).unwrap();
            let receiver = Arc::new(RwLock::new(None));
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::BoundedBatchStream(receiver.clone()),
            };
            let finish = serialized
                .try_into_physical_plan(&registry, &runtime, &codec)
                .unwrap();
            let partial_plan = PhysicalPlanNode::try_from_physical_plan(
                input,
                &ArroyoPhysicalExtensionCodec {
                    context: DecodingContext::Planning,
                },
            )
            .unwrap()
            .encode_to_vec();
            let finish_timestamp_schema = add_timestamp_field_arrow((*finish.schema()).clone());
            let mut operator = NativeWindow {
                width: Duration::from_nanos(if exact {
                    0
                } else if hopping {
                    20
                } else {
                    10
                }),
                slide: Duration::from_nanos(if exact { 0 } else { 10 }),
                hopping,
                binning: Arc::new(datafusion::physical_expr::expressions::Literal::new(
                    ScalarValue::TimestampNanosecond(Some(0), None),
                )),
                partial: StatelessPhysicalExecutor::new(&partial_plan, &registry).unwrap(),
                finish,
                task_context: SessionContext::new_with_config_rt(
                    Default::default(),
                    runtime.clone(),
                )
                .task_ctx(),
                finish_receiver: receiver,
                projection: None,
                finish_timestamp_schema: finish_timestamp_schema.clone(),
                partial_schema: schema.clone(),
                group_converter: None,
                group_fields: 0,
                collection_columns: if collection {
                    vec![(0, true), (1, true), (2, false)]
                } else {
                    vec![]
                },
                collection_ordering_pairs: vec![],
                collection_output: collection,
                replay_columns: vec![],
                partial_state_per_row: 0,
                fixed_state_bytes: 0,
                partial_payload_columns: vec![],
                limits: WindowStateConfig {
                    key_bytes: 128,
                    partial_bytes: 8192,
                    page_bytes: 32768,
                    page_entries: 1,
                    write_bytes: 65536,
                    write_operations: 16,
                    max_resident_bytes: 8 * 1024 * 1024,
                },
                identity: vec![],
                store: Some(store),
            };
            let (control, _control_rx) = channel(16);
            let mut ctx = OperatorContext::new(
                Arc::new(arroyo_types::get_test_task_info()),
                None,
                control,
                1,
                vec![Arc::new(
                    ArroyoSchema::from_schema_unkeyed(add_timestamp_field_arrow((*schema).clone()))
                        .unwrap(),
                )],
                Some(Arc::new(
                    ArroyoSchema::from_schema_unkeyed(finish_timestamp_schema).unwrap(),
                )),
                HashMap::new(),
            )
            .await;
            if diagnostic {
                diagnostic_operator_phases(&mut operator, &mut ctx, &row, collection).await;
                drop(operator);
                drop(ctx);
                let (backend, root) = diagnostic_owner.unwrap();
                backend.close().await.unwrap();
                std::fs::remove_dir_all(root).unwrap();
                return;
            }
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(
                    UNIX_EPOCH + Duration::from_nanos(if exact { 2 } else { 20 }),
                ),
            );
            if exact {
                assert_eq!(operator.bin_start(19).unwrap(), 19);
                // The exact bin at the watermark is admitted; an older bin is late.
                operator.process_batch(row.clone(), &mut ctx).await.unwrap();
                assert_eq!(
                    operator.store().unwrap().earliest_time().await.unwrap(),
                    Some(0)
                );
                operator.binning = Arc::new(datafusion::physical_expr::expressions::Literal::new(
                    ScalarValue::TimestampNanosecond(Some(2), None),
                ));
                if !collection {
                    operator.process_batch(row.clone(), &mut ctx).await.unwrap();
                }
            }
            if collection {
                // The output value is bounded before any collector call or expiry.
                let original = operator.limits;
                operator.limits.partial_bytes = 512;
                let mut refused = RecordedCollector::default();
                let error = operator
                    .handle_watermark(&mut ctx, &mut refused)
                    .await
                    .unwrap_err();
                assert!(
                    error.to_string().contains("configured partial limit"),
                    "{error}"
                );
                assert!(refused.0.is_empty());
                assert_eq!(operator.store().unwrap().progress().await.unwrap(), None);
                assert_eq!(
                    operator.store().unwrap().earliest_time().await.unwrap(),
                    Some(0)
                );
                drop(
                    state
                        .try_decoded_value(state.config().decoded_value_bytes)
                        .unwrap(),
                );
                operator.limits = original;
            }
            let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let mut collector = BlockedCollector {
                started: Some(started_tx),
                release: release_rx,
            };
            let mut pending = Box::pin(operator.handle_watermark(&mut ctx, &mut collector));
            let weak = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut pending => panic!("closure failed before slow output: {result:?}"),
                    started = &mut started_rx => started.unwrap(),
                }
            }).await.unwrap();
            assert!(futures::poll!(&mut pending).is_pending());
            assert!(weak.upgrade().is_some());
            assert!(
                state
                    .try_decoded_value(state.config().decoded_value_bytes - 6 * 8192 + 1)
                    .is_err()
            );
            drop(pending);
            drop(collector);
            assert!(release_tx.send(()).is_err());
            assert!(weak.upgrade().is_none());
            drop(
                state
                    .try_decoded_value(state.config().decoded_value_bytes)
                    .unwrap(),
            );
            assert_eq!(operator.store().unwrap().progress().await.unwrap(), None);
            assert_eq!(
                operator.store().unwrap().earliest_time().await.unwrap(),
                Some(0)
            );
            assert!(operator.finish_receiver.read().unwrap().is_none());

            let mut resumed = RecordedCollector::default();
            operator
                .handle_watermark(&mut ctx, &mut resumed)
                .await
                .unwrap();
            assert_eq!(resumed.0.len(), 2);
            if exact {
                for (batch, time) in resumed.0.iter().zip([0, 1]) {
                    assert_eq!(
                        batch
                            .column(batch.num_columns() - 1)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
                            .unwrap()
                            .value(0),
                        time
                    );
                }
            }
            if collection {
                let values = resumed.0[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(0);
                assert_eq!(values.len(), 128);
                assert!(
                    values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .all(|value| value == Some(1))
                );
                assert_eq!(
                    resumed.0[0]
                        .column(1)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .value(0)
                        .len(),
                    1
                );
                assert_eq!(
                    resumed.0[0]
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    1
                );
            } else {
                assert_eq!(
                    resumed.0[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    128
                );
                assert_eq!(
                    resumed.0[1]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    if hopping { 129 } else { 1 }
                );
            }
            assert_eq!(
                operator.store().unwrap().progress().await.unwrap(),
                Some(if exact { 1 } else { 20 })
            );
            assert_eq!(
                operator.store().unwrap().earliest_time().await.unwrap(),
                Some(if hopping {
                    10
                } else if exact {
                    2
                } else {
                    20
                })
            );
            // Repeating the watermark neither re-emits nor closes the equal bin.
            operator
                .handle_watermark(&mut ctx, &mut resumed)
                .await
                .unwrap();
            assert_eq!(resumed.0.len(), 2);
            if exact {
                let directory = std::env::temp_dir().join(format!(
                    "native-exact-operator-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                // Adjacent nanosecond bins0/1 closed; bin2 is still open.
                // Checkpoint this retained state and replace its live owner.
                let fresh = Box::pin(
                    operator
                        .store
                        .take()
                        .unwrap()
                        .checkpoint_restore_for_test(&directory),
                )
                .await;
                operator.store = Some(fresh);
                let mut equality = RecordedCollector::default();
                operator
                    .handle_watermark(&mut ctx, &mut equality)
                    .await
                    .unwrap();
                assert!(equality.0.is_empty());
                assert_eq!(
                    operator.store().unwrap().earliest_time().await.unwrap(),
                    Some(2)
                );
                std::fs::remove_dir_all(directory).unwrap();
            }
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(arroyo_types::from_nanos(u64::MAX as u128)),
            );
            operator
                .handle_watermark(&mut ctx, &mut resumed)
                .await
                .unwrap();
            assert_eq!(resumed.0.len(), if hopping { 49 } else { 48 });
            if exact && !collection {
                assert_eq!(
                    resumed.0[2]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    2
                );
            }
            assert_eq!(
                operator.store().unwrap().earliest_time().await.unwrap(),
                None
            );
            drop(
                state
                    .try_decoded_value(state.config().decoded_value_bytes)
                    .unwrap(),
            );
        }
    }
    struct ScanDiagnosticCollector {
        expected: std::collections::BTreeMap<i64, i64>,
        collection: bool,
        outputs: usize,
    }

    #[async_trait::async_trait]
    impl Collector for ScanDiagnosticCollector {
        async fn collect(&mut self, batch: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            assert_eq!(batch.num_rows(), 1);
            let start = batch
                .column(batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
                .unwrap()
                .value(0);
            let count = self
                .expected
                .remove(&start)
                .expect("unexpected/duplicate window");
            if self.collection {
                for (column, length) in [(0, count as usize), (1, 1)] {
                    let items = batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .value(0);
                    assert_eq!(items.len(), length);
                    assert!(
                        items
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .all(|v| v == Some(1))
                    );
                }
                assert_eq!(
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    1
                );
            } else {
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    count
                );
            }
            self.outputs += 1;
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            unreachable!()
        }
    }

    fn diagnostic_process_sample() -> (u64, u64, u64, u64) {
        let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
        let fields = stat[stat.rfind(')').unwrap() + 2..]
            .split_whitespace()
            .collect::<Vec<_>>();
        let ticks = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
        let io = std::fs::read_to_string("/proc/self/io").unwrap();
        let counter = |name: &str| {
            io.lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let rss = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        (ticks, counter("read_bytes:"), counter("write_bytes:"), rss)
    }

    async fn diagnostic_insert(
        operator: &mut NativeWindow,
        ctx: &mut OperatorContext,
        row: &RecordBatch,
        oracle: &mut std::collections::BTreeMap<i64, i64>,
        time: i64,
        count: usize,
        accepted: bool,
    ) {
        operator.binning = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            ScalarValue::TimestampNanosecond(Some(time), None),
        ));
        for _ in 0..count {
            operator.process_batch(row.clone(), ctx).await.unwrap();
            if accepted {
                *oracle.entry(time).or_default() += 1;
                if operator.hopping {
                    *oracle.entry(time - 10).or_default() += 1;
                }
            }
        }
    }

    async fn diagnostic_watermark(
        operator: &mut NativeWindow,
        ctx: &mut OperatorContext,
        oracle: &mut std::collections::BTreeMap<i64, i64>,
        case: &str,
        phase: &str,
        watermark: u128,
        declared_retained_rows: usize,
    ) {
        let collection = operator.collection_output;
        let terminal = watermark == u64::MAX as u128;
        let width = if operator.hopping { 20 } else { 10 };
        let keys = oracle
            .keys()
            .copied()
            .filter(|start| terminal || (*start + width) as u128 <= watermark)
            .collect::<Vec<_>>();
        let expected = keys
            .into_iter()
            .map(|key| (key, oracle.remove(&key).unwrap()))
            .collect();
        let mut collector = ScanDiagnosticCollector {
            expected,
            collection,
            outputs: 0,
        };
        let expected_outputs = collector.expected.len();
        ctx.watermarks.set(
            0,
            arroyo_types::Watermark::EventTime(arroyo_types::from_nanos(watermark)),
        );
        let before = diagnostic_process_sample();
        eprintln!(
            "STR124 operator_begin case={case} phase={phase} watermark={watermark} expected_outputs={expected_outputs} declared_retained_rows={declared_retained_rows}"
        );
        let start = std::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(30),
            operator.handle_watermark(ctx, &mut collector),
        )
        .await
        .unwrap()
        .unwrap();
        let wall_ns = start.elapsed().as_nanos();
        let after = diagnostic_process_sample();
        assert!(collector.expected.is_empty());
        assert_eq!(collector.outputs, expected_outputs);
        eprintln!(
            "STR124 operator_end case={case} phase={phase} outputs={} wall_ns={wall_ns} cpu_ticks={} read_bytes={} write_bytes={} rss_begin_kib={} rss_end_kib={}",
            collector.outputs,
            after.0 - before.0,
            after.1 - before.1,
            after.2 - before.2,
            before.3,
            after.3
        );
    }

    async fn diagnostic_operator_phases(
        operator: &mut NativeWindow,
        ctx: &mut OperatorContext,
        row: &RecordBatch,
        collection: bool,
    ) {
        let case = format!(
            "{}-{}",
            if operator.hopping { "hop" } else { "tumble" },
            if collection { "collection" } else { "scalar" }
        );
        let mut oracle = std::collections::BTreeMap::new();
        for (time, count) in [(0, 16), (10, 1), (20, 1), (1_000_000_000, 1)] {
            diagnostic_insert(operator, ctx, row, &mut oracle, time, count, true).await;
        }
        diagnostic_watermark(operator, ctx, &mut oracle, &case, "equal-small", 0, 19).await;
        diagnostic_insert(operator, ctx, row, &mut oracle, 0, 112, true).await;
        for time in [30, 40, 50, 60] {
            diagnostic_insert(operator, ctx, row, &mut oracle, time, 1, true).await;
        }
        diagnostic_watermark(operator, ctx, &mut oracle, &case, "equal-growth", 0, 135).await;
        diagnostic_watermark(operator, ctx, &mut oracle, &case, "repeat-equal", 0, 135).await;
        diagnostic_watermark(
            operator,
            ctx,
            &mut oracle,
            &case,
            "large-jump",
            1_000_000_000,
            135,
        )
        .await;
        diagnostic_watermark(
            operator,
            ctx,
            &mut oracle,
            &case,
            "repeat-jump",
            1_000_000_000,
            1,
        )
        .await;
        diagnostic_insert(operator, ctx, row, &mut oracle, 20, 1, false).await;
        diagnostic_insert(operator, ctx, row, &mut oracle, 1_000_000_000, 1, true).await;
        diagnostic_insert(operator, ctx, row, &mut oracle, 1_000_000_010, 1, true).await;
        diagnostic_watermark(
            operator,
            ctx,
            &mut oracle,
            &case,
            "equal-boundary-growth",
            1_000_000_000,
            3,
        )
        .await;
        diagnostic_watermark(
            operator,
            ctx,
            &mut oracle,
            &case,
            "EOF-MAX",
            u64::MAX as u128,
            3,
        )
        .await;
        diagnostic_watermark(
            operator,
            ctx,
            &mut oracle,
            &case,
            "repeat-EOF",
            u64::MAX as u128,
            0,
        )
        .await;
        assert!(oracle.is_empty());
    }

    #[tokio::test]
    #[ignore = "STR-124 finite native operator watermark measurements"]
    async fn str124_operator_watermarks() {
        for hopping in [false, true] {
            for collection in [false, true] {
                closure_fixture(hopping, collection, false, true).await;
            }
        }
    }
}
