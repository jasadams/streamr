#[path = "calendar_native.rs"]
mod calendar_native;
use calendar_native::{CalendarAggregate, CalendarContributionChange, CalendarInput};

use crate::arrow::aggregate_codec::{EncodedGroup, decode_group, encode_group};
use crate::arrow::aggregate_store::{AggregateScope, AggregateStore, AggregateStoreLimits};
use crate::arrow::decode_aggregate;
use crate::arrow::execution::{ExecutionResources, configured_execution_resources};
use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::compute::{SortOptions, filter, max_array};
use arrow::row::{RowConverter, SortField};
use arrow_array::builder::{
    BinaryBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, RecordBatch, StructArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder, TimeUnit};
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_rpc::config::AggregateStateConfig;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{
    api::UpdatingAggregateOperator,
    rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
};
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_fields};
use arroyo_state::live::{
    LiveStateBackend,
    worker::{ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend},
};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark, to_nanos};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::functions_aggregate::min_max::Max;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{AggregateMode, aggregate_expressions};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use futures::StreamExt;
use itertools::Itertools;
use prost::Message;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime};
use std::{collections::HashMap, mem, sync::Arc};
use tracing::debug;
use tracing::log::warn;

#[derive(Debug, Copy, Clone)]
struct BatchData {
    count: u64,
    generation: u64,
}

/// One accepted contribution with its raw row clock and calendar inputs.
struct NativeCalendarEvent<'a> {
    group_key: &'a [u8],
    inputs: &'a [AggregateInput],
    row: usize,
    retract: bool,
    row_id: Option<&'a [u8]>,
    calendar_inputs: &'a [CalendarInput],
    result_timestamp_nanos: Option<i64>,
}

/// One indexed aggregate member change within an admitted state scope.
struct NativeMemberChange<'a> {
    group: &'a [u8],
    generation: u64,
    aggregate_index: usize,
    values: &'a [ArrayRef],
    retract: bool,
    ordinal: u64,
    /// Stable changelog row identity, required for the planner-injected
    /// event-time MAX. A CDC before row carries its new envelope time rather
    /// than the timestamp of the member it removes.
    row_id: Option<&'a [u8]>,
}

/// Persistent, per-group collection admission. All counters include duplicate
/// occurrences; DISTINCT still needs those occurrences for exact retractions.
#[derive(Clone, Copy, Default)]
struct CollectionStats {
    members: u64,
    encoded_args_bytes: u64,
    scalar_bytes: u64,
}

impl CollectionStats {
    fn decode(value: Option<Vec<u8>>) -> Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        ensure!(value.len() == 24, "invalid native collection member totals");
        Ok(Self {
            members: u64::from_be_bytes(value[..8].try_into()?),
            encoded_args_bytes: u64::from_be_bytes(value[8..16].try_into()?),
            scalar_bytes: u64::from_be_bytes(value[16..].try_into()?),
        })
    }

    fn encode(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.members.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.encoded_args_bytes.to_be_bytes());
        bytes[16..].copy_from_slice(&self.scalar_bytes.to_be_bytes());
        bytes
    }
}

impl BatchData {
    fn new(generation: u64) -> Self {
        Self {
            count: 1,
            generation,
        }
    }

    fn inc(&mut self) {
        self.count += 1;
        self.generation += 1;
    }

    fn dec(&mut self) {
        self.count = self.count.saturating_sub(1);
        self.generation += 1;
    }
}

/// Abstract over aggregations that support retracts (sliding accumulators), which we can use
/// directly, and those that don't in which case we just need to store the raw values and aggregate
/// them on demand
#[derive(Debug)]
enum IncrementalState {
    Sliding {
        expr: Arc<AggregateFunctionExpr>,
        accumulator: Box<dyn Accumulator>,
    },
    Batch {
        expr: Arc<AggregateFunctionExpr>,
        data: HashMap<Key, BatchData>,
        row_converter: Arc<RowConverter>,
        changed_values: HashSet<Key>,
        ordered: bool,
    },
}

impl IncrementalState {
    fn update_batch(&mut self, new_geneeration: u64, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => {
                accumulator.update_batch(batch)?;
            }
            IncrementalState::Batch {
                data,
                row_converter,
                changed_values,
                ordered,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    let encoded = encode_args_row(r.as_ref(), *ordered);
                    let r = encoded.as_slice();
                    if data.contains_key(r) {
                        data.get_mut(r).unwrap().inc();
                        changed_values.insert(data.get_key_value(r).unwrap().0.clone());
                    } else {
                        let key = Key(Arc::new(r.to_vec()));
                        data.insert(key.clone(), BatchData::new(new_geneeration));
                        changed_values.insert(key);
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.retract_batch(batch),
            IncrementalState::Batch {
                data,
                row_converter,
                changed_values,
                ordered,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    let encoded = encode_args_row(r.as_ref(), *ordered);
                    let r = encoded.as_slice();
                    match data.get(r).map(|d| d.count) {
                        Some(0) => {
                            debug!(
                                "tried to retract value for key with count 0; this implies an \
                            append was lost or a retract was duplicated"
                            );
                        }
                        Some(_) => {
                            data.get_mut(r).unwrap().dec();
                            changed_values.insert(data.get_key_value(r).unwrap().0.clone());
                        }
                        None => {
                            debug!(
                                "tried to retract value for missing key: {:?}; this \
                            implies an append was lost (possibly from source)",
                                batch
                            )
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate(),
            IncrementalState::Batch {
                expr,
                data,
                row_converter,
                ordered,
                ..
            } => {
                let parser = row_converter.parser();
                let rows = data
                    .iter()
                    .filter(|(_, c)| c.count > 0)
                    .map(|(v, _)| decode_args_row(&v.0, *ordered))
                    .collect::<DFResult<Vec<_>>>()?;
                let input =
                    row_converter.convert_rows(rows.into_iter().map(|r| parser.parse(r)))?;
                let mut acc = expr.create_accumulator()?;
                acc.update_batch(&input)?;
                acc.evaluate_mut()
            }
        }
    }
}

// Ordered fallback checkpoints previously omitted every ORDER BY column. Those
// rows cannot be upgraded: replay is required to recover the discarded values.
const ORDERED_ARGS_V1: &[u8] = b"\xffstreamr.ordered-args.v1\0";

const INPUT_SEMANTICS_KEY: &str = "streamr.aggregate-input-semantics";
const INPUT_SEMANTICS_V1: &str = "order-and-filter.v1";

fn versioned_state_timestamp(field: Field, versioned: bool) -> Field {
    if versioned {
        let mut metadata = field.metadata().clone();
        metadata.insert(INPUT_SEMANTICS_KEY.into(), INPUT_SEMANTICS_V1.into());
        field.with_metadata(metadata)
    } else {
        field
    }
}

fn encode_args_row(row: &[u8], ordered: bool) -> Vec<u8> {
    if ordered {
        [ORDERED_ARGS_V1, row].concat()
    } else {
        row.to_vec()
    }
}

fn decode_args_row(row: &[u8], ordered: bool) -> DFResult<&[u8]> {
    if ordered {
        row.strip_prefix(ORDERED_ARGS_V1).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "legacy or unsupported ordered aggregate checkpoint: ORDER BY values were not \
                 persisted; restart from source replay with a fresh checkpoint rather than \
                 restoring this ordered fallback state"
                    .into(),
            )
        })
    } else {
        Ok(row)
    }
}

struct AggregateInput {
    values: Vec<ArrayRef>,
    filter: Option<BooleanArray>,
}

impl AggregateInput {
    fn selected_values(&self, index: Option<usize>) -> DFResult<Option<Vec<ArrayRef>>> {
        if let Some(index) = index {
            if self
                .filter
                .as_ref()
                .is_some_and(|mask| mask.is_null(index) || !mask.value(index))
            {
                return Ok(None);
            }
            return Ok(Some(
                self.values.iter().map(|v| v.slice(index, 1)).collect(),
            ));
        }
        if let Some(mask) = &self.filter {
            // SQL FILTER includes only true; null is false, independently of
            // each aggregate's input null handling.
            let mask = BooleanArray::from_iter(mask.iter().map(|v| Some(v.unwrap_or(false))));
            if mask.true_count() == 0 {
                return Ok(None);
            }
            return self
                .values
                .iter()
                .map(|v| Ok(filter(v, &mask)?))
                .collect::<DFResult<Vec<_>>>()
                .map(Some);
        }
        Ok(Some(self.values.clone()))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AccumulatorType {
    Sliding,
    /// Native append-only aggregates use reconstructible ordinary state.
    AppendOnly,
    Batch,
}

impl AccumulatorType {
    fn state_fields(&self, agg: &AggregateFunctionExpr) -> DFResult<Vec<FieldRef>> {
        Ok(match self {
            AccumulatorType::Sliding => agg.sliding_state_fields()?,
            AccumulatorType::AppendOnly => agg.state_fields()?,
            // state for batch tables is handled separately
            AccumulatorType::Batch => vec![],
        })
    }
}

#[derive(Debug)]
struct Aggregator {
    func: Arc<AggregateFunctionExpr>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    accumulator_type: AccumulatorType,
    row_converter: Arc<RowConverter>,
    state_cols: Vec<usize>,
    /// Ordered member-key codec for the bounded native fallback index.
    index_converter: Option<Arc<RowConverter>>,
    index_columns: Vec<usize>,
    collection: bool,
    collection_output_limit: Option<usize>,
    collection_member_bound: usize,
    injected_timestamp: bool,
}

/// Event-time validity of a retained current result on the selected
/// rolling-result composition path. Each input row stamps its group's expiry
/// deadline at the result timestamp plus one window slide; an event-time
/// watermark past the deadline retracts the retained result. If no watermark
/// advances, no expiry applies.
struct EventTimeExpiryRuntime {
    result_timestamp_expr: Arc<dyn PhysicalExpr>,
    delay_nanos: i64,
}

pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Aggregator>,
    accumulators: UpdatingCache<Vec<IncrementalState>>,
    updated_keys: HashMap<Key, Option<Vec<ScalarValue>>>,
    sliding_state_schema: Arc<ArroyoSchema>,
    batch_state_schema: Arc<ArroyoSchema>,
    schema_without_metadata: Arc<Schema>,
    ttl: Duration,
    key_converter: RowConverter,
    new_generation: u64,
    native_config: Option<AggregateStateConfig>,
    native_execution_resources: Option<Arc<ExecutionResources>>,
    native_store: Option<AggregateStore>,
    retain_indefinitely: bool,
    native_schema_identity: Vec<u8>,
    native_append_only: bool,
    event_time_expiry: Option<EventTimeExpiryRuntime>,
    calendars: Vec<CalendarAggregate>,
}

const GLOBAL_KEY: Vec<u8> = vec![];
const NATIVE_AGGREGATE_TABLE: &str = "native-aggregate-v1";

fn native_limits(config: AggregateStateConfig) -> AggregateStoreLimits {
    AggregateStoreLimits {
        key_bytes: config.key_bytes,
        value_bytes: config.value_bytes,
        page_bytes: config.page_bytes,
        page_entries: config.page_entries,
        write_bytes: config.write_bytes,
        write_operations: config.write_operations,
        overlay_bytes: config.overlay_bytes,
    }
}

fn native_index_codec(
    aggregate: &AggregateFunctionExpr,
    input_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<(Arc<RowConverter>, Vec<usize>)> {
    let name = aggregate.fun().name().to_ascii_lowercase();
    let (indices, options): (Vec<usize>, Vec<SortOptions>) = match name.as_str() {
        "min" | "max" => {
            ensure!(
                input_exprs.len() == 1,
                "native {name} requires one argument"
            );
            (
                vec![0],
                vec![SortOptions {
                    descending: name == "max",
                    nulls_first: false,
                }],
            )
        }
        "first_value" | "last_value" => {
            let order = aggregate.order_bys().ok_or_else(|| {
                anyhow!("native {name} requires an explicit ORDER BY for bounded retraction")
            })?;
            ensure!(
                !order.is_empty() && order.len() < input_exprs.len(),
                "native {name} has incompatible ORDER BY input columns"
            );
            let first = input_exprs.len() - order.len();
            let options = order
                .iter()
                .map(|item| {
                    let mut options = item.options;
                    if name == "last_value" {
                        options.descending = !options.descending;
                        options.nulls_first = !options.nulls_first;
                    }
                    options
                })
                .collect();
            ((first..input_exprs.len()).collect(), options)
        }
        "array_agg"
            if aggregate
                .fun()
                .inner()
                .as_any()
                .is::<datafusion::functions_aggregate::array_agg::ArrayAgg>() =>
        {
            ensure!(
                !input_exprs.is_empty(),
                "native ARRAY_AGG requires one argument"
            );
            if let Some(order) = aggregate.order_bys() {
                ensure!(
                    !order.is_empty() && order.len() < input_exprs.len(),
                    "native ARRAY_AGG has incompatible ORDER BY input columns"
                );
                let first = input_exprs.len() - order.len();
                (
                    (first..input_exprs.len()).collect(),
                    order.iter().map(|item| item.options).collect(),
                )
            } else {
                // SQL leaves unordered ARRAY_AGG order unspecified. Sorting by
                // value keeps duplicates adjacent and makes restore stable.
                (vec![0], vec![SortOptions::default()])
            }
        }
        _ => bail!(
            "native aggregate '{}' has no bounded retraction index codec",
            aggregate.fun().name()
        ),
    };
    let fields = indices
        .iter()
        .zip(options)
        .map(|(index, options)| {
            Ok(SortField::new_with_options(
                input_exprs[*index].data_type(input_schema)?,
                options,
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok((Arc::new(RowConverter::new(fields)?), indices))
}

fn native_group_key(prefix: u8, group: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(group.len())?;
    let mut key = Vec::with_capacity(5 + group.len());
    key.push(prefix);
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

fn native_generation_prefix(kind: u8, group: &[u8], generation: u64) -> Result<Vec<u8>> {
    let mut key = native_group_key(kind, group)?;
    key.extend_from_slice(&generation.to_be_bytes());
    Ok(key)
}

fn native_member_prefix(group: &[u8], generation: u64, aggregate: usize) -> Result<Vec<u8>> {
    let mut key = native_generation_prefix(b'M', group, generation)?;
    key.extend_from_slice(&u32::try_from(aggregate)?.to_be_bytes());
    Ok(key)
}

fn native_tuple_prefix(
    group: &[u8],
    generation: u64,
    aggregate: usize,
    args_row: &[u8],
) -> Result<Vec<u8>> {
    let mut key = native_group_key(b'R', group)?;
    key.extend_from_slice(&generation.to_be_bytes());
    key.extend_from_slice(&u32::try_from(aggregate)?.to_be_bytes());
    key.extend_from_slice(&u32::try_from(args_row.len())?.to_be_bytes());
    key.extend_from_slice(args_row);
    Ok(key)
}

fn native_expiry_key(deadline_nanos: i64, group: &[u8]) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(9 + group.len() + 4);
    key.push(b'E');
    key.extend_from_slice(&deadline_nanos.to_be_bytes());
    key.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

/// Event-time deadline index for a retained current result, ordered by
/// deadline so due expirations drain in bounded pages.
fn native_validity_key(deadline_nanos: i64, group: &[u8]) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(9 + group.len() + 4);
    key.push(b'Y');
    key.extend_from_slice(&((deadline_nanos as u64) ^ (1 << 63)).to_be_bytes());
    key.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

fn native_cleanup_key(group: &[u8], generation: u64) -> Result<Vec<u8>> {
    let mut key = native_group_key(b'C', group)?;
    key.extend_from_slice(&generation.to_be_bytes());
    Ok(key)
}

fn native_collection_totals_key(group: &[u8], aggregate: usize) -> Result<Vec<u8>> {
    let mut key = native_group_key(b'L', group)?;
    key.extend_from_slice(&u32::try_from(aggregate)?.to_be_bytes());
    Ok(key)
}

// A keyed GROUP BY has no output row after its last input is retracted.
// Aggregate values cannot establish that fact: COUNT(*) evaluates to zero,
// while filtered aggregates may be NULL even for a live group.
fn native_live_rows_key(group: &[u8]) -> Result<Vec<u8>> {
    native_group_key(b'P', group)
}

fn native_collection_member(value: &[u8]) -> Result<(usize, &[u8])> {
    ensure!(value.len() >= 8, "invalid native collection member value");
    Ok((
        usize::try_from(u64::from_be_bytes(value[..8].try_into()?))?,
        &value[8..],
    ))
}

// Member rows are stored in RowConverter form. Materialization reconstructs
// fresh, compact Arrow arrays from those bytes; a one-row slice of an incoming
// batch can retain the entire source buffer and is not a measure of the
// collection's retained or eventual output size.
fn native_collection_scalar_bytes(converter: &RowConverter, args: &[u8]) -> Result<usize> {
    let parser = converter.parser();
    let columns = converter.convert_rows(std::iter::once(parser.parse(args)))?;
    columns.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(ScalarValue::try_from_array(value, 0)?.size())
            .context("native collection scalar size overflow")
    })
}

/// Admission for the one-row Arrow decode used to account a collection
/// member. Arrow's row decoder can materialize children for a NULL struct, so
/// its shape must be charged independently of the encoded row length. This
/// codec currently admits scalar leaves and one flat struct layer only.
fn native_collection_shape_bytes(data_type: &DataType) -> Result<usize> {
    let children: &[FieldRef] = match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                ensure!(
                    !matches!(field.data_type(), DataType::Struct(_))
                        && native_collection_scalar_type(field.data_type()),
                    "native ARRAY_AGG collection value has unsupported nested type: {}",
                    field.data_type()
                );
            }
            &fields[..]
        }
        other if native_collection_scalar_type(other) => &[],
        other => bail!("native ARRAY_AGG collection value has unsupported nested type: {other}"),
    };
    let mut shape = data_type.size();
    let array_wrapper = [
        std::mem::size_of::<StructArray>(),
        std::mem::size_of::<arrow_array::StringArray>(),
        std::mem::size_of::<arrow_array::LargeStringArray>(),
        std::mem::size_of::<arrow_array::BinaryArray>(),
        std::mem::size_of::<arrow_array::LargeBinaryArray>(),
        std::mem::size_of::<arrow_array::FixedSizeBinaryArray>(),
        std::mem::size_of::<arrow_array::NullArray>(),
        std::mem::size_of::<arrow_array::BooleanArray>(),
        std::mem::size_of::<arrow_array::Int64Array>(),
        std::mem::size_of::<arrow_array::Decimal256Array>(),
    ]
    .into_iter()
    .max()
    .context("native collection array wrapper is missing")?;
    for node in std::iter::once(data_type).chain(children.iter().map(|field| field.data_type())) {
        // Arrow 55's flat row decode can hold an ArrayData, its builder and a
        // copied child ArrayData entry, the largest admitted concrete array
        // wrapper, up to four Buffer objects, two ArrayRefs, Arc headers and
        // three owned buffer headers. Three rounded cache lines cover the
        // one-row validity, values and offsets. The fixed-width payload is
        // charged even when the parent or child is NULL.
        shape = shape
            .checked_add(node.size())
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<arrow::array::ArrayData>()))
            .and_then(|n| n.checked_add(std::mem::size_of::<arrow::array::ArrayDataBuilder>()))
            .and_then(|n| n.checked_add(array_wrapper))
            .and_then(|n| n.checked_add(4 * std::mem::size_of::<arrow::buffer::Buffer>()))
            .and_then(|n| n.checked_add(std::mem::size_of::<ScalarValue>()))
            .and_then(|n| n.checked_add(std::mem::size_of::<ArrayRef>() * 2))
            .and_then(|n| n.checked_add(std::mem::size_of::<usize>() * (2 + 3 * 8)))
            .and_then(|n| n.checked_add(3 * 64))
            .and_then(|n| match node {
                DataType::FixedSizeBinary(width) => usize::try_from(*width)
                    .ok()
                    .and_then(|width| n.checked_add(width)),
                _ => Some(n),
            })
            .context("native collection shape size overflow")?;
    }
    Ok(shape)
}

fn native_collection_scalar_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::FixedSizeBinary(_)
    )
}

fn native_collection_decode_bound(types: &[DataType], encoded_bytes: usize) -> Result<usize> {
    let shape = types.iter().try_fold(0usize, |total, data_type| {
        total
            .checked_add(native_collection_shape_bytes(data_type)?)
            .context("native collection decoded shape overflow")
    })?;
    // Encoded row, decoded Arrow buffers, and ScalarValue materialization can
    // coexist. Charge three copies of payload and schema-driven wrappers.
    encoded_bytes
        .checked_add(shape)
        .and_then(|n| n.checked_mul(3))
        .context("native collection decoded bound overflow")
}

impl IncrementalAggregatingFunc {
    fn native_collection_budget(
        &self,
        scope: &AggregateScope<'_>,
        stats: impl Iterator<Item = (usize, CollectionStats)>,
    ) -> Result<()> {
        let config = self
            .native_config
            .context("native collection limits are missing")?;
        // A collection's output and last-emitted value are both materialized
        // during a flush. Charge each member, including NULL and duplicate
        // occurrences, for scalar slots, Arrow buffers and transient copies.
        // The threefold pending-output reservation is acquired before flush.
        let mut working = 0usize;
        for (index, stats) in stats {
            let aggregate = &self.aggregates[index];
            let mut count = usize::try_from(stats.members)?;
            let mut bytes = usize::try_from(stats.encoded_args_bytes)?;
            let mut scalar_bytes = usize::try_from(stats.scalar_bytes)?;
            if let Some(limit) = aggregate.collection_output_limit {
                // All members remain durable. Only the selected prefix is decoded
                // and materialized, with worst-case per-member admission.
                count = count.min(limit);
                bytes = bytes.min(
                    count
                        .checked_mul(scope.limits().key_bytes)
                        .context("bounded collection encoded size overflow")?,
                );
                scalar_bytes = scalar_bytes.min(
                    count
                        .checked_mul(aggregate.collection_member_bound)
                        .context("bounded collection scalar size overflow")?,
                );
            }
            let scalar_slots = count
                .checked_mul(self.aggregates[index].input_exprs.len())
                .and_then(|n| n.checked_mul(std::mem::size_of::<ScalarValue>()))
                .context("native collection scalar workspace overflow")?;
            working = working
                .checked_add(bytes)
                .and_then(|n| n.checked_add(scalar_bytes))
                .and_then(|n| n.checked_add(scalar_slots))
                .and_then(|n| n.checked_add(count.checked_mul(32)?))
                .context("native collection workspace overflow")?;
        }
        let admitted = working
            .checked_mul(8)
            .context("native collection workspace overflow")?;
        ensure!(
            // Keep a conservative per-group preflight under the value limit.
            // Arrow IPC framing is checked exactly by encode_group before the
            // group is written; the eightfold transient materialization
            // allowance belongs to the separate pending-output pool.
            working <= scope.limits().value_bytes
                && admitted <= config.max_pending_output_bytes / 3,
            "native collection exceeds configured encoded-state or pending-output budget"
        );
        Ok(())
    }

    async fn native_collection_change(
        &self,
        scope: &mut AggregateScope<'_>,
        group: &[u8],
        aggregate_index: usize,
        encoded_args_bytes: usize,
        scalar_bytes: usize,
        retract: bool,
    ) -> Result<()> {
        let key = native_collection_totals_key(group, aggregate_index)?;
        let mut updated = CollectionStats::decode(scope.get(&key).await?)?;
        let bytes = u64::try_from(encoded_args_bytes)?;
        let scalar_bytes = u64::try_from(scalar_bytes)?;
        if retract {
            updated.members = updated
                .members
                .checked_sub(1)
                .context("native collection member underflow")?;
            updated.encoded_args_bytes = updated
                .encoded_args_bytes
                .checked_sub(bytes)
                .context("native collection byte count underflow")?;
            updated.scalar_bytes = updated
                .scalar_bytes
                .checked_sub(scalar_bytes)
                .context("native collection scalar byte count underflow")?;
        } else {
            updated.members = updated
                .members
                .checked_add(1)
                .context("native collection member overflow")?;
            updated.encoded_args_bytes = updated
                .encoded_args_bytes
                .checked_add(bytes)
                .context("native collection byte count overflow")?;
            updated.scalar_bytes = updated
                .scalar_bytes
                .checked_add(scalar_bytes)
                .context("native collection scalar byte count overflow")?;
        }
        let mut totals = Vec::new();
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            if aggregate.collection {
                let stats = if index == aggregate_index {
                    updated
                } else {
                    CollectionStats::decode(
                        scope
                            .get(&native_collection_totals_key(group, index)?)
                            .await?,
                    )?
                };
                totals.push((index, stats));
            }
        }
        self.native_collection_budget(scope, totals.into_iter())?;
        if updated.members == 0 {
            ensure!(
                updated.encoded_args_bytes == 0 && updated.scalar_bytes == 0,
                "native collection empty member count has retained bytes"
            );
            scope.delete(&key)?;
        } else {
            scope.put(&key, &updated.encode())?;
        }
        Ok(())
    }

    fn native_state_types(&self) -> Vec<DataType> {
        self.aggregates
            .iter()
            .flat_map(|aggregate| aggregate.state_cols.iter())
            .map(|index| {
                self.sliding_state_schema
                    .schema
                    .field(*index)
                    .data_type()
                    .clone()
            })
            .collect()
    }

    fn native_output_types(&self) -> Vec<DataType> {
        let fields = self.schema_without_metadata.fields();
        fields[fields.len() - self.aggregates.len()..]
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    }

    fn native_has_group_keys(&self) -> bool {
        self.schema_without_metadata.fields().len() > self.aggregates.len()
    }

    async fn native_group_is_live(
        &self,
        scope: &AggregateScope<'_>,
        group_key: &[u8],
    ) -> Result<bool> {
        if !self.native_has_group_keys() {
            // A global SQL aggregate retains its single row on empty input.
            return Ok(true);
        }
        let count = scope
            .get(&native_live_rows_key(group_key)?)
            .await?
            .context("native aggregate keyed group is missing its live-row count")?;
        ensure!(count.len() == 8, "invalid native aggregate live-row count");
        Ok(u64::from_be_bytes(count.as_slice().try_into()?) > 0)
    }

    fn native_accumulators(&self, group: Option<&EncodedGroup>) -> Result<Vec<IncrementalState>> {
        let mut accumulators = self.make_accumulators();
        if let Some(group) = group {
            let mut position = 0;
            for (aggregate, state) in self.aggregates.iter().zip(accumulators.iter_mut()) {
                let IncrementalState::Sliding { accumulator, .. } = state else {
                    continue;
                };
                let end = position + aggregate.state_cols.len();
                let values = group
                    .accumulator_state
                    .get(position..end)
                    .ok_or_else(|| anyhow!("aggregate accumulator state is incomplete"))?;
                let arrays = values
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<DFResult<Vec<_>>>()?;
                accumulator.merge_batch(&arrays)?;
                position = end;
            }
            ensure!(
                position == group.accumulator_state.len(),
                "aggregate accumulator state has extra values"
            );
        }
        Ok(accumulators)
    }

    fn native_state_values(
        &self,
        accumulators: &mut [IncrementalState],
    ) -> Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        for state in accumulators {
            if let IncrementalState::Sliding { accumulator, .. } = state {
                values.extend(accumulator.state()?);
            }
        }
        ensure!(
            values.len() == self.native_state_types().len(),
            "aggregate accumulator state width changed"
        );
        Ok(values)
    }

    fn native_member_keys(
        &self,
        group: &[u8],
        generation: u64,
        aggregate_index: usize,
        values: &[ArrayRef],
        ordinal: u64,
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let aggregate = &self.aggregates[aggregate_index];
        let codec = aggregate
            .index_converter
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate has no member index codec"))?;
        let sort_columns = aggregate
            .index_columns
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>();
        let sort_rows = codec.convert_columns(&sort_columns)?;
        let args_rows = aggregate.row_converter.convert_columns(values)?;
        ensure!(
            sort_rows.num_rows() == 1 && args_rows.num_rows() == 1,
            "native aggregate member update requires one input row"
        );
        let args = args_rows.row(0).as_ref().to_vec();
        let mut primary = native_member_prefix(group, generation, aggregate_index)?;
        primary.extend_from_slice(sort_rows.row(0).as_ref());
        primary.extend_from_slice(&ordinal.to_be_bytes());
        let mut secondary = native_tuple_prefix(group, generation, aggregate_index, &args)?;
        secondary.extend_from_slice(&ordinal.to_be_bytes());
        Ok((primary, secondary, args))
    }

    async fn native_member_delta(
        &self,
        scope: &mut AggregateScope<'_>,
        change: NativeMemberChange<'_>,
    ) -> Result<()> {
        let aggregate = &self.aggregates[change.aggregate_index];
        let name = aggregate.func.fun().name().to_ascii_lowercase();
        ensure!(
            !aggregate.injected_timestamp || !change.values[0].is_null(0),
            "native injected event time cannot be NULL"
        );
        if change.values[0].is_null(0)
            && (matches!(name.as_str(), "min" | "max") || aggregate.func.ignore_nulls())
        {
            return Ok(());
        }
        let (primary, mut secondary, args) = self.native_member_keys(
            change.group,
            change.generation,
            change.aggregate_index,
            change.values,
            change.ordinal,
        )?;
        if aggregate.injected_timestamp {
            let id = change
                .row_id
                .context("native injected timestamp requires a changelog row ID")?;
            ensure!(id.len() == 16, "native changelog row ID must have 16 bytes");
            secondary =
                native_tuple_prefix(change.group, change.generation, change.aggregate_index, id)?;
        }
        let scalar_bytes = if aggregate.collection && !change.retract {
            // Bound the encoded member before reconstructing its compact Arrow
            // value for accounting. The scope already holds its decoded-value
            // reservation; even a nested member cannot expand from an
            // unbounded encoded input here.
            ensure!(
                args.len() <= scope.limits().value_bytes,
                "native collection exceeds configured encoded-state or pending-output budget"
            );
            let decoded_allowance = scope
                .limits()
                .value_bytes
                .checked_mul(3)
                .context("native collection decoded allowance overflow")?;
            let types = change
                .values
                .iter()
                .map(|value| value.data_type().clone())
                .collect::<Vec<_>>();
            let decode_bound = native_collection_decode_bound(&types, args.len())?;
            ensure!(
                decode_bound <= decoded_allowance,
                "native collection exceeds configured encoded-state or pending-output budget"
            );
            let store = self
                .native_store
                .as_ref()
                .context("native collection store was not initialized")?;
            let _decoded_permit = store.resources().try_decoded_value(decode_bound)?;
            let scalar_bytes = native_collection_scalar_bytes(&aggregate.row_converter, &args)?;
            ensure!(
                scalar_bytes <= decode_bound,
                "native collection member exceeds configured decoded allowance"
            );
            scalar_bytes
        } else {
            0
        };
        if change.retract {
            let (secondary_key, primary_key) = if aggregate.injected_timestamp {
                let primary_key = scope
                    .get(&secondary)
                    .await?
                    .context("native aggregate retracts a nonmatching indexed row ID")?;
                (secondary.clone(), primary_key)
            } else {
                let prefix = &secondary[..secondary.len() - 8];
                scope
                    .first(prefix)
                    .await?
                    .context("native aggregate retracts a nonmatching indexed member")?
            };
            ensure!(
                primary_key.starts_with(&native_member_prefix(
                    change.group,
                    change.generation,
                    change.aggregate_index,
                )?),
                "native aggregate row ID points outside its member index"
            );
            let stored = scope
                .get(&primary_key)
                .await?
                .context("aggregate member index is missing")?;
            if aggregate.collection {
                let (retained_scalar_bytes, stored_args) = native_collection_member(&stored)?;
                ensure!(stored_args == args, "aggregate member indexes disagree");
                self.native_collection_change(
                    scope,
                    change.group,
                    change.aggregate_index,
                    args.len(),
                    retained_scalar_bytes,
                    true,
                )
                .await?;
            } else if !aggregate.injected_timestamp {
                ensure!(stored == args, "aggregate member indexes disagree");
            }
            scope.delete(&secondary_key)?;
            scope.delete(&primary_key)?;
        } else {
            if aggregate.injected_timestamp {
                ensure!(
                    scope.get(&secondary).await?.is_none(),
                    "native aggregate appends a duplicate live row ID"
                );
            }
            if aggregate.collection {
                self.native_collection_change(
                    scope,
                    change.group,
                    change.aggregate_index,
                    args.len(),
                    scalar_bytes,
                    false,
                )
                .await?;
            }
            if aggregate.collection {
                let mut stored = Vec::with_capacity(8 + args.len());
                stored.extend_from_slice(&u64::try_from(scalar_bytes)?.to_be_bytes());
                stored.extend_from_slice(&args);
                scope.put(&primary, &stored)?;
            } else {
                scope.put(&primary, &args)?;
            }
            scope.put(&secondary, &primary)?;
        }
        Ok(())
    }

    async fn native_fallback_value(
        &self,
        scope: &AggregateScope<'_>,
        group: &[u8],
        generation: u64,
        aggregate_index: usize,
    ) -> Result<ScalarValue> {
        let aggregate = &self.aggregates[aggregate_index];
        let prefix = native_member_prefix(group, generation, aggregate_index)?;
        let mut accumulator = aggregate.func.create_accumulator()?;
        let mut after = None;
        if aggregate.collection_output_limit == Some(0) {
            let stats = CollectionStats::decode(
                scope
                    .get(&native_collection_totals_key(group, aggregate_index)?)
                    .await?,
            )?;
            if stats.members > 0 {
                let DataType::List(field) = aggregate.func.field().data_type().clone() else {
                    bail!("bounded ARRAY_AGG result is not a list");
                };
                return Ok(ScalarValue::List(ScalarValue::new_list(
                    &[],
                    field.data_type(),
                    true,
                )));
            }
        }
        let mut selected = 0usize;
        while aggregate
            .collection_output_limit
            .is_none_or(|limit| selected < limit)
        {
            let Some((key, stored)) = scope.first_from(&prefix, after.as_deref()).await? else {
                break;
            };
            let args = if aggregate.collection {
                native_collection_member(&stored)?.1
            } else {
                stored.as_slice()
            };
            let parser = aggregate.row_converter.parser();
            let columns = aggregate
                .row_converter
                .convert_rows(std::iter::once(parser.parse(args)))?;
            accumulator.update_batch(&columns)?;
            selected += 1;
            if !aggregate.collection {
                break;
            }
            after = Some(key);
        }
        Ok(accumulator.evaluate_mut()?)
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    async fn native_process_event(
        &self,
        scope: &mut AggregateScope<'_>,
        group_key: &[u8],
        inputs: &[AggregateInput],
        row: usize,
        retract: bool,
        row_id: Option<&[u8]>,
        result_timestamp_nanos: Option<i64>,
    ) -> Result<()> {
        self.native_process_calendar_event(
            scope,
            NativeCalendarEvent {
                group_key,
                inputs,
                row,
                retract,
                row_id,
                calendar_inputs: &[],
                result_timestamp_nanos,
            },
        )
        .await
    }

    async fn native_process_calendar_event(
        &self,
        scope: &mut AggregateScope<'_>,
        event: NativeCalendarEvent<'_>,
    ) -> Result<()> {
        let NativeCalendarEvent {
            group_key,
            inputs,
            row,
            retract,
            row_id,
            calendar_inputs,
            result_timestamp_nanos,
        } = event;
        ensure!(
            !retract || !self.native_append_only,
            "append-only native aggregate received a retraction",
        );
        let storage_key = native_group_key(b'G', group_key)?;
        let previous = scope
            .get(&storage_key)
            .await?
            .map(|bytes| {
                decode_group(
                    &bytes,
                    &self.native_state_types(),
                    &self.native_output_types(),
                    scope.limits().value_bytes,
                )
            })
            .transpose()?;
        let live_rows_key = self
            .native_has_group_keys()
            .then(|| native_live_rows_key(group_key))
            .transpose()?;
        let prior_rows = if let Some(key) = &live_rows_key {
            let encoded = scope.get(key).await?;
            ensure!(
                previous.is_some() == encoded.is_some(),
                "native aggregate keyed group is missing its live-row count"
            );
            encoded
                .map(|bytes| {
                    ensure!(bytes.len() == 8, "invalid native aggregate live-row count");
                    Ok(u64::from_be_bytes(bytes.as_slice().try_into()?))
                })
                .transpose()?
                .unwrap_or(0)
        } else {
            0
        };
        let now = to_nanos(SystemTime::now()) as i64;
        let ttl_nanos = i64::try_from(self.ttl.as_nanos())?;
        let expired = !self.retain_indefinitely
            && previous
                .as_ref()
                .is_some_and(|group| now.saturating_sub(group.last_update_nanos) >= ttl_nanos);
        let next_group_rows = if let Some(key) = &live_rows_key {
            let rows = if expired { 0 } else { prior_rows };
            let next_rows = if retract {
                rows.checked_sub(1)
                    .context("native aggregate retracts a missing keyed row")?
            } else {
                rows.checked_add(1)
                    .context("native aggregate live-row count overflow")?
            };
            scope.put(key, &next_rows.to_be_bytes())?;
            Some(next_rows)
        } else {
            None
        };
        let generation = previous
            .as_ref()
            .map_or(0, |group| group.generation)
            .checked_add(u64::from(expired))
            .ok_or_else(|| anyhow!("aggregate generation overflow"))?;
        let ordinal = if expired {
            0
        } else {
            previous.as_ref().map_or(0, |group| group.next_ordinal)
        };
        let next_ordinal = if retract {
            ordinal
        } else {
            ordinal
                .checked_add(1)
                .ok_or_else(|| anyhow!("aggregate member ordinal overflow"))?
        };
        let mut accumulators =
            self.native_accumulators(if expired { None } else { previous.as_ref() })?;
        if expired {
            for (index, aggregate) in self.aggregates.iter().enumerate() {
                if aggregate.collection {
                    scope.delete(&native_collection_totals_key(group_key, index)?)?;
                }
            }
        }
        // Index presence distinguishes an absent deadline from the valid
        // Unix-epoch boundary zero. G alone cannot make that distinction.
        let mut validity_deadline = None;
        if !expired && self.event_time_expiry.is_some()
            && let Some(group) = &previous
            && scope.get(&native_validity_key(group.validity_deadline_nanos, group_key)?).await?.is_some()
        {
            validity_deadline = Some(group.validity_deadline_nanos);
        }
        if let Some(group) = &previous {
            if !self.retain_indefinitely {
                let old_deadline = group.last_update_nanos.saturating_add(ttl_nanos);
                scope.delete(&native_expiry_key(old_deadline, group_key)?)?;
            }
            if self.event_time_expiry.is_some() {
                scope.delete(&native_validity_key(
                    group.validity_deadline_nanos,
                    group_key,
                )?)?;
            }
            if expired {
                scope.put(&native_cleanup_key(group_key, group.generation)?, b"M")?;
            }
        }
        let mut calendar_families = HashMap::new();
        for (index, (input, state)) in inputs.iter().zip(accumulators.iter_mut()).enumerate() {
            let calendar = self
                .calendars
                .iter()
                .enumerate()
                .find(|(_, calendar)| calendar.aggregate_index == index);
            let contribution_day = calendar
                .map(|(position, _)| calendar_inputs[position].contribution_day(row))
                .transpose()?
                .flatten();
            let selected = input.selected_values(Some(row))?;
            let storage_index = calendar.map_or(index, |(_, calendar)| calendar.storage_index);
            let family_already_changed =
                calendar.is_some() && calendar_families.contains_key(&storage_index);
            let (selected, contribution_day) =
                if let Some(original) = calendar_families.get(&storage_index) {
                    let original: &(Option<Vec<ArrayRef>>, Option<i32>) = original;
                    original.clone()
                } else if !self.calendars.is_empty()
                    && self.aggregates[index].accumulator_type == AccumulatorType::Sliding
                {
                    self.calendar_original_input(
                        scope,
                        CalendarContributionChange {
                            group: group_key,
                            generation,
                            index: storage_index,
                            row_id,
                            retract,
                            selected,
                            day: contribution_day,
                            argument_types: input
                                .values
                                .iter()
                                .map(|value| value.data_type().clone())
                                .collect(),
                        },
                    )
                    .await?
                } else {
                    (selected, contribution_day)
                };
            if let Some((position, calendar)) = calendar {
                if !family_already_changed {
                    if let (Some(values), Some(day)) = (&selected, contribution_day) {
                        self.calendar_bucket_delta(
                            scope,
                            calendar,
                            day,
                            NativeMemberChange {
                                group: group_key,
                                generation,
                                aggregate_index: index,
                                values,
                                retract,
                                ordinal,
                                row_id,
                            },
                        )
                        .await?;
                    }
                    calendar_families.insert(storage_index, (selected.clone(), contribution_day));
                }
                let reference = calendar_inputs[position].reference_day(row)?;
                self.calendar_set_reference(scope, group_key, generation, calendar, reference)?;
                *state = self
                    .calendar_recalculate(scope, group_key, generation, calendar, reference)
                    .await?;
                continue;
            }
            let Some(values) = selected else {
                continue;
            };
            match state {
                IncrementalState::Sliding { accumulator, .. } => {
                    if retract {
                        accumulator.retract_batch(&values)?;
                    } else {
                        accumulator.update_batch(&values)?;
                    }
                }
                IncrementalState::Batch { .. } => {
                    self.native_member_delta(
                        scope,
                        NativeMemberChange {
                            group: group_key,
                            generation,
                            aggregate_index: index,
                            values: &values,
                            retract,
                            ordinal,
                            row_id,
                        },
                    )
                    .await?;
                }
            }
        }
        if next_group_rows == Some(0) {
            for (index, aggregate) in self.aggregates.iter().enumerate() {
                if aggregate.collection {
                    ensure!(
                        scope
                            .get(&native_collection_totals_key(group_key, index)?)
                            .await?
                            .is_none(),
                        "native aggregate empty group retains collection totals"
                    );
                }
                if aggregate.accumulator_type == AccumulatorType::Batch {
                    let primary = native_member_prefix(group_key, generation, index)?;
                    let mut secondary = native_generation_prefix(b'R', group_key, generation)?;
                    secondary.extend_from_slice(&u32::try_from(index)?.to_be_bytes());
                    ensure!(
                        scope.first(&primary).await?.is_none()
                            && scope.first(&secondary).await?.is_none(),
                        "native aggregate retracts a nonmatching keyed row"
                    );
                }
            }
        }
        if let Some(expiry) = &self.event_time_expiry
            && let Some(stamp) = result_timestamp_nanos
            && !retract
        {
            let stamped = stamp.saturating_add(expiry.delay_nanos);
            validity_deadline = Some(validity_deadline.map_or(stamped, |prior: i64| prior.max(stamped)));
        }
        if next_group_rows == Some(0) {
            validity_deadline = None;
        }
        let validity_deadline_nanos = validity_deadline.unwrap_or(0);
        if !self.calendars.is_empty() {
            let reference = calendar_inputs
                .iter()
                .map(|input| input.reference_day(row))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min()
                .context("calendar aggregate reference is missing")?;
            self.calendar_schedule(scope, group_key, generation, reference)
                .await?;
        }
        let next = EncodedGroup {
            last_update_nanos: now,
            generation,
            next_ordinal,
            validity_deadline_nanos,
            accumulator_state: self.native_state_values(&mut accumulators)?,
            last_emitted: previous.and_then(|group| group.last_emitted),
        };
        let encoded = encode_group(&next, scope.limits().value_bytes)?;
        scope.put(&storage_key, &encoded)?;
        scope.put(&native_group_key(b'D', group_key)?, &[1])?;
        if !self.retain_indefinitely {
            scope.put(
                &native_expiry_key(now.saturating_add(ttl_nanos), group_key)?,
                &[1],
            )?;
        }
        if self.event_time_expiry.is_some() && let Some(deadline) = validity_deadline {
            scope.put(
                &native_validity_key(deadline, group_key)?,
                &[1],
            )?;
        }
        Ok(())
    }

    async fn process_native_batch(
        &self,
        batch: &RecordBatch,
        ctx: &mut OperatorContext,
    ) -> Result<()> {
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store was not initialized"))?;
        let execution = self
            .native_execution_resources
            .as_ref()
            .context("native aggregate requires worker.execution-resources")?;
        let max_input_batch_bytes = execution.limits.max_batch_bytes;
        let _source = execution.reserve_batch("native aggregate input", batch)?;
        let input_schema = &ctx.in_schemas[0];
        let keys = if input_schema
            .routing_keys()
            .is_some_and(|keys| !keys.is_empty())
        {
            let columns = input_schema
                .sort_columns(batch, false)
                .into_iter()
                .map(|column| column.values)
                .collect::<Vec<_>>();
            self.key_converter
                .convert_columns(&columns)?
                .iter()
                .map(|row| row.as_ref().to_vec())
                .collect::<Vec<_>>()
        } else {
            vec![GLOBAL_KEY; batch.num_rows()]
        };
        let inputs = self.compute_inputs(batch)?;
        let calendar_inputs = self.calendar_inputs(batch)?;
        let input_working_bytes = inputs.iter().try_fold(0usize, |total, input| {
            let total = input.values.iter().try_fold(total, |total, value| {
                total.checked_add(value.get_array_memory_size())
            })?;
            total.checked_add(
                input
                    .filter
                    .as_ref()
                    .map_or(0, |filter| filter.get_array_memory_size()),
            )
        });
        let calendar_bytes = calendar_inputs.iter().fold(0usize, |bytes, input| {
            bytes
                .saturating_add(input.contribution.get_array_memory_size())
                .saturating_add(input.reference.get_array_memory_size())
        });
        let input_working_bytes = input_working_bytes
            .and_then(|bytes| bytes.checked_add(calendar_bytes))
            .and_then(|total| {
                keys.iter()
                    .try_fold(total, |total, key| total.checked_add(key.len()))
            })
            .ok_or_else(|| anyhow!("native aggregate input working-set size overflow"))?;
        ensure!(
            input_working_bytes <= max_input_batch_bytes,
            "native aggregate expressions exceed configured max-batch-bytes"
        );
        // Expressions have the same cooperative boundary as projection: their
        // resulting buffers can be measured once evaluated. Keep the charge
        // through every state scope/commit, including blocked snapshot waits.
        let mut _working = MemoryConsumer::new("native aggregate expressions and keys")
            .register(&execution.runtime.memory_pool);
        _working.try_grow(input_working_bytes)?;
        ensure!(
            !self.native_append_only || batch.column_by_name(UPDATING_META_FIELD).is_none(),
            "append-only native aggregate received changelog metadata",
        );
        let changelog = if self.native_append_only {
            None
        } else {
            Some(native_changelog_columns(batch)?)
        };
        let expiry_stamps = self
            .event_time_expiry
            .as_ref()
            .map(|expiry| {
                let stamps = expiry
                    .result_timestamp_expr
                    .evaluate(batch)?
                    .into_array(batch.num_rows())?;
                stamps
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .cloned()
                    .context("event-time expiry result timestamp is not nanosecond timestamp")
            })
            .transpose()?;
        let limits = store.limits();
        let fallback = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
            .count();
        let collections = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.collection)
            .count();
        let worst_operations = (2 + usize::from(self.native_has_group_keys()))
            .checked_add(fallback.saturating_mul(2))
            .and_then(|value| value.checked_add(collections.saturating_mul(2)))
            .and_then(|value| value.checked_add(3 * usize::from(!self.retain_indefinitely)))
            .and_then(|value| value.checked_add(self.calendar_write_operations()))
            .ok_or_else(|| anyhow!("native aggregate operation count overflow"))?;
        let worst_overlay_bytes = worst_operations
            .checked_mul(limits.key_bytes.saturating_add(limits.value_bytes))
            .ok_or_else(|| anyhow!("native aggregate byte budget overflow"))?;
        let worst_write_bytes = worst_operations
            .checked_mul(store.max_encoded_entry_bytes())
            .ok_or_else(|| anyhow!("native aggregate encoded write budget overflow"))?;
        let rows_per_chunk = (limits.write_operations / worst_operations)
            .min(limits.write_bytes / worst_write_bytes)
            .min(limits.overlay_bytes / worst_overlay_bytes);
        ensure!(
            rows_per_chunk > 0,
            "native aggregate budget cannot admit one worst-case event"
        );
        for start in (0..batch.num_rows()).step_by(rows_per_chunk) {
            let end = batch.num_rows().min(start + rows_per_chunk);
            // Without member indexes, the existing point-read path is safe
            // even for retractions. Indexed appends (including collections
            // and TTL rollover) also use only keyed reads and this owner's overlay.
            let has_retracts =
                changelog.is_some_and(|(flags, _)| (start..end).any(|row| flags.value(row)));
            let mut scope = if self.calendars.is_empty() && (fallback == 0 || !has_retracts) {
                store.begin_point().await?
            } else {
                store.begin().await?
            };
            for (row, key) in keys.iter().enumerate().take(end).skip(start) {
                let retract = changelog.is_some_and(|(flags, _)| flags.value(row));
                let row_id = changelog.map(|(_, ids)| ids.value(row));
                let result_timestamp_nanos = expiry_stamps
                    .as_ref()
                    .and_then(|stamps| stamps.is_valid(row).then(|| stamps.value(row)));
                self.native_process_calendar_event(
                    &mut scope,
                    NativeCalendarEvent {
                        group_key: key,
                        inputs: &inputs,
                        row,
                        retract,
                        row_id,
                        calendar_inputs: &calendar_inputs,
                        result_timestamp_nanos,
                    },
                )
                .await?;
            }
            scope.commit().await?;
        }
        Ok(())
    }

    /// One admitted page of due expirations. The caller drains these pages in
    /// the same flush, yielding and emitting bounded changelog batches between
    /// pages so finite TTL does not acquire a new delayed-expiry policy.
    async fn expire_native(&self) -> Result<bool> {
        if self.retain_indefinitely {
            return Ok(false);
        }
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let ttl_nanos = i64::try_from(self.ttl.as_nanos())?;
        let now = to_nanos(SystemTime::now()) as i64;
        let operations_per_expiry = 4
            + usize::from(self.native_has_group_keys())
            + self.aggregates.iter().filter(|a| a.collection).count();
        let rows_per_chunk = (store.limits().write_operations / operations_per_expiry)
            .min(
                store.limits().write_bytes
                    / (operations_per_expiry * store.max_encoded_entry_bytes()),
            )
            .min(
                store.limits().overlay_bytes
                    / (operations_per_expiry
                        * (store.limits().key_bytes + store.limits().value_bytes)),
            )
            .min(store.limits().page_entries);
        ensure!(
            rows_per_chunk > 0,
            "native aggregate budget cannot process one expiry"
        );
        let mut after = None;
        let mut scope = store.begin().await?;
        let mut processed = 0usize;
        while processed < rows_per_chunk {
            let Some((key, _)) = scope.first_from(b"E", after.as_deref()).await? else {
                break;
            };
            ensure!(key.len() >= 13, "invalid aggregate expiry key");
            let deadline = i64::from_be_bytes(key[1..9].try_into()?);
            if deadline > now {
                break;
            }
            let group_key = &key[13..];
            let storage_key = native_group_key(b'G', group_key)?;
            if let Some(bytes) = scope.get(&storage_key).await? {
                let mut group = decode_group(
                    &bytes,
                    &self.native_state_types(),
                    &self.native_output_types(),
                    scope.limits().value_bytes,
                )?;
                if group.last_update_nanos.saturating_add(ttl_nanos) == deadline {
                    let old_generation = group.generation;
                    group.generation = group
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("aggregate generation overflow"))?;
                    group.next_ordinal = 0;
                    if self.event_time_expiry.is_some() {
                        scope.delete(&native_validity_key(
                            group.validity_deadline_nanos,
                            group_key,
                        )?)?;
                        group.validity_deadline_nanos = 0;
                    }
                    group.accumulator_state =
                        self.native_state_values(&mut self.native_accumulators(None)?)?;
                    group.last_update_nanos = now;
                    scope.put(
                        &storage_key,
                        &encode_group(&group, scope.limits().value_bytes)?,
                    )?;
                    scope.put(&native_group_key(b'D', group_key)?, &[1])?;
                    if self.native_has_group_keys() {
                        scope.put(&native_live_rows_key(group_key)?, &0_u64.to_be_bytes())?;
                    }
                    scope.put(&native_cleanup_key(group_key, old_generation)?, b"M")?;
                    for (index, aggregate) in self.aggregates.iter().enumerate() {
                        if aggregate.collection {
                            scope.delete(&native_collection_totals_key(group_key, index)?)?;
                        }
                    }
                }
            }
            scope.delete(&key)?;
            after = Some(key);
            processed += 1;
        }
        if processed > 0 {
            scope.commit().await?;
        }
        Ok(processed > 0)
    }

    /// One admitted page of due event-time result expirations. A retained
    /// current result is retracted once the event-time watermark reaches its
    /// stamped deadline; pages are bounded and drained like wall-clock
    /// expiry so output and cleanup stay bounded.
    async fn expire_native_event_time(&self, watermark_nanos: i64) -> Result<bool> {
        if self.event_time_expiry.is_none() {
            return Ok(false);
        }
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let operations_per_expiry = 4
            + usize::from(self.native_has_group_keys())
            + self.aggregates.iter().filter(|a| a.collection).count();
        let rows_per_chunk = (store.limits().write_operations / operations_per_expiry)
            .min(
                store.limits().write_bytes
                    / (operations_per_expiry * store.max_encoded_entry_bytes()),
            )
            .min(
                store.limits().overlay_bytes
                    / (operations_per_expiry
                        * (store.limits().key_bytes + store.limits().value_bytes)),
            )
            .min(store.limits().page_entries);
        ensure!(
            rows_per_chunk > 0,
            "native aggregate budget cannot process one event-time expiry"
        );
        let now = to_nanos(SystemTime::now()) as i64;
        let mut after = None;
        let mut scope = store.begin().await?;
        let mut processed = 0usize;
        while processed < rows_per_chunk {
            let Some((key, _)) = scope.first_from(b"Y", after.as_deref()).await? else {
                break;
            };
            ensure!(key.len() >= 13, "invalid aggregate validity key");
            let deadline = (u64::from_be_bytes(key[1..9].try_into()?) ^ (1 << 63)) as i64;
            if deadline > watermark_nanos {
                break;
            }
            let group_key = &key[13..];
            let storage_key = native_group_key(b'G', group_key)?;
            if let Some(bytes) = scope.get(&storage_key).await? {
                let mut group = decode_group(
                    &bytes,
                    &self.native_state_types(),
                    &self.native_output_types(),
                    scope.limits().value_bytes,
                )?;
                if group.validity_deadline_nanos == deadline {
                    let old_generation = group.generation;
                    group.generation = group
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("aggregate generation overflow"))?;
                    group.next_ordinal = 0;
                    group.validity_deadline_nanos = 0;
                    group.accumulator_state =
                        self.native_state_values(&mut self.native_accumulators(None)?)?;
                    group.last_update_nanos = now;
                    scope.put(
                        &storage_key,
                        &encode_group(&group, scope.limits().value_bytes)?,
                    )?;
                    scope.put(&native_group_key(b'D', group_key)?, &[1])?;
                    if self.native_has_group_keys() {
                        scope.put(&native_live_rows_key(group_key)?, &0_u64.to_be_bytes())?;
                    }
                    scope.put(&native_cleanup_key(group_key, old_generation)?, b"M")?;
                    for (index, aggregate) in self.aggregates.iter().enumerate() {
                        if aggregate.collection {
                            scope.delete(&native_collection_totals_key(group_key, index)?)?;
                        }
                    }
                }
            }
            scope.delete(&key)?;
            after = Some(key);
            processed += 1;
        }
        if processed > 0 {
            scope.commit().await?;
        }
        Ok(processed > 0)
    }

    async fn cleanup_native(&self) -> Result<bool> {
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let mut scope = store.begin().await?;
        let Some((cleanup_key, phase)) = scope.first(b"C").await? else {
            return Ok(false);
        };
        ensure!(cleanup_key.len() >= 13, "invalid aggregate cleanup key");
        let group_end = cleanup_key.len() - 8;
        let group = &cleanup_key[5..group_end];
        let generation = u64::from_be_bytes(cleanup_key[group_end..].try_into()?);
        ensure!(
            phase == b"M"
                || phase == b"R"
                || phase == b"B"
                || phase == b"J"
                || phase == b"Q"
                || phase == b"U"
                || phase == b"T",
            "invalid aggregate cleanup phase"
        );
        if phase == b"T" {
            if !self.native_has_group_keys() {
                scope.delete(&cleanup_key)?;
                scope.commit().await?;
                return Ok(true);
            }
            let cleanup_prefix = native_group_key(b'C', group)?;
            if scope
                .first_from(&cleanup_prefix, Some(&cleanup_key))
                .await?
                .is_some()
            {
                // A later generation still needs cleanup. Its marker will
                // reclaim the tombstone after every earlier generation ends.
                scope.delete(&cleanup_key)?;
            } else {
                let group_key = native_group_key(b'G', group)?;
                let rows_key = native_live_rows_key(group)?;
                let encoded_group = scope.get(&group_key).await?;
                let encoded_rows = scope.get(&rows_key).await?;
                ensure!(
                    encoded_group.is_some() == encoded_rows.is_some(),
                    "native aggregate keyed group is missing its live-row count"
                );
                if let (Some(_), Some(rows)) = (encoded_group, encoded_rows) {
                    ensure!(rows.len() == 8, "invalid native aggregate live-row count");
                    if u64::from_be_bytes(rows.as_slice().try_into()?) == 0
                        && scope.get(&native_group_key(b'D', group)?).await?.is_none()
                    {
                        // The serial owner has already drained the dirty row,
                        // including its emitted retract. The pair is one
                        // admitted atomic write batch. Leave the harmless T
                        // marker until the next cleanup step; its only
                        // remaining action is to delete itself.
                        scope.delete(&group_key)?;
                        scope.delete(&rows_key)?;
                    } else {
                        scope.delete(&cleanup_key)?;
                    }
                } else {
                    scope.delete(&cleanup_key)?;
                }
            }
            scope.commit().await?;
            return Ok(true);
        }
        let prefix = native_generation_prefix(phase[0], group, generation)?;
        let mut after = None;
        let mut exhausted = false;
        let operations_per_entry = if phase == b"U" { 2 } else { 1 };
        let max_entries = store
            .limits()
            .page_entries
            .min(store.limits().write_operations.saturating_sub(1) / operations_per_entry)
            .min(
                (store.limits().write_bytes / store.max_encoded_entry_bytes()).saturating_sub(1)
                    / operations_per_entry,
            )
            .min(
                (store.limits().overlay_bytes
                    / (store.limits().key_bytes + store.limits().value_bytes))
                    .saturating_sub(1)
                    / operations_per_entry,
            );
        ensure!(
            max_entries > 0,
            "native aggregate cleanup cannot admit one entry and progress marker"
        );
        for _ in 0..max_entries {
            let Some((key, _)) = scope.first_from(&prefix, after.as_deref()).await? else {
                exhausted = true;
                break;
            };
            if phase == b"U" {
                let value = scope
                    .get(&key)
                    .await?
                    .context("calendar due pointer is missing")?;
                ensure!(value.len() == 8, "invalid calendar due pointer");
                let deadline = i64::from_be_bytes(value.as_slice().try_into()?);
                let due_key = calendar_native::calendar_due_key(deadline, group)?;
                if scope
                    .get(&due_key)
                    .await?
                    .is_some_and(|value| value == generation.to_be_bytes())
                {
                    scope.delete(&due_key)?;
                }
            }
            scope.delete(&key)?;
            after = Some(key);
        }
        if exhausted {
            if phase == b"M" {
                scope.put(&cleanup_key, b"R")?;
            } else {
                let next = match phase.as_slice() {
                    b"R" if !self.calendars.is_empty() => b"B",
                    b"B" => b"J",
                    b"J" => b"Q",
                    b"Q" => b"U",
                    _ => b"T",
                };
                scope.put(&cleanup_key, next)?;
            }
        }
        scope.commit().await?;
        Ok(true)
    }

    async fn flush_native(
        &self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // Existing dirty groups are emitted before expiration, matching the
        // legacy flush order (updates first, then TTL retractions).
        self.drain_native_dirty(ctx, collector).await?;
        while self.expire_native().await? {
            self.drain_native_dirty(ctx, collector).await?;
            tokio::task::yield_now().await;
        }
        // Each invocation owns at most one admitted cleanup page. Drain the
        // finite backlog through EOF/barriers as well as timer flushes, so an
        // idle keyed group does not retain a tombstone indefinitely.
        while self.cleanup_native().await? {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn drain_native_dirty(
        &self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store was not initialized"))?;
        let configured = self
            .native_config
            .ok_or_else(|| anyhow!("native aggregate limits are missing"))?;
        ensure!(
            configured.max_pending_output_rows >= 2,
            "native aggregate pending-output rows must fit one retract/append pair"
        );
        let store_limits = store.limits();
        let writes_per_group = 2
            + usize::from(self.native_has_group_keys())
            + usize::from(self.native_has_group_keys() && !self.retain_indefinitely);
        let max_dirty_groups = (configured.max_pending_output_rows / 2)
            .min(store_limits.write_operations / writes_per_group)
            .min(
                store_limits.write_bytes
                    / store
                        .max_encoded_entry_bytes()
                        .saturating_mul(writes_per_group),
            )
            .min(
                store_limits.overlay_bytes
                    / store_limits
                        .key_bytes
                        .saturating_add(store_limits.value_bytes)
                        .saturating_mul(writes_per_group),
            );
        ensure!(
            max_dirty_groups > 0,
            "native aggregate budget cannot flush one dirty group"
        );
        let mut after = None;
        loop {
            let _output_permit = store
                .resources()
                .try_decoded_value(configured.max_pending_output_bytes.saturating_mul(3))?;
            let mut scope = store.begin().await?;
            let mut output_keys = Vec::new();
            let mut output_values = vec![Vec::new(); self.aggregates.len()];
            let mut is_retracts = Vec::new();
            let mut retained_output_bytes = 0usize;
            let mut scanned = 0usize;
            let mut exhausted = false;
            while scanned < max_dirty_groups {
                let Some((dirty_key, _)) = scope.first_from(b"D", after.as_deref()).await? else {
                    exhausted = true;
                    break;
                };
                ensure!(dirty_key.len() >= 5, "invalid native aggregate dirty key");
                let group_key = dirty_key[5..].to_vec();
                let group_storage = native_group_key(b'G', &group_key)?;
                if let Some(bytes) = scope.get(&group_storage).await? {
                    let mut group = decode_group(
                        &bytes,
                        &self.native_state_types(),
                        &self.native_output_types(),
                        scope.limits().value_bytes,
                    )?;
                    let mut accumulators = self.native_accumulators(Some(&group))?;
                    let mut next = Vec::with_capacity(self.aggregates.len());
                    for (index, state) in accumulators.iter_mut().enumerate() {
                        next.push(match state {
                            IncrementalState::Sliding { accumulator, .. } => {
                                accumulator.evaluate()?
                            }
                            IncrementalState::Batch { .. } => {
                                self.native_fallback_value(
                                    &scope,
                                    &group_key,
                                    group.generation,
                                    index,
                                )
                                .await?
                            }
                        });
                    }
                    let append = self.native_group_is_live(&scope, &group_key).await?;
                    let unchanged = append
                        && group.last_emitted.as_ref().is_some_and(|old| {
                            old.iter()
                                .zip(next.iter())
                                .take(old.len().saturating_sub(1))
                                .all(|(old, new)| old == new)
                        });
                    if !unchanged {
                        let old_bytes = group.last_emitted.as_ref().map_or(0, |old| {
                            old.iter()
                                .map(ScalarValue::size)
                                .fold(0usize, usize::saturating_add)
                                .saturating_add(group_key.len())
                                .saturating_add(std::mem::size_of::<Vec<u8>>())
                        });
                        let new_bytes = if append {
                            next.iter()
                                .map(ScalarValue::size)
                                .fold(0usize, usize::saturating_add)
                                .saturating_add(group_key.len())
                                .saturating_add(std::mem::size_of::<Vec<u8>>())
                        } else {
                            0
                        };
                        let admitted = retained_output_bytes
                            .checked_add(old_bytes)
                            .and_then(|bytes| bytes.checked_add(new_bytes))
                            .context("aggregate retained output size overflow")?;
                        if admitted > configured.max_pending_output_bytes / 3 {
                            ensure!(
                                scanned > 0,
                                "one native aggregate changelog pair exceeds configured pending-output budget"
                            );
                            break;
                        }
                        retained_output_bytes = admitted;
                        if let Some(old) = group.last_emitted.take() {
                            is_retracts.push(true);
                            output_keys.push(group_key.clone());
                            for (column, value) in output_values.iter_mut().zip(old) {
                                column.push(value);
                            }
                        }
                        if append {
                            is_retracts.push(false);
                            output_keys.push(group_key.clone());
                            for (column, value) in output_values.iter_mut().zip(next.iter()) {
                                column.push(value.clone());
                            }
                            group.last_emitted = Some(next);
                        }
                    }
                    if !append && self.native_has_group_keys() {
                        // Retain an empty tombstone only while the old member
                        // generation is being cleaned. A reinsert gets the new
                        // generation and cannot be erased by that cleanup.
                        let old_generation = group.generation;
                        group.generation = group
                            .generation
                            .checked_add(1)
                            .context("aggregate generation overflow")?;
                        group.next_ordinal = 0;
                        scope.put(&native_cleanup_key(&group_key, old_generation)?, b"M")?;
                        if !self.retain_indefinitely {
                            let deadline = group
                                .last_update_nanos
                                .saturating_add(i64::try_from(self.ttl.as_nanos())?);
                            scope.delete(&native_expiry_key(deadline, &group_key)?)?;
                        }
                        if self.event_time_expiry.is_some() {
                            scope.delete(&native_validity_key(
                                group.validity_deadline_nanos,
                                &group_key,
                            )?)?;
                            group.validity_deadline_nanos = 0;
                        }
                    }
                    scope.put(
                        &group_storage,
                        &encode_group(&group, scope.limits().value_bytes)?,
                    )?;
                }
                scope.delete(&dirty_key)?;
                after = Some(dirty_key);
                scanned += 1;
            }
            if scanned == 0 {
                return Ok(());
            }
            let final_batch = if !output_keys.is_empty() {
                let parser = self.key_converter.parser();
                let mut columns = self
                    .key_converter
                    .convert_rows(output_keys.iter().map(|key| parser.parse(key.as_slice())))?;
                for column in output_values {
                    columns.push(ScalarValue::iter_to_array(column)?);
                }
                let record_batch =
                    RecordBatch::try_new(self.schema_without_metadata.clone(), columns)?;
                ensure!(
                    record_batch.get_array_memory_size() <= configured.max_pending_output_bytes,
                    "native aggregate output exceeds configured pending-output budget"
                );
                let metadata = self
                    .metadata_expr
                    .evaluate(&record_batch)?
                    .into_array(record_batch.num_rows())?;
                let metadata =
                    set_retract_metadata(metadata, Arc::new(BooleanArray::from(is_retracts)));
                let mut final_columns = record_batch.columns().to_vec();
                final_columns.push(metadata);
                let final_batch = RecordBatch::try_new(
                    ctx.out_schema
                        .as_ref()
                        .ok_or_else(|| anyhow!("aggregate output schema missing"))?
                        .schema
                        .clone(),
                    final_columns,
                )?;
                ensure!(
                    final_batch.get_array_memory_size() <= configured.max_pending_output_bytes,
                    "native aggregate output metadata exceeds configured pending-output budget"
                );
                Some(final_batch)
            } else {
                None
            };
            scope.commit().await?;
            if let Some(final_batch) = final_batch {
                collector.collect(final_batch).await?;
            }
            if exhausted {
                return Ok(());
            }
        }
    }

    async fn initialize_native(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        let Some(limits) = self.native_config else {
            bail!("native aggregate limits are not configured");
        };
        limits.validate()?;
        let indexed = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
            .count();
        let collections = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.collection)
            .count();
        let required_operations = (2 + usize::from(self.native_has_group_keys()))
            .checked_add(
                indexed
                    .checked_mul(2)
                    .context("aggregate index count overflow")?,
            )
            .and_then(|count| count.checked_add(collections.saturating_mul(2)))
            .and_then(|count| count.checked_add(3 * usize::from(!self.retain_indefinitely)))
            .and_then(|count| count.checked_add(self.calendar_write_operations()))
            .context("aggregate operation count overflow")?;
        ensure!(
            limits.write_operations >= required_operations
                && limits.overlay_bytes
                    >= required_operations
                        .saturating_mul(limits.key_bytes.saturating_add(limits.value_bytes)),
            "native aggregate limits cannot admit one worst-case event with {indexed} indexed aggregates"
        );
        let resources = configured_worker_resources()?
            .ok_or_else(|| anyhow!("native aggregate requires worker.live-state-resources"))?;
        ensure!(
            limits
                .overlay_bytes
                .saturating_add(limits.max_pending_output_bytes.saturating_mul(3))
                .saturating_add(limits.value_bytes.saturating_mul(3))
                <= resources.config().decoded_value_bytes,
            "native aggregate decoded pool cannot hold output, scope, and one read together"
        );
        let generation = match ctx.task_info.checkpoint_file_path_layout {
            arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
            _ => 0,
        };
        let backend: Arc<dyn LiveStateBackend> = construct_configured_backend(
            ConfiguredBackendOwner {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask: ctx.task_info.task_index,
                generation,
            },
            limits.max_resident_bytes,
            resources.clone(),
        )
        .await?;
        let table = ctx
            .table_manager
            .register_live_table(NATIVE_AGGREGATE_TABLE, backend.clone())
            .await?;
        let store = AggregateStore::new(backend, table, resources, native_limits(limits))?;
        ensure!(
            limits.write_bytes
                >= required_operations.saturating_mul(store.max_encoded_entry_bytes()),
            "native aggregate write budget cannot admit one worst-case event with {indexed} indexed aggregates"
        );
        self.native_store = Some(store);
        Ok(())
    }
    fn update_batch(
        &mut self,
        key: &[u8],
        batch: &[AggregateInput],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify_and_update(key, Instant::now(), |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    if let Some(values) = inputs.selected_values(idx)? {
                        accs.update_batch(self.new_generation, &values)?;
                    }
                }
                Ok(())
            })
            .expect("tried to update for non-existent key")
    }

    fn retract_batch(
        &mut self,
        key: &[u8],
        batch: &[AggregateInput],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify(key, |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    if let Some(values) = inputs.selected_values(idx)? {
                        accs.retract_batch(&values)?;
                    }
                }
                Ok::<(), datafusion::common::DataFusionError>(())
            })
            .expect("tried to retract state for non-existent key")?;

        Ok(())
    }

    fn evaluate(&mut self, key: &[u8]) -> DFResult<Vec<ScalarValue>> {
        self.accumulators
            .get_mut(key)
            .expect("tried to evaluate non-existent key")
            .iter_mut()
            .map(|s| s.evaluate())
            .collect::<DFResult<_>>()
    }

    fn checkpoint_sliding(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        let mut states = vec![vec![]; self.sliding_state_schema.schema.fields.len()];
        let parser = self.key_converter.parser();

        let mut generation_builder = UInt64Builder::with_capacity(self.updated_keys.len());

        let mut cols = self
            .key_converter
            .convert_rows(self.updated_keys.keys().map(|k| {
                let (accumulators, generation) = self
                    .accumulators
                    .get_mut_generation(k.0.as_ref())
                    .expect("missing accumulator in cache during checkpoint");

                generation_builder.append_value(generation);

                for (state, agg) in accumulators.iter_mut().zip(self.aggregates.iter()) {
                    let IncrementalState::Sliding { expr, accumulator } = state else {
                        continue;
                    };

                    let state = accumulator.state().unwrap_or_else(|_| {
                        // if it doesn't support immutable state, we'll use the mutable one and
                        // copy and restore the state -- this should in practice never happen,
                        // because the accumulators that don't support immutable state also don't
                        // support retract, but we have this fallback in case someone implements
                        // a new aggregator that doesn't uphold that relationship
                        let state = accumulator.state().unwrap();
                        *accumulator = expr.create_sliding_accumulator().unwrap();
                        let states: Vec<_> =
                            state.iter().map(|s| s.to_array()).try_collect().unwrap();
                        accumulator.merge_batch(&states).unwrap();
                        state
                    });

                    assert_eq!(
                        agg.state_cols.len(),
                        state.len(),
                        "wrong state in {}",
                        agg.func.name()
                    );

                    for (idx, v) in agg.state_cols.iter().zip(state) {
                        states[*idx].push(v);
                    }
                }
                parser.parse(k.0.as_ref())
            }))?;

        cols.extend(
            states
                .into_iter()
                .skip(cols.len())
                .map(|c| ScalarValue::iter_to_array(c).unwrap()),
        );

        let generations = generation_builder.finish();
        self.new_generation = self
            .new_generation
            .max(max_array::<UInt64Type, _>(&generations).unwrap());

        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    fn checkpoint_batch(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self
            .aggregates
            .iter()
            .all(|agg| agg.accumulator_type == AccumulatorType::Sliding)
        {
            return Ok(None);
        }

        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        // this is an under-estimate but getting the real value seems too expensive to be worth it
        let size = self.updated_keys.len();

        let mut rows = Vec::with_capacity(size);
        let mut accumulator_builder = UInt32Builder::with_capacity(size);
        let mut args_row_builder = BinaryBuilder::with_capacity(size, size * 4);
        let mut count_builder = UInt64Builder::with_capacity(size);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(size);
        let mut generation_builder = UInt64Builder::with_capacity(size);

        // TODO: the timestamp should really be coming from the original _timestamp column of
        //       the rows, as it does for sliding fields
        let now = to_nanos(SystemTime::now()) as i64;

        let parser = self.key_converter.parser();
        for k in self.updated_keys.keys() {
            let row = parser.parse(&k.0);
            for (i, state) in self
                .accumulators
                .get_mut(k.0.as_ref())
                .expect("missing accumulator in cache during checkpoint")
                .iter_mut()
                .enumerate()
            {
                let IncrementalState::Batch {
                    data,
                    changed_values,
                    ..
                } = state
                else {
                    continue;
                };

                for vk in changed_values.iter() {
                    if let Some(count) = data.get(vk) {
                        accumulator_builder.append_value(i as u32);
                        args_row_builder.append_value(&*vk.0);
                        count_builder.append_value(count.count);
                        generation_builder.append_value(count.generation);
                        timestamp_builder.append_value(now);
                        rows.push(row.to_owned())
                    }
                }

                // once we've checkpointed them, we can clear out keys with 0 counts
                data.retain(|_, v| v.count > 0);
            }
        }

        let mut cols = self.key_converter.convert_rows(rows)?;

        cols.push(Arc::new(accumulator_builder.finish()));
        cols.push(Arc::new(args_row_builder.finish()));
        cols.push(Arc::new(count_builder.finish()));
        cols.push(Arc::new(timestamp_builder.finish()));

        let generations = generation_builder.finish();
        self.new_generation = self
            .new_generation
            .max(max_array::<UInt64Type, _>(&generations).unwrap());
        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    fn restore_sliding(
        &mut self,
        key: &[u8],
        now: Instant,
        i: usize,
        aggregate_states: &Vec<Vec<ArrayRef>>,
        generation: u64,
    ) -> Result<()> {
        let mut accumulators = self.make_accumulators();
        for ((_, state_cols), acc) in self
            .aggregates
            .iter()
            .zip(aggregate_states.iter())
            .zip(accumulators.iter_mut())
        {
            if let IncrementalState::Sliding { accumulator, .. } = acc {
                accumulator.merge_batch(&state_cols.iter().map(|c| c.slice(i, 1)).collect_vec())?
            }
        }

        self.accumulators
            .insert(Arc::new(key.to_vec()), now, generation, accumulators);

        Ok(())
    }

    fn validate_checkpoint_semantics(&self, batch: &RecordBatch) -> Result<()> {
        if self
            .aggregates
            .iter()
            .any(|agg| agg.filter.is_some() || agg.func.order_bys().is_some())
        {
            let schema = batch.schema();
            let version = schema
                .field_with_name(TIMESTAMP_FIELD)?
                .metadata()
                .get(INPUT_SEMANTICS_KEY)
                .map(String::as_str);
            if version != Some(INPUT_SEMANTICS_V1) {
                bail!(
                    "legacy or unsupported ordered/filtered aggregate checkpoint: historical \
                       ORDER BY or FILTER inputs were not persisted correctly; restart from \
                       source replay with a fresh checkpoint"
                );
            }
        }
        Ok(())
    }

    fn restore_sliding_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.validate_checkpoint_semantics(batch)?;
        let key_converter = RowConverter::new(self.sliding_state_schema.sort_fields(false))?;
        let key_cols: Vec<_> = self
            .sliding_state_schema
            .sort_columns(batch, false)
            .into_iter()
            .map(|c| c.values)
            .collect();

        let aggregate_states = self
            .aggregates
            .iter()
            .map(|agg| {
                agg.state_cols
                    .iter()
                    .map(|idx| batch.column(*idx).clone())
                    .collect_vec()
            })
            .collect_vec();

        let generations = batch.columns().last().unwrap().as_primitive::<UInt64Type>();

        let now = Instant::now();

        if key_cols.is_empty() {
            // global aggregate
            self.restore_sliding(&GLOBAL_KEY, now, 0, &aggregate_states, generations.value(0))?;
        } else {
            let key_rows = key_converter.convert_columns(&key_cols)?;
            for ((i, row), generation) in key_rows.iter().enumerate().zip(generations) {
                self.restore_sliding(row.as_ref(), now, i, &aggregate_states, generation.unwrap())?;
            }
        }
        Ok(())
    }

    fn restore_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.validate_checkpoint_semantics(batch)?;
        let key_cols: Vec<_> = self
            .sliding_state_schema
            .sort_columns(batch, false)
            .into_iter()
            .map(|c| c.values)
            .collect();

        let count_column = batch
            .column(self.batch_state_schema.schema.index_of("count").unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let accumulator_column = batch
            .column(
                self.batch_state_schema
                    .schema
                    .index_of("accumulator")
                    .unwrap(),
            )
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let args_row_column = batch
            .column(self.batch_state_schema.schema.index_of("args_row").unwrap())
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let generations = batch.columns().last().unwrap().as_primitive::<UInt64Type>();

        // Check every row before mutating the cache. A checkpoint can contain
        // rows for several aggregates, including legacy ordered rows alongside
        // current rows, and a late failure must not leave a partial restore.
        for i in 0..batch.num_rows() {
            let accumulator_idx = accumulator_column.value(i) as usize;
            let agg = self
                .aggregates
                .get(accumulator_idx)
                .ok_or_else(|| anyhow!("invalid checkpoint aggregate index {accumulator_idx}"))?;
            decode_args_row(args_row_column.value(i), agg.func.order_bys().is_some())?;
        }

        let key_rows = if key_cols.is_empty() {
            vec![GLOBAL_KEY; batch.num_rows()]
        } else {
            self.key_converter
                .convert_columns(&key_cols)?
                .iter()
                .map(|k| k.as_ref().to_vec())
                .collect()
        };

        for (i, row) in key_rows.iter().enumerate() {
            let accumulator_idx = accumulator_column.value(i) as usize;
            let args_row = args_row_column.value(i);
            let Some(accumulators) = self.accumulators.get_mut(row.as_ref()) else {
                debug!(
                    "missing accumulator for key {:?} while restoring batch",
                    row
                );
                continue;
            };

            let count = count_column.value(i);
            let generation = generations.value(i);

            let IncrementalState::Batch { data, .. } = &mut accumulators[accumulator_idx] else {
                bail!(
                    "expected aggregate {accumulator_idx} to be a batch accumulator, but was sliding"
                );
            };

            if let Some(existing) = data.get_mut(args_row) {
                if existing.generation < generation {
                    existing.count = count;
                    existing.generation = generation;
                }
            } else {
                data.insert(
                    Key(Arc::new(args_row.to_vec())),
                    BatchData { count, generation },
                );
            }
        }
        Ok(())
    }

    async fn initialize(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        let table = ctx.table_manager.get_uncached_key_value_view("a").await?;

        // initialize the sliding accumulator cache
        let mut stream = Box::pin(table.get_all());
        while let Some(batch) = stream.next().await {
            let batch = batch?;

            if batch.num_rows() == 0 {
                continue;
            }

            self.restore_sliding_batch(&batch)?;
        }

        drop(stream);

        // initialize the batch accumulator cache, if there are batch accumulators
        if self
            .aggregates
            .iter()
            .any(|agg| agg.accumulator_type == AccumulatorType::Batch)
        {
            let table = ctx.table_manager.get_uncached_key_value_view("b").await?;
            let mut stream = Box::pin(table.get_all());
            while let Some(batch) = stream.next().await {
                let batch = batch?;

                if batch.num_rows() == 0 {
                    continue;
                }

                self.restore_batch(&batch)?;
            }
        }

        let mut deleted_keys = vec![];
        for (k, v) in self.accumulators.iter_mut() {
            // the state system doesn't yet support deletes, so we'll determine if a value
            // is deleted by the timestamp being null
            let is_deleted = v
                .last_mut()
                .ok_or_else(|| anyhow!("no aggregrates"))?
                .evaluate()?
                .is_null();

            if is_deleted {
                deleted_keys.push(k.clone());
            } else {
                // clear empty entries from the batch accumulators
                for is in v {
                    if let IncrementalState::Batch { data, .. } = is {
                        data.retain(|_, v| v.count > 0);
                    }
                }
            }
        }
        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }

        Ok(())
    }

    async fn checkpoint(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        if self.updated_keys.is_empty() {
            return Ok(());
        }

        if let Some(sliding) = self.checkpoint_sliding()? {
            let table = ctx.table_manager.get_uncached_key_value_view("a").await?;
            table.insert_batch(sliding).await?;
        }

        if let Some(batch) = self.checkpoint_batch()? {
            let table = ctx.table_manager.get_uncached_key_value_view("b").await?;
            table.insert_batch(batch).await?;
        }

        Ok(())
    }

    async fn flush(&mut self, ctx: &mut OperatorContext) -> Result<Option<RecordBatch>> {
        self.checkpoint(ctx).await?;

        let mut output_keys = Vec::with_capacity(self.updated_keys.len() * 2);
        let mut output_values =
            vec![Vec::with_capacity(self.updated_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(self.updated_keys.len() * 2);

        let (updated_keys, updated_values): (Vec<_>, Vec<_>) =
            mem::take(&mut self.updated_keys).into_iter().unzip();

        let mut deleted_keys = vec![];

        for (k, retract) in updated_keys.iter().zip(updated_values) {
            let append = self.evaluate(&k.0)?;

            if let Some(v) = retract {
                // don't bother emitting updates that just retract / append the same values (excluding
                // the last, timestamp field)
                if v.iter()
                    .zip(append.iter())
                    .take(v.len() - 1)
                    .all(|(a, b)| a == b)
                {
                    continue;
                }

                is_retracts.push(true);
                output_keys.push(k);
                for (out, v) in output_values.iter_mut().zip(v) {
                    out.push(v);
                }
            }

            if !append.last().unwrap().is_null() {
                // if the timestamp is null, that means we've removed all of the data from
                // this key, and we shouldn't emit an append
                is_retracts.push(false);
                output_keys.push(k);
                for (out, v) in output_values.iter_mut().zip(append) {
                    out.push(v);
                }
            } else {
                deleted_keys.push(k);
            }
        }

        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }

        let mut ttld_keys = vec![];

        for (k, mut v) in self.accumulators.time_out(Instant::now()) {
            // retract items that are being ttl'd
            is_retracts.push(true);
            ttld_keys.push(k);

            for (out, v) in output_values
                .iter_mut()
                .zip(v.iter_mut().map(|s| s.evaluate()))
            {
                out.push(v?);
            }
        }

        if output_keys.is_empty() {
            return Ok(None);
        }

        let row_parser = self.key_converter.parser();
        let mut result_cols = self.key_converter.convert_rows(
            output_keys
                .iter()
                .map(|k| row_parser.parse(k.0.as_slice()))
                .chain(ttld_keys.iter().map(|k| row_parser.parse(k.as_slice()))),
        )?;

        for acc in output_values.into_iter() {
            result_cols.push(ScalarValue::iter_to_array(acc).unwrap());
        }

        // push the metadata column
        let record_batch =
            RecordBatch::try_new(self.schema_without_metadata.clone(), result_cols).unwrap();

        let metadata = self
            .metadata_expr
            .evaluate(&record_batch)
            .unwrap()
            .into_array(record_batch.num_rows())
            .unwrap();
        let metadata = set_retract_metadata(metadata, Arc::new(BooleanArray::from(is_retracts)));

        let mut final_batch = record_batch.columns().to_vec();
        final_batch.push(metadata);

        Ok(Some(RecordBatch::try_new(
            ctx.out_schema.as_ref().unwrap().schema.clone(),
            final_batch,
        )?))
    }

    fn get_retracts(batch: &RecordBatch) -> Option<&BooleanArray> {
        if let Some(meta_col) = batch.column_by_name(UPDATING_META_FIELD) {
            let meta_struct = meta_col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("_updating_meta must be StructArray");

            let is_retract_array = meta_struct
                .column_by_name("is_retract")
                .expect("meta struct must have is_retract");
            let is_retract = is_retract_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("is_retract must be BooleanArray");

            Some(is_retract)
        } else {
            None
        }
    }

    fn make_accumulators(&self) -> Vec<IncrementalState> {
        self.aggregates
            .iter()
            .map(|agg| match agg.accumulator_type {
                AccumulatorType::Sliding => IncrementalState::Sliding {
                    expr: agg.func.clone(),
                    accumulator: agg.func.create_sliding_accumulator().unwrap(),
                },
                AccumulatorType::AppendOnly => IncrementalState::Sliding {
                    expr: agg.func.clone(),
                    accumulator: agg.func.create_accumulator().unwrap(),
                },
                AccumulatorType::Batch => IncrementalState::Batch {
                    expr: agg.func.clone(),
                    data: Default::default(),
                    row_converter: agg.row_converter.clone(),
                    changed_values: Default::default(),
                    ordered: agg.func.order_bys().is_some(),
                },
            })
            .collect()
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let aggregate_input_cols = self.compute_inputs(batch)?;

        let mut first = false;

        // workaround for https://github.com/rust-lang/rust-clippy/issues/13934
        #[allow(clippy::map_entry)]
        if !self.accumulators.contains_key(&GLOBAL_KEY) {
            first = true;
            self.accumulators.insert(
                Arc::new(GLOBAL_KEY),
                Instant::now(),
                self.new_generation,
                self.make_accumulators(),
            );
        }

        if !self.updated_keys.contains_key(GLOBAL_KEY.as_slice()) {
            if first {
                self.updated_keys.insert(Key(Arc::new(GLOBAL_KEY)), None);
            } else {
                let v = Some(self.evaluate(&GLOBAL_KEY)?);
                self.updated_keys.insert(Key(Arc::new(GLOBAL_KEY)), v);
            }
        }

        // update / retract the values against the accumulators

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                if r.unwrap_or_default() {
                    self.retract_batch(&GLOBAL_KEY, &aggregate_input_cols, Some(i))?;
                } else {
                    self.update_batch(&GLOBAL_KEY, &aggregate_input_cols, Some(i))?;
                }
            }
        } else {
            // if these are all appends, we can much more efficiently do all of the updates at once
            self.update_batch(&GLOBAL_KEY, &aggregate_input_cols, None)
                .unwrap();
        }

        Ok(())
    }

    fn keyed_aggregate(&mut self, batch: &RecordBatch, ctx: &OperatorContext) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let sort_columns = &ctx.in_schemas[0]
            .sort_columns(batch, false)
            .into_iter()
            .map(|e| e.values)
            .collect::<Vec<_>>();

        let keys = self.key_converter.convert_columns(sort_columns).unwrap();

        // store the initial values for keys which we are updating for the first time for the current
        // flush, so that we can retract them
        for k in &keys {
            if !self.updated_keys.contains_key(k.as_ref()) {
                if let Some((key, accs)) = self.accumulators.get_mut_key_value(k.as_ref()) {
                    self.updated_keys.insert(
                        key,
                        Some(
                            accs.iter_mut()
                                .map(|s| s.evaluate())
                                .collect::<DFResult<_>>()?,
                        ),
                    );
                } else {
                    self.updated_keys
                        .insert(Key(Arc::new(k.as_ref().to_vec())), None);
                }
            }
        }

        // then update the states with the new data
        let aggregate_input_cols = self.compute_inputs(batch)?;

        for (i, key) in keys.iter().enumerate() {
            if self.accumulators.contains_key(key.as_ref()) {
                self.accumulators.get_mut(key.as_ref()).unwrap()
            } else {
                let new_accumulators = self.make_accumulators();
                self.accumulators.insert(
                    Arc::new(key.as_ref().to_vec()),
                    Instant::now(),
                    0,
                    new_accumulators,
                );
                self.accumulators.get_mut(key.as_ref()).unwrap()
            };

            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            if retract {
                self.retract_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            } else {
                self.update_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            }
        }

        Ok(())
    }

    fn compute_inputs(&self, batch: &RecordBatch) -> DFResult<Vec<AggregateInput>> {
        self.aggregates
            .iter()
            .map(|agg| {
                let values = agg
                    .input_exprs
                    .iter()
                    .map(|ex| ex.evaluate(batch)?.into_array(batch.num_rows()))
                    .collect::<DFResult<Vec<_>>>()?;
                let filter = agg
                    .filter
                    .as_ref()
                    .map(|ex| {
                        let values = ex.evaluate(batch)?.into_array(batch.num_rows())?;
                        values
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .cloned()
                            .ok_or_else(|| {
                                datafusion::common::DataFusionError::Execution(
                                    "aggregate FILTER expression must return Boolean".into(),
                                )
                            })
                    })
                    .transpose()?;
                Ok(AggregateInput { values, filter })
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl ArrowOperator for IncrementalAggregatingFunc {
    fn name(&self) -> String {
        "UpdatingAggregatingFunc".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        let aggregates = self
            .aggregates
            .iter()
            .map(|agg| format!("{} ({:?})", agg.func.name(), agg.accumulator_type))
            .collect::<Vec<_>>();

        DisplayableOperator {
            name: Cow::Borrowed("UpdatingAggregatingFunc"),
            fields: vec![
                ("flush_interval", AsDisplayable::Debug(&self.flush_interval)),
                ("ttl", AsDisplayable::Debug(&self.ttl)),
                (
                    "state_schema",
                    AsDisplayable::Schema(&self.sliding_state_schema.schema),
                ),
                ("aggregates", AsDisplayable::List(aggregates)),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.native_config.is_some() {
            self.process_native_batch(&batch, ctx).await?;
            return Ok(());
        }
        let input_schema = &ctx.in_schemas[0];

        if input_schema
            .routing_keys()
            .map(|k| !k.is_empty())
            .unwrap_or_default()
        {
            self.keyed_aggregate(&batch, ctx)?
        } else {
            self.global_aggregate(&batch)?
        };
        Ok(())
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        if self.native_config.is_some()
            && self.event_time_expiry.is_some()
            && let Watermark::EventTime(time) = watermark
        {
            let watermark_nanos = if time == arroyo_types::from_nanos(u64::MAX as u128) {
                i64::MAX
            } else {
                arroyo_types::event_time::to_signed_nanos(time)
                    .context("event-time result watermark exceeds timestamp range")?
            };
            // Same-watermark ordering: preceding data batches have already
            // refreshed group state and deadlines; emit those refreshes first,
            // then drain each due expiry page's retractions before the next.
            self.flush_native(ctx, collector).await?;
            while self.expire_native_event_time(watermark_nanos).await? {
                self.drain_native_dirty(ctx, collector).await?;
                tokio::task::yield_now().await;
            }
            while self.cleanup_native().await? {
                tokio::task::yield_now().await;
            }
        }
        if self.native_config.is_some()
            && !self.calendars.is_empty()
            && let Watermark::EventTime(time) = watermark
        {
            // Terminal infinity closes ordinary windows but cannot invent a
            // calendar date. Idle carries no event-time progress either.
            if time != arroyo_types::from_nanos(u64::MAX as u128) {
                let nanos = arroyo_types::event_time::to_signed_nanos(time)
                    .context("calendar watermark exceeds timestamp range")?;
                self.flush_native(ctx, collector).await?;
                while self.advance_calendar_watermark(nanos).await? {
                    self.drain_native_dirty(ctx, collector).await?;
                    tokio::task::yield_now().await;
                }
            }
        }
        Ok(Some(watermark))
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.native_config.is_some() {
            return self.flush_native(ctx, collector).await;
        }
        if let Some(batch) = self.flush(ctx).await? {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.native_config.is_some() {
            return HashMap::from([(
                NATIVE_AGGREGATE_TABLE.to_string(),
                TableConfig {
                    table_type: TableEnum::DiskKeyedMap.into(),
                    state_version: 1,
                    config: DiskKeyedTableConfig {
                        table_name: NATIVE_AGGREGATE_TABLE.to_string(),
                        encoding_version: 1,
                        schema_identity: self.native_schema_identity.clone(),
                    }
                    .encode_to_vec(),
                },
            )]);
        }
        vec![
            (
                "a".to_string(),
                timestamp_table_config(
                    "a",
                    "accumulator_state",
                    self.ttl,
                    true,
                    self.sliding_state_schema.as_ref().clone(),
                ),
            ),
            (
                "b".to_string(),
                timestamp_table_config(
                    "b",
                    "batch_state",
                    self.ttl,
                    true,
                    self.batch_state_schema.as_ref().clone(),
                ),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(self.flush_interval)
    }

    async fn handle_tick(
        &mut self,
        _tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.native_config.is_some() {
            return self.flush_native(ctx, collector).await;
        }
        if let Some(batch) = self.flush(ctx).await? {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.native_config.is_some() {
            if matches!(final_message, Some(SignalMessage::EndOfData)) {
                self.flush_native(ctx, collector).await?;
            }
            return Ok(());
        }
        if let Some(SignalMessage::EndOfData) = final_message
            && let Some(batch) = self.flush(ctx).await?
        {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if self.native_config.is_some() {
            self.initialize_native(ctx).await?;
        } else {
            self.initialize(ctx).await?;
        }
        Ok(())
    }
}

fn set_retract_metadata(metadata: ArrayRef, is_retract: Arc<BooleanArray>) -> ArrayRef {
    let metadata = metadata.as_struct();

    let arrays: Vec<Arc<dyn Array>> = vec![is_retract, metadata.column(1).clone()];
    Arc::new(StructArray::new(updating_meta_fields(), arrays, None))
}

fn native_changelog_columns(batch: &RecordBatch) -> Result<(&BooleanArray, &FixedSizeBinaryArray)> {
    let metadata = batch
        .column_by_name(UPDATING_META_FIELD)
        .context("native changelog input has no updating metadata")?
        .as_any()
        .downcast_ref::<StructArray>()
        .context("native changelog metadata is not a struct")?;
    ensure!(
        metadata.null_count() == 0,
        "native changelog metadata contains NULL"
    );
    let retracts = metadata
        .column_by_name("is_retract")
        .context("native changelog metadata has no retract flag")?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("native changelog retract flag is not Boolean")?;
    let ids = metadata
        .column_by_name("id")
        .context("native changelog metadata has no row ID")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("native changelog row ID is not FixedSizeBinary")?;
    ensure!(
        retracts.null_count() == 0 && ids.null_count() == 0 && ids.value_length() == 16,
        "native changelog requires non-null Boolean retractions and 16-byte row IDs"
    );
    Ok((retracts, ids))
}

pub struct IncrementalAggregatingConstructor;

impl OperatorConstructor for IncrementalAggregatingConstructor {
    type ConfigT = UpdatingAggregateOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(ConstructedOperator::from_operator(Box::new(Self::build(
            config, registry,
        )?)))
    }
}

impl IncrementalAggregatingConstructor {
    fn build(
        config: UpdatingAggregateOperator,
        registry: Arc<Registry>,
    ) -> Result<IncrementalAggregatingFunc> {
        Self::build_with_native_config(
            config,
            registry,
            arroyo_rpc::config::config().worker.aggregate_state,
        )
    }

    fn build_with_native_config(
        config: UpdatingAggregateOperator,
        registry: Arc<Registry>,
        native_config: Option<AggregateStateConfig>,
    ) -> Result<IncrementalAggregatingFunc> {
        if config.retain_indefinitely == Some(true) && native_config.is_none() {
            bail!("SET updating_ttl = NULL requires worker.aggregate-state native backend limits");
        }
        if config.event_time_expiry.is_some() && native_config.is_none() {
            bail!("event-time result expiry requires worker.aggregate-state native backend limits");
        }
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-updating-aggregate.v1");
        identity.update(&config.aggregate_exec);
        let mut bounds: Vec<_> = config.collection_output_limits.iter().collect();
        bounds.sort_unstable_by_key(|(index, _)| **index);
        for (index, limit) in bounds {
            identity.update(index.to_be_bytes());
            identity.update(limit.to_be_bytes());
        }

        identity.update(&config.metadata_expr);
        identity.update(config.ttl_micros.to_be_bytes());
        identity.update([u8::from(config.retain_indefinitely == Some(true))]);
        match &config.event_time_expiry {
            Some(expiry) => {
                identity.update([1u8]);
                identity.update(b"current-result-deadline.signed-y.v1");
                identity.update(&expiry.result_timestamp_expr);
                identity.update(expiry.delay_nanos.to_be_bytes());
            }
            None => identity.update([0u8]),
        }
        if let Some(schema) = &config.input_schema {
            identity.update(schema.encode_to_vec());
        }
        if let Some(schema) = &config.final_schema {
            identity.update(schema.encode_to_vec());
        }
        if !config.calendar_aggregates.is_empty() {
            identity.update(b"\0calendar-day-state.v1");
            identity.update(u64::try_from(config.calendar_aggregates.len())?.to_be_bytes());
            for descriptor in &config.calendar_aggregates {
                let encoded = descriptor.encode_to_vec();
                identity.update(u64::try_from(encoded.len())?.to_be_bytes());
                identity.update(encoded);
            }
        }
        ensure!(
            config.calendar_aggregates.is_empty() || native_config.is_some(),
            "maintained calendar FILTER requires worker.aggregate-state native backend limits"
        );
        let ttl = Duration::from_micros(if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        });

        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let native_append_only =
            native_config.is_some() && input_schema.schema.index_of(UPDATING_META_FIELD).is_err();
        let final_schema: ArroyoSchema = config.final_schema.unwrap().try_into()?;
        let mut schema_without_metadata = SchemaBuilder::from((*final_schema.schema).clone());
        schema_without_metadata.remove(final_schema.schema.index_of(UPDATING_META_FIELD).unwrap());

        let metadata_expr = parse_physical_expr(
            &PhysicalExprNode::decode(&mut config.metadata_expr.as_slice())?,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;
        let event_time_expiry = config
            .event_time_expiry
            .map(|expiry| -> Result<EventTimeExpiryRuntime> {
                Ok(EventTimeExpiryRuntime {
                    result_timestamp_expr: parse_physical_expr(
                        &PhysicalExprNode::decode(&mut expiry.result_timestamp_expr.as_slice())?,
                        registry.as_ref(),
                        &input_schema.schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?,
                    delay_nanos: expiry.delay_nanos,
                })
            })
            .transpose()?;

        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) =
            aggregate_exec.physical_plan_type.unwrap()
        else {
            bail!("invalid proto -- expected aggregate exec");
        };

        // the state schema is made up of the key fields + the state fields for each aggregator
        // (if it supports retraction) otherwise, the input data + a count
        let mut sliding_state_fields = input_schema
            .routing_keys()
            .map(|v| {
                v.iter()
                    .map(|idx| input_schema.schema.field(*idx).clone())
                    .collect_vec()
            })
            .unwrap_or_default();

        let mut batch_state_fields = sliding_state_fields.clone();

        let key_fields = (0..sliding_state_fields.len()).collect_vec();

        if !aggregate_exec.filter_expr.is_empty()
            && aggregate_exec.filter_expr.len() != aggregate_exec.aggr_expr.len()
        {
            bail!("aggregate FILTER count does not match aggregate expressions");
        }
        let filters = aggregate_exec
            .filter_expr
            .iter()
            .map(|filter| {
                filter
                    .expr
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(
                            expr,
                            registry.as_ref(),
                            &input_schema.schema,
                            &DefaultPhysicalExtensionCodec {},
                        )
                    })
                    .transpose()
            })
            .collect::<DFResult<Vec<_>>>()?;

        let aggregates: Vec<_> = aggregate_exec
            .aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .enumerate()
            .map(|(index, (expr, name))| -> DFResult<_> {
                Ok((
                    decode_aggregate(&input_schema.schema, name, expr, registry.as_ref())?,
                    filters.get(index).cloned().flatten(),
                ))
            })
            .map_ok(|(agg, filter)| {
                let native_state = native_config.is_some();
                let function = agg.fun().name().to_ascii_lowercase();
                let retract = match agg.create_sliding_accumulator() {
                    Ok(s) => s.supports_retract_batch(),
                    _ => false,
                };

                // Moving MIN/MAX and FIRST/LAST need the member index on a
                // changelog input. On a declared append-only input, ordinary
                // DataFusion state holds only the current winner and its order
                // tuple (if any); its state() is reconstructible without member
                // history. Other native aggregates retain their admission gate.
                let native_sliding = matches!(function.as_str(), "count" | "sum" | "avg");
                let append_only_state = native_append_only
                    && matches!(
                        function.as_str(),
                        "min" | "max" | "first_value" | "last_value"
                    );

                (
                    agg,
                    if append_only_state {
                        AccumulatorType::AppendOnly
                    } else if retract && (!native_state || native_sliding) {
                        AccumulatorType::Sliding
                    } else {
                        AccumulatorType::Batch
                    },
                    filter,
                )
            })
            .map_ok(|(agg, t, filter)| {
                let input_exprs =
                    aggregate_expressions(std::slice::from_ref(&agg), &AggregateMode::Single, 0)?
                        .pop()
                        .unwrap();
                let collection = native_config.is_some()
                    && agg
                        .fun()
                        .inner()
                        .as_any()
                        .is::<datafusion::functions_aggregate::array_agg::ArrayAgg>();
                if collection {
                    ensure!(
                        agg.expressions().len() == 1,
                        "native ARRAY_AGG requires one value argument"
                    );
                    let types = input_exprs
                        .iter()
                        .map(|expr| expr.data_type(&input_schema.schema))
                        .collect::<DFResult<Vec<_>>>()?;
                    let bound = native_collection_decode_bound(&types, 0)?;
                    let allowance = native_config
                        .context("native ARRAY_AGG limits are missing")?
                        .value_bytes
                        .checked_mul(3)
                        .context("native collection decoded allowance overflow")?;
                    ensure!(
                        bound <= allowance,
                        "native ARRAY_AGG collection schema exceeds configured decoded allowance"
                    );
                }
                let row_converter = Arc::new(RowConverter::new(
                    input_exprs
                        .iter()
                        .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                        .collect::<DFResult<_>>()?,
                )?);

                let fields = t.state_fields(&agg)?;
                let first_state_col = sliding_state_fields.len();
                sliding_state_fields.extend(fields.into_iter().enumerate().map(|(part, field)| {
                    let field = (*field).clone();
                    if t == AccumulatorType::AppendOnly {
                        // FIRST/LAST state contains repeated names such as is_set.
                        // Names are internal; ordinals identify the persisted state.
                        field.with_name(format!("_native_state_{}", first_state_col + part))
                    } else {
                        field
                    }
                }));
                let state_cols = (first_state_col..sliding_state_fields.len()).collect_vec();

                let (index_converter, index_columns) =
                    if t == AccumulatorType::Batch && native_config.is_some() {
                        let (codec, columns) =
                            native_index_codec(&agg, &input_exprs, &input_schema.schema)?;
                        (Some(codec), columns)
                    } else {
                        (None, Vec::new())
                    };

                Ok::<_, anyhow::Error>((
                    agg,
                    t,
                    row_converter,
                    state_cols,
                    input_exprs,
                    filter,
                    index_converter,
                    index_columns,
                    collection,
                ))
            })
            // The second map_ok returns an inner Result. flatten_ok would
            // silently omit an aggregate when its native index codec fails.
            .map(|result| {
                result
                    .map_err(anyhow::Error::from)
                    .and_then(std::convert::identity)
            })
            .collect::<Result<_>>()?;

        let state_schema = Schema::new(sliding_state_fields);

        let versioned_inputs = aggregates.iter().any(|(agg, _, _, _, _, filter, _, _, _)| {
            filter.is_some() || agg.order_bys().is_some()
        });
        // The planner appends this engine-owned event-time MAX after caller
        // expressions. Validate its physical expression, rather than treating
        // any caller MAX or alias named `_timestamp` as the row-ID index.
        let injected_timestamp_index = if native_config.is_some() && !native_append_only {
            let index = aggregates
                .len()
                .checked_sub(1)
                .context("native aggregate has no injected event-time MAX")?;
            let (agg, kind, _, _, args, filter, _, _, _) = &aggregates[index];
            let event_column = args
                .first()
                .and_then(|arg| arg.as_any().downcast_ref::<Column>());
            ensure!(
                *kind == AccumulatorType::Batch
                    && agg.fun().inner().as_any().is::<Max>()
                    && agg.name() == TIMESTAMP_FIELD
                    && args.len() == 1
                    && event_column
                        .is_some_and(|column| column.index() == input_schema.timestamp_index)
                    && input_schema
                        .schema
                        .field(input_schema.timestamp_index)
                        .name()
                        == TIMESTAMP_FIELD
                    && input_schema
                        .schema
                        .field(input_schema.timestamp_index)
                        .data_type()
                        == &DataType::Timestamp(TimeUnit::Nanosecond, None)
                    && filter.is_none()
                    && agg.order_bys().is_none()
                    && !agg.is_distinct()
                    && !agg.ignore_nulls()
                    && final_schema.schema.fields().len()
                        == key_fields.len() + aggregates.len() + 1
                    && final_schema.timestamp_index + 1 == final_schema.schema.fields().len() - 1
                    && final_schema
                        .schema
                        .field(final_schema.timestamp_index)
                        .name()
                        == TIMESTAMP_FIELD
                    && final_schema
                        .schema
                        .field(final_schema.timestamp_index)
                        .data_type()
                        == &DataType::Timestamp(TimeUnit::Nanosecond, None),
                "native aggregate final event-time MAX does not match the planner-injected expression"
            );
            Some(index)
        } else {
            None
        };
        for (index, limit) in config
            .collection_output_limits
            .iter()
            .filter(|_| native_config.is_some())
        {
            let (agg, _, _, _, _, _, _, _, collection) = aggregates
                .get(*index as usize)
                .context("bounded collection aggregate ordinal is invalid")?;
            ensure!(
                *collection && !agg.is_distinct() && agg.order_bys().is_some(),
                "bounded collection requires ordered non-distinct native ARRAY_AGG"
            );
            usize::try_from(*limit).context("bounded collection limit exceeds platform size")?;
        }
        let aggregates: Vec<Aggregator> = aggregates
            .into_iter()
            .enumerate()
            .map(
                |(
                    index,
                    (
                        agg,
                        t,
                        row_converter,
                        state_cols,
                        input_exprs,
                        filter,
                        index_converter,
                        index_columns,
                        collection,
                    ),
                )|
                 -> Result<Aggregator> {
                    Ok(Aggregator {
                        collection_member_bound: if collection {
                            native_collection_decode_bound(
                                &input_exprs
                                    .iter()
                                    .map(|expr| expr.data_type(&input_schema.schema))
                                    .collect::<DFResult<Vec<_>>>()?,
                                native_config
                                    .context("native collection config missing")?
                                    .key_bytes,
                            )?
                        } else {
                            0
                        },
                        func: agg,
                        input_exprs,
                        filter,
                        accumulator_type: t,
                        row_converter,
                        state_cols,
                        index_converter,
                        index_columns,
                        collection,
                        collection_output_limit: config
                            .collection_output_limits
                            .get(&(index as u32))
                            .filter(|_| native_config.is_some())
                            .map(|limit| usize::try_from(*limit))
                            .transpose()?,
                        injected_timestamp: Some(index) == injected_timestamp_index,
                    })
                },
            )
            .collect::<Result<_>>()?;

        let calendars = CalendarAggregate::decode(
            &config.calendar_aggregates,
            &aggregates,
            &input_schema.schema,
            registry.as_ref(),
        )?;

        // Only the append-only ordinary-state layout is new. Preserve existing
        // checkpoint identity for unchanged native COUNT/SUM/AVG and changelog
        // index formats while rejecting old indexed checkpoints for this layout.
        if aggregates
            .iter()
            .any(|aggregate| aggregate.accumulator_type == AccumulatorType::AppendOnly)
        {
            identity.update(b"\0append-only-ordinary-state.v1");
        }
        // Keyed native groups now persist their input-row cardinality alongside
        // the accumulator. An older checkpoint lacks that key and cannot safely
        // distinguish an empty group from NULL-valued aggregate results.
        if final_schema.schema.fields().len() > aggregates.len() + 1 {
            identity.update(b"\0keyed-live-rows.v1");
        }
        if injected_timestamp_index.is_some() {
            // Previous R keys contain the timestamp argument, not the stable
            // source row ID. They cannot recover a CDC before row's old time.
            identity.update(b"\0injected-timestamp-row-id.v1");
        }
        let native_schema_identity = identity.finalize().to_vec();

        // An indexed-only plan has no sliding accumulator state to carry the
        // legacy timestamp field. Keep its key columns intact and supply the
        // schema sentinel explicitly; native state still persists through G/M/R.
        let mut state_fields = state_schema.fields().to_vec();
        let timestamp_field = if state_fields.len() == key_fields.len() {
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )
        } else {
            (*state_fields.pop().unwrap())
                .clone()
                .with_name(TIMESTAMP_FIELD)
        };
        state_fields.push(Arc::new(versioned_state_timestamp(
            timestamp_field,
            versioned_inputs,
        )));

        let sliding_state_schema = Arc::new(ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(state_fields)),
            key_fields.clone(),
        )?);

        batch_state_fields.push(Field::new("accumulator", DataType::UInt32, false));
        batch_state_fields.push(Field::new("args_row", DataType::Binary, false));
        batch_state_fields.push(Field::new("count", DataType::UInt64, false));
        batch_state_fields.push(versioned_state_timestamp(
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            versioned_inputs,
        ));
        let timestamp_index = batch_state_fields.len() - 1;

        let mut storage_key_fields = key_fields.clone();
        // include accumulator and args_row in the keys
        storage_key_fields.push(storage_key_fields.len());
        storage_key_fields.push(storage_key_fields.len());

        let batch_state_schema = Arc::new(ArroyoSchema::new(
            Arc::new(Schema::new(batch_state_fields)),
            timestamp_index,
            Some(storage_key_fields),
            // only include the actual keys in the routing keys
            Some(key_fields),
        ));

        Ok(IncrementalAggregatingFunc {
            flush_interval: Duration::from_micros(config.flush_interval_micros),
            metadata_expr,
            ttl,
            aggregates,
            accumulators: UpdatingCache::with_time_to_idle(ttl),
            schema_without_metadata: Arc::new(schema_without_metadata.finish()),
            updated_keys: Default::default(),
            key_converter: RowConverter::new(input_schema.sort_fields(false))?,
            sliding_state_schema,
            batch_state_schema,
            new_generation: 0,
            native_config,
            native_execution_resources: if native_config.is_some() {
                configured_execution_resources()?
            } else {
                None
            },
            native_store: None,
            retain_indefinitely: config.retain_indefinitely == Some(true),
            native_schema_identity,
            native_append_only,
            event_time_expiry,
            calendars,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::SortOptions;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arroyo_state::live::{
        Ownership,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
        table::LiveTableManager,
        write::AdmittedWriteBatch,
    };
    use datafusion::execution::FunctionRegistry;
    use datafusion::functions_aggregate::{
        array_agg::array_agg_udaf,
        count::count_udaf,
        first_last::{first_value_udaf, last_value_udaf},
        min_max::max_udaf,
    };
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_proto::physical_plan::to_proto::{
        serialize_physical_aggr_expr, serialize_physical_expr,
    };
    use datafusion_proto::protobuf::{AggregateExecNode, MaybeFilter};

    #[derive(Default)]
    struct AggregateCollector {
        batches: Vec<RecordBatch>,
    }

    #[async_trait::async_trait]
    impl Collector for AggregateCollector {
        async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
            self.batches.push(batch);
            Ok(())
        }

        async fn broadcast_watermark(
            &mut self,
            _watermark: arroyo_types::Watermark,
        ) -> DataflowResult<()> {
            Ok(())
        }
    }

    fn input_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, true),
            Field::new("sequence", DataType::Int64, false),
            Field::new("include", DataType::Boolean, true),
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]))
    }

    // Exercise the production protobuf decoder and constructor, with generic
    // physical expressions rather than application SQL or replacement logic.
    fn native_config() -> (UpdatingAggregateOperator, Arc<Registry>) {
        native_config_with_unordered(false)
    }

    fn native_config_with_unordered(
        include_unordered: bool,
    ) -> (UpdatingAggregateOperator, Arc<Registry>) {
        let schema = input_schema();
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let sequence: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sequence", 1));
        let include: Arc<dyn PhysicalExpr> = Arc::new(Column::new("include", 2));
        let timestamp: Arc<dyn PhysicalExpr> = Arc::new(Column::new(TIMESTAMP_FIELD, 3));
        let mut registry = Registry::default();
        for function in [
            first_value_udaf(),
            last_value_udaf(),
            count_udaf(),
            max_udaf(),
        ] {
            registry.register_udaf(function).unwrap();
        }
        let mut expressions = vec![];
        let mut filters = vec![];
        for (name, function, descending, filtered) in [
            ("first_asc", first_value_udaf(), false, false),
            ("last_asc", last_value_udaf(), false, false),
            ("first_desc", first_value_udaf(), true, false),
            ("last_desc", last_value_udaf(), true, false),
            ("selected_last", last_value_udaf(), false, true),
        ] {
            let mut builder = AggregateExprBuilder::new(function, vec![value.clone()])
                .schema(schema.clone())
                .alias(name)
                .order_by(LexOrdering::new(vec![PhysicalSortExpr::new(
                    sequence.clone(),
                    SortOptions {
                        descending,
                        nulls_first: false,
                    },
                )]));
            if name == "first_asc" {
                builder = builder.ignore_nulls();
            }
            expressions.push(Arc::new(builder.build().unwrap()));
            filters.push(filtered.then(|| include.clone()));
        }
        let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        for (name, filtered) in [("all_count", false), ("selected_count", true)] {
            expressions.push(Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![one.clone()])
                    .schema(schema.clone())
                    .alias(name)
                    .build()
                    .unwrap(),
            ));
            filters.push(filtered.then(|| include.clone()));
        }
        expressions.push(Arc::new(
            AggregateExprBuilder::new(max_udaf(), vec![value.clone()])
                .schema(schema.clone())
                .alias("max_value")
                .build()
                .unwrap(),
        ));
        filters.push(None);
        expressions.push(Arc::new(
            AggregateExprBuilder::new(max_udaf(), vec![timestamp])
                .schema(schema.clone())
                .alias(TIMESTAMP_FIELD)
                .build()
                .unwrap(),
        ));
        filters.push(None);
        if include_unordered {
            for (name, function, argument, filtered) in [
                ("arrival_first", first_value_udaf(), sequence.clone(), false),
                ("arrival_last", last_value_udaf(), sequence.clone(), false),
                (
                    "selected_arrival_last",
                    last_value_udaf(),
                    value.clone(),
                    true,
                ),
                (
                    "arrival_first_value",
                    first_value_udaf(),
                    value.clone(),
                    false,
                ),
            ] {
                expressions.push(Arc::new(
                    AggregateExprBuilder::new(function, vec![argument])
                        .schema(schema.clone())
                        .alias(name)
                        .build()
                        .unwrap(),
                ));
                filters.push(filtered.then(|| include.clone()));
            }
        }
        let codec = DefaultPhysicalExtensionCodec {};
        let aggregate = AggregateExecNode {
            aggr_expr: expressions
                .iter()
                .map(|expr| serialize_physical_aggr_expr(expr.clone(), &codec).unwrap())
                .collect(),
            aggr_expr_name: expressions
                .iter()
                .map(|expr| expr.name().to_string())
                .collect(),
            filter_expr: filters
                .iter()
                .map(|filter| MaybeFilter {
                    expr: filter
                        .as_ref()
                        .map(|expr| serialize_physical_expr(expr, &codec).unwrap()),
                })
                .collect(),
            ..Default::default()
        };
        let mut final_fields: Vec<_> = expressions.iter().map(|expr| expr.field()).collect();
        final_fields.push(Arc::new(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        )));
        let metadata: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Null));
        (
            UpdatingAggregateOperator {
                name: "native-aggregate-correctness".into(),
                input_schema: Some(ArroyoSchema::from_schema_unkeyed(schema).unwrap().into()),
                final_schema: Some(
                    ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(final_fields)))
                        .unwrap()
                        .into(),
                ),
                aggregate_exec: PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(aggregate))),
                }
                .encode_to_vec(),
                metadata_expr: serialize_physical_expr(&metadata, &codec)
                    .unwrap()
                    .encode_to_vec(),
                flush_interval_micros: 1_000_000,
                ttl_micros: 3_600_000_000,
                retain_indefinitely: None,
                collection_output_limits: Default::default(),
                event_time_expiry: None,
                calendar_aggregates: Vec::new(),
            },
            Arc::new(registry),
        )
    }

    fn operator() -> IncrementalAggregatingFunc {
        let (config, registry) = native_config();
        IncrementalAggregatingConstructor::build(config, registry).unwrap()
    }

    fn native_operator() -> IncrementalAggregatingFunc {
        let (mut config, registry) = native_config();
        // Retraction fixtures declare a changelog input. An undeclared metadata
        // column must never silently select the indexed recovery format.
        let mut schema = SchemaBuilder::from(input_schema().as_ref().clone());
        schema.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        config.input_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(schema.finish()))
                .unwrap()
                .into(),
        );
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.retain_indefinitely = true;
        assert!(!operator.native_append_only);
        operator
    }

    fn native_append_only_operator() -> IncrementalAggregatingFunc {
        let (config, registry) = native_config();
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.retain_indefinitely = true;
        assert!(operator.native_append_only);
        operator
    }

    fn native_array_operator(
        distinct: bool,
        ignore_nulls: bool,
        filtered: bool,
    ) -> IncrementalAggregatingFunc {
        native_array_operator_with_limit(distinct, ignore_nulls, filtered, None).unwrap()
    }

    fn native_array_operator_with_limit(
        distinct: bool,
        ignore_nulls: bool,
        filtered: bool,
        limit: Option<u64>,
    ) -> Result<IncrementalAggregatingFunc> {
        let schema = input_schema();
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let sequence: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sequence", 1));
        let include: Arc<dyn PhysicalExpr> = Arc::new(Column::new("include", 2));
        let timestamp: Arc<dyn PhysicalExpr> = Arc::new(Column::new(TIMESTAMP_FIELD, 3));
        let mut registry = Registry::default();
        registry.register_udaf(array_agg_udaf()).unwrap();
        registry.register_udaf(max_udaf()).unwrap();
        let mut builder = AggregateExprBuilder::new(array_agg_udaf(), vec![value])
            .schema(schema.clone())
            .alias("items");
        if distinct {
            builder = builder.distinct();
        } else {
            builder = builder.order_by(LexOrdering::new(vec![PhysicalSortExpr::new(
                sequence,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )]));
        }
        if ignore_nulls {
            builder = builder.ignore_nulls();
        }
        let expressions = [
            Arc::new(builder.build().unwrap()),
            Arc::new(
                AggregateExprBuilder::new(max_udaf(), vec![timestamp])
                    .schema(schema.clone())
                    .alias(TIMESTAMP_FIELD)
                    .build()
                    .unwrap(),
            ),
        ];
        let codec = DefaultPhysicalExtensionCodec {};
        let aggregate = AggregateExecNode {
            aggr_expr: expressions
                .iter()
                .map(|expr| serialize_physical_aggr_expr(expr.clone(), &codec).unwrap())
                .collect(),
            aggr_expr_name: expressions
                .iter()
                .map(|expr| expr.name().to_string())
                .collect(),
            filter_expr: vec![
                MaybeFilter {
                    expr: filtered.then(|| serialize_physical_expr(&include, &codec).unwrap()),
                },
                MaybeFilter { expr: None },
            ],
            ..Default::default()
        };
        let mut fields: Vec<_> = expressions.iter().map(|expr| expr.field()).collect();
        fields.push(Arc::new(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        )));
        let mut input_fields = SchemaBuilder::from(schema.as_ref().clone());
        input_fields.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        let metadata: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Null));
        let config = UpdatingAggregateOperator {
            name: "native-array-aggregate".into(),
            input_schema: Some(
                ArroyoSchema::from_schema_unkeyed(Arc::new(input_fields.finish()))
                    .unwrap()
                    .into(),
            ),
            final_schema: Some(
                ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields)))
                    .unwrap()
                    .into(),
            ),
            aggregate_exec: PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(aggregate))),
            }
            .encode_to_vec(),
            metadata_expr: serialize_physical_expr(&metadata, &codec)
                .unwrap()
                .encode_to_vec(),
            flush_interval_micros: 1_000_000,
            ttl_micros: 3_600_000_000,
            retain_indefinitely: Some(true),
            collection_output_limits: limit.map(|limit| (0, limit)).into_iter().collect(),
            event_time_expiry: None,
            calendar_aggregates: Vec::new(),
        };
        IncrementalAggregatingConstructor::build_with_native_config(
            config,
            Arc::new(registry),
            Some(native_test_config()),
        )
    }

    fn native_calendar_operator() -> IncrementalAggregatingFunc {
        use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;
        let (mut config, registry) = native_config();
        let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else {
            panic!("expected aggregate");
        };
        aggregate.aggr_expr[7] = aggregate.aggr_expr[6].clone();
        aggregate.aggr_expr_name[7] = "selected_week".into();
        aggregate.filter_expr[7] = aggregate.filter_expr[6].clone();
        let static_filter = aggregate.filter_expr[6]
            .expr
            .as_ref()
            .map(|expression| expression.encode_to_vec());
        config.aggregate_exec = plan.encode_to_vec();
        let schema: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
        let mut fields = schema.schema.fields().to_vec();
        fields[7] = Arc::new(fields[6].as_ref().clone().with_name("selected_week"));
        config.final_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields)))
                .unwrap()
                .into(),
        );
        let codec = DefaultPhysicalExtensionCodec {};
        let date: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Date32(Some(200))));
        let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        for (index, horizon) in [(6, 1), (7, 7)] {
            config
                .calendar_aggregates
                .push(CalendarAggregateDescriptor {
                    aggregate_index: index,
                    horizon_days: horizon,
                    argument: serialize_physical_expr(&one, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    static_filter: static_filter.clone(),
                    contribution_date: serialize_physical_expr(&date, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    reference_date: serialize_physical_expr(&date, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    context_id: "generic-test-clock".into(),
                });
        }
        config.retain_indefinitely = Some(true);
        let mut input_fields = SchemaBuilder::from(input_schema().as_ref().clone());
        input_fields.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        config.input_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(input_fields.finish()))
                .unwrap()
                .into(),
        );
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        operator
    }

    async fn calendar_test_event(
        operator: &mut IncrementalAggregatingFunc,
        day: i32,
        reference: i32,
        include: Option<bool>,
        retract: bool,
        id: &[u8],
    ) {
        let input = batch(&[Some("value")], &[1], &[include]);
        for calendar in &mut operator.calendars {
            calendar.test_dates(day, reference);
        }
        let calendar_inputs = operator.calendar_inputs(&input).unwrap();
        let inputs = operator.compute_inputs(&input).unwrap();
        let store = operator.native_store.as_ref().unwrap();
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_calendar_event(
                &mut scope,
                NativeCalendarEvent {
                    group_key: &GLOBAL_KEY,
                    inputs: &inputs,
                    row: 0,
                    retract,
                    row_id: Some(id),
                    calendar_inputs: &calendar_inputs,
                    result_timestamp_nanos: None,
                },
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
    }

    async fn calendar_test_values(operator: &IncrementalAggregatingFunc) -> Vec<ScalarValue> {
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let bytes = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = decode_group(
            &bytes,
            &operator.native_state_types(),
            &operator.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut state = operator.native_accumulators(Some(&group)).unwrap();
        [5, 6, 7]
            .into_iter()
            .map(|index| state[index].evaluate().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn calendar_due_many_keys_use_single_entry_pages_and_cleanup_deleted_owner() {
        let mut operator = native_calendar_operator();
        let mut limits = native_test_store().limits();
        limits.page_entries = 1;
        operator.native_store = Some(native_test_store_with_limits(limits));
        let input = batch(&[Some("value")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let dates = operator.calendar_inputs(&input).unwrap();
        let store = operator.native_store.as_ref().unwrap();
        for index in 0_u8..24 {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_calendar_event(
                    &mut scope,
                    NativeCalendarEvent {
                        group_key: &[index],
                        inputs: &inputs,
                        row: 0,
                        retract: false,
                        row_id: Some(&[index; 16]),
                        calendar_inputs: &dates,
                        result_timestamp_nanos: None,
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        // Retiring one owner uses the existing generation cleanup, including
        // its reverse U pointer and global H deadline entry.
        {
            let mut scope = store.begin().await.unwrap();
            let bytes = scope
                .get(&native_group_key(b'G', &[0]).unwrap())
                .await
                .unwrap()
                .unwrap();
            let group = decode_group(
                &bytes,
                &operator.native_state_types(),
                &operator.native_output_types(),
                scope.limits().value_bytes,
            )
            .unwrap();
            scope
                .delete(&native_group_key(b'G', &[0]).unwrap())
                .unwrap();
            scope
                .put(&native_cleanup_key(&[0], group.generation).unwrap(), b"M")
                .unwrap();
            scope.commit().await.unwrap();
        }
        while operator.cleanup_native().await.unwrap() {}
        {
            let scope = store.begin().await.unwrap();
            assert!(
                scope
                    .get(
                        &calendar_native::calendar_due_key(201 * 86_400_000_000_000, &[0]).unwrap()
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let mut processed = 0;
        while operator
            .advance_calendar_watermark(201 * 86_400_000_000_000)
            .await
            .unwrap()
        {
            processed += 1;
            assert!(processed <= 23);
        }
        assert_eq!(processed, 23);
        let scope = store.begin().await.unwrap();
        for index in 1_u8..24 {
            let bytes = scope
                .get(&native_group_key(b'G', &[index]).unwrap())
                .await
                .unwrap()
                .unwrap();
            let group = decode_group(
                &bytes,
                &operator.native_state_types(),
                &operator.native_output_types(),
                scope.limits().value_bytes,
            )
            .unwrap();
            let mut accumulators = operator.native_accumulators(Some(&group)).unwrap();
            assert_eq!(
                accumulators[5].evaluate().unwrap(),
                ScalarValue::Int64(Some(1))
            );
            assert_eq!(
                accumulators[6].evaluate().unwrap(),
                ScalarValue::Int64(Some(0))
            );
            assert_eq!(
                accumulators[7].evaluate().unwrap(),
                ScalarValue::Int64(Some(1))
            );
        }
    }

    #[tokio::test]
    async fn calendar_watermark_recalculates_quiet_horizons_and_preserves_lifetime() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
        let day = 86_400_000_000_000_i64;
        assert!(
            !operator
                .advance_calendar_watermark(201 * day - 1)
                .await
                .unwrap()
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        assert!(
            operator
                .advance_calendar_watermark(201 * day)
                .await
                .unwrap()
        );
        assert!(
            !operator
                .advance_calendar_watermark(201 * day)
                .await
                .unwrap()
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(1))
            ]
        );
        assert!(
            operator
                .advance_calendar_watermark(207 * day)
                .await
                .unwrap()
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(0))
            ]
        );
    }

    #[tokio::test]
    async fn calendar_due_cancellation_and_stale_generation_do_not_lose_pending_state() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
        let day = 86_400_000_000_000_i64;
        let store = operator.native_store.as_ref().unwrap();
        {
            let mut scope = store.begin().await.unwrap();
            operator
                .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 201)
                .await
                .unwrap();
            // Cancellation drops the uncommitted owner mutation and its H/U changes.
        }
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        {
            let mut scope = store.begin().await.unwrap();
            scope
                .put(
                    &calendar_native::calendar_due_key(199 * day, &GLOBAL_KEY).unwrap(),
                    &999_u64.to_be_bytes(),
                )
                .unwrap();
            scope.commit().await.unwrap();
        }
        assert!(
            operator
                .advance_calendar_watermark(201 * day)
                .await
                .unwrap()
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        assert!(
            operator
                .advance_calendar_watermark(201 * day)
                .await
                .unwrap()
        );
        assert!(
            !operator
                .advance_calendar_watermark(201 * day)
                .await
                .unwrap()
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(1))
            ]
        );
    }

    #[tokio::test]
    async fn calendar_idle_and_terminal_watermarks_leave_pending_calendar_date_unchanged() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
        let mut ctx = native_input_context(input_schema()).await;
        let mut collector = AggregateCollector::default();
        for watermark in [
            Watermark::Idle,
            Watermark::EventTime(arroyo_types::from_nanos(u64::MAX as u128)),
        ] {
            operator
                .handle_watermark(watermark, &mut ctx, &mut collector)
                .await
                .unwrap();
        }
        assert!(collector.batches.is_empty());
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.push(Arc::new(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        )));
        ctx.out_schema = Some(Arc::new(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields))).unwrap(),
        ));
        let progress = Watermark::EventTime(
            SystemTime::UNIX_EPOCH + Duration::from_nanos(201 * 86_400_000_000_000),
        );
        operator
            .handle_watermark(progress, &mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(
            collector.batches.len(),
            2,
            "refresh precedes quiet calendar replacement"
        );
        let (retracts, _) = native_changelog_columns(&collector.batches[1]).unwrap();
        assert_eq!(retracts.len(), 2);
        assert!(retracts.value(0) && !retracts.value(1));
        assert_eq!(
            collector.batches[1]
                .column(6)
                .as_primitive::<arrow_array::types::Int64Type>()
                .values(),
            &[1, 0]
        );
        let mut repeated = AggregateCollector::default();
        operator
            .handle_watermark(progress, &mut ctx, &mut repeated)
            .await
            .unwrap();
        assert!(repeated.batches.is_empty());
    }

    #[tokio::test]
    async fn calendar_pre_epoch_finite_watermark_recalculates_without_unsigned_clock() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, -2, -2, Some(true), false, &[1; 16]).await;
        let mut ctx = native_input_context(input_schema()).await;
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.push(Arc::new(Field::new(UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()), false)));
        ctx.out_schema = Some(Arc::new(ArroyoSchema::from_schema_unkeyed(
            Arc::new(Schema::new(fields))).unwrap()));
        let mut collector = AggregateCollector::default();
        operator.handle_watermark(Watermark::EventTime(
            arroyo_types::event_time::from_signed_nanos(-86_400_000_000_000).unwrap()),
            &mut ctx, &mut collector).await.unwrap();
        assert_eq!(calendar_test_values(&operator).await, vec![
            ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(1)),
        ]);
        assert_eq!(collector.batches.len(), 2);
    }

    #[tokio::test]
    async fn calendar_due_checkpoint_restores_pending_expiry_on_both_backends() {
        use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root = std::env::temp_dir().join(format!("calendar-due-{}", uuid::Uuid::new_v4()));
            let remote = root.join("checkpoint");
            std::fs::create_dir_all(&remote).unwrap();
            // The production exporter requests pages up to 1 MiB; Rocks
            // admits their copies/cursors independently of aggregate pages.
            let resources = WorkerStateResources::new(ResourceConfig {
                max_open_databases: 2,
                scan_page_bytes: 16 * 1024 * 1024,
                queued_write_bytes: 16 * 1024 * 1024,
                decoded_value_bytes: 32 * 1024 * 1024,
                ..native_test_store().resources().config().clone()
            })
            .unwrap();
            let mut native = Vec::new();
            let mut backends: Vec<Arc<dyn LiveStateBackend>> = Vec::new();
            for generation in [0, 1] {
                if rocks {
                    let backend = Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.join("live"),
                                job_id: "calendar-due".into(),
                                operator_id: "aggregate".into(),
                                subtask: 0,
                                generation,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    );
                    backends.push(backend.clone());
                    native.push(backend);
                } else {
                    backends.push(Arc::new(
                        MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap(),
                    ));
                }
            }
            let ownership = Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            };
            let mut manager =
                LiveTableManager::new(backends[0].clone(), ownership.clone()).unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let namespace = table.namespace().clone();
            let limits = native_test_store().limits();
            let store =
                AggregateStore::new(backends[0].clone(), table, resources.clone(), limits).unwrap();
            let mut operator = native_calendar_operator();
            operator.native_store = Some(store);
            calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
            let snapshot = backends[0].snapshot().await.unwrap();
            let storage =
                arroyo_state::get_storage_provider(&arroyo_state::StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{}", remote.display())),
                })
                .await
                .unwrap();
            let config = DiskKeyedTableConfig {
                table_name: NATIVE_AGGREGATE_TABLE.into(),
                encoding_version: 1,
                schema_identity: operator.native_schema_identity.clone(),
            };
            let receipt = checkpoint::export(
                &snapshot,
                &namespace,
                &config,
                &storage,
                "checkpoint-1/aggregate",
                1,
                0,
                0,
            )
            .await
            .unwrap();
            checkpoint::restore(
                backends[1].as_ref(),
                &namespace,
                &config,
                &receipt,
                &storage,
            )
            .await
            .unwrap();
            let mut restored_manager =
                LiveTableManager::new(backends[1].clone(), ownership).unwrap();
            let restored_table = restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let restored_store = AggregateStore::new(
                backends[1].clone(),
                restored_table,
                resources.clone(),
                limits,
            )
            .unwrap();
            let mut fresh = native_calendar_operator();
            fresh.native_store = Some(restored_store);
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![ScalarValue::Int64(Some(1)); 3]
            );
            assert!(
                !fresh
                    .advance_calendar_watermark(201 * 86_400_000_000_000 - 1)
                    .await
                    .unwrap()
            );
            assert!(
                fresh
                    .advance_calendar_watermark(201 * 86_400_000_000_000)
                    .await
                    .unwrap()
            );
            assert!(
                !fresh
                    .advance_calendar_watermark(201 * 86_400_000_000_000)
                    .await
                    .unwrap()
            );
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1))
                ]
            );
            drop(snapshot);
            drop(operator);
            drop(fresh);
            drop(manager);
            drop(restored_manager);
            drop(backends);
            for backend in native {
                Arc::try_unwrap(backend)
                    .unwrap_or_else(|_| panic!("retained Rocks backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn calendar_shared_day_buckets_use_raw_reference_and_keep_lifetime() {
        let mut operator = native_calendar_operator();
        assert_eq!(
            operator.calendars[0].storage_index,
            operator.calendars[1].storage_index
        );
        for (ordinal, day) in [194, 193, 200, 201].into_iter().enumerate() {
            calendar_test_event(
                &mut operator,
                day,
                200,
                Some(true),
                false,
                &[ordinal as u8; 16],
            )
            .await;
        }
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(4)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2))
            ]
        );
        // A static false row advances D without contributing to gated horizons.
        calendar_test_event(&mut operator, 202, 201, Some(false), false, &[9; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(5)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2))
            ]
        );
        calendar_test_event(&mut operator, 202, 200, None, false, &[10; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(6)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2))
            ]
        );
        calendar_test_event(&mut operator, 300, 300, Some(false), false, &[11; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(7)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(0))
            ]
        );
        // Callback uses real progress, leaves lifetime intact and persists due work.
        let mut scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let next = operator
            .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 201)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next, 202 * 86_400_000_000_000);
        scope.commit().await.unwrap();
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(7)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2))
            ]
        );
    }

    #[tokio::test]
    async fn calendar_correction_uses_persisted_day_gate_and_original_value() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
        // Envelope date/gate differ; signed removal still uses original metadata.
        calendar_test_event(&mut operator, 300, 300, Some(false), true, &[1; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(0))
            ]
        );
        calendar_test_event(&mut operator, 201, 201, Some(true), false, &[1; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(1))
            ]
        );
    }

    #[tokio::test]
    async fn calendar_nullable_count_sum_corrections_preserve_recent_and_lifetime() {
        use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;
        use datafusion::functions_aggregate::sum::sum_udaf;

        let mut fields = input_schema().fields().to_vec();
        fields[1] = Arc::new(fields[1].as_ref().clone().with_nullable(true));
        let schema = Arc::new(Schema::new(fields));
        let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        let amount: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sequence", 1));
        let timestamp: Arc<dyn PhysicalExpr> = Arc::new(Column::new(TIMESTAMP_FIELD, 3));
        let codec = DefaultPhysicalExtensionCodec {};
        let expressions: Vec<_> = [
            ("lifetime_rows", count_udaf(), one.clone()),
            ("lifetime_nonnull", count_udaf(), amount.clone()),
            ("lifetime_sum", sum_udaf(), amount.clone()),
            ("recent_rows", count_udaf(), one.clone()),
            ("recent_nonnull", count_udaf(), amount.clone()),
            ("recent_sum", sum_udaf(), amount.clone()),
            (TIMESTAMP_FIELD, max_udaf(), timestamp),
        ]
        .into_iter()
        .map(|(name, function, argument)| {
            Arc::new(
                AggregateExprBuilder::new(function, vec![argument])
                    .schema(schema.clone())
                    .alias(name)
                    .build()
                    .unwrap(),
            )
        })
        .collect();
        let aggregate = AggregateExecNode {
            aggr_expr: expressions
                .iter()
                .map(|expr| serialize_physical_aggr_expr(expr.clone(), &codec).unwrap())
                .collect(),
            aggr_expr_name: expressions
                .iter()
                .map(|expr| expr.name().to_string())
                .collect(),
            filter_expr: vec![MaybeFilter { expr: None }; expressions.len()],
            ..Default::default()
        };
        let (mut config, _) = native_config();
        config.aggregate_exec = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(aggregate))),
        }
        .encode_to_vec();
        let metadata = Arc::new(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        let mut input_fields = schema.fields().to_vec();
        input_fields.push(metadata.clone());
        config.input_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(input_fields)))
                .unwrap()
                .into(),
        );
        let mut output_fields: Vec<_> = expressions.iter().map(|expr| expr.field()).collect();
        output_fields.push(metadata);
        config.final_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(output_fields)))
                .unwrap()
                .into(),
        );
        config.retain_indefinitely = Some(true);
        let date: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Date32(Some(200))));
        for (index, argument) in [(3, one), (4, amount.clone()), (5, amount)] {
            config
                .calendar_aggregates
                .push(CalendarAggregateDescriptor {
                    aggregate_index: index,
                    horizon_days: 7,
                    argument: serialize_physical_expr(&argument, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    static_filter: None,
                    contribution_date: serialize_physical_expr(&date, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    reference_date: serialize_physical_expr(&date, &codec)
                        .unwrap()
                        .encode_to_vec(),
                    context_id: "nullable-test-clock".into(),
                });
        }
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            Arc::new(arroyo_planner::physical::new_registry()),
            Some(native_test_config()),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        // Retractions deliberately carry a different amount. The persisted NULL
        // flag/value must remove the original contribution, never the envelope.
        for (retract, value, expected) in [
            (false, None, (1, 0, None)),
            (true, Some(99), (0, 0, None)),
            (false, Some(7), (1, 1, Some(7))),
            (true, None, (0, 0, None)),
            (false, None, (1, 0, None)),
            (true, Some(99), (0, 0, None)),
        ] {
            let input = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some("row")])),
                    Arc::new(Int64Array::from(vec![value])),
                    Arc::new(BooleanArray::from(vec![Some(true)])),
                    Arc::new(TimestampNanosecondArray::from(vec![1])),
                ],
            )
            .unwrap();
            let inputs = operator.compute_inputs(&input).unwrap();
            let dates = operator.calendar_inputs(&input).unwrap();
            let store = operator.native_store.as_ref().unwrap();
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_calendar_event(
                    &mut scope,
                    NativeCalendarEvent {
                        group_key: &GLOBAL_KEY,
                        inputs: &inputs,
                        row: 0,
                        retract,
                        row_id: Some(&[1; 16]),
                        calendar_inputs: &dates,
                        result_timestamp_nanos: None,
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            let scope = store.begin().await.unwrap();
            let bytes = scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .unwrap();
            let group = decode_group(
                &bytes,
                &operator.native_state_types(),
                &operator.native_output_types(),
                scope.limits().value_bytes,
            )
            .unwrap();
            let mut state = operator.native_accumulators(Some(&group)).unwrap();
            let actual: Vec<_> = state[..6]
                .iter_mut()
                .map(|state| state.evaluate().unwrap())
                .collect();
            let (rows, nonnull, sum) = expected;
            let expected = vec![
                ScalarValue::Int64(Some(rows)),
                ScalarValue::Int64(Some(nonnull)),
                ScalarValue::Int64(sum),
            ];
            assert_eq!(actual, [expected.clone(), expected].concat());
            drop(scope);
            // Every event scope and decoded evaluation has released its budget.
            let reservation = store
                .resources()
                .try_decoded_value(store.resources().config().decoded_value_bytes)
                .unwrap();
            drop(reservation);
        }
    }

    #[test]
    fn calendar_descriptor_rejects_argument_and_filter_mismatch() {
        use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;
        let operator = native_calendar_operator();
        let codec = DefaultPhysicalExtensionCodec {};
        let date: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Date32(Some(200))));
        let aggregate = &operator.aggregates[6];
        let mut descriptor = CalendarAggregateDescriptor {
            aggregate_index: 6,
            horizon_days: 7,
            argument: serialize_physical_expr(&aggregate.input_exprs[0], &codec)
                .unwrap()
                .encode_to_vec(),
            static_filter: aggregate.filter.as_ref().map(|filter| {
                serialize_physical_expr(filter, &codec)
                    .unwrap()
                    .encode_to_vec()
            }),
            contribution_date: serialize_physical_expr(&date, &codec)
                .unwrap()
                .encode_to_vec(),
            reference_date: serialize_physical_expr(&date, &codec)
                .unwrap()
                .encode_to_vec(),
            context_id: "generic-test-clock".into(),
        };
        let registry = arroyo_planner::physical::new_registry();
        assert!(
            CalendarAggregate::decode(
                &[descriptor.clone()],
                &operator.aggregates,
                &input_schema(),
                &registry
            )
            .is_ok()
        );
        descriptor.argument = serialize_physical_expr(
            &(Arc::new(Literal::new(ScalarValue::Int64(Some(2)))) as Arc<dyn PhysicalExpr>),
            &codec,
        )
        .unwrap()
        .encode_to_vec();
        let error = CalendarAggregate::decode(
            &[descriptor.clone()],
            &operator.aggregates,
            &input_schema(),
            &registry,
        )
        .unwrap_err();
        assert!(error.to_string().contains("argument does not match"));
        descriptor.argument = serialize_physical_expr(&aggregate.input_exprs[0], &codec)
            .unwrap()
            .encode_to_vec();
        descriptor.static_filter = None;
        let error = CalendarAggregate::decode(
            &[descriptor],
            &operator.aggregates,
            &input_schema(),
            &registry,
        )
        .unwrap_err();
        assert!(error.to_string().contains("static filter does not match"));
    }

    #[tokio::test]
    async fn calendar_capacity_error_discards_contribution_and_releases_scope() {
        let mut operator = native_calendar_operator();
        operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
            key_bytes: 256,
            value_bytes: 256,
            page_bytes: 8192,
            page_entries: 4,
            write_bytes: 64 * 1024,
            write_operations: 64,
            overlay_bytes: 64 * 1024,
        }));
        let input = batch(&[Some("value")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let dates = operator.calendar_inputs(&input).unwrap();
        let store = operator.native_store.as_ref().unwrap();
        let mut scope = store.begin().await.unwrap();
        let error = operator
            .native_process_calendar_event(
                &mut scope,
                NativeCalendarEvent {
                    group_key: &GLOBAL_KEY,
                    inputs: &inputs,
                    row: 0,
                    retract: false,
                    row_id: Some(&[1; 16]),
                    calendar_inputs: &dates,
                    result_timestamp_nanos: None,
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds configured limit"));
        drop(scope);
        let decoded = store
            .resources()
            .try_decoded_value(store.resources().config().decoded_value_bytes)
            .unwrap();
        drop(decoded);
        let scope = store.begin().await.unwrap();
        for prefix in [b"G", b"J", b"B", b"H", b"Q", b"U", b"M", b"R"] {
            assert!(scope.first(prefix).await.unwrap().is_none());
        }
    }

    fn native_test_config() -> AggregateStateConfig {
        AggregateStateConfig {
            key_bytes: 256,
            value_bytes: 16 * 1024,
            page_bytes: 64 * 1024,
            page_entries: 4,
            write_bytes: 512 * 1024,
            write_operations: 64,
            overlay_bytes: 512 * 1024,
            max_pending_output_rows: 64,
            max_pending_output_bytes: 256 * 1024,
            max_resident_bytes: 8 * 1024 * 1024,
        }
    }

    fn native_test_store() -> AggregateStore {
        native_test_store_with_limits(AggregateStoreLimits {
            key_bytes: 256,
            value_bytes: 16 * 1024,
            page_bytes: 64 * 1024,
            page_entries: 4,
            write_bytes: 512 * 1024,
            write_operations: 64,
            overlay_bytes: 512 * 1024,
        })
    }

    fn native_test_store_with_limits(limits: AggregateStoreLimits) -> AggregateStore {
        native_test_store_with_write_budget(limits, 4 * 1024 * 1024)
    }

    fn native_test_store_with_write_budget(
        limits: AggregateStoreLimits,
        queued_write_bytes: usize,
    ) -> AggregateStore {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes,
            decoded_value_bytes: 4 * 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        AggregateStore::new(backend, table, resources, limits).unwrap()
    }

    async fn native_input_context(schema: Arc<Schema>) -> OperatorContext {
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control_tx,
            1,
            vec![Arc::new(ArroyoSchema::from_schema_unkeyed(schema).unwrap())],
            None,
            HashMap::new(),
        )
        .await
    }

    fn native_test_changelog(base: &RecordBatch, retracts: &[bool]) -> RecordBatch {
        assert_eq!(base.num_rows(), retracts.len());
        let ids = (0..base.num_rows())
            .map(|row| test_row_id(base, row).to_vec())
            .collect::<Vec<_>>();
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![
                Arc::new(BooleanArray::from(retracts.to_vec())),
                Arc::new(FixedSizeBinaryArray::try_from_iter(ids.into_iter()).unwrap()),
            ],
            None,
        );
        let mut fields = base.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            UPDATING_META_FIELD,
            metadata.data_type().clone(),
            false,
        )));
        let mut columns = base.columns().to_vec();
        columns.push(Arc::new(metadata));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[tokio::test]
    async fn native_indexed_append_chunk_does_not_acquire_a_snapshot() {
        for collection in [false, true] {
            let base = batch(
                &[Some("z"), Some("a"), None, Some("ignored")],
                &[4, 1, 3, 2],
                &[Some(true), Some(true), Some(true), Some(false)],
            );
            let input = native_test_changelog(&base, &[false; 4]);
            let mut operator = if collection {
                native_array_operator(false, false, true)
            } else {
                native_operator()
            };
            let limits = native_test_store().limits();
            let write_scope_bytes =
                AdmittedWriteBatch::reservation_bytes(limits.write_bytes, limits.write_operations)
                    .unwrap();
            operator.native_store = Some(native_test_store_with_write_budget(
                limits,
                3 * write_scope_bytes,
            ));
            operator.native_execution_resources = Some(Arc::new(
                ExecutionResources::new(arroyo_rpc::config::ExecutionResourceConfig {
                    memory_bytes: 1024 * 1024,
                    max_batch_bytes: 128 * 1024,
                })
                .unwrap(),
            ));
            let store = operator.native_store.as_ref().unwrap();
            let first = store.begin().await.unwrap();
            let second = store.begin().await.unwrap();
            let mut ctx = native_input_context(input.schema()).await;
            let mut processing = Box::pin(operator.process_native_batch(&input, &mut ctx));
            // Both real snapshot permits are held. A stable scope would be
            // pending here; point processing must finish without a scan.
            let std::task::Poll::Ready(result) = futures::poll!(&mut processing) else {
                panic!("indexed append acquired a snapshot permit");
            };
            result.unwrap();
            drop(processing);
            drop(first);
            drop(second);
            let scope = store.begin().await.unwrap();
            if collection {
                assert_eq!(
                    operator
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, 0)
                        .await
                        .unwrap(),
                    ScalarValue::List(ScalarValue::new_list(
                        &[
                            ScalarValue::Utf8(Some("a".into())),
                            ScalarValue::Utf8(None),
                            ScalarValue::Utf8(Some("z".into())),
                        ],
                        &DataType::Utf8,
                        true,
                    )),
                );
            } else {
                assert_eq!(
                    operator
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, 7)
                        .await
                        .unwrap(),
                    ScalarValue::Utf8(Some("z".into())),
                );
                assert_eq!(
                    operator
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                        .await
                        .unwrap(),
                    ScalarValue::TimestampNanosecond(Some(4), None),
                );
            }
        }
    }

    #[tokio::test]
    async fn native_point_append_handles_keyed_expiry_collections_filters_and_nulls() {
        for collection in [false, true] {
            for ignore_nulls in [false, true] {
                let mut operator = if collection {
                    native_array_operator(false, ignore_nulls, true)
                } else {
                    native_operator()
                };
                operator.retain_indefinitely = false;
                let mut fields = operator.schema_without_metadata.fields().to_vec();
                fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
                operator.schema_without_metadata = Arc::new(Schema::new(fields));
                let group = b"opaque-key";
                operator.native_store = Some(native_test_store());
                let store = operator.native_store.as_ref().unwrap();
                let old = batch(&[Some("old")], &[0], &[Some(true)]);
                let mut scope = store.begin_point().await.unwrap();
                operator
                    .native_process_event(
                        &mut scope,
                        group,
                        &operator.compute_inputs(&old).unwrap(),
                        0,
                        false,
                        Some(&test_row_id(&old, 0)),
                        None,
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                let mut scope = store.begin_point().await.unwrap();
                let key = native_group_key(b'G', group).unwrap();
                let mut previous = decode_group(
                    &scope.get(&key).await.unwrap().unwrap(),
                    &operator.native_state_types(),
                    &operator.native_output_types(),
                    scope.limits().value_bytes,
                )
                .unwrap();
                let ttl = i64::try_from(operator.ttl.as_nanos()).unwrap();
                scope
                    .delete(
                        &native_expiry_key(previous.last_update_nanos.saturating_add(ttl), group)
                            .unwrap(),
                    )
                    .unwrap();
                previous.last_update_nanos = 0;
                scope
                    .put(
                        &key,
                        &encode_group(&previous, scope.limits().value_bytes).unwrap(),
                    )
                    .unwrap();
                scope
                    .put(&native_expiry_key(ttl, group).unwrap(), &[1])
                    .unwrap();
                scope.commit().await.unwrap();
                let input = batch(
                    &[
                        Some("z"),
                        Some("a"),
                        None,
                        Some("ignored"),
                        Some("null-filter"),
                    ],
                    &[4, 1, 3, 2, 5],
                    &[Some(true), Some(true), Some(true), Some(false), None],
                );
                let inputs = operator.compute_inputs(&input).unwrap();
                // The actual point scope rejects first()/first_from(). This
                // exercises rollover and read-own-writes without scan escape.
                for start in (0..input.num_rows()).step_by(2) {
                    let mut scope = store.begin_point().await.unwrap();
                    for row in start..input.num_rows().min(start + 2) {
                        operator
                            .native_process_event(
                                &mut scope,
                                group,
                                &inputs,
                                row,
                                false,
                                Some(&test_row_id(&input, row)),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    scope.commit().await.unwrap();
                }
                let scope = store.begin().await.unwrap();
                let current = decode_group(
                    &scope.get(&key).await.unwrap().unwrap(),
                    &operator.native_state_types(),
                    &operator.native_output_types(),
                    scope.limits().value_bytes,
                )
                .unwrap();
                assert_eq!(current.generation, 1);
                assert_eq!(current.next_ordinal, 5);
                assert_eq!(
                    scope
                        .get(&native_live_rows_key(group).unwrap())
                        .await
                        .unwrap()
                        .unwrap(),
                    5_u64.to_be_bytes()
                );
                assert_eq!(
                    scope
                        .get(&native_cleanup_key(group, 0).unwrap())
                        .await
                        .unwrap(),
                    Some(b"M".to_vec())
                );
                assert!(
                    scope
                        .first(&native_member_prefix(group, 0, 0).unwrap())
                        .await
                        .unwrap()
                        .is_some()
                );
                if collection {
                    let value = operator
                        .native_fallback_value(&scope, group, 1, 0)
                        .await
                        .unwrap();
                    let mut wanted = vec![ScalarValue::Utf8(Some("a".into()))];
                    if !ignore_nulls {
                        wanted.push(ScalarValue::Utf8(None));
                    }
                    wanted.push(ScalarValue::Utf8(Some("z".into())));
                    assert_eq!(
                        value,
                        ScalarValue::List(ScalarValue::new_list(&wanted, &DataType::Utf8, true))
                    );
                } else {
                    assert_eq!(
                        operator
                            .native_fallback_value(&scope, group, 1, 4)
                            .await
                            .unwrap(),
                        ScalarValue::Utf8(Some("z".into()))
                    );
                    assert_eq!(
                        operator
                            .native_fallback_value(&scope, group, 1, 8)
                            .await
                            .unwrap(),
                        ScalarValue::TimestampNanosecond(Some(5), None)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn native_input_and_expressions_share_execution_budget_before_state_mutation() {
        let input = batch(&[Some("value")], &[1], &[Some(true)]);
        let source_bytes = input.get_array_memory_size();
        let resources = Arc::new(
            ExecutionResources::new(arroyo_rpc::config::ExecutionResourceConfig {
                memory_bytes: 128 * 1024,
                max_batch_bytes: 128 * 1024,
            })
            .unwrap(),
        );
        let mut operator = native_append_only_operator();
        operator.native_store = Some(native_test_store());
        operator.native_execution_resources = Some(resources.clone());
        let mut ctx = native_input_context(input.schema()).await;
        let mut occupied = MemoryConsumer::new("other concurrent operator")
            .register(&resources.runtime.memory_pool);
        occupied
            .try_grow(resources.limits.memory_bytes - source_bytes)
            .unwrap();
        let error = operator
            .process_native_batch(&input, &mut ctx)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<datafusion::common::DataFusionError>(),
            Some(datafusion::common::DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(
            resources.runtime.memory_pool.reserved(),
            resources.limits.memory_bytes - source_bytes
        );
        let store = operator.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let group_key = native_group_key(b'G', &GLOBAL_KEY).unwrap();
        assert!(scope.get(&group_key).await.unwrap().is_none());
        drop(scope);
        drop(occupied);
        operator
            .process_native_batch(&input, &mut ctx)
            .await
            .unwrap();
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        assert!(
            store
                .begin()
                .await
                .unwrap()
                .get(&group_key)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn native_input_cancellation_releases_execution_and_pending_state_scope() {
        let base = batch(&[Some("value")], &[1], &[Some(true)]);
        let input = native_test_changelog(&base, &[true]);
        let seed = native_test_changelog(&base, &[false]);
        let resources = Arc::new(
            ExecutionResources::new(arroyo_rpc::config::ExecutionResourceConfig {
                memory_bytes: 1024 * 1024,
                max_batch_bytes: 128 * 1024,
            })
            .unwrap(),
        );
        let mut operator = native_operator();
        let limits = AggregateStoreLimits {
            key_bytes: 256,
            value_bytes: 16 * 1024,
            page_bytes: 64 * 1024,
            page_entries: 4,
            write_bytes: 512 * 1024,
            write_operations: 64,
            overlay_bytes: 512 * 1024,
        };
        // Two scopes hold the snapshot permits; processing must admit its
        // third write scope before it can suspend on snapshot acquisition.
        let write_scope_bytes =
            AdmittedWriteBatch::reservation_bytes(limits.write_bytes, limits.write_operations)
                .unwrap();
        operator.native_store = Some(native_test_store_with_write_budget(
            limits,
            3 * write_scope_bytes,
        ));
        operator.native_execution_resources = Some(resources.clone());
        let store = operator.native_store.as_ref().unwrap();
        let mut ctx = native_input_context(input.schema()).await;
        // A real matching retraction still needs a stable scan. Seed its
        // member before exhausting permits; cancellation must leave it intact.
        operator
            .process_native_batch(&seed, &mut ctx)
            .await
            .unwrap();
        // Exhaust the real backend's snapshot permits so processing suspends
        // after admitting input/expressions and acquiring its state scope.
        let first = store.begin().await.unwrap();
        let second = store.begin().await.unwrap();
        let scope_bytes = store.limits().overlay_bytes + 3 * store.limits().value_bytes;
        let otherwise_free = store.resources().config().decoded_value_bytes - 2 * scope_bytes;
        assert!(store.resources().try_decoded_value(otherwise_free).is_ok());
        assert!(
            store
                .resources()
                .try_queued_write(write_scope_bytes)
                .is_ok()
        );
        let mut processing = Box::pin(operator.process_native_batch(&input, &mut ctx));
        assert!(futures::poll!(&mut processing).is_pending());
        assert!(resources.runtime.memory_pool.reserved() > input.get_array_memory_size());
        assert!(store.resources().try_decoded_value(otherwise_free).is_err());
        assert!(store.resources().try_queued_write(1).is_err());
        drop(processing);
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        assert!(store.resources().try_decoded_value(otherwise_free).is_ok());
        assert!(
            store
                .resources()
                .try_queued_write(write_scope_bytes)
                .is_ok()
        );
        drop(first);
        drop(second);
        operator
            .process_native_batch(&input, &mut ctx)
            .await
            .unwrap();
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        assert!(
            store
                .resources()
                .try_decoded_value(store.resources().config().decoded_value_bytes)
                .is_ok()
        );
        assert!(
            store
                .resources()
                .try_queued_write(3 * write_scope_bytes)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn native_slow_collector_holds_output_budget_until_completion_or_cancellation() {
        struct SlowCollector {
            started: Option<tokio::sync::oneshot::Sender<()>>,
            release: tokio::sync::oneshot::Receiver<()>,
        }

        #[async_trait::async_trait]
        impl Collector for SlowCollector {
            async fn collect(&mut self, output: RecordBatch) -> DataflowResult<()> {
                assert_eq!(output.num_rows(), 1);
                self.started.take().unwrap().send(()).unwrap();
                (&mut self.release).await.unwrap();
                drop(output);
                Ok(())
            }

            async fn broadcast_watermark(
                &mut self,
                _: arroyo_types::Watermark,
            ) -> DataflowResult<()> {
                Ok(())
            }
        }

        for cancel in [false, true] {
            let input = batch(&[Some("value")], &[1], &[Some(true)]);
            let resources = Arc::new(
                ExecutionResources::new(arroyo_rpc::config::ExecutionResourceConfig {
                    memory_bytes: 1024 * 1024,
                    max_batch_bytes: 128 * 1024,
                })
                .unwrap(),
            );
            let mut operator = native_append_only_operator();
            operator.native_store = Some(native_test_store());
            operator.native_execution_resources = Some(resources.clone());
            let metadata = StructArray::new(
                updating_meta_fields(),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    ScalarValue::FixedSizeBinary(16, None).to_array().unwrap(),
                ],
                None,
            );
            operator.metadata_expr =
                Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
            let mut ctx = native_input_context(input.schema()).await;
            ctx.out_schema = Some(Arc::new(
                native_config().0.final_schema.unwrap().try_into().unwrap(),
            ));
            operator
                .process_native_batch(&input, &mut ctx)
                .await
                .unwrap();
            assert_eq!(resources.runtime.memory_pool.reserved(), 0);
            let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let mut collector = SlowCollector {
                started: Some(started_tx),
                release: release_rx,
            };
            let mut flushing = Box::pin(operator.drain_native_dirty(&mut ctx, &mut collector));
            assert!(futures::poll!(&mut flushing).is_pending());
            started_rx.try_recv().unwrap();
            let state_resources = operator.native_store.as_ref().unwrap().resources();
            let output_bytes = 3 * native_test_config().max_pending_output_bytes;
            let otherwise_free = state_resources.config().decoded_value_bytes - output_bytes;
            assert!(state_resources.try_decoded_value(otherwise_free).is_ok());
            assert!(
                state_resources
                    .try_decoded_value(otherwise_free + 1)
                    .is_err()
            );
            if cancel {
                drop(flushing);
                drop(collector);
                assert!(release_tx.send(()).is_err());
            } else {
                release_tx.send(()).unwrap();
                flushing.await.unwrap();
            }
            assert!(
                state_resources
                    .try_decoded_value(state_resources.config().decoded_value_bytes)
                    .is_ok()
            );
            assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        }
    }

    async fn array_members(
        store: &AggregateStore,
        operator: &IncrementalAggregatingFunc,
    ) -> Vec<ScalarValue> {
        let scope = store.begin().await.unwrap();
        let ScalarValue::List(list) = operator
            .native_fallback_value(&scope, &GLOBAL_KEY, 0, 0)
            .await
            .unwrap()
        else {
            panic!("ARRAY_AGG did not produce a list")
        };
        let values = list.value(0);
        (0..values.len())
            .map(|index| ScalarValue::try_from_array(&values, index).unwrap())
            .collect()
    }

    #[test]
    fn native_bounded_array_refuses_distinct_plan_bounds() {
        assert!(native_array_operator_with_limit(true, false, false, Some(1)).is_err());
        for limit in [0, 1, 5] {
            let operator =
                native_array_operator_with_limit(false, false, false, Some(limit)).unwrap();
            assert_eq!(
                operator.aggregates[0].collection_output_limit,
                Some(limit as usize)
            );
        }
    }

    #[tokio::test]
    async fn native_bounded_array_retains_all_members_and_selects_prefix_after_retraction() {
        let mut operator = native_array_operator_with_limit(false, false, true, Some(1)).unwrap();
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        // Each write commits independently, so overlays remain bounded even for
        // a group whose complete output exceeds the ordinary collection limit.
        let input = batch(
            &[Some("z"), Some("a"), None, Some("ignored")],
            &[3, 1, 2, 0],
            &[Some(true), Some(true), Some(true), Some(false)],
        );
        let inputs = operator.compute_inputs(&input).unwrap();
        for row in 0..input.num_rows() {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&input, row)),
                    None,
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        assert_eq!(
            array_members(store, &operator).await,
            vec![ScalarValue::Utf8(Some("a".into()))]
        );
        let mut fresh = native_array_operator(false, false, true);
        fresh.native_store = operator.native_store.take();
        let store = fresh.native_store.as_ref().unwrap();
        fresh.aggregates[0].collection_output_limit = Some(1);
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                1,
                true,
                Some(&test_row_id(&input, 1)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        assert_eq!(
            array_members(store, &fresh).await,
            vec![ScalarValue::Utf8(None)]
        );
        fresh.aggregates[0].collection_output_limit = Some(5);
        assert_eq!(
            array_members(store, &fresh).await,
            vec![ScalarValue::Utf8(None), ScalarValue::Utf8(Some("z".into()))]
        );
        fresh.aggregates[0].collection_output_limit = Some(0);
        assert!(array_members(store, &fresh).await.is_empty());
        let totals = CollectionStats::decode(
            store
                .begin()
                .await
                .unwrap()
                .get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(totals.members, 2);
        for row in [0, 2] {
            let mut scope = store.begin().await.unwrap();
            fresh
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    true,
                    Some(&test_row_id(&input, row)),
                    None,
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        let bounded_empty = fresh
            .native_fallback_value(&scope, &GLOBAL_KEY, 0, 0)
            .await
            .unwrap();
        fresh.aggregates[0].collection_output_limit = None;
        assert_eq!(
            bounded_empty,
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 0)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn native_bounded_array_checkpoint_restores_all_indexes_on_both_backends() {
        use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root = std::env::temp_dir().join(format!("bounded-array-{}", uuid::Uuid::new_v4()));
            let remote = root.join("checkpoint");
            std::fs::create_dir_all(&remote).unwrap();
            // The production exporter requests pages up to 1 MiB; Rocks
            // admits their copies/cursors independently of aggregate pages.
            let resources = WorkerStateResources::new(ResourceConfig {
                max_open_databases: 2,
                scan_page_bytes: 16 * 1024 * 1024,
                queued_write_bytes: 16 * 1024 * 1024,
                decoded_value_bytes: 32 * 1024 * 1024,
                ..native_test_store().resources().config().clone()
            })
            .unwrap();
            let mut native = Vec::new();
            let mut backends: Vec<Arc<dyn LiveStateBackend>> = Vec::new();
            for generation in [0, 1] {
                if rocks {
                    let backend = Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.join("live"),
                                job_id: "bounded-array".into(),
                                operator_id: "aggregate".into(),
                                subtask: 0,
                                generation,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    );
                    backends.push(backend.clone());
                    native.push(backend);
                } else {
                    backends.push(Arc::new(
                        MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap(),
                    ));
                }
            }
            let ownership = Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            };
            let mut manager =
                LiveTableManager::new(backends[0].clone(), ownership.clone()).unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let namespace = table.namespace().clone();
            let limits = native_test_store().limits();
            let store =
                AggregateStore::new(backends[0].clone(), table, resources.clone(), limits).unwrap();
            let mut operator = native_array_operator(false, false, false);
            operator.aggregates[0].collection_output_limit = Some(1);
            operator.native_store = Some(store);
            let store = operator.native_store.as_ref().unwrap();
            let input = batch(
                &[Some("winner"), Some("next"), None],
                &[1, 2, 3],
                &[Some(true); 3],
            );
            let inputs = operator.compute_inputs(&input).unwrap();
            for row in 0..input.num_rows() {
                let mut scope = store.begin_point().await.unwrap();
                operator
                    .native_process_event(
                        &mut scope,
                        &GLOBAL_KEY,
                        &inputs,
                        row,
                        false,
                        Some(&test_row_id(&input, row)),
                        None,
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
            }
            let snapshot = backends[0].snapshot().await.unwrap();
            let storage =
                arroyo_state::get_storage_provider(&arroyo_state::StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{}", remote.display())),
                })
                .await
                .unwrap();
            let config = DiskKeyedTableConfig {
                table_name: NATIVE_AGGREGATE_TABLE.into(),
                encoding_version: 1,
                schema_identity: operator.native_schema_identity.clone(),
            };
            let receipt = checkpoint::export(
                &snapshot,
                &namespace,
                &config,
                &storage,
                "checkpoint-1/aggregate",
                1,
                0,
                0,
            )
            .await
            .unwrap();
            checkpoint::restore(
                backends[1].as_ref(),
                &namespace,
                &config,
                &receipt,
                &storage,
            )
            .await
            .unwrap();
            let mut restored_manager =
                LiveTableManager::new(backends[1].clone(), ownership).unwrap();
            let restored_table = restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let restored_store = AggregateStore::new(
                backends[1].clone(),
                restored_table,
                resources.clone(),
                limits,
            )
            .unwrap();
            let mut fresh = native_array_operator(false, false, false);
            fresh.aggregates[0].collection_output_limit = Some(1);
            fresh.native_store = Some(restored_store);
            let restored_store = fresh.native_store.as_ref().unwrap();
            assert_eq!(
                array_members(restored_store, &fresh).await,
                vec![ScalarValue::Utf8(Some("winner".into()))]
            );
            let mut scope = restored_store.begin().await.unwrap();
            let totals = CollectionStats::decode(
                scope
                    .get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap())
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(totals.members, 3);
            fresh
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    0,
                    true,
                    Some(&test_row_id(&input, 0)),
                    None,
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                array_members(restored_store, &fresh).await,
                vec![ScalarValue::Utf8(Some("next".into()))]
            );
            drop(snapshot);
            drop(operator);
            drop(fresh);
            drop(manager);
            drop(restored_manager);
            drop(backends);
            for backend in native {
                Arc::try_unwrap(backend)
                    .unwrap_or_else(|_| panic!("retained Rocks backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn native_bounded_array_admits_high_cardinality_without_full_output() {
        let mut operator = native_array_operator_with_limit(false, false, false, Some(1)).unwrap();
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        for sequence in 0..1_000 {
            let input = batch(&[Some("member")], &[sequence], &[Some(true)]);
            let inputs = operator.compute_inputs(&input).unwrap();
            let mut scope = store.begin_point().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    0,
                    false,
                    Some(&test_row_id(&input, 0)),
                    None,
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        assert_eq!(
            array_members(store, &operator).await,
            vec![ScalarValue::Utf8(Some("member".into()))]
        );
        let scope = store.begin().await.unwrap();
        let totals = CollectionStats::decode(
            scope
                .get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(totals.members, 1_000);
        operator
            .native_collection_budget(&scope, std::iter::once((0, totals)))
            .unwrap();
        let mut ordinary = native_array_operator(false, false, false);
        ordinary.aggregates[0].collection_output_limit = None;
        assert!(
            ordinary
                .native_collection_budget(&scope, std::iter::once((0, totals)))
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_array_index_preserves_order_nulls_duplicates_filter_and_retractions() {
        let mut operator = native_array_operator(false, false, true);
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        let input = batch(
            &[Some("z"), Some("a"), None, Some("a"), Some("ignored")],
            &[4, 1, 3, 2, 5],
            &[Some(true), Some(true), Some(true), Some(true), Some(false)],
        );
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut scope = store.begin().await.unwrap();
        for row in 0..input.num_rows() {
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&input, row)),
                    None,
                )
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();
        assert_eq!(
            array_members(store, &operator).await,
            vec![
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(None),
                ScalarValue::Utf8(Some("z".into())),
            ]
        );
        // A fresh operator sees only the durable indexed state. A matching
        // retraction removes one occurrence, not every equal value.
        let fresh = native_array_operator(false, false, true);
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                1,
                true,
                Some(&test_row_id(&input, 1)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        assert_eq!(
            array_members(store, &fresh).await,
            vec![
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(None),
                ScalarValue::Utf8(Some("z".into())),
            ]
        );
        assert_eq!(
            CollectionStats::decode(
                store
                    .begin()
                    .await
                    .unwrap()
                    .get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap())
                    .await
                    .unwrap()
            )
            .unwrap()
            .members,
            3
        );
    }

    #[tokio::test]
    async fn keyed_native_group_tracks_last_retraction_independently_of_aggregate_values() {
        let store = native_test_store();
        let mut operator = native_operator();
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        operator.key_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let group_columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["same-group"]))];
        let group = operator
            .key_converter
            .convert_columns(&group_columns)
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec();
        let input = batch(&[Some("value")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();

        let mut scope = store.begin().await.unwrap();
        assert!(operator.native_group_is_live(&scope, &group).await.is_err());
        assert!(
            operator
                .native_process_event(
                    &mut scope,
                    &group,
                    &inputs,
                    0,
                    true,
                    Some(&test_row_id(&input, 0)),
                    None
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("retracts a missing keyed row")
        );
        operator
            .native_process_event(
                &mut scope,
                &group,
                &inputs,
                0,
                false,
                Some(&test_row_id(&input, 0)),
                None,
            )
            .await
            .unwrap();
        assert!(operator.native_group_is_live(&scope, &group).await.unwrap());
        scope.commit().await.unwrap();

        let id = ScalarValue::FixedSizeBinary(16, None).to_array().unwrap();
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![Arc::new(BooleanArray::from(vec![false])), id],
            None,
        );
        operator.metadata_expr = Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
        let mut output = SchemaBuilder::from(operator.schema_without_metadata.as_ref().clone());
        output.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        let output_schema = ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap();
        let (config, _) = native_config();
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into().unwrap();
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        let mut ctx = OperatorContext::new(
            Arc::new(arroyo_types::TaskInfo {
                job_id: "native-keyed-empty-group".into(),
                operator_idx: 0,
                operator_name: "UpdatingAggregate".into(),
                operator_id: "native-keyed-empty-group".into(),
                task_index: 0,
                parallelism: 1,
                key_range: 0..=u64::MAX,
                checkpoint_file_path_layout: Default::default(),
            }),
            None,
            control_tx,
            1,
            vec![Arc::new(input_schema)],
            Some(Arc::new(output_schema)),
            HashMap::new(),
        )
        .await;
        operator.native_store = Some(store);
        let mut collector = AggregateCollector::default();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(collector.batches.len(), 1);
        assert_eq!(collector.batches[0].num_rows(), 1);
        assert!(
            !IncrementalAggregatingFunc::get_retracts(&collector.batches[0])
                .unwrap()
                .value(0)
        );

        let store = operator.native_store.as_ref().unwrap();
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &group,
                &inputs,
                0,
                true,
                Some(&test_row_id(&input, 0)),
                None,
            )
            .await
            .unwrap();
        assert!(!operator.native_group_is_live(&scope, &group).await.unwrap());
        scope.commit().await.unwrap();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(collector.batches.len(), 2);
        assert_eq!(collector.batches[1].num_rows(), 1);
        assert!(
            IncrementalAggregatingFunc::get_retracts(&collector.batches[1])
                .unwrap()
                .value(0)
        );
        for _ in 0..8 {
            if !operator.cleanup_native().await.unwrap() {
                break;
            }
        }
        let reloaded = store.begin().await.unwrap();
        assert!(
            reloaded
                .get(&native_group_key(b'G', &group).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reloaded
                .get(&native_live_rows_key(&group).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(reloaded.first(b"C").await.unwrap().is_none());
        let unkeyed = native_operator();
        assert!(
            unkeyed
                .native_group_is_live(&reloaded, &GLOBAL_KEY)
                .await
                .unwrap()
        );
        drop(reloaded);
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &group,
                &inputs,
                0,
                false,
                Some(&test_row_id(&input, 0)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(collector.batches.len(), 3);
        assert!(
            !IncrementalAggregatingFunc::get_retracts(&collector.batches[2])
                .unwrap()
                .value(0)
        );
    }

    #[tokio::test]
    async fn native_array_distinct_and_ignore_nulls_keep_occurrence_counts() {
        let mut operator = native_array_operator(true, true, false);
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        let input = batch(
            &[Some("b"), None, Some("b"), Some("a")],
            &[1, 2, 3, 4],
            &[Some(true); 4],
        );
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut scope = store.begin().await.unwrap();
        for row in 0..input.num_rows() {
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&input, row)),
                    None,
                )
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();
        let mut values = array_members(store, &operator).await;
        values.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            values,
            vec![
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(Some("b".into()))
            ]
        );
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                true,
                Some(&test_row_id(&input, 0)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        assert_eq!(array_members(store, &operator).await.len(), 2);
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                2,
                true,
                Some(&test_row_id(&input, 2)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        assert_eq!(
            array_members(store, &operator).await,
            vec![ScalarValue::Utf8(Some("a".into()))]
        );
    }

    #[tokio::test]
    async fn native_array_rejects_large_member_before_writing_any_index() {
        let mut operator = native_array_operator(false, false, false);
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        let large = "x".repeat(8 * 1024);
        let input = batch(&[Some(&large)], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut scope = store.begin().await.unwrap();
        assert!(
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    0,
                    false,
                    Some(&test_row_id(&input, 0)),
                    None
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("native collection exceeds")
        );
        drop(scope);
        let scope = store.begin().await.unwrap();
        assert!(scope.first(b"M").await.unwrap().is_none());
        assert!(
            scope
                .get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_array_counts_zero_byte_members_and_accepts_typed_struct_rows() {
        let store = native_test_store();
        let operator = native_array_operator(false, false, false);
        let scope = store.begin().await.unwrap();
        assert!(
            operator
                .native_collection_budget(
                    &scope,
                    std::iter::once((
                        0,
                        CollectionStats {
                            members: 1_000,
                            encoded_args_bytes: 0,
                            scalar_bytes: 0,
                        }
                    )),
                )
                .is_err()
        );

        let fields = vec![Arc::new(Field::new("label", DataType::Utf8, true))].into();
        let value = Arc::new(
            StructArray::try_new(
                fields,
                vec![Arc::new(StringArray::from(vec![Some("typed"); 1_024]))],
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let value = value.slice(0, 1);
        let converter = RowConverter::new(vec![SortField::new(value.data_type().clone())]).unwrap();
        let rows = converter
            .convert_columns(std::slice::from_ref(&value))
            .unwrap();
        let stored_args = rows.row(0);
        let scalar_bytes =
            native_collection_scalar_bytes(&converter, stored_args.as_ref()).unwrap();
        assert!(scalar_bytes < 4_096);
        assert!(
            operator
                .native_collection_budget(
                    &scope,
                    std::iter::once((
                        0,
                        CollectionStats {
                            members: 12,
                            encoded_args_bytes: u64::try_from(stored_args.as_ref().len() * 12)
                                .unwrap(),
                            scalar_bytes: u64::try_from(scalar_bytes * 12).unwrap(),
                        }
                    )),
                )
                .is_ok()
        );
        let decoded = converter.convert_rows(rows.iter()).unwrap();
        assert_eq!(decoded[0].data_type(), value.data_type());
        assert_eq!(
            ScalarValue::try_from_array(&decoded[0], 0).unwrap(),
            ScalarValue::try_from_array(&value, 0).unwrap()
        );

        let null_parent = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new("label", DataType::Utf8, true))].into(),
                vec![Arc::new(StringArray::from(vec![Some("typed"); 1_024]))],
                Some(arrow::buffer::NullBuffer::new_null(1_024)),
            )
            .unwrap(),
        ) as ArrayRef;
        let null_parent = null_parent.slice(0, 1);
        let null_rows = converter
            .convert_columns(std::slice::from_ref(&null_parent))
            .unwrap();
        assert!(
            native_collection_scalar_bytes(&converter, null_rows.row(0).as_ref()).unwrap() < 4_096
        );
    }

    #[test]
    fn native_array_shape_preflight_admits_flat_struct_and_rejects_wide_or_nested_values() {
        let selected_fields = vec![
            Arc::new(Field::new("row_id", DataType::Int64, false)),
            Arc::new(Field::new("sort_pos", DataType::Int64, false)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ];
        let selected = DataType::Struct(selected_fields.into());
        assert!(native_collection_decode_bound(&[selected], 128).unwrap() < 3 * 32 * 1024);

        let wide_fields = (0..512)
            .map(|index| Arc::new(Field::new(format!("f{index}"), DataType::Null, true)))
            .collect::<Vec<_>>();
        let wide = DataType::Struct(wide_fields.into());
        assert!(native_collection_decode_bound(&[wide], 0).unwrap() > 3 * 32 * 1024);
        assert!(
            native_collection_decode_bound(&[DataType::FixedSizeBinary(128 * 1024)], 0).unwrap()
                > 3 * 32 * 1024
        );
        let null_parent_with_wide_child = DataType::Struct(
            vec![Arc::new(Field::new(
                "child",
                DataType::FixedSizeBinary(128 * 1024),
                true,
            ))]
            .into(),
        );
        assert!(
            native_collection_decode_bound(&[null_parent_with_wide_child], 0).unwrap()
                > 3 * 32 * 1024
        );
        assert!(
            native_collection_decode_bound(
                &[DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                )))],
                0
            )
            .unwrap_err()
            .to_string()
            .contains("unsupported nested type")
        );
    }

    #[tokio::test]
    async fn native_dirty_flush_emits_one_pair_with_one_group_write_budget() {
        let store = native_test_store_with_limits(AggregateStoreLimits {
            key_bytes: 256,
            value_bytes: 16 * 1024,
            page_bytes: 64 * 1024,
            page_entries: 4,
            write_bytes: 64 * 1024,
            write_operations: 2,
            overlay_bytes: 64 * 1024,
        });
        let mut operator = native_operator();
        let input = batch(&[Some("current")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let timestamp_values = inputs[8].selected_values(Some(0)).unwrap().unwrap();
        let (primary, secondary, args) = operator
            .native_member_keys(&GLOBAL_KEY, 0, 8, &timestamp_values, 0)
            .unwrap();
        let mut scope = store.begin().await.unwrap();
        scope.put(&primary, &args).unwrap();
        scope.put(&secondary, &primary).unwrap();
        scope.commit().await.unwrap();

        let scope = store.begin().await.unwrap();
        let mut accumulators = operator.native_accumulators(None).unwrap();
        let mut current = Vec::new();
        for (index, state) in accumulators.iter_mut().enumerate() {
            current.push(match state {
                IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate().unwrap(),
                IncrementalState::Batch { .. } => operator
                    .native_fallback_value(&scope, &GLOBAL_KEY, 0, index)
                    .await
                    .unwrap(),
            });
        }
        let mut previous = current.clone();
        previous[0] = ScalarValue::Utf8(Some("prior".into()));
        let encoded = encode_group(
            &EncodedGroup {
                last_update_nanos: to_nanos(SystemTime::now()) as i64,
                generation: 0,
                next_ordinal: 1,
                validity_deadline_nanos: 0,
                accumulator_state: operator.native_state_values(&mut accumulators).unwrap(),
                last_emitted: Some(previous),
            },
            scope.limits().value_bytes,
        )
        .unwrap();
        drop(scope);
        let mut scope = store.begin().await.unwrap();
        scope
            .put(&native_group_key(b'G', &GLOBAL_KEY).unwrap(), &encoded)
            .unwrap();
        scope
            .put(&native_group_key(b'D', &GLOBAL_KEY).unwrap(), &[1])
            .unwrap();
        scope.commit().await.unwrap();

        let id = ScalarValue::FixedSizeBinary(16, None).to_array().unwrap();
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![Arc::new(BooleanArray::from(vec![false])), id],
            None,
        );
        operator.metadata_expr = Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
        operator.native_store = Some(store);
        let (config, _) = native_config();
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into().unwrap();
        let output_schema: ArroyoSchema = config.final_schema.unwrap().try_into().unwrap();
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        let mut ctx = OperatorContext::new(
            Arc::new(arroyo_types::TaskInfo {
                job_id: "native-aggregate-boundary".into(),
                operator_idx: 0,
                operator_name: "UpdatingAggregate".into(),
                operator_id: "native-aggregate-boundary".into(),
                task_index: 0,
                parallelism: 1,
                key_range: 0..=u64::MAX,
                checkpoint_file_path_layout: Default::default(),
            }),
            None,
            control_tx,
            1,
            vec![Arc::new(input_schema)],
            Some(Arc::new(output_schema)),
            HashMap::new(),
        )
        .await;
        let mut collector = AggregateCollector::default();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(collector.batches.len(), 1);
        assert_eq!(collector.batches[0].num_rows(), 2);
        assert!(
            operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap()
                .get(&native_group_key(b'D', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn append_only_native_state_keeps_winners_without_member_history() {
        let store = native_test_store();
        let operator = native_append_only_operator();
        assert_ne!(
            operator.native_schema_identity,
            native_operator().native_schema_identity
        );
        for aggregate in &operator.aggregates {
            if matches!(
                aggregate.func.fun().name().to_ascii_lowercase().as_str(),
                "min" | "max" | "first_value" | "last_value"
            ) {
                assert_eq!(aggregate.accumulator_type, AccumulatorType::AppendOnly);
            }
        }
        let values = (0..100)
            .map(|rank| format!("v{rank:03}"))
            .collect::<Vec<_>>();
        let refs = values
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let order = (0usize..100)
            .map(|rank| ((rank * 37) % 101) as i64)
            .collect::<Vec<_>>();
        let include = (0..100).map(|rank| Some(rank % 3 != 0)).collect::<Vec<_>>();
        let input = batch(&refs, &order, &include);
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut first_size = 0;
        for row in 0..input.num_rows() {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None, None)
                .await
                .unwrap();
            scope.commit().await.unwrap();
            if row == 9 {
                let read = store.begin().await.unwrap();
                first_size = read
                    .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .len();
            }
        }
        let scope = store.begin().await.unwrap();
        assert!(scope.first(b"M").await.unwrap().is_none());
        assert!(scope.first(b"R").await.unwrap().is_none());
        let encoded = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(
            encoded.len() <= first_size + 64,
            "append-only state grew with input cardinality"
        );
        let fresh = native_append_only_operator();
        let group = decode_group(
            &encoded,
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut states = fresh.native_accumulators(Some(&group)).unwrap();
        let actual = states
            .iter_mut()
            .map(IncrementalState::evaluate)
            .collect::<DFResult<Vec<_>>>()
            .unwrap();
        let smallest = (0..100).min_by_key(|&rank| order[rank]).unwrap();
        let largest = (0..100).max_by_key(|&rank| order[rank]).unwrap();
        let selected = (0..100)
            .filter(|&rank| include[rank] == Some(true))
            .max_by_key(|&rank| order[rank])
            .unwrap();
        assert_eq!(actual[0], ScalarValue::Utf8(Some(values[smallest].clone())));
        assert_eq!(actual[1], ScalarValue::Utf8(Some(values[largest].clone())));
        assert_eq!(actual[2], ScalarValue::Utf8(Some(values[largest].clone())));
        assert_eq!(actual[3], ScalarValue::Utf8(Some(values[smallest].clone())));
        assert_eq!(actual[4], ScalarValue::Utf8(Some(values[selected].clone())));
        assert_eq!(actual[5], ScalarValue::Int64(Some(100)));
        assert_eq!(actual[6], ScalarValue::Int64(Some(66)));
        assert_eq!(actual[7], ScalarValue::Utf8(Some("v099".into())));
        assert_eq!(
            actual[8],
            ScalarValue::TimestampNanosecond(Some(*order.iter().max().unwrap()), None)
        );
    }

    #[tokio::test]
    async fn append_only_native_restores_filter_null_and_tied_order_state() {
        let store = native_test_store();
        let operator = native_append_only_operator();
        let rows = batch(
            &[None, Some("B"), None, Some("C"), Some("D")],
            &[0, 3, 5, 1, 1],
            &[Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        let inputs = operator.compute_inputs(&rows).unwrap();
        for row in 0..rows.num_rows() {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None, None)
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let fresh = native_append_only_operator();
        let scope = store.begin().await.unwrap();
        let bytes = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = decode_group(
            &bytes,
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut states = fresh.native_accumulators(Some(&group)).unwrap();
        let actual = states
            .iter_mut()
            .map(IncrementalState::evaluate)
            .collect::<DFResult<Vec<_>>>()
            .unwrap();
        // Both non-null rows share the earliest eligible ORDER BY key.
        assert!(matches!(&actual[0], ScalarValue::Utf8(Some(value))
            if value == "C" || value == "D"));
        assert_eq!(actual[1], ScalarValue::Utf8(None));
        assert_eq!(actual[4], ScalarValue::Utf8(None));
        assert_eq!(actual[5], ScalarValue::Int64(Some(5)));
        assert_eq!(actual[6], ScalarValue::Int64(Some(4)));
        assert_eq!(actual[7], ScalarValue::Utf8(Some("D".into())));
        assert_eq!(actual[2], ScalarValue::Utf8(None));
        assert_eq!(actual[3], ScalarValue::Utf8(None));
    }

    #[tokio::test]
    async fn append_only_native_unordered_first_last_restore_across_chunks() {
        let store = native_test_store();
        let (config, registry) = native_config_with_unordered(true);
        let operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        for index in 9..13 {
            assert_eq!(
                operator.aggregates[index].accumulator_type,
                AccumulatorType::AppendOnly
            );
        }
        let first_chunk = batch(
            &[None, Some("start")],
            &[100, 50],
            &[Some(false), Some(false)],
        );
        let inputs = operator.compute_inputs(&first_chunk).unwrap();
        for row in 0..first_chunk.num_rows() {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None, None)
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        let checkpoint_bytes = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        let checkpoint_group = decode_group(
            &checkpoint_bytes,
            &operator.native_state_types(),
            &operator.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut checkpoint_state = operator
            .native_accumulators(Some(&checkpoint_group))
            .unwrap();
        assert_eq!(
            checkpoint_state[9].evaluate().unwrap(),
            ScalarValue::Int64(Some(100))
        );
        assert_eq!(
            checkpoint_state[10].evaluate().unwrap(),
            ScalarValue::Int64(Some(50))
        );
        assert_eq!(
            checkpoint_state[11].evaluate().unwrap(),
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            checkpoint_state[12].evaluate().unwrap(),
            ScalarValue::Utf8(None)
        );
        drop(scope);

        // Construct another operator and continue from the serialized group.
        let (config, registry) = native_config_with_unordered(true);
        let fresh = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        assert_eq!(
            operator.native_schema_identity,
            fresh.native_schema_identity
        );
        let second_chunk = batch(
            &[Some(""), Some("end"), None],
            &[40, 30, 20],
            &[Some(false), Some(true), Some(false)],
        );
        let inputs = fresh.compute_inputs(&second_chunk).unwrap();
        for row in 0..second_chunk.num_rows() {
            let mut scope = store.begin().await.unwrap();
            fresh
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None, None)
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        assert!(scope.first(b"M").await.unwrap().is_none());
        assert!(scope.first(b"R").await.unwrap().is_none());
        let group = decode_group(
            &scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .unwrap(),
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut states = fresh.native_accumulators(Some(&group)).unwrap();
        assert_eq!(states[9].evaluate().unwrap(), ScalarValue::Int64(Some(100)));
        assert_eq!(states[10].evaluate().unwrap(), ScalarValue::Int64(Some(20)));
        assert_eq!(
            states[11].evaluate().unwrap(),
            ScalarValue::Utf8(Some("end".into()))
        );
        assert_eq!(states[12].evaluate().unwrap(), ScalarValue::Utf8(None));
    }

    #[test]
    fn native_unordered_first_last_still_rejects_changelog_input() {
        let (mut config, registry) = native_config_with_unordered(true);
        let mut schema = SchemaBuilder::from(input_schema().as_ref().clone());
        schema.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        config.input_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(schema.finish()))
                .unwrap()
                .into(),
        );
        let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
        assert!(input.schema.index_of(UPDATING_META_FIELD).is_ok());
        let plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = plan.physical_plan_type else {
            panic!("expected aggregate plan");
        };
        assert_eq!(aggregate.aggr_expr.len(), 13);
        for index in 9..13 {
            let decoded = decode_aggregate(
                &input.schema,
                &aggregate.aggr_expr_name[index],
                &aggregate.aggr_expr[index],
                registry.as_ref(),
            )
            .unwrap();
            assert!(
                decoded.order_bys().is_none(),
                "aggregate {index} has ORDER BY"
            );
        }
        match IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        ) {
            Ok(operator) => panic!(
                "unordered changelog aggregate admitted: append_only={}, types={:?}",
                operator.native_append_only,
                operator
                    .aggregates
                    .iter()
                    .map(|a| a.accumulator_type)
                    .collect::<Vec<_>>()
            ),
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("requires an explicit ORDER BY for bounded retraction")
            ),
        }
    }

    #[tokio::test]
    async fn append_only_native_rejects_undeclared_retraction_before_writes() {
        let store = native_test_store();
        let operator = native_append_only_operator();
        let input = batch(&[Some("v")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut scope = store.begin().await.unwrap();
        let error = operator
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, None, None)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("append-only native aggregate received a retraction")
        );
        assert!(
            scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_owner_reloads_ordered_and_filtered_state_after_retraction() {
        let store = native_test_store();
        let operator = native_operator();
        let initial = batch(
            &[Some("early"), Some("late"), Some("excluded")],
            &[1, 3, 2],
            &[Some(true), Some(true), Some(false)],
        );
        let inputs = operator.compute_inputs(&initial).unwrap();
        let mut scope = store.begin().await.unwrap();
        for row in 0..initial.num_rows() {
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&initial, row)),
                    None,
                )
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();

        let fresh = native_operator();
        let scope = store.begin().await.unwrap();
        let group = decode_group(
            &scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .unwrap(),
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        let mut accumulators = fresh.native_accumulators(Some(&group)).unwrap();
        assert_eq!(
            accumulators[5].evaluate().unwrap(),
            ScalarValue::Int64(Some(3))
        );
        assert_eq!(
            accumulators[6].evaluate().unwrap(),
            ScalarValue::Int64(Some(2))
        );
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 0)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("early".into()))
        );
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 1)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("late".into()))
        );
        assert!(matches!(
            fresh.aggregates[7].accumulator_type,
            AccumulatorType::Batch
        ));
        assert!(matches!(
            fresh.aggregates[8].accumulator_type,
            AccumulatorType::Batch
        ));
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(3), None)
        );
        drop(scope);

        let removed = batch(&[Some("late")], &[3], &[Some(true)]);
        let inputs = fresh.compute_inputs(&removed).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                true,
                Some(&test_row_id(&removed, 0)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 1)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("excluded".into()))
        );
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(2), None)
        );
        scope.commit().await.unwrap();
    }

    #[tokio::test]
    async fn injected_event_time_max_retracts_original_member_by_row_id_after_reload() {
        let store = native_test_store();
        let operator = native_operator();
        assert!(operator.aggregates[8].injected_timestamp);
        assert!(!operator.aggregates[7].injected_timestamp);
        let input = batch_at(
            &[Some("a"), Some("b"), Some("c")],
            &[1, 3, 2],
            &[10, 30, 20],
        );
        let inputs = operator.compute_inputs(&input).unwrap();
        let ids = [[1_u8; 16], [2_u8; 16], [3_u8; 16]];
        let mut scope = store.begin().await.unwrap();
        for (row, id) in ids.iter().enumerate() {
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, Some(id), None)
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();

        let fresh = native_operator();
        let before = batch_at(&[Some("b")], &[3], &[99]);
        let inputs = fresh.compute_inputs(&before).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                true,
                Some(&ids[1]),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(20), None)
        );
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 7)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("c".into()))
        );
        scope.commit().await.unwrap();

        let replacement = batch_at(&[Some("b")], &[3], &[99]);
        let inputs = fresh.compute_inputs(&replacement).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                false,
                Some(&ids[1]),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(99), None)
        );
        assert!(
            fresh
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    0,
                    false,
                    Some(&ids[1]),
                    None
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate live row ID")
        );
        drop(scope);
    }

    #[tokio::test]
    async fn injected_event_time_max_keeps_duplicate_times_with_distinct_ids() {
        let store = native_test_store();
        let operator = native_operator();
        let input = batch_at(&[Some("a"), Some("b")], &[1, 2], &[10, 10]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let ids = [[1_u8; 16], [2_u8; 16]];
        let mut scope = store.begin().await.unwrap();
        for (row, id) in ids.iter().enumerate() {
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, Some(id), None)
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();

        let fresh = native_operator();
        let before_a = batch_at(&[Some("a")], &[1], &[90]);
        let inputs = fresh.compute_inputs(&before_a).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                true,
                Some(&ids[0]),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(10), None)
        );
        scope.commit().await.unwrap();

        let before_b = batch_at(&[Some("b")], &[2], &[100]);
        let inputs = fresh.compute_inputs(&before_b).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &inputs,
                0,
                true,
                Some(&ids[1]),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(None, None)
        );
        scope.commit().await.unwrap();
    }

    #[tokio::test]
    async fn injected_event_time_id_moves_between_groups_in_one_scope() {
        let store = native_test_store();
        let mut operator = native_operator();
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        operator.key_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let group_columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["left", "right"]))];
        let groups = operator
            .key_converter
            .convert_columns(&group_columns)
            .unwrap();
        let left = groups.row(0).as_ref().to_vec();
        let right = groups.row(1).as_ref().to_vec();
        let id = [9_u8; 16];
        let first = batch_at(&[Some("v")], &[1], &[10]);
        let inputs = operator.compute_inputs(&first).unwrap();
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(&mut scope, &left, &inputs, 0, false, Some(&id), None)
            .await
            .unwrap();
        scope.commit().await.unwrap();

        let before = batch_at(&[Some("v")], &[1], &[100]);
        let after = batch_at(&[Some("v")], &[1], &[110]);
        let before_inputs = operator.compute_inputs(&before).unwrap();
        let after_inputs = operator.compute_inputs(&after).unwrap();
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(&mut scope, &left, &before_inputs, 0, true, Some(&id), None)
            .await
            .unwrap();
        operator
            .native_process_event(&mut scope, &right, &after_inputs, 0, false, Some(&id), None)
            .await
            .unwrap();
        assert!(!operator.native_group_is_live(&scope, &left).await.unwrap());
        assert!(operator.native_group_is_live(&scope, &right).await.unwrap());
        assert_eq!(
            operator
                .native_fallback_value(&scope, &right, 0, 8)
                .await
                .unwrap(),
            ScalarValue::TimestampNanosecond(Some(110), None)
        );
        scope.commit().await.unwrap();
    }

    #[tokio::test]
    async fn native_ordered_tie_survives_reload_and_exact_retraction() {
        let store = native_test_store();
        let operator = native_operator();
        let first = batch(&[Some("z")], &[1], &[Some(true)]);
        let tied = batch(&[Some("a")], &[1], &[Some(true)]);
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &operator.compute_inputs(&first).unwrap(),
                0,
                false,
                Some(&test_row_id(&first, 0)),
                None,
            )
            .await
            .unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &operator.compute_inputs(&tied).unwrap(),
                0,
                false,
                Some(&test_row_id(&tied, 0)),
                None,
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let fresh = native_operator();
        let mut scope = store.begin().await.unwrap();
        let stored = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = decode_group(
            &stored,
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        assert_eq!(group.next_ordinal, 2);
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 0)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("z".into()))
        );
        fresh
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &fresh.compute_inputs(&first).unwrap(),
                0,
                true,
                Some(&test_row_id(&first, 0)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 0)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("a".into()))
        );
    }

    #[tokio::test]
    async fn native_ordered_ignore_nulls_skips_only_null_members() {
        let store = native_test_store();
        let operator = native_operator();
        assert!(operator.aggregates[0].func.ignore_nulls());
        assert!(!operator.aggregates[1].func.ignore_nulls());
        let rows = batch(
            &[None, Some("non-null"), None],
            &[0, 1, 2],
            &[Some(true), Some(true), Some(true)],
        );
        let inputs = operator.compute_inputs(&rows).unwrap();
        let mut scope = store.begin().await.unwrap();
        for row in 0..rows.num_rows() {
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&rows, row)),
                    None,
                )
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        let stored = scope
            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = decode_group(
            &stored,
            &operator.native_state_types(),
            &operator.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 0)
                .await
                .unwrap(),
            ScalarValue::Utf8(Some("non-null".into()))
        );
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, group.generation, 1)
                .await
                .unwrap(),
            ScalarValue::Utf8(None)
        );
    }

    #[tokio::test]
    async fn native_indexed_max_ignores_all_null_values() {
        let store = native_test_store();
        let operator = native_operator();
        let rows = batch(&[None, None], &[1, 2], &[Some(true), Some(true)]);
        let inputs = operator.compute_inputs(&rows).unwrap();
        let mut scope = store.begin().await.unwrap();
        for row in 0..rows.num_rows() {
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&test_row_id(&rows, row)),
                    None,
                )
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 7)
                .await
                .unwrap(),
            ScalarValue::Utf8(None)
        );
    }

    #[tokio::test]
    async fn finite_ttl_expires_all_due_groups_in_bounded_pages() {
        let mut operator = native_operator();
        operator.retain_indefinitely = false;
        operator.ttl = Duration::from_secs(1);
        operator.native_store = Some(native_test_store());
        let store = operator.native_store.as_ref().unwrap();
        let state = operator
            .native_state_values(&mut operator.native_accumulators(None).unwrap())
            .unwrap();
        let mut scope = store.begin().await.unwrap();
        for number in 0..5 {
            let group = format!("k{number}");
            let encoded = encode_group(
                &EncodedGroup {
                    last_update_nanos: 0,
                    generation: 0,
                    next_ordinal: 0,
                    validity_deadline_nanos: 0,
                    accumulator_state: state.clone(),
                    last_emitted: None,
                },
                scope.limits().value_bytes,
            )
            .unwrap();
            scope
                .put(&native_group_key(b'G', group.as_bytes()).unwrap(), &encoded)
                .unwrap();
            scope
                .put(
                    &native_expiry_key(1_000_000_000, group.as_bytes()).unwrap(),
                    &[1],
                )
                .unwrap();
        }
        scope.commit().await.unwrap();
        assert!(operator.expire_native().await.unwrap());
        assert!(operator.expire_native().await.unwrap());
        assert!(!operator.expire_native().await.unwrap());
        let scope = store.begin().await.unwrap();
        for number in 0..5 {
            let group = format!("k{number}");
            let bytes = scope
                .get(&native_group_key(b'G', group.as_bytes()).unwrap())
                .await
                .unwrap()
                .unwrap();
            let restored = decode_group(
                &bytes,
                &operator.native_state_types(),
                &operator.native_output_types(),
                scope.limits().value_bytes,
            )
            .unwrap();
            assert_eq!(restored.generation, 1);
            assert!(
                scope
                    .get(&native_group_key(b'D', group.as_bytes()).unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }

    // A keyed current-result aggregate with event-time validity: the group
    // key is an opaque Utf8 row and the result timestamp is the event-time
    // column, matching the planner's selected composition lowering.
    fn event_time_expiry_operator() -> (IncrementalAggregatingFunc, Vec<u8>) {
        let mut operator = native_append_only_operator();
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        operator.key_converter = RowConverter::new(vec![SortField::new_with_options(
            DataType::Utf8,
            SortOptions::default(),
        )])
        .unwrap();
        operator.event_time_expiry = Some(EventTimeExpiryRuntime {
            result_timestamp_expr: Arc::new(Column::new(TIMESTAMP_FIELD, 3)),
            delay_nanos: 2,
        });
        operator.native_store = Some(native_test_store());
        let group_rows = operator
            .key_converter
            .convert_columns(&[Arc::new(StringArray::from(vec!["keyA"]))])
            .unwrap();
        let group = group_rows.iter().next().unwrap().as_ref().to_vec();
        (operator, group)
    }

    #[test]
    fn event_time_expiry_requires_native_aggregate_state() {
        let (mut config, registry) = native_config();
        config.event_time_expiry = Some(arroyo_rpc::grpc::api::EventTimeExpiry {
            result_timestamp_expr: serialize_physical_expr(
                &(Arc::new(Column::new(TIMESTAMP_FIELD, 3)) as Arc<dyn PhysicalExpr>),
                &DefaultPhysicalExtensionCodec {},
            )
            .unwrap()
            .encode_to_vec(),
            delay_nanos: 2,
        });
        let error = match IncrementalAggregatingConstructor::build_with_native_config(
            config, registry, None,
        ) {
            Ok(_) => panic!("event-time expiry must reject non-native aggregate state"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("event-time result expiry"),
            "received {error}"
        );
    }

    #[tokio::test]
    async fn event_time_expiry_orders_negative_zero_and_positive_deadlines() {
        let (operator, _) = event_time_expiry_operator();
        let input = batch_at(&[Some("past"), Some("epoch"), Some("future")], &[1, 2, 3], &[-4, -2, 10]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let groups = operator.key_converter.convert_columns(&[
            Arc::new(StringArray::from(vec!["past", "epoch", "future"]))
        ]).unwrap();
        let store = operator.native_store.as_ref().unwrap();
        for (row, stamp) in [-4, -2, 10].into_iter().enumerate() {
            let mut scope = store.begin().await.unwrap();
            operator.native_process_event(&mut scope, groups.row(row).as_ref(), &inputs,
                row, false, Some(&test_row_id(&input, row)), Some(stamp)).await.unwrap();
            scope.commit().await.unwrap();
        }
        {
            let mut scope = store.begin().await.unwrap();
            // Calendar's independent singleton frontier cannot be scanned as
            // a current-result deadline, even in the same native namespace.
            scope.put(b"V", b"independent-cleanup-frontier").unwrap();
            assert!(scope.get(&native_validity_key(-2, groups.row(0).as_ref()).unwrap()).await.unwrap().is_some());
            assert!(scope.get(&native_validity_key(0, groups.row(1).as_ref()).unwrap()).await.unwrap().is_some());
            scope.commit().await.unwrap();
        }
        assert!(!operator.expire_native_event_time(-3).await.unwrap());
        assert!(operator.expire_native_event_time(-2).await.unwrap());
        assert!(!operator.expire_native_event_time(-1).await.unwrap());
        assert!(operator.expire_native_event_time(0).await.unwrap());
        assert!(!operator.expire_native_event_time(0).await.unwrap());
        let scope = store.begin().await.unwrap();
        assert!(scope.get(&native_validity_key(12, groups.row(2).as_ref()).unwrap()).await.unwrap().is_some());
        assert!(scope.get(b"V").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn event_time_expiry_retracts_only_after_the_watermark_reaches_the_deadline() {
        let (operator, group) = event_time_expiry_operator();
        // A result stamped at event time 10 stays valid through the first
        // empty closed boundary at 12: one slide past its timestamp.
        let input = batch_at(&[Some("v")], &[1], &[10]);
        let inputs = operator.compute_inputs(&input).unwrap();
        {
            let store = operator.native_store.as_ref().unwrap();
            let mut scope = store.begin_point().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &group,
                    &inputs,
                    0,
                    false,
                    Some(&test_row_id(&input, 0)),
                    Some(10),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            let scope = store.begin().await.unwrap();
            let retained = decode_group(
                &scope
                    .get(&native_group_key(b'G', &group).unwrap())
                    .await
                    .unwrap()
                    .unwrap(),
                &operator.native_state_types(),
                &operator.native_output_types(),
                scope.limits().value_bytes,
            )
            .unwrap();
            assert_eq!(retained.validity_deadline_nanos, 12);
            assert!(
                scope
                    .get(&native_validity_key(12, &group).unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        // No watermark progress leaves the pending expiry untouched; only
        // event time applies it. A watermark before the boundary is not due.
        assert!(!operator.expire_native_event_time(11).await.unwrap());
        {
            let store = operator.native_store.as_ref().unwrap();
            let scope = store.begin().await.unwrap();
            assert_eq!(
                decode_group(
                    &scope
                        .get(&native_group_key(b'G', &group).unwrap())
                        .await
                        .unwrap()
                        .unwrap(),
                    &operator.native_state_types(),
                    &operator.native_output_types(),
                    scope.limits().value_bytes,
                )
                .unwrap()
                .validity_deadline_nanos,
                12
            );
        }
        assert!(operator.expire_native_event_time(12).await.unwrap());
        // Repeated watermark advances must not repeat the transition.
        assert!(!operator.expire_native_event_time(12).await.unwrap());
        assert!(!operator.expire_native_event_time(13).await.unwrap());
        let store = operator.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let expired = decode_group(
            &scope
                .get(&native_group_key(b'G', &group).unwrap())
                .await
                .unwrap()
                .unwrap(),
            &operator.native_state_types(),
            &operator.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        assert_eq!(expired.validity_deadline_nanos, 0);
        assert_eq!(expired.generation, 1);
        assert_eq!(
            scope
                .get(&native_live_rows_key(&group).unwrap())
                .await
                .unwrap()
                .unwrap(),
            0_u64.to_be_bytes()
        );
        assert!(
            scope
                .get(&native_validity_key(12, &group).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            scope
                .get(&native_group_key(b'D', &group).unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn event_time_expiry_deadlines_survive_restore_and_apply_on_resumed_watermarks() {
        let (mut operator, group) = event_time_expiry_operator();
        let input = batch_at(&[Some("v")], &[1], &[10]);
        let inputs = operator.compute_inputs(&input).unwrap();
        {
            let store = operator.native_store.as_ref().unwrap();
            let mut scope = store.begin_point().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &group,
                    &inputs,
                    0,
                    false,
                    Some(&test_row_id(&input, 0)),
                    Some(10),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        // A fresh operator over the restored store owns the same pending
        // expiry: the deadline and its index entries are persistent state.
        let mut fresh = event_time_expiry_operator().0;
        fresh.native_store = operator.native_store.take();
        assert!(!fresh.expire_native_event_time(11).await.unwrap());
        assert!(fresh.expire_native_event_time(12).await.unwrap());
        let store = fresh.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let restored = decode_group(
            &scope
                .get(&native_group_key(b'G', &group).unwrap())
                .await
                .unwrap()
                .unwrap(),
            &fresh.native_state_types(),
            &fresh.native_output_types(),
            scope.limits().value_bytes,
        )
        .unwrap();
        assert_eq!(restored.validity_deadline_nanos, 0);
        assert!(
            scope
                .get(&native_validity_key(12, &group).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn event_time_watermark_emits_refresh_before_the_expiry_retraction() {
        let (mut operator, group) = event_time_expiry_operator();
        let mut values: Vec<FieldRef> = operator.schema_without_metadata.fields().to_vec();
        values.push(Arc::new(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        )));
        let output_schema = Arc::new(Schema::new(values));
        let id = ScalarValue::FixedSizeBinary(16, Some(vec![7; 16]))
            .to_array()
            .unwrap();
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![Arc::new(BooleanArray::from(vec![false])), id],
            None,
        );
        operator.metadata_expr = Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
        let input = batch_at(&[Some("v")], &[1], &[10]);
        let inputs = operator.compute_inputs(&input).unwrap();
        {
            let store = operator.native_store.as_ref().unwrap();
            let mut scope = store.begin_point().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &group,
                    &inputs,
                    0,
                    false,
                    Some(&test_row_id(&input, 0)),
                    Some(10),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        let mut ctx = OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control_tx,
            1,
            vec![Arc::new(
                ArroyoSchema::from_schema_unkeyed(input.schema()).unwrap(),
            )],
            Some(Arc::new(
                ArroyoSchema::from_schema_unkeyed(output_schema).unwrap(),
            )),
            HashMap::new(),
        )
        .await;
        let mut collector = AggregateCollector::default();
        let watermark = Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_nanos(12));
        let forwarded = operator
            .handle_watermark(watermark, &mut ctx, &mut collector)
            .await
            .unwrap();
        assert_eq!(
            forwarded,
            Some(Watermark::EventTime(
                SystemTime::UNIX_EPOCH + Duration::from_nanos(12)
            ))
        );
        // The data refresh is emitted first, then the expiry retraction; a
        // later watermark emits nothing more.
        assert_eq!(collector.batches.len(), 2);
        let (refresh_retracts, _) = native_changelog_columns(&collector.batches[0]).unwrap();
        let (expiry_retracts, _) = native_changelog_columns(&collector.batches[1]).unwrap();
        assert_eq!(refresh_retracts.len(), 1);
        assert!(!refresh_retracts.value(0));
        assert_eq!(expiry_retracts.len(), 1);
        assert!(expiry_retracts.value(0));
        let mut collector = AggregateCollector::default();
        operator
            .handle_watermark(
                Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_nanos(13)),
                &mut ctx,
                &mut collector,
            )
            .await
            .unwrap();
        assert!(collector.batches.is_empty());
    }

    fn batch(values: &[Option<&str>], sequence: &[i64], include: &[Option<bool>]) -> RecordBatch {
        RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(StringArray::from(values.to_vec())),
                Arc::new(Int64Array::from(sequence.to_vec())),
                Arc::new(BooleanArray::from(include.to_vec())),
                Arc::new(TimestampNanosecondArray::from(sequence.to_vec())),
            ],
        )
        .unwrap()
    }

    fn batch_at(values: &[Option<&str>], sequence: &[i64], event_time: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(StringArray::from(values.to_vec())),
                Arc::new(Int64Array::from(sequence.to_vec())),
                Arc::new(BooleanArray::from(vec![Some(true); values.len()])),
                Arc::new(TimestampNanosecondArray::from(event_time.to_vec())),
            ],
        )
        .unwrap()
    }

    // Direct operator tests provide a stable row identity explicitly. Runtime
    // never derives one from aggregate values: it reads `_updating_meta.id`.
    fn test_row_id(batch: &RecordBatch, row: usize) -> [u8; 16] {
        let mut digest = Sha256::new();
        digest.update(
            batch
                .column(1)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row)
                .to_be_bytes(),
        );
        let value = batch.column(0).as_string::<i32>();
        if value.is_null(row) {
            digest.update([0]);
        } else {
            digest.update([1]);
            digest.update(value.value(row).as_bytes());
        }
        digest.finalize()[..16].try_into().unwrap()
    }

    #[test]
    fn native_changelog_rejects_missing_or_null_row_identity() {
        let base = batch(&[Some("value")], &[1], &[Some(true)]);
        assert!(
            native_changelog_columns(&base)
                .unwrap_err()
                .to_string()
                .contains("no updating metadata")
        );
        for id in [
            ScalarValue::FixedSizeBinary(16, None),
            ScalarValue::FixedSizeBinary(16, Some(vec![7; 16])),
        ] {
            let metadata = StructArray::try_new(
                updating_meta_fields(),
                vec![
                    Arc::new(BooleanArray::from(vec![false])),
                    id.to_array().unwrap(),
                ],
                None,
            )
            .unwrap();
            let mut fields = base.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            )));
            let mut columns = base.columns().to_vec();
            columns.push(Arc::new(metadata));
            let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            if id.is_null() {
                assert!(
                    native_changelog_columns(&input)
                        .unwrap_err()
                        .to_string()
                        .contains("non-null")
                );
            } else {
                assert_eq!(
                    native_changelog_columns(&input).unwrap().1.value(0),
                    &[7; 16]
                );
            }
        }
    }

    fn assert_values(
        operator: &mut IncrementalAggregatingFunc,
        expected: &[Option<&str>],
        all: i64,
        selected: i64,
    ) {
        let actual = operator.evaluate(&GLOBAL_KEY).unwrap();
        for (value, expected) in actual.iter().zip(expected) {
            assert_eq!(value, &ScalarValue::Utf8(expected.map(str::to_string)));
        }
        assert_eq!(actual[5], ScalarValue::Int64(Some(all)));
        assert_eq!(actual[6], ScalarValue::Int64(Some(selected)));
    }

    fn retract(
        operator: &mut IncrementalAggregatingFunc,
        batch: &RecordBatch,
        index: Option<usize>,
    ) {
        let inputs = operator.compute_inputs(batch).unwrap();
        operator.retract_batch(&GLOBAL_KEY, &inputs, index).unwrap();
    }

    fn checkpoint_rows(schema: &ArroyoSchema, columns: Vec<ArrayRef>) -> RecordBatch {
        let mut fields = schema.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("generation", DataType::UInt64, false)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn native_ordered_values_across_batches_and_retractions() {
        let mut operator = operator();
        operator
            .global_aggregate(&batch(
                &[Some("u"), Some("v")],
                &[1, 2],
                &[Some(true), Some(true)],
            ))
            .unwrap();
        assert_values(
            &mut operator,
            &[Some("u"), Some("v"), Some("v"), Some("u"), Some("v")],
            2,
            2,
        );
        operator
            .global_aggregate(&batch(
                &[Some("early"), Some("late")],
                &[0, 3],
                &[Some(false), None],
            ))
            .unwrap();
        assert_values(
            &mut operator,
            &[
                Some("early"),
                Some("late"),
                Some("late"),
                Some("early"),
                Some("v"),
            ],
            4,
            2,
        );
        // Removing an excluded row must leave the independently filtered values
        // and count unchanged while retracting the unfiltered companion.
        retract(
            &mut operator,
            &batch(&[Some("late")], &[3], &[None]),
            Some(0),
        );
        assert_values(
            &mut operator,
            &[
                Some("early"),
                Some("v"),
                Some("v"),
                Some("early"),
                Some("v"),
            ],
            3,
            2,
        );
        retract(
            &mut operator,
            &batch(&[Some("v")], &[2], &[Some(true)]),
            None,
        );
        assert_values(
            &mut operator,
            &[
                Some("early"),
                Some("u"),
                Some("u"),
                Some("early"),
                Some("u"),
            ],
            2,
            1,
        );
    }

    #[test]
    fn native_ordered_repeated_values_keep_distinct_sequences() {
        let mut operator = operator();
        operator
            .global_aggregate(&batch(
                &[Some("same"), Some("middle"), Some("same")],
                &[1, 2, 3],
                &[Some(true); 3],
            ))
            .unwrap();
        retract(
            &mut operator,
            &batch(&[Some("same")], &[3], &[Some(true)]),
            Some(0),
        );
        assert_values(
            &mut operator,
            &[
                Some("same"),
                Some("middle"),
                Some("middle"),
                Some("same"),
                Some("middle"),
            ],
            2,
            2,
        );
    }

    #[test]
    fn native_ordered_equal_keys_retain_remaining_duplicate() {
        let mut operator = operator();
        operator
            .global_aggregate(&batch(
                &[Some("tied"), Some("tied"), Some("later")],
                &[1, 1, 2],
                &[Some(true), Some(true), Some(false)],
            ))
            .unwrap();
        retract(
            &mut operator,
            &batch(&[Some("tied")], &[1], &[Some(true)]),
            Some(0),
        );
        assert_values(
            &mut operator,
            &[
                Some("tied"),
                Some("later"),
                Some("later"),
                Some("tied"),
                Some("tied"),
            ],
            2,
            1,
        );
    }

    #[test]
    fn native_filter_null_values_and_row_updates() {
        let mut operator = operator();
        operator
            .global_aggregate(&batch(
                &[Some("u"), None, Some("")],
                &[1, 2, 3],
                &[Some(true), Some(true), Some(false)],
            ))
            .unwrap();
        assert_values(
            &mut operator,
            &[Some("u"), Some(""), Some(""), Some("u"), None],
            3,
            2,
        );
        let next = batch(
            &[Some("null-filter"), Some("selected")],
            &[4, 5],
            &[None, Some(true)],
        );
        let inputs = operator.compute_inputs(&next).unwrap();
        operator
            .update_batch(&GLOBAL_KEY, &inputs, Some(0))
            .unwrap();
        operator
            .update_batch(&GLOBAL_KEY, &inputs, Some(1))
            .unwrap();
        assert_values(
            &mut operator,
            &[
                Some("u"),
                Some("selected"),
                Some("selected"),
                Some("u"),
                Some("selected"),
            ],
            5,
            3,
        );
    }

    #[test]
    fn native_ordered_checkpoint_rows_reload_into_fresh_operator() {
        let mut original = operator();
        original
            .global_aggregate(&batch(
                &[Some("u"), Some("v"), Some("w")],
                &[1, 2, 3],
                &[Some(true), Some(false), Some(true)],
            ))
            .unwrap();
        let sliding = checkpoint_rows(
            &original.sliding_state_schema.clone(),
            original.checkpoint_sliding().unwrap().unwrap(),
        );
        let fallback = checkpoint_rows(
            &original.batch_state_schema.clone(),
            original.checkpoint_batch().unwrap().unwrap(),
        );
        drop(original);
        let mut fresh = operator();
        // Production checkpoint serializers and restore methods; this is a
        // fresh operator row-codec reload, not a remote TableManager checkpoint.
        fresh.restore_sliding_batch(&sliding).unwrap();
        fresh.restore_batch(&fallback).unwrap();
        assert_values(
            &mut fresh,
            &[Some("u"), Some("w"), Some("w"), Some("u"), Some("w")],
            3,
            2,
        );
        retract(
            &mut fresh,
            &batch(&[Some("w")], &[3], &[Some(true)]),
            Some(0),
        );
        fresh
            .global_aggregate(&batch(&[Some("later")], &[4], &[Some(true)]))
            .unwrap();
        assert_values(
            &mut fresh,
            &[
                Some("u"),
                Some("later"),
                Some("later"),
                Some("u"),
                Some("later"),
            ],
            3,
            2,
        );
    }

    #[test]
    fn native_ordered_legacy_checkpoint_rows_reject_before_input() {
        let mut original = operator();
        original
            .global_aggregate(&batch(&[Some("u")], &[1], &[Some(true)]))
            .unwrap();
        let mut columns = original.checkpoint_batch().unwrap().unwrap();
        let index = original
            .batch_state_schema
            .schema
            .index_of("args_row")
            .unwrap();
        let rows = columns[index]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut legacy = BinaryBuilder::new();
        // Keep earlier rows current so a late legacy row tests atomic validation.
        for (index, row) in rows.iter().enumerate() {
            let row = row.unwrap();
            legacy.append_value(if index + 1 == rows.len() {
                row.strip_prefix(ORDERED_ARGS_V1).unwrap()
            } else {
                row
            });
        }
        columns[index] = Arc::new(legacy.finish());
        let checkpoint = checkpoint_rows(&original.batch_state_schema, columns);
        let mut fresh = operator();
        let error = fresh.restore_batch(&checkpoint).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("legacy or unsupported ordered aggregate checkpoint")
        );
        assert!(error.to_string().contains("source replay"));
        assert!(fresh.accumulators.iter_mut().next().is_none());
    }
    #[test]
    fn native_filtered_legacy_checkpoint_rejects_before_input() {
        let mut original = operator();
        original
            .global_aggregate(&batch(&[Some("u")], &[1], &[Some(true)]))
            .unwrap();
        let columns = original.checkpoint_sliding().unwrap().unwrap();
        let mut checkpoint = checkpoint_rows(&original.sliding_state_schema, columns);
        let fields: Vec<_> = checkpoint
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let mut metadata = field.metadata().clone();
                metadata.remove(INPUT_SEMANTICS_KEY);
                Arc::new((**field).clone().with_metadata(metadata))
            })
            .collect();
        checkpoint =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), checkpoint.columns().to_vec())
                .unwrap();
        let mut fresh = operator();
        let error = fresh.restore_sliding_batch(&checkpoint).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ordered/filtered aggregate checkpoint")
        );
        assert!(error.to_string().contains("source replay"));
        assert!(fresh.accumulators.iter_mut().next().is_none());
    }

    #[test]
    fn native_unordered_unfiltered_legacy_checkpoint_remains_compatible() {
        let (mut config, registry) = native_config();
        let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(ref mut aggregate)) = plan.physical_plan_type else {
            panic!("expected aggregate");
        };
        // Keep only the two ordinary COUNTs and timestamp MAX; remove FILTER.
        let selected = ["all_count", "selected_count", TIMESTAMP_FIELD];
        let indices = selected.map(|name| {
            aggregate
                .aggr_expr_name
                .iter()
                .position(|candidate| candidate == name)
                .unwrap_or_else(|| panic!("missing fixture aggregate {name}"))
        });
        aggregate.aggr_expr = indices
            .iter()
            .map(|&index| aggregate.aggr_expr[index].clone())
            .collect();
        aggregate.aggr_expr_name = selected.iter().map(|name| (*name).to_string()).collect();
        aggregate.filter_expr = vec![MaybeFilter::default(); 3];
        config.aggregate_exec = plan.encode_to_vec();
        let mut original =
            IncrementalAggregatingConstructor::build(config.clone(), registry.clone()).unwrap();
        original
            .global_aggregate(&batch(
                &[Some("u"), Some("v")],
                &[1, 2],
                &[None, Some(false)],
            ))
            .unwrap();
        let columns = original.checkpoint_sliding().unwrap().unwrap();
        let checkpoint = checkpoint_rows(&original.sliding_state_schema, columns);
        assert!(
            !checkpoint
                .schema()
                .field_with_name(TIMESTAMP_FIELD)
                .unwrap()
                .metadata()
                .contains_key(INPUT_SEMANTICS_KEY)
        );
        let mut fresh = IncrementalAggregatingConstructor::build(config, registry).unwrap();
        fresh.restore_sliding_batch(&checkpoint).unwrap();
        let actual = fresh.evaluate(&GLOBAL_KEY).unwrap();
        assert_eq!(actual[0], ScalarValue::Int64(Some(2)));
        assert_eq!(actual[1], ScalarValue::Int64(Some(2)));
    }
}
