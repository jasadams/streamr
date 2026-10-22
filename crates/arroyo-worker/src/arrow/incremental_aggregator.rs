#[path = "calendar_native.rs"]
mod calendar_native;
use calendar_native::{
    CalendarAggregate, CalendarContributionChange, CalendarInput, CalendarInputProgress,
};

use crate::arrow::aggregate_codec::{CalendarReferences, EncodedGroup, GroupCodec, decode_group};
use crate::arrow::aggregate_store::{
    AggregateExpiryProgress, AggregateScope, AggregateStore, AggregateStoreLimits,
    aggregate_decoded_foreground, aggregate_encoded_key_bound,
};
use crate::arrow::decode_aggregate;
use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::compute::{SortOptions, filter, max_array};
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::builder::{
    BinaryBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, RecordBatch, StructArray,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder, TimeUnit};
use arroyo_operator::context::Collector;
use arroyo_operator::operator::EventResponseRole;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_planner::aggregate_capability::{
    AggregateCapability, AggregateDescriptor, aggregate_descriptor, is_extremum,
    supports_native_result,
};
use arroyo_planner::grouped_selector::{SelectorBudget, SelectorSpec, bounded_row_size};
use arroyo_planner::grouped_selector_udaf::{classify, selector_spec};
use arroyo_rpc::config::AggregateStateConfig;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{
    api::UpdatingAggregateOperator,
    rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
};
use arroyo_rpc::{
    SOURCE_ENVELOPE_FIELD, TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_fields,
};
use arroyo_state::live::{
    LiveStateBackend, Ownership, StateNamespace,
    checkpoint::checkpoint_decoded_reservation,
    worker::{ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend},
};
use arroyo_state::timestamp_table_config;
use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};
use arroyo_types::{CheckpointBarrier, SignalMessage, to_nanos};
use datafusion::common::{Result as DFResult, ScalarValue};
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

/// Volatile, admitted progress for one serial cleanup drain. No snapshot or
/// index survives a page, flush, cancellation, or checkpoint restore.
#[derive(Default)]
struct NativeCleanupProgress {
    marker: Option<Vec<u8>>,
    member_after: Option<Vec<u8>>,
    phase: Option<u8>,
    completed: bool,
    _permit: Option<arroyo_state::live::resources::ResourcePermit>,
}

impl NativeCleanupProgress {
    fn new(store: &AggregateStore) -> Self {
        let limits = store.limits();
        let charge = limits.key_bytes.saturating_mul(2);
        let encoded_key = store.max_encoded_entry_bytes() - limits.value_bytes - 1;
        // AggregateScope holds overlay + three maximum values. Rocks read_many
        // separately admits one value, two encoded keys, and result containers.
        // Optional progress must leave that existing mandatory peak available.
        let required = limits
            .overlay_bytes
            .saturating_add(limits.value_bytes.saturating_mul(4))
            .saturating_add(encoded_key.saturating_mul(2))
            .saturating_add(mem::size_of::<Vec<u8>>())
            .saturating_add(mem::size_of::<Option<Vec<u8>>>())
            .saturating_add(charge);
        let required = store
            .optional_progress_peak(0, required.saturating_sub(charge))
            .and_then(|mandatory| mandatory.checked_add(charge))
            .unwrap_or(usize::MAX);
        let permit = (required <= store.resources().config().decoded_value_bytes)
            .then(|| {
                // Shared consumers may already hold part of a valid pool. Keep
                // the mandatory peak reserved while testing optional headroom.
                let mandatory = store
                    .resources()
                    .try_decoded_value(required - charge)
                    .ok()?;
                let cursor = store.resources().try_decoded_value(charge).ok();
                drop(mandatory);
                cursor
            })
            .flatten();
        Self {
            _permit: permit,
            ..Self::default()
        }
    }

    fn committed(
        &mut self,
        marker: Vec<u8>,
        phase: u8,
        member_after: Option<Vec<u8>>,
        completed: bool,
    ) {
        if self._permit.is_some() {
            // Decoded backend keys can have geometric Vec spare capacity.
            // Retain only their exact lengths under the two-key reservation.
            self.marker = Some(marker.into_boxed_slice().into_vec());
            self.member_after = member_after.map(|key| key.into_boxed_slice().into_vec());
            self.phase = Some(phase);
            self.completed = completed;
        }
    }
}

/// One accepted contribution with its raw row clock and calendar inputs.
struct NativeCalendarEvent<'a> {
    group_key: &'a [u8],
    inputs: &'a [AggregateInput],
    row: usize,
    retract: bool,
    row_id: Option<&'a [u8]>,
    calendar_inputs: &'a [CalendarInput],
}

fn native_decoded_resource_exhausted(error: &anyhow::Error) -> bool {
    use arroyo_state::live::{LiveStateError, resources::ResourceError};
    let resource = error.downcast_ref::<ResourceError>().or_else(|| {
        match error.downcast_ref::<LiveStateError>() {
            Some(LiveStateError::Resource(resource)) => Some(resource),
            _ => None,
        }
    });
    matches!(
        resource,
        Some(ResourceError::ResourceExhausted {
            resource: "decoded_value_bytes",
            ..
        })
    )
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
    // Monotone maximum until empty; a delete may retain conservative headroom.
    max_member_workspace: u64,
}

#[derive(Clone, Copy)]
struct CollectionMemberSize {
    encoded_args_bytes: usize,
    scalar_bytes: usize,
    workspace: usize,
}

impl CollectionStats {
    fn decode(value: Option<Vec<u8>>) -> Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        ensure!(
            value.len() == 32,
            "invalid native member totals v2 (expected 32 bytes)"
        );
        let stats = Self {
            members: u64::from_be_bytes(value[..8].try_into()?),
            encoded_args_bytes: u64::from_be_bytes(value[8..16].try_into()?),
            scalar_bytes: u64::from_be_bytes(value[16..24].try_into()?),
            max_member_workspace: u64::from_be_bytes(value[24..32].try_into()?),
        };
        ensure!(
            (stats.members == 0
                && stats.encoded_args_bytes == 0
                && stats.scalar_bytes == 0
                && stats.max_member_workspace == 0)
                || (stats.members > 0 && stats.max_member_workspace > 0),
            "invalid native member totals count/workspace"
        );
        Ok(stats)
    }

    fn encode(self) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&self.members.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.encoded_args_bytes.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.scalar_bytes.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.max_member_workspace.to_be_bytes());
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
    /// Retain and replay all admitted members rather than just an indexed winner.
    replay_members: bool,
    /// ARRAY_AGG has an array result and may expose an admitted prefix limit.
    array_result: bool,
    /// Verified builtin accumulator storage retained per replayed input row.
    descriptor: Option<AggregateDescriptor>,
    member_types: Vec<DataType>,
    member_decode_base: usize,
    collection_output_limit: Option<usize>,
    collection_member_bound: usize,
    injected_timestamp: bool,
    selector: Option<SelectorSpec>,
}

impl Aggregator {
    fn member_workspace(
        &self,
        encoded: usize,
        sort_bytes: usize,
        peer_index: bool,
    ) -> Result<usize> {
        let copies = if peer_index { 4 } else { 3 };
        let mut bytes = encoded
            .checked_mul(copies)
            .and_then(|n| n.checked_add(sort_bytes.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.member_decode_base))
            .context("native member typed workspace overflow")?;
        if let Some(descriptor) = self
            .descriptor
            .filter(|descriptor| matches!(descriptor.capability, AggregateCapability::Fixed { .. }))
        {
            let scratch = descriptor
                .retained_bound(1, encoded)
                .and_then(|full| full.checked_sub(descriptor.retained_bound(1, 0)?))
                .context("native member payload scratch overflow")?;
            bytes = bytes
                .checked_add(scratch)
                .context("native member workspace overflow")?;
        }
        Ok(bytes)
    }
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
    native_store: Option<AggregateStore>,
    native_codec: Option<GroupCodec>,
    retain_indefinitely: bool,
    native_schema_identity: Vec<u8>,
    native_append_only: bool,
    emit_per_event: bool,
    response_root_operator: Option<u32>,
    response_previous: Option<EventContext>,
    event_output: Option<Arc<EventEdgeSpec>>,
    response_schemas: Option<(ArroyoSchema, ArroyoSchema)>,
    calendars: Vec<CalendarAggregate>,
}

struct PreparedNativeResponse {
    response: EventResponse,
    // Output ownership, not a snapshot/working/scan guard. It accompanies
    // publication through success, typed failure and future cancellation.
    _output: Option<arroyo_state::live::resources::ResourcePermit>,
}

struct NativeGroupDelta {
    append: bool,
    unchanged: bool,
    next: Vec<ScalarValue>,
    retained_bytes: usize,
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

// A flush retains its output, the scope's overlay/group read, and (for
// shared selectors) one independently charged decoded candidate concurrently.
fn native_decoded_workspace(config: AggregateStateConfig, has_selector: bool) -> Result<usize> {
    config
        .overlay_bytes
        .checked_add(
            config
                .max_pending_output_bytes
                .checked_mul(3)
                .context("native aggregate output workspace overflow")?,
        )
        .and_then(|bytes| {
            bytes.checked_add(
                config
                    .value_bytes
                    .checked_mul(if has_selector { 6 } else { 3 })?,
            )
        })
        .context("native aggregate decoded workspace overflow")
}

fn validate_selector_owner_decoded(
    config: AggregateStateConfig,
    resources: &arroyo_state::live::resources::ResourceConfig,
    encoded_key: usize,
) -> Result<usize> {
    let foreground = aggregate_decoded_foreground(
        native_limits(config),
        config.max_pending_output_bytes,
        true,
        encoded_key,
    )?;
    let checkpoint = checkpoint_decoded_reservation(resources)?;
    let required = foreground
        .checked_add(checkpoint)
        .context("aggregate/checkpoint workspace overflow")?;
    ensure!(
        required <= resources.decoded_value_bytes,
        "native selector owner requires {required} decoded bytes for foreground and its one checkpoint writer"
    );
    Ok(required)
}

fn native_index_codec(
    aggregate: &AggregateFunctionExpr,
    input_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    replay_members: bool,
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
        _ if replay_members => {
            // Verified replay companions retain physical SQL order if present;
            // otherwise their persisted write ordinal defines traversal.
            if let Some(order) = aggregate.order_bys().filter(|order| !order.is_empty()) {
                ensure!(
                    order.len() <= input_exprs.len(),
                    "native replay aggregate ORDER BY shape mismatch"
                );
                let first = input_exprs.len() - order.len();
                (
                    (first..input_exprs.len()).collect(),
                    order.iter().map(|item| item.options).collect(),
                )
            } else {
                (Vec::new(), Vec::new())
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
    for node in std::iter::once(data_type).chain(children.iter().map(|field| field.data_type())) {
        shape = shape
            .checked_add(arroyo_planner::grouped_selector::row_node_decode_bytes(
                node,
            )?)
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
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
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
    fn native_group_codec(&self) -> Result<&GroupCodec> {
        self.native_codec
            .as_ref()
            .context("native aggregate codec is missing")
    }

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
            if let Some(descriptor) = aggregate.descriptor.filter(|_| !aggregate.array_result)
                && matches!(descriptor.capability, AggregateCapability::Fixed { .. })
            {
                // Streaming fixed-state replay retains only one admitted
                // decoded member and the verified accumulator workspace.
                let member = usize::try_from(stats.max_member_workspace)?;
                let state = descriptor
                    .retained_bound(1, 0)
                    .context("native fixed replay bound overflow")?;
                working = working
                    .checked_add(member)
                    .and_then(|n| n.checked_add(state))
                    .context("native fixed replay workspace overflow")?;
                continue;
            }
            let scalar_slots = count
                .checked_mul(self.aggregates[index].input_exprs.len())
                .and_then(|n| n.checked_mul(std::mem::size_of::<ScalarValue>()))
                .context("native collection scalar workspace overflow")?;
            let state = aggregate
                .descriptor
                .map(|descriptor| {
                    descriptor
                        .retained_bound(count, bytes)
                        .context("native retained accumulator bound overflow")
                })
                .transpose()?
                .unwrap_or(0);
            working = working
                .checked_add(bytes)
                .and_then(|n| n.checked_add(scalar_bytes))
                .and_then(|n| n.checked_add(scalar_slots))
                .and_then(|n| n.checked_add(count.checked_mul(32)?))
                .and_then(|n| n.checked_add(state))
                .context("native collection workspace overflow")?;
        }
        let admitted = working
            .checked_mul(8)
            .context("native collection workspace overflow")?;
        ensure!(
            // Keep a conservative per-group preflight under the value limit.
            // Group framing is checked exactly by the cached codec before the
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
        size: CollectionMemberSize,
        retract: bool,
    ) -> Result<()> {
        let key = native_collection_totals_key(group, aggregate_index)?;
        let mut updated = CollectionStats::decode(scope.get(&key).await?)?;
        let bytes = u64::try_from(size.encoded_args_bytes)?;
        let scalar_bytes = u64::try_from(size.scalar_bytes)?;
        let workspace = u64::try_from(size.workspace)?;
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
        if retract {
            ensure!(
                workspace <= updated.max_member_workspace,
                "retained member workspace exceeds admitted maximum"
            );
        }
        if updated.members == 0 {
            updated.max_member_workspace = 0;
        } else if !retract {
            updated.max_member_workspace = updated.max_member_workspace.max(workspace);
        }
        let mut totals = Vec::new();
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            if aggregate.replay_members {
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
                updated.encoded_args_bytes == 0
                    && updated.scalar_bytes == 0
                    && updated.max_member_workspace == 0,
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

    #[cfg(test)]
    async fn native_group_is_live(
        &self,
        scope: &AggregateScope<'_>,
        group_key: &[u8],
    ) -> Result<bool> {
        if !self.native_has_group_keys() {
            return Ok(true);
        }
        let bytes = scope
            .get(&native_group_key(b'G', group_key)?)
            .await?
            .context("native aggregate keyed group is missing")?;
        Ok(self
            .native_group_codec()?
            .decode(&bytes, scope.limits().value_bytes)?
            .live_rows
            > 0)
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
                if let Some(descriptor) = aggregate.descriptor {
                    descriptor.validate_partial_state(&arrays, 0)?;
                }
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
        change: &NativeMemberChange<'_>,
        limits: AggregateStoreLimits,
        identity: Option<&[u8]>,
        omit_secondary: bool,
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let (group, generation, aggregate_index, values, ordinal) = (
            change.group,
            change.generation,
            change.aggregate_index,
            change.values,
            change.ordinal,
        );
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
        let prefix = native_member_prefix(group, generation, aggregate_index)?;
        let args_limit = if identity.is_none() && !omit_secondary {
            limits.value_bytes.min(
                limits
                    .key_bytes
                    .checked_sub(prefix.len() + 12)
                    .context("native aggregate argument index cannot fit configured key limit")?,
            )
        } else {
            limits.value_bytes
        };
        bounded_row_size(values, args_limit)?;
        let key_payload = limits
            .key_bytes
            .checked_sub(prefix.len() + 8)
            .context("native aggregate member key cannot fit configured limit")?;
        bounded_row_size(&sort_columns, key_payload)?;
        let sort_rows = if sort_columns.is_empty() {
            None
        } else {
            Some(codec.convert_columns(&sort_columns)?)
        };
        let args_rows = aggregate.row_converter.convert_columns(values)?;
        ensure!(
            sort_rows.as_ref().is_none_or(|rows| rows.num_rows() == 1) && args_rows.num_rows() == 1,
            "native aggregate member update requires one input row"
        );
        let args = args_rows.row(0).as_ref().to_vec();
        let mut primary = prefix;
        if let Some(sort_rows) = sort_rows {
            primary.extend_from_slice(sort_rows.row(0).as_ref());
        }
        primary.extend_from_slice(&ordinal.to_be_bytes());
        let secondary = if omit_secondary {
            Vec::new()
        } else if let Some(identity) = identity {
            native_tuple_prefix(group, generation, aggregate_index, identity)?
        } else {
            let mut key = native_tuple_prefix(group, generation, aggregate_index, &args)?;
            key.extend_from_slice(&ordinal.to_be_bytes());
            key
        };
        Ok((primary, secondary, args))
    }

    fn native_selector_budget(&self, scope: &AggregateScope<'_>) -> Result<SelectorBudget> {
        Ok(SelectorBudget {
            key_bytes: scope.limits().key_bytes,
            value_bytes: scope.limits().value_bytes,
            decoded_bytes: scope
                .limits()
                .value_bytes
                .checked_mul(3)
                .context("grouped selector decoded allowance overflow")?,
        })
    }

    async fn native_selector_delta(
        &self,
        scope: &mut AggregateScope<'_>,
        change: NativeMemberChange<'_>,
        selector: &SelectorSpec,
    ) -> Result<()> {
        let budget = self.native_selector_budget(scope)?;
        let prefix = native_member_prefix(change.group, change.generation, change.aggregate_index)?;
        if self.native_append_only {
            ensure!(
                !change.retract,
                "append-only grouped selector received a retraction"
            );
            let Some(candidate) = selector.encode(change.values, change.ordinal, budget)? else {
                return Ok(());
            };
            let mut winner = scope
                .get(&prefix)
                .await?
                .map(|bytes| selector.decode_candidate(&bytes, budget))
                .transpose()?;
            if selector.choose(&mut winner, candidate) {
                scope.put(
                    &prefix,
                    &selector.encode_candidate(winner.as_ref().unwrap(), budget)?,
                )?;
            }
            return Ok(());
        }
        let identity_key = change
            .row_id
            .map(|id| {
                ensure!(
                    id.len() == 16,
                    "grouped selector changelog row ID must have 16 bytes"
                );
                native_tuple_prefix(change.group, change.generation, change.aggregate_index, id)
            })
            .transpose()?;
        if change.retract {
            let identity_key = identity_key
                .context("grouped selector retraction requires stable changelog row ID")?;
            // An excluded NULL has no candidate. FILTER is applied before this
            // call; null eligibility remains owned by the shared descriptor.
            if selector
                .encode(change.values, change.ordinal, budget)?
                .is_none()
            {
                ensure!(
                    scope.get(&identity_key).await?.is_none(),
                    "grouped selector retract before-image changes null eligibility"
                );
                return Ok(());
            }
            let primary = scope
                .get(&identity_key)
                .await?
                .context("grouped selector retracts a missing row identity/version")?;
            ensure!(
                primary.starts_with(&prefix),
                "grouped selector identity points outside member index"
            );
            let stored = scope
                .get(&primary)
                .await?
                .context("grouped selector member index is missing")?;
            let candidate = selector.decode_candidate(&stored, budget)?;
            ensure!(
                primary[prefix.len()..] == candidate.key,
                "grouped selector member key disagrees with retained candidate"
            );
            let _decoded = scope
                .resources()
                .try_decoded_value(selector.decoded_bound(&candidate)?)?;
            let aggregate = &self.aggregates[change.aggregate_index];
            let declared_for = classify(aggregate.func.fun())
                .context("selector descriptor missing")?
                .1;
            // Debezium before rows carry the current triggering _timestamp.
            // Only the implicit primary clock is exempt; user values and every
            // ordinary secondary expression must match the stored version.
            let ignored = if declared_for {
                &[1_usize][..]
            } else {
                &[][..]
            };
            selector.validate_before(&candidate, change.values, ignored, budget)?;
            scope.delete(&primary)?;
            scope.delete(&identity_key)?;
        } else {
            let Some(candidate) = selector.encode(change.values, change.ordinal, budget)? else {
                if let Some(key) = identity_key {
                    ensure!(
                        scope.get(&key).await?.is_none(),
                        "grouped selector appends duplicate live row identity"
                    );
                }
                return Ok(());
            };
            let stored = selector.encode_candidate(&candidate, budget)?;
            let mut primary = prefix;
            primary.extend_from_slice(&candidate.key);
            if let Some(key) = &identity_key {
                ensure!(
                    scope.get(key).await?.is_none(),
                    "grouped selector appends duplicate live row identity"
                );
            }
            scope.put(&primary, &stored)?;
            if let Some(key) = identity_key {
                scope.put(&key, &primary)?;
            }
        }
        Ok(())
    }

    async fn native_member_delta(
        &self,
        scope: &mut AggregateScope<'_>,
        change: NativeMemberChange<'_>,
    ) -> Result<()> {
        let aggregate = &self.aggregates[change.aggregate_index];
        if let Some(selector) = &aggregate.selector {
            return self.native_selector_delta(scope, change, selector).await;
        }
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
        let omit_secondary =
            self.native_append_only && aggregate.replay_members && !aggregate.array_result;
        ensure!(
            !omit_secondary || !change.retract,
            "append-only replay aggregate received a retraction"
        );
        let identity = if aggregate.injected_timestamp
            || (aggregate.replay_members && !aggregate.array_result && !self.native_append_only)
        {
            let id = change
                .row_id
                .context("native indexed aggregate requires a changelog row ID")?;
            ensure!(id.len() == 16, "native changelog row ID must have 16 bytes");
            Some(id)
        } else {
            None
        };
        let identity_index = identity.is_some();
        // Complete typed argument/order admission precedes either RowConverter
        // allocation. Three argument copies (four for a peer-argument R index)
        // and two ordering-key copies cover encoding/decode temporaries.
        let member_workspace = if aggregate.replay_members {
            let encoded = bounded_row_size(change.values, scope.limits().value_bytes)?;
            ensure!(
                change.values.len() == aggregate.member_types.len()
                    && change
                        .values
                        .iter()
                        .zip(&aggregate.member_types)
                        .all(|(array, expected)| array.data_type() == expected),
                "native member argument schema changed"
            );
            let sort = aggregate
                .index_columns
                .iter()
                .map(|index| change.values[*index].clone())
                .collect::<Vec<_>>();
            let key_prefix =
                native_member_prefix(change.group, change.generation, change.aggregate_index)?;
            let sort_limit = scope
                .limits()
                .key_bytes
                .checked_sub(key_prefix.len() + 8)
                .context("native member key cannot fit configured limit")?;
            let sort_bytes = bounded_row_size(&sort, sort_limit)?;
            let bound = aggregate.member_workspace(
                encoded,
                sort_bytes,
                !identity_index && !omit_secondary,
            )?;
            ensure!(
                bound
                    <= scope
                        .limits()
                        .value_bytes
                        .checked_mul(3)
                        .context("native member workspace overflow")?,
                "native member typed workspace exceeds configured allowance before materialization"
            );
            bound
        } else {
            0
        };
        let (primary, secondary, args) =
            self.native_member_keys(&change, scope.limits(), identity, omit_secondary)?;
        let scalar_bytes = if aggregate.replay_members && !change.retract {
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
            let _decoded_permit = scope.resources().try_decoded_value(decode_bound)?;
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
            let (secondary_key, primary_key) = if identity_index {
                let primary_key = scope
                    .get(&secondary)
                    .await?
                    .context("native aggregate retracts a nonmatching indexed row ID")?;
                (secondary.clone(), primary_key)
            } else {
                let prefix = &secondary[..secondary.len() - 8];
                scope
                    .first_member(prefix)
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
            if aggregate.replay_members {
                let (retained_scalar_bytes, stored_args) = native_collection_member(&stored)?;
                ensure!(stored_args == args, "aggregate member indexes disagree");
                self.native_collection_change(
                    scope,
                    change.group,
                    change.aggregate_index,
                    CollectionMemberSize {
                        encoded_args_bytes: args.len(),
                        scalar_bytes: retained_scalar_bytes,
                        workspace: member_workspace,
                    },
                    true,
                )
                .await?;
            } else if !aggregate.injected_timestamp {
                ensure!(stored == args, "aggregate member indexes disagree");
            }
            scope.delete(&secondary_key)?;
            scope.delete(&primary_key)?;
            if !identity_index {
                scope.member_deleted(&secondary_key);
            }
        } else {
            // An append can create an earlier ordinal in a helper/replay or a
            // new generation. Conservatively invalidate before any member put.
            scope.reset_member_progress();
            if identity_index {
                ensure!(
                    scope.get(&secondary).await?.is_none(),
                    "native aggregate appends a duplicate live row ID"
                );
            }
            if aggregate.replay_members {
                self.native_collection_change(
                    scope,
                    change.group,
                    change.aggregate_index,
                    CollectionMemberSize {
                        encoded_args_bytes: args.len(),
                        scalar_bytes,
                        workspace: member_workspace,
                    },
                    false,
                )
                .await?;
            }
            if aggregate.replay_members {
                let mut stored = Vec::with_capacity(8 + args.len());
                stored.extend_from_slice(&u64::try_from(scalar_bytes)?.to_be_bytes());
                stored.extend_from_slice(&args);
                scope.put(&primary, &stored)?;
            } else {
                scope.put(&primary, &args)?;
            }
            if !omit_secondary {
                scope.put(&secondary, &primary)?;
            }
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
        if let Some(selector) = &aggregate.selector {
            let budget = self.native_selector_budget(scope)?;
            let stored = if self.native_append_only {
                scope
                    .get(&prefix)
                    .await?
                    .map(|value| (prefix.clone(), value))
            } else {
                scope.first(&prefix).await?
            };
            let candidate = stored
                .map(|(key, value)| -> Result<_> {
                    let candidate = selector.decode_candidate(&value, budget)?;
                    ensure!(
                        self.native_append_only || key[prefix.len()..] == candidate.key,
                        "grouped selector member key disagrees with retained candidate"
                    );
                    Ok(candidate)
                })
                .transpose()?;
            let bound = candidate
                .as_ref()
                .map(|c| selector.decoded_bound(c))
                .transpose()?
                .unwrap_or(0);
            let _decoded = scope
                .resources()
                .try_decoded_value(bound)
                .map_err(|error| {
                    anyhow!("selector candidate decoded reservation requested={bound}: {error}")
                })?;
            return Ok(selector.evaluate(candidate.as_ref(), budget)?);
        }
        // Verify combined retained-state allowance before constructing or
        // feeding a growing companion accumulator. Read only known L keys.
        let replay_stats = if aggregate.replay_members && !aggregate.array_result {
            let mut totals = Vec::new();
            let mut selected = CollectionStats::default();
            for (index, member) in self.aggregates.iter().enumerate() {
                if member.replay_members {
                    let stats = CollectionStats::decode(
                        scope
                            .get(&native_collection_totals_key(group, index)?)
                            .await?,
                    )?;
                    if index == aggregate_index {
                        selected = stats;
                    }
                    totals.push((index, stats));
                }
            }
            self.native_collection_budget(scope, totals.into_iter())?;
            Some(selected)
        } else {
            None
        };
        let mut accumulator = aggregate.func.create_accumulator()?;
        if aggregate.array_result && aggregate.collection_output_limit == Some(0) {
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
        let mut visited = CollectionStats::default();
        let mut update = |key: &[u8], stored: &[u8]| -> Result<()> {
            let args = if aggregate.replay_members {
                let (scalar_bytes, args) = native_collection_member(stored)?;
                if let Some(expected) = replay_stats {
                    visited.members = visited
                        .members
                        .checked_add(1)
                        .context("native replay member count overflow")?;
                    visited.encoded_args_bytes = visited
                        .encoded_args_bytes
                        .checked_add(u64::try_from(args.len())?)
                        .context("native replay byte count overflow")?;
                    visited.scalar_bytes = visited
                        .scalar_bytes
                        .checked_add(u64::try_from(scalar_bytes)?)
                        .context("native replay scalar count overflow")?;
                    ensure!(
                        visited.members <= expected.members
                            && visited.encoded_args_bytes <= expected.encoded_args_bytes
                            && visited.scalar_bytes <= expected.scalar_bytes,
                        "native replay member relation exceeds admitted statistics before accumulator update"
                    );
                }
                args
            } else {
                stored
            };
            if let Some(expected) = replay_stats {
                let sort_bytes = key
                    .len()
                    .checked_sub(prefix.len() + 8)
                    .context("native replay member key lacks ordinal")?;
                let workspace = aggregate.member_workspace(args.len(), sort_bytes, false)?;
                ensure!(
                    u64::try_from(workspace)? <= expected.max_member_workspace,
                    "native replay member workspace exceeds admitted maximum before decode"
                );
            }
            let parser = aggregate.row_converter.parser();
            let columns = aggregate
                .row_converter
                .convert_rows(std::iter::once(parser.parse(args)))?;
            accumulator.update_batch(&columns)?;
            Ok(())
        };
        if aggregate.replay_members {
            scope
                .visit_prefix(&prefix, aggregate.collection_output_limit, |key, stored| {
                    update(key, stored)
                })
                .await?;
        } else if let Some((key, stored)) = scope.first_tagged(&prefix, "accepted_M").await? {
            update(&key, &stored)?;
        }
        if let Some(expected) = replay_stats {
            ensure!(
                visited.members == expected.members
                    && visited.encoded_args_bytes == expected.encoded_args_bytes
                    && visited.scalar_bytes == expected.scalar_bytes,
                "native replay member statistics disagree with retained relation"
            );
        }
        Ok(accumulator.evaluate_mut()?)
    }

    #[cfg(test)]
    async fn native_process_event(
        &self,
        scope: &mut AggregateScope<'_>,
        group_key: &[u8],
        inputs: &[AggregateInput],
        row: usize,
        retract: bool,
        row_id: Option<&[u8]>,
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
            },
        )
        .await
    }

    #[cfg(test)]
    async fn native_process_calendar_event(
        &self,
        scope: &mut AggregateScope<'_>,
        event: NativeCalendarEvent<'_>,
    ) -> Result<()> {
        self.native_process_calendar_event_with_progress(scope, event, &mut None)
            .await
    }

    async fn native_process_calendar_event_with_progress(
        &self,
        scope: &mut AggregateScope<'_>,
        event: NativeCalendarEvent<'_>,
        progress: &mut Option<CalendarInputProgress>,
    ) -> Result<()> {
        let NativeCalendarEvent {
            group_key,
            inputs,
            row,
            retract,
            row_id,
            calendar_inputs,
        } = event;
        ensure!(
            !retract || !self.native_append_only,
            "append-only native aggregate received a retraction",
        );
        let storage_key = native_group_key(b'G', group_key)?;
        let mut previous = scope
            .get(&storage_key)
            .await?
            .map(|bytes| {
                self.native_group_codec()?
                    .decode(&bytes, scope.limits().value_bytes)
            })
            .transpose()?;
        let prior_rows = previous.as_ref().map_or(0, |group| group.live_rows);
        let now = to_nanos(SystemTime::now()) as i64;
        let ttl_nanos = i64::try_from(self.ttl.as_nanos())?;
        let expired = !self.retain_indefinitely
            && previous
                .as_ref()
                .is_some_and(|group| now.saturating_sub(group.last_update_nanos) >= ttl_nanos);
        let next_group_rows = if self.native_has_group_keys() {
            let rows = if expired { 0 } else { prior_rows };
            let next_rows = if retract {
                rows.checked_sub(1)
                    .context("native aggregate retracts a missing keyed row")?
            } else {
                rows.checked_add(1)
                    .context("native aggregate live-row count overflow")?
            };
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
                if aggregate.replay_members {
                    scope.delete(&native_collection_totals_key(group_key, index)?)?;
                }
            }
        }
        if let Some(group) = &previous {
            if !self.retain_indefinitely {
                let old_deadline = group.last_update_nanos.saturating_add(ttl_nanos);
                scope.delete(&native_expiry_key(old_deadline, group_key)?)?;
            }
            if expired {
                scope.put(&native_cleanup_key(group_key, group.generation)?, b"M")?;
            }
        }
        let mut calendar_references = if expired {
            CalendarReferences::new(self.calendars.len(), scope.limits().value_bytes)?
        } else if let Some(group) = &mut previous {
            std::mem::take(&mut group.calendar_references)
        } else {
            CalendarReferences::new(self.calendars.len(), scope.limits().value_bytes)?
        };
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
                            progress,
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
                // G already holds this horizon's accumulator. Reuse it only
                // when G proves an unchanged reference and this original CDC
                // contribution cannot affect membership. Relevant changes still
                // rebuild in bucket order, including floating-point SUM.
                let first_day = reference
                    .checked_sub(calendar.horizon_days() - 1)
                    .context("calendar reference horizon exceeds Date32 range")?;
                let affects_horizon = selected.is_some()
                    && contribution_day.is_some_and(|day| day <= reference && day >= first_day);
                // An absent G starts with empty accumulators. Valid-state cleanup
                // removes all old generation contributions before deleting G;
                // an irrelevant first append cannot add a bucket in this horizon.
                // Existing tombstones and TTL rollovers retain G and do not qualify.
                let fresh_empty = previous.is_none() && !retract && !affects_horizon;
                // Relevant changes rebuild in bucket order; only the proof
                // belonging to this accumulator can permit reuse.
                let reuse = !affects_horizon
                    && !expired
                    && previous.is_some()
                    && calendar_references.get(position)? == Some(reference);
                if !reuse {
                    calendar_references.set(position, reference)?;
                    if !fresh_empty {
                        *state = self
                            .calendar_recalculate_with_progress(
                                scope, group_key, generation, calendar, reference, progress,
                            )
                            .await?;
                    }
                }
                continue;
            }
            let Some(values) = selected else {
                // FILTER-excluded before rows cannot silently leave a retained
                // selector candidate behind under the same stable identity.
                if retract
                    && (self.aggregates[index].selector.is_some()
                        || (self.aggregates[index].replay_members
                            && !self.aggregates[index].array_result))
                {
                    let id = row_id
                        .context("grouped selector retraction requires stable changelog row ID")?;
                    let identity = native_tuple_prefix(group_key, generation, index, id)?;
                    ensure!(
                        scope.get(&identity).await?.is_none(),
                        "indexed aggregate retract before-image changes FILTER eligibility"
                    );
                }
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
                if aggregate.replay_members {
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
                        scope.first_tagged(&primary, "empty_M").await?.is_none()
                            && scope.first_tagged(&secondary, "empty_R").await?.is_none(),
                        "native aggregate retracts a nonmatching keyed row"
                    );
                }
            }
        }
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
            live_rows: next_group_rows.unwrap_or(0),
            calendar_references,
            accumulator_state: self.native_state_values(&mut accumulators)?,
            last_emitted: previous.and_then(|group| group.last_emitted),
        };
        let encoded = self
            .native_group_codec()?
            .encode(&next, scope.limits().value_bytes)?;
        scope.put(&storage_key, &encoded)?;
        if !self.emit_per_event && self.response_root_operator.is_none() {
            scope.put(&native_group_key(b'D', group_key)?, &[1])?;
        }
        if !self.retain_indefinitely {
            scope.put(
                &native_expiry_key(now.saturating_add(ttl_nanos), group_key)?,
                &[1],
            )?;
        }
        Ok(())
    }

    async fn native_accepted_values(
        &self,
        scope: &AggregateScope<'_>,
        group_key: &[u8],
        group: &EncodedGroup,
    ) -> Result<Vec<ScalarValue>> {
        let mut accumulators = self.native_accumulators(Some(group))?;
        let mut values = Vec::with_capacity(self.aggregates.len());
        for (index, state) in accumulators.iter_mut().enumerate() {
            values.push(match state {
                IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate()?,
                IncrementalState::Batch { .. } => {
                    self.native_fallback_value(scope, group_key, group.generation, index)
                        .await?
                }
            });
        }
        Ok(values)
    }

    /// Apply one whole source operation through the ordinary native mutation
    /// kernel. Callers own publication/commit policy, never alternate arithmetic.
    async fn native_apply_operation(
        &self,
        scope: &mut AggregateScope<'_>,
        keys: &Rows,
        inputs: &[AggregateInput],
        calendar_inputs: &[CalendarInput],
        rows: std::ops::Range<usize>,
        changes: Option<(&BooleanArray, &FixedSizeBinaryArray)>,
    ) -> Result<Vec<usize>> {
        let mut affected = Vec::<usize>::new();
        for row in rows {
            let key = keys.row(row);
            if !affected
                .iter()
                .any(|known| keys.row(*known).as_ref() == key.as_ref())
            {
                affected.push(row);
            }
            let (retract, row_id) = changes.map_or((false, None), |(retracts, ids)| {
                (retracts.value(row), Some(ids.value(row)))
            });
            self.native_process_calendar_event_with_progress(
                scope,
                NativeCalendarEvent {
                    group_key: key.as_ref(),
                    inputs,
                    row,
                    retract,
                    row_id,
                    calendar_inputs,
                },
                &mut None,
            )
            .await?;
        }
        Ok(affected)
    }

    /// Presence and final native values are independent of delivery. Absence
    /// cannot become an implicit MERGE delete; unchanged values remain a result.
    async fn native_prepare_group_result(
        &self,
        scope: &AggregateScope<'_>,
        key: &[u8],
    ) -> Result<(EncodedGroup, Option<Vec<ScalarValue>>)> {
        let bytes = scope
            .get(&native_group_key(b'G', key)?)
            .await?
            .context("native operation lost its affected group")?;
        let group = self
            .native_group_codec()?
            .decode(&bytes, scope.limits().value_bytes)?;
        let values = if self.native_has_group_keys() && group.live_rows == 0 {
            None
        } else {
            Some(self.native_accepted_values(scope, key, &group).await?)
        };
        Ok((group, values))
    }

    fn native_operation_needs_snapshot(&self) -> bool {
        !self.calendars.is_empty()
            || self.aggregates.iter().any(|aggregate| {
                aggregate.accumulator_type == AccumulatorType::Batch && aggregate.selector.is_none()
            })
    }

    /// Preserve every accepted append prefix without dirty-key enumeration or
    /// ordinary changelog replacement/equality suppression. Aggregate and
    /// target remain independent owners and recover from an aligned checkpoint.
    async fn process_native_event_batch(
        &self,
        batch: &RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let store = self
            .native_store
            .as_ref()
            .context("native aggregate store is not initialized")?;
        if !self.native_append_only {
            return self
                .process_native_correction_batch(batch, ctx, collector)
                .await;
        }
        if batch.column_by_name(UPDATING_META_FIELD).is_some() {
            return Err(
                anyhow!("per-event aggregate MERGE received an unexpected changelog").into(),
            );
        }
        let input_schema = &ctx.in_schemas[0];
        let keys = self.key_converter.convert_columns(
            &input_schema
                .sort_columns(batch, false)
                .into_iter()
                .map(|column| column.values)
                .collect::<Vec<_>>(),
        )?;
        let inputs = self.compute_inputs(batch)?;
        let calendar_inputs = self.calendar_inputs(batch)?;
        self.validate_calendar_inputs(&calendar_inputs).await?;
        let configured = self
            .native_config
            .context("per-event aggregate requires native limits")?;
        let limits = store.limits();
        // Reuse ordinary native admission, including calendar bucket, identity
        // and due writes. Output has an independent admitted bound below.
        let operations = self.native_operations_per_image()?;
        let chunk_rows = (limits.write_operations / operations)
            .min(configured.max_pending_output_rows)
            .min(limits.write_bytes / store.max_encoded_entry_bytes().saturating_mul(operations))
            .min(
                limits.overlay_bytes
                    / limits
                        .key_bytes
                        .saturating_add(limits.value_bytes)
                        .saturating_mul(operations),
            );
        if chunk_rows == 0 {
            return Err(anyhow!("per-event aggregate budget cannot admit one event").into());
        }
        let mut start = 0;
        while start < batch.num_rows() {
            // Covers owned row batches, their concatenation and evaluation
            // scratch; released only after downstream collection finishes.
            let _output = store
                .resources()
                .try_decoded_value(configured.max_pending_output_bytes.saturating_mul(3))
                .map_err(anyhow::Error::from)?;
            let mut end = batch.num_rows().min(start.saturating_add(chunk_rows));
            loop {
                let mut scope = if self.native_operation_needs_snapshot() {
                    store.begin().await?
                } else {
                    store.begin_point().await?
                };
                let mut output = Vec::new();
                let mut output_bytes = 0usize;
                let mut shorter = None;
                for row in start..end {
                    let key = keys.row(row);
                    self.native_apply_operation(
                        &mut scope,
                        &keys,
                        &inputs,
                        &calendar_inputs,
                        row..row + 1,
                        None,
                    )
                    .await?;
                    let (mut group, values) = self
                        .native_prepare_group_result(&scope, key.as_ref())
                        .await?;
                    let mut values = values.context("append operation has no accepted group")?;
                    if !self.calendars.is_empty() {
                        // Maintenance compares native relation values (including
                        // its stored clock); the public event clock below is a
                        // separate, original-operation projection.
                        group.last_emitted = Some(values.clone());
                        scope.put(
                            &native_group_key(b'G', key.as_ref())?,
                            &self
                                .native_group_codec()?
                                .encode(&group, limits.value_bytes)?,
                        )?;
                    }
                    // The injected MAX remains native state, but it is not this
                    // event's clock. User-declared MAX columns remain untouched.
                    *values
                        .last_mut()
                        .context("per-event aggregate has no timestamp")? =
                        ScalarValue::try_from_array(
                            batch.column(input_schema.timestamp_index),
                            row,
                        )?;
                    let mut columns = self.key_converter.convert_rows([key])?;
                    columns.extend(
                        values
                            .into_iter()
                            .map(|value| value.to_array())
                            .collect::<DFResult<Vec<_>>>()?,
                    );
                    let accepted =
                        RecordBatch::try_new(self.schema_without_metadata.clone(), columns)?;
                    let next_bytes = output_bytes
                        .checked_add(accepted.get_array_memory_size())
                        .context("per-event aggregate output size overflow")?;
                    if next_bytes > configured.max_pending_output_bytes / 3 {
                        if output.is_empty() {
                            return Err(anyhow!("one per-event aggregate result exceeds configured pending-output budget").into());
                        }
                        shorter = Some(row);
                        break;
                    }
                    output_bytes = next_bytes;
                    output.push(accepted);
                }
                if let Some(next_end) = shorter {
                    // No writes have committed. Reprepare the fitting prefix in
                    // a fresh scope, excluding the event that crossed the bound.
                    // Input expressions are computed once and native replay
                    // has no external effects or processing-time SQL evaluation.
                    end = next_end;
                    continue;
                }
                // Every fallible row evaluation and output allocation precedes the
                // live commit. A delivery error fails this attempt; never retry it.
                let output =
                    arrow::compute::concat_batches(&self.schema_without_metadata, &output)?;
                if output.get_array_memory_size() > configured.max_pending_output_bytes {
                    return Err(anyhow!(
                        "per-event aggregate output exceeds configured pending-output budget"
                    )
                    .into());
                }
                scope.commit().await?;
                collector.collect(output).await?;
                start = end;
                break;
            }
        }
        Ok(())
    }

    /// A callback contains complete surviving source operations. The planner
    /// proves that no physical/transport step can split or combine callbacks.
    /// Marker ordinals restart for every callback; they are never durable IDs.
    async fn process_native_correction_batch(
        &self,
        batch: &RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let (retracts, ids) = native_changelog_columns(batch)?;
        let ranges = native_event_ranges(batch, retracts, ctx.in_schemas[0].timestamp_index)?;
        let store = self
            .native_store
            .as_ref()
            .context("native aggregate store is not initialized")?;
        let configured = self
            .native_config
            .context("per-event aggregate requires native limits")?;
        let input_schema = &ctx.in_schemas[0];
        let keys = self.key_converter.convert_columns(
            &input_schema
                .sort_columns(batch, false)
                .into_iter()
                .map(|column| column.values)
                .collect::<Vec<_>>(),
        )?;
        let inputs = self.compute_inputs(batch)?;
        let calendar_inputs = self.calendar_inputs(batch)?;
        self.validate_calendar_inputs(&calendar_inputs).await?;
        // Admission covers the WHOLE operation, including both keys of a move.
        // It never falls back to admitting just the before image.
        let operations_per_image = self.native_operations_per_image()?;
        for range in ranges {
            let operations = operations_per_image
                .checked_mul(range.len())
                .context("per-event operation count overflow")?;
            let limits = store.limits();
            if operations > limits.write_operations
                || operations.saturating_mul(store.max_encoded_entry_bytes()) > limits.write_bytes
                || operations.saturating_mul(limits.key_bytes.saturating_add(limits.value_bytes))
                    > limits.overlay_bytes
            {
                return Err(anyhow!(
                    "per-event aggregate budget cannot admit one complete source operation"
                )
                .into());
            }
            let mut output_diagnostic =
                crate::arrow::aggregate_store::scan_diagnostics::OutputProbe::new();
            let _output = store
                .resources()
                .try_decoded_value(configured.max_pending_output_bytes.saturating_mul(3))
                .map_err(anyhow::Error::from)?;
            let mut scope = store.begin().await?;
            output_diagnostic.bind(scope.diagnostic_scope_id());
            let affected = self
                .native_apply_operation(
                    &mut scope,
                    &keys,
                    &inputs,
                    &calendar_inputs,
                    range.clone(),
                    Some((retracts, ids)),
                )
                .await?;
            let trigger = range.end - 1;
            let mut output = Vec::new();
            let mut output_bytes = 0usize;
            for row in affected {
                let key = keys.row(row);
                let storage_key = native_group_key(b'G', key.as_ref())?;
                let (mut group, values) = self
                    .native_prepare_group_result(&scope, key.as_ref())
                    .await?;
                if group.live_rows == 0 {
                    // Noncalendar retractions have no deferred lifecycle state.
                    // Calendar generations retire through ordinary B/J/U/H
                    // cleanup; absence still emits no implicit target DELETE.
                    if self.calendars.is_empty() {
                        scope.delete(&storage_key)?;
                    } else {
                        self.retire_native_group(&mut scope, key.as_ref(), &mut group)?;
                        scope.put(
                            &storage_key,
                            &self
                                .native_group_codec()?
                                .encode(&group, limits.value_bytes)?,
                        )?;
                    }
                    continue;
                }
                if output.len() >= configured.max_pending_output_rows {
                    return Err(anyhow!("one per-event aggregate operation exceeds configured pending-output row budget").into());
                }
                let mut values = values.context("present operation result has no values")?;
                if !self.calendars.is_empty() {
                    group.last_emitted = Some(values.clone());
                    scope.put(
                        &storage_key,
                        &self
                            .native_group_codec()?
                            .encode(&group, limits.value_bytes)?,
                    )?;
                }
                *values
                    .last_mut()
                    .context("per-event aggregate has no timestamp")? =
                    ScalarValue::try_from_array(
                        batch.column(input_schema.timestamp_index),
                        trigger,
                    )?;
                let mut columns = self.key_converter.convert_rows([key])?;
                columns.extend(
                    values
                        .into_iter()
                        .map(|value| value.to_array())
                        .collect::<DFResult<Vec<_>>>()?,
                );
                let accepted = RecordBatch::try_new(self.schema_without_metadata.clone(), columns)?;
                output_bytes = output_bytes
                    .checked_add(accepted.get_array_memory_size())
                    .context("per-event aggregate output size overflow")?;
                if output_bytes > configured.max_pending_output_bytes / 3 {
                    return Err(anyhow!(
                        "one per-event aggregate operation exceeds configured pending-output budget"
                    )
                    .into());
                }
                output.push(accepted);
            }
            let accepted = arrow::compute::concat_batches(&self.schema_without_metadata, &output)?;
            if accepted.get_array_memory_size() > configured.max_pending_output_bytes {
                return Err(anyhow!(
                    "per-event aggregate output exceeds configured pending-output budget"
                )
                .into());
            }
            scope.commit().await?;
            if accepted.num_rows() != 0 {
                output_diagnostic.event("collector_begin");
                let delivered = collector.collect(accepted).await;
                output_diagnostic.event(if delivered.is_ok() {
                    "collector_complete"
                } else {
                    "collector_error"
                });
                delivered?;
            }
        }
        Ok(())
    }

    /// One callback is one complete original operation. WHERE has already
    /// selected surviving images; aggregate FILTER does not remove touched keys.
    /// Apply all images in one native working scope, then evaluate only those
    /// known keys. No dirty traversal or per-row publication occurs here.
    async fn prepare_native_response(
        &mut self,
        response: EventResponse,
        ctx: &mut OperatorContext,
    ) -> Result<PreparedNativeResponse> {
        let root = self
            .response_root_operator
            .context("aggregate is not configured for event responses")?;
        ensure!(
            response.context.version == EVENT_RESPONSE_VERSION
                && response.context.root_operator == root
                && response.context.root_subtask == 0
                && response.touched_keys.is_none(),
            "branch response aggregate input context differs from configured raw root"
        );
        if let Some(previous) = self.response_previous {
            ensure!(
                previous.attempt_nonce == response.context.attempt_nonce
                    && previous.sequence.checked_add(1) == Some(response.context.sequence),
                "branch response aggregate input skips or repeats an original operation"
            );
        } else {
            ensure!(
                response.context.sequence == 1,
                "branch response aggregate must begin at original operation one"
            );
        }
        let input = &ctx.in_schemas[0];
        let raw = EventEdgeSpec {
            root_operator: root,
            root_subtask: 0,
            schema: input.schema.clone(),
            layout: EventLayout::Raw,
        };
        raw.validate(&response).map_err(anyhow::Error::msg)?;
        let Some(batch) = response.rows.as_ref() else {
            self.response_previous = Some(response.context);
            return Ok(PreparedNativeResponse {
                response: EventResponse {
                    native_body: None,
                    rows: None,
                    touched_keys: None,
                    context: response.context,
                },
                _output: None,
            });
        };
        let stamps = batch
            .column(input.timestamp_index)
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .context("branch response aggregate input clock is not TimestampNanosecond")?;
        ensure!(
            stamps.null_count() == 0
                && stamps
                    .values()
                    .iter()
                    .all(|time| *time == response.context.trigger_nanos),
            "branch response aggregate input clock differs from original operation"
        );
        let changes = if self.native_append_only {
            ensure!(
                batch.num_rows() == 1,
                "append branch response contains multiple original rows"
            );
            None
        } else {
            let (retracts, ids) = native_changelog_columns(batch)?;
            if batch.num_rows() == 2 {
                ensure!(
                    retracts.value(0) && !retracts.value(1),
                    "branch response replacement must retract then append"
                );
            }
            Some((retracts, ids))
        };
        let store = self
            .native_store
            .as_ref()
            .context("native aggregate store is not initialized")?;
        let configured = self
            .native_config
            .context("branch response aggregate requires native limits")?;
        let keys = self.key_converter.convert_columns(
            &input
                .sort_columns(batch, false)
                .into_iter()
                .map(|column| column.values)
                .collect::<Vec<_>>(),
        )?;
        let inputs = self.compute_inputs(batch)?;
        let mut affected = Vec::new();
        for row in 0..batch.num_rows() {
            if !affected
                .iter()
                .any(|known: &usize| keys.row(*known).as_ref() == keys.row(row).as_ref())
            {
                affected.push(row);
            }
        }
        ensure!(
            affected.len() <= 2,
            "branch response aggregate touches more than two full keys"
        );
        let operations = self
            .native_operations_per_image()?
            .checked_mul(batch.num_rows())
            .and_then(|images| images.checked_add(affected.len().checked_mul(2)?))
            .context("response operation count overflow")?;
        let limits = store.limits();
        ensure!(
            operations <= limits.write_operations
                && operations.saturating_mul(store.max_encoded_entry_bytes()) <= limits.write_bytes
                && operations.saturating_mul(limits.key_bytes.saturating_add(limits.value_bytes))
                    <= limits.overlay_bytes,
            "branch response aggregate budget cannot admit one complete original operation"
        );
        // Same existing decoded-output allowance as native flush. Output
        // ownership remains through delivery; the working snapshot does not.
        // Keys are <=2 existing bounded encoded keys.
        let output_permit = store
            .resources()
            .try_decoded_value(
                configured
                    .max_pending_output_bytes
                    .checked_mul(3)
                    .context("response output workspace overflow")?,
            )
            .map_err(anyhow::Error::from)?;
        let mut scope = store.begin().await?;
        let affected = self
            .native_apply_operation(
                &mut scope,
                &keys,
                &inputs,
                &[],
                0..batch.num_rows(),
                changes,
            )
            .await?;
        let mut output_keys = Vec::new();
        let mut output_values = vec![Vec::new(); self.aggregates.len()];
        let mut retracts = Vec::new();
        let mut retained = 0usize;
        let mut touched = BinaryBuilder::new();
        for row in affected {
            let key = keys.row(row);
            let key = key.as_ref();
            ensure!(
                key.len() <= limits.key_bytes,
                "branch response touched key exceeds native key allowance"
            );
            touched.append_value(key);
            let storage = native_group_key(b'G', key)?;
            let (mut group, values) = self.native_prepare_group_result(&scope, key).await?;
            let delta = Self::native_group_delta_from_values(key, &group, values)?;
            retained = retained
                .checked_add(delta.retained_bytes)
                .context("response output size overflow")?;
            ensure!(
                retained <= configured.max_pending_output_bytes / 3,
                "one branch response exceeds native pending-output byte allowance"
            );
            Self::append_native_group_delta(
                key,
                &mut group,
                delta,
                &mut output_keys,
                &mut output_values,
                &mut retracts,
            );
            ensure!(
                output_keys.len() <= configured.max_pending_output_rows && output_keys.len() <= 4,
                "one branch response exceeds native pending-output row allowance"
            );
            if group.live_rows == 0 {
                // Restricted indefinite COUNT/MAX mode has no calendar/TTL
                // state. The native retraction verified empty M/R membership.
                scope.delete(&storage)?;
            } else {
                scope.put(
                    &storage,
                    &self
                        .native_group_codec()?
                        .encode(&group, limits.value_bytes)?,
                )?;
            }
            scope.delete(&native_group_key(b'D', key)?)?;
        }
        let rows = self.native_changelog_batch(ctx, output_keys, output_values, retracts)?;
        let keys = RecordBatch::try_new(
            EventEdgeSpec::keys_schema(),
            vec![Arc::new(touched.finish())],
        )?;
        let result = EventResponse {
            native_body: None,
            context: response.context,
            rows,
            touched_keys: Some(keys),
        };
        EventEdgeSpec {
            root_operator: root,
            root_subtask: 0,
            schema: ctx
                .out_schema
                .as_ref()
                .context("response aggregate output schema missing")?
                .schema
                .clone(),
            layout: EventLayout::Native {
                max_rows: configured.max_pending_output_rows.min(4),
                owner_output_bytes: configured.max_pending_output_bytes,
                encoded_key_bytes: limits.key_bytes,
            },
        }
        .validate(&result)
        .map_err(anyhow::Error::msg)?;
        scope.commit().await?; // Consumes the working scope and snapshot.
        self.response_previous = Some(response.context);
        Ok(PreparedNativeResponse {
            response: result,
            _output: Some(output_permit),
        })
    }

    fn native_operations_per_image(&self) -> Result<usize> {
        let fallback = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
            .count();
        let collections = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.replay_members)
            .count();
        // Share the existing conservative admission across ordinary and
        // accepted-output execution, including calendar identity/due writes.
        (2 + usize::from(self.native_has_group_keys()))
            .checked_add(fallback.saturating_mul(2))
            .and_then(|value| value.checked_add(collections.saturating_mul(2)))
            .and_then(|value| value.checked_add(3 * usize::from(!self.retain_indefinitely)))
            .and_then(|value| value.checked_add(self.calendar_write_operations()))
            .context("native aggregate operation count overflow")
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
        self.validate_calendar_inputs(&calendar_inputs).await?;
        ensure!(
            !self.native_append_only || batch.column_by_name(UPDATING_META_FIELD).is_none(),
            "append-only native aggregate received changelog metadata",
        );
        let changelog = if self.native_append_only {
            None
        } else {
            Some(native_changelog_columns(batch)?)
        };
        let limits = store.limits();
        let fallback = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
            .count();
        // Retain the conservative keyed workspace allowance so membership
        // co-location does not change admitted chunk/commit boundaries.
        let worst_operations = self.native_operations_per_image()?;
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
        // One optional logical member bound for this serial input batch only.
        // Error/cancellation, the next input batch and recovery are always cold.
        let mut member_progress = None;
        let mut allow_member_progress = true;
        let mut allow_calendar_progress = true;
        let unknown_members = self.aggregates.iter().any(|aggregate| {
            aggregate.accumulator_type == AccumulatorType::Batch && !aggregate.injected_timestamp
        });
        for start in (0..batch.num_rows()).step_by(rows_per_chunk) {
            let end = batch.num_rows().min(start + rows_per_chunk);
            // Without member indexes, the existing point-read path is safe
            // even for retractions. Indexed appends (including collections
            // and TTL rollover) also use only keyed reads and this owner's overlay.
            let has_retracts =
                changelog.is_some_and(|(flags, _)| (start..end).any(|row| flags.value(row)));
            loop {
                let mut had_progress = member_progress.is_some();
                let prepared: Result<AggregateScope<'_>> = async {
                    // Taking ownership before admission makes every failure and
                    // cancellation drop the optional key with its permit.
                    let mut pending = member_progress.take();
                    let mut scope = if self.calendars.is_empty() && (fallback == 0 || !has_retracts)
                    {
                        store.begin_point().await?
                    } else {
                        store.begin().await?
                    };
                    if allow_member_progress && has_retracts && unknown_members && pending.is_none()
                    {
                        pending = store.try_member_progress(
                            self.native_config
                                .context("native aggregate limits missing")?
                                .max_pending_output_bytes,
                        );
                    }
                    had_progress |= pending.is_some();
                    scope.set_member_progress(pending);
                    // Construct inside this preparation attempt: never carry W
                    // across a commit, callback, cancellation or cold retry.
                    let mut progress = if allow_calendar_progress && !self.calendars.is_empty() {
                        CalendarInputProgress::try_new(
                            store,
                            self.native_config
                                .context("native aggregate limits missing")?
                                .max_pending_output_bytes,
                        )
                    } else {
                        None
                    };
                    had_progress |= progress.is_some();
                    for (row, key) in keys.iter().enumerate().take(end).skip(start) {
                        let retract = changelog.is_some_and(|(flags, _)| flags.value(row));
                        let row_id = changelog.map(|(_, ids)| ids.value(row));
                        self.native_process_calendar_event_with_progress(
                            &mut scope,
                            NativeCalendarEvent {
                                group_key: key,
                                inputs: &inputs,
                                row,
                                retract,
                                row_id,
                                calendar_inputs: &calendar_inputs,
                            },
                            &mut progress,
                        )
                        .await?;
                    }
                    Ok(scope)
                }
                .await;
                let scope = match prepared {
                    Err(error) if had_progress && native_decoded_resource_exhausted(&error) => {
                        // Preparation never writes the backend. Its failed
                        // scope has dropped; retry only this uncommitted chunk
                        // cold and keep optional progress disabled this batch.
                        allow_member_progress = false;
                        allow_calendar_progress = false;
                        member_progress = None;
                        continue;
                    }
                    result => result?,
                };
                // Never retry a commit: application may precede its ACK.
                member_progress = scope.commit_member_progress().await?;
                break;
            }
        }
        Ok(())
    }

    /// One admitted page of due expirations. The caller drains these pages in
    /// the same flush, yielding and emitting bounded changelog batches between
    /// pages so finite TTL does not acquire a new delayed-expiry policy.
    async fn expire_native(&self, progress: Option<&mut AggregateExpiryProgress>) -> Result<bool> {
        if self.retain_indefinitely {
            return Ok(false);
        }
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let ttl_nanos = i64::try_from(self.ttl.as_nanos())?;
        let now = to_nanos(SystemTime::now()) as i64;
        // Retain the conservative keyed workspace allowance so membership
        // co-location does not change admitted chunk/commit boundaries.
        let operations_per_expiry = 4
            + usize::from(self.native_has_group_keys())
            + self.aggregates.iter().filter(|a| a.replay_members).count();
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
        let mut after = progress
            .as_ref()
            .and_then(|progress| progress.after.clone());
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
                let mut group = self
                    .native_group_codec()?
                    .decode(&bytes, scope.limits().value_bytes)?;
                if group.last_update_nanos.saturating_add(ttl_nanos) == deadline {
                    let old_generation = group.generation;
                    group.generation = group
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("aggregate generation overflow"))?;
                    group.next_ordinal = 0;
                    group.live_rows = 0;
                    group.calendar_references.clear();
                    group.accumulator_state =
                        self.native_state_values(&mut self.native_accumulators(None)?)?;
                    group.last_update_nanos = now;
                    scope.put(
                        &storage_key,
                        &self
                            .native_group_codec()?
                            .encode(&group, scope.limits().value_bytes)?,
                    )?;
                    scope.put(&native_group_key(b'D', group_key)?, &[1])?;
                    scope.put(&native_cleanup_key(group_key, old_generation)?, b"M")?;
                    for (index, aggregate) in self.aggregates.iter().enumerate() {
                        if aggregate.replay_members {
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
            // Commit before publishing an exclusive logical lower bound. A
            // cancelled/failed flush drops it and the next flush starts at E.
            if let Some(progress) = progress {
                progress.after = after.map(|key| key.into_boxed_slice().into_vec());
            }
        }
        Ok(processed > 0)
    }

    #[cfg(test)]
    async fn cleanup_native(&self) -> Result<bool> {
        self.cleanup_native_with_progress(&mut NativeCleanupProgress::default())
            .await
    }

    async fn cleanup_native_with_progress(
        &self,
        progress: &mut NativeCleanupProgress,
    ) -> Result<bool> {
        let result = self.cleanup_native_step(progress).await;
        // Another owner can consume decoded headroom after cursor creation.
        // The failed step has released its uncommitted scope before we discard
        // optional keys and retry the established cold path. Other failures,
        // including write admission, keep their original error and cursor.
        if progress._permit.is_some()
            && result
                .as_ref()
                .is_err_and(native_decoded_resource_exhausted)
        {
            *progress = NativeCleanupProgress::default();
            self.cleanup_native_step(progress).await
        } else {
            result
        }
    }

    async fn cleanup_native_step(&self, progress: &mut NativeCleanupProgress) -> Result<bool> {
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let mut scope = store.begin().await?;
        let pending = if progress._permit.is_some() {
            match progress.marker.as_deref() {
                Some(key) if !progress.completed => match scope.get(key).await? {
                    Some(phase) => Some((key.to_vec(), phase)),
                    // An atomic terminal deletion may have completed before its
                    // future was cancelled. Its absent marker is safe to pass.
                    None => scope.first_from(b"C", Some(key)).await?,
                },
                after => scope.first_from(b"C", after).await?,
            }
        } else {
            scope.first(b"C").await?
        };
        let Some((cleanup_key, phase)) = pending else {
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
                || phase == b"U"
                || phase == b"T",
            "invalid aggregate cleanup phase"
        );
        if phase == b"T" {
            if !self.native_has_group_keys() {
                scope.delete(&cleanup_key)?;
                scope.commit().await?;
                progress.committed(cleanup_key, b'T', None, true);
                return Ok(true);
            }
            let mut marker_retired = true;
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
                if let Some(bytes) = scope.get(&group_key).await? {
                    let current = self
                        .native_group_codec()?
                        .decode(&bytes, scope.limits().value_bytes)?;
                    if current.live_rows == 0
                        && scope.get(&native_group_key(b'D', group)?).await?.is_none()
                    {
                        // The serial owner has already drained the dirty row,
                        // including its emitted retract. G owns membership
                        // atomically. Leave the harmless T
                        // marker until the next cleanup step; its only
                        // remaining action is to delete itself.
                        scope.delete(&group_key)?;
                        marker_retired = false;
                    } else {
                        scope.delete(&cleanup_key)?;
                    }
                } else {
                    scope.delete(&cleanup_key)?;
                }
            }
            scope.commit().await?;
            progress.committed(cleanup_key, b'T', None, marker_retired);
            return Ok(true);
        }
        let prefix = native_generation_prefix(phase[0], group, generation)?;
        let mut after = if progress._permit.is_some()
            && progress.marker.as_ref() == Some(&cleanup_key)
            && progress.phase == Some(phase[0])
            && !progress.completed
        {
            progress.member_after.clone()
        } else {
            None
        };
        let mut exhausted = false;
        // U scans one pointer but may delete both that pointer and its owned
        // deadline. Divide write/overlay capacities, never the scan-page cap.
        let writes_per_member = if phase == b"U" { 2 } else { 1 };
        let max_entries = store
            .limits()
            .page_entries
            .min(store.limits().write_operations.saturating_sub(1) / writes_per_member)
            .min(
                (store.limits().write_bytes / store.max_encoded_entry_bytes()).saturating_sub(1)
                    / writes_per_member,
            )
            .min(
                (store.limits().overlay_bytes
                    / (store.limits().key_bytes + store.limits().value_bytes))
                    .saturating_sub(1)
                    / writes_per_member,
            );
        if phase == b"U" {
            ensure!(
                max_entries > 0,
                "calendar cleanup cannot admit a due pointer"
            );
        } else {
            ensure!(
                max_entries > 0,
                "native aggregate cleanup requires two write operations"
            );
        }
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
                    b"J" => b"U",
                    _ => b"T",
                };
                scope.put(&cleanup_key, next)?;
            }
        }
        scope.commit().await?;
        progress.committed(
            cleanup_key,
            phase[0],
            if exhausted { None } else { after },
            false,
        );
        Ok(true)
    }

    async fn flush_native(
        &self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if (self.emit_per_event && self.calendars.is_empty())
            || self.response_root_operator.is_some()
        {
            return Ok(());
        }
        self.prune_calendar_buckets().await?;
        // Existing dirty groups are emitted before expiration, matching the
        // legacy flush order (updates first, then TTL retractions).
        self.drain_native_dirty(ctx, collector).await?;
        // Input cannot interleave with this serial flush. Dirty emission only
        // deletes expiry entries, so no earlier E insertion can be skipped.
        // Keep only an admitted logical key across fresh per-chunk snapshots;
        // cancellation, errors, the next flush and recovery all start cold.
        let store = self
            .native_store
            .as_ref()
            .context("native aggregate store missing")?;
        let output_bytes = self
            .native_config
            .context("native aggregate limits missing")?
            .max_pending_output_bytes;
        let mut expiry_progress = store.try_expiry_progress(output_bytes);
        while self.expire_native(expiry_progress.as_mut()).await? {
            self.drain_native_dirty(ctx, collector).await?;
            tokio::task::yield_now().await;
        }
        drop(expiry_progress);
        // Each invocation owns at most one admitted cleanup page. Drain the
        // finite backlog through EOF/barriers as well as timer flushes, so an
        // idle keyed group does not retain a tombstone indefinitely.
        let mut cleanup = NativeCleanupProgress::new(
            self.native_store
                .as_ref()
                .context("native aggregate store missing")?,
        );
        while self.cleanup_native_with_progress(&mut cleanup).await? {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// Same known-key final evaluation for ordinary flush and response mode.
    /// It never walks the dirty index, and leaves admission/mutation to caller.
    async fn native_group_delta(
        &self,
        scope: &AggregateScope<'_>,
        key: &[u8],
        group: &EncodedGroup,
    ) -> Result<NativeGroupDelta> {
        let next = self.native_accepted_values(scope, key, group).await?;
        Self::native_group_delta_from_values(
            key,
            group,
            (!self.native_has_group_keys() || group.live_rows > 0).then_some(next),
        )
    }

    fn native_group_delta_from_values(
        key: &[u8],
        group: &EncodedGroup,
        values: Option<Vec<ScalarValue>>,
    ) -> Result<NativeGroupDelta> {
        let append = values.is_some();
        let next = values.unwrap_or_default();
        let unchanged = append
            && group.last_emitted.as_ref().is_some_and(|old| {
                old.iter()
                    .zip(next.iter())
                    .take(old.len().saturating_sub(1))
                    .all(|(old, new)| old == new)
            });
        let row_bytes = |values: &[ScalarValue]| {
            values
                .iter()
                .map(ScalarValue::size)
                .fold(0usize, usize::saturating_add)
                .saturating_add(key.len())
                .saturating_add(std::mem::size_of::<Vec<u8>>())
        };
        let retained_bytes = if unchanged {
            0
        } else {
            group
                .last_emitted
                .as_ref()
                .map_or(0, |old| row_bytes(old))
                .checked_add(if append { row_bytes(&next) } else { 0 })
                .context("aggregate retained output size overflow")?
        };
        Ok(NativeGroupDelta {
            append,
            unchanged,
            next,
            retained_bytes,
        })
    }
    fn append_native_group_delta(
        key: &[u8],
        group: &mut EncodedGroup,
        delta: NativeGroupDelta,
        keys: &mut Vec<Vec<u8>>,
        values: &mut [Vec<ScalarValue>],
        retracts: &mut Vec<bool>,
    ) {
        if delta.unchanged {
            return;
        }
        if let Some(old) = group.last_emitted.take() {
            keys.push(key.to_vec());
            retracts.push(true);
            for (column, value) in values.iter_mut().zip(old) {
                column.push(value);
            }
        }
        if delta.append {
            keys.push(key.to_vec());
            retracts.push(false);
            for (column, value) in values.iter_mut().zip(&delta.next) {
                column.push(value.clone());
            }
            group.last_emitted = Some(delta.next);
        }
    }

    /// Build the ordinary native before/after relation; no accepted-snapshot
    /// timestamp override occurs inside a branch.
    fn native_changelog_batch(
        &self,
        ctx: &OperatorContext,
        output_keys: Vec<Vec<u8>>,
        output_values: Vec<Vec<ScalarValue>>,
        is_retracts: Vec<bool>,
    ) -> Result<Option<RecordBatch>> {
        if output_keys.is_empty() {
            return Ok(None);
        }
        let configured = self
            .native_config
            .context("native aggregate limits are missing")?;
        let parser = self.key_converter.parser();
        let mut columns = self
            .key_converter
            .convert_rows(output_keys.iter().map(|key| parser.parse(key.as_slice())))?;
        for column in output_values {
            columns.push(ScalarValue::iter_to_array(column)?);
        }
        let record_batch = RecordBatch::try_new(self.schema_without_metadata.clone(), columns)?;
        ensure!(
            record_batch.get_array_memory_size() <= configured.max_pending_output_bytes,
            "native aggregate output exceeds configured pending-output budget"
        );
        if self.emit_per_event {
            // This private direct boundary publishes accepted snapshots. Native
            // maintenance still computes/commits its ordinary replacement, but
            // only the final present relation is a MERGE USING row. Absence
            // never implicitly deletes the target. Keep its stored native clock.
            let accepted = arrow::compute::filter_record_batch(
                &record_batch,
                &BooleanArray::from(
                    is_retracts
                        .iter()
                        .map(|retract| !retract)
                        .collect::<Vec<_>>(),
                ),
            )?;
            return Ok((accepted.num_rows() != 0).then_some(accepted));
        }
        let metadata = self
            .metadata_expr
            .evaluate(&record_batch)?
            .into_array(record_batch.num_rows())?;
        let metadata = set_retract_metadata(metadata, Arc::new(BooleanArray::from(is_retracts)));
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
        Ok(Some(final_batch))
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
        // Retain the conservative keyed workspace allowance so membership
        // co-location does not change admitted chunk/commit boundaries.
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
            let requested = configured
                .max_pending_output_bytes
                .checked_mul(3)
                .context("native output workspace overflow")?;
            let _output_permit =
                store
                    .resources()
                    .try_decoded_value(requested)
                    .map_err(|error| {
                        anyhow!(
                            "aggregate output decoded reservation requested={requested}: {error}"
                        )
                    })?;
            let mut scope = store.begin().await?;
            let mut output_keys = Vec::new();
            let mut output_values = vec![Vec::new(); self.aggregates.len()];
            let mut is_retracts = Vec::new();
            let mut retained_output_bytes = 0usize;
            let mut scanned = 0usize;
            let mut exhausted = false;
            while scanned < max_dirty_groups {
                let Some((dirty_key, _)) = scope.first_dirty_from(after.as_deref()).await? else {
                    exhausted = true;
                    break;
                };
                ensure!(dirty_key.len() >= 5, "invalid native aggregate dirty key");
                let group_key = dirty_key[5..].to_vec();
                let group_storage = native_group_key(b'G', &group_key)?;
                if let Some(bytes) = scope.get(&group_storage).await? {
                    let mut group = self
                        .native_group_codec()?
                        .decode(&bytes, scope.limits().value_bytes)?;
                    let delta = self.native_group_delta(&scope, &group_key, &group).await?;
                    let append = delta.append;
                    let admitted = retained_output_bytes
                        .checked_add(delta.retained_bytes)
                        .context("aggregate retained output size overflow")?;
                    if admitted > configured.max_pending_output_bytes / 3 {
                        ensure!(
                            scanned > 0,
                            "one native aggregate changelog pair exceeds configured pending-output budget"
                        );
                        break;
                    }
                    retained_output_bytes = admitted;
                    Self::append_native_group_delta(
                        &group_key,
                        &mut group,
                        delta,
                        &mut output_keys,
                        &mut output_values,
                        &mut is_retracts,
                    );
                    if !append && self.native_has_group_keys() {
                        self.retire_native_group(&mut scope, &group_key, &mut group)?;
                    }
                    scope.put(
                        &group_storage,
                        &self
                            .native_group_codec()?
                            .encode(&group, scope.limits().value_bytes)?,
                    )?;
                }
                scope.delete(&dirty_key)?;
                after = Some(dirty_key);
                scanned += 1;
            }
            if scanned == 0 {
                return Ok(());
            }
            let final_batch =
                self.native_changelog_batch(ctx, output_keys, output_values, is_retracts)?;
            scope.commit().await?;
            if let Some(final_batch) = final_batch {
                collector.collect(final_batch).await?;
            }
            if exhausted {
                return Ok(());
            }
        }
    }

    fn retire_native_group(
        &self,
        scope: &mut AggregateScope<'_>,
        key: &[u8],
        group: &mut EncodedGroup,
    ) -> Result<()> {
        // Share ordinary retirement, including calendar B/J/U/H cleanup. A
        // recreate uses a new generation and cannot be erased by old cleanup.
        let old_generation = group.generation;
        group.generation = group
            .generation
            .checked_add(1)
            .context("aggregate generation overflow")?;
        group.next_ordinal = 0;
        group.calendar_references.clear();
        group.last_emitted = None;
        scope.put(&native_cleanup_key(key, old_generation)?, b"M")?;
        if !self.retain_indefinitely {
            let deadline = group
                .last_update_nanos
                .saturating_add(i64::try_from(self.ttl.as_nanos())?);
            scope.delete(&native_expiry_key(deadline, key)?)?;
        }
        Ok(())
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
            .filter(|aggregate| aggregate.replay_members)
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
            native_decoded_workspace(
                limits,
                self.aggregates
                    .iter()
                    .any(|aggregate| aggregate.selector.is_some())
            )? <= resources.config().decoded_value_bytes,
            "native aggregate decoded pool cannot hold output, scope, and selector read together"
        );
        let has_selector = self
            .aggregates
            .iter()
            .any(|aggregate| aggregate.selector.is_some());
        if has_selector {
            let namespace = StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: ctx.task_info.task_index,
                    parallelism: ctx.task_info.parallelism,
                },
                table: NATIVE_AGGREGATE_TABLE.as_bytes().to_vec(),
            };
            let encoded_key = aggregate_encoded_key_bound(&namespace, limits.key_bytes)?;
            validate_selector_owner_decoded(limits, resources.config(), encoded_key)?;
        }
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
        let mut store = AggregateStore::new(backend, table, resources, native_limits(limits))?;
        if has_selector {
            store.preserve_selector_checkpoint_headroom();
        }
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
    fn event_response_role(&self) -> EventResponseRole {
        self.response_root_operator
            .map_or(EventResponseRole::Ordinary, EventResponseRole::Aggregate)
    }
    fn event_response_output(&self) -> Option<Arc<EventEdgeSpec>> {
        self.event_output.clone()
    }
    fn bind_event_response(
        &mut self,
        inputs: &[Arc<EventEdgeSpec>],
        context: &OperatorContext,
    ) -> DataflowResult<Option<Arc<EventEdgeSpec>>> {
        let output = context.out_schema.as_deref().ok_or_else(|| {
            arroyo_rpc::errors::DataflowError::ArgumentError(
                "response operator output schema missing".into(),
            )
        })?;
        let root = self
            .response_root_operator
            .ok_or_else(|| anyhow!("aggregate has no planned response mode"))?;
        if inputs.len() != 1
            || inputs[0].root_operator != root
            || inputs[0].root_subtask != 0
            || !matches!(inputs[0].layout, EventLayout::Raw)
            || self.event_output.is_some()
        {
            return Err(
                anyhow!("aggregate response binding requires one matching raw root").into(),
            );
        }
        let (native_input, native_output) = self
            .response_schemas
            .as_ref()
            .context("response aggregate constructed schemas missing")?;
        if context.in_schemas.len() != 1
            || context.in_schemas[0].as_ref() != native_input
            || output != native_output
        {
            return Err(anyhow!(
                "response aggregate graph schema differs from actual constructed native owner"
            )
            .into());
        }
        let limits = self
            .native_config
            .context("aggregate response native limits missing")?;
        let spec = Arc::new(EventEdgeSpec {
            root_operator: root,
            root_subtask: 0,
            schema: output.schema.clone(),
            layout: EventLayout::Native {
                max_rows: limits.max_pending_output_rows.min(4),
                owner_output_bytes: limits.max_pending_output_bytes,
                encoded_key_bytes: limits.key_bytes,
            },
        });
        crate::event_response_codec::ResponseSpec::from_edge(&spec)?;
        self.event_output = Some(spec.clone());
        Ok(Some(spec))
    }

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

    async fn process_response_index(
        &mut self,
        index: usize,
        in_partitions: usize,
        response: EventResponse,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if index != 0 || in_partitions != 1 || ctx.in_schemas.len() != 1 {
            return Err(anyhow!("branch response aggregate requires one input partition").into());
        }
        let prepared = self.prepare_native_response(response, ctx).await?;
        // Preserve typed collector errors and retain output ownership while
        // publication awaits. Native working scope/snapshot already dropped.
        let delivered = collector.collect_response(prepared.response).await;
        drop(prepared._output);
        delivered
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.response_root_operator.is_some() {
            return Err(anyhow!("ordinary Data on response aggregate input").into());
        }
        if self.emit_per_event {
            return self
                .process_native_event_batch(&batch, ctx, collector)
                .await;
        }
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
        watermark: arroyo_types::Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<arroyo_types::Watermark>> {
        if self.native_config.is_some() {
            self.calendar_watermark(watermark).await?;
            if !self.calendars.is_empty()
                && let arroyo_types::Watermark::EventTime(time) = watermark
                && time != arroyo_types::from_nanos(u64::MAX as u128)
            {
                // Emit accepted data first. Every due owner commits G/H/U/D
                // atomically, drops its snapshot and drains normal CDC before
                // the next owner or downstream watermark can advance.
                self.flush_native(ctx, collector).await?;
                let store = self
                    .native_store
                    .as_ref()
                    .context("calendar aggregate store missing")?;
                let output_bytes = self
                    .native_config
                    .context("native aggregate limits missing")?
                    .max_pending_output_bytes;
                // Reuse TTL's optional backend-state key reservation, including
                // the existing D-output peak. Tight configurations stay cold.
                let mut continuation = store.try_expiry_progress(output_bytes);
                while self.dispatch_calendar_due(continuation.as_mut()).await? {
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
        ((!self.emit_per_event || !self.calendars.is_empty())
            && self.response_root_operator.is_none())
        .then_some(self.flush_interval)
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
            self.prune_calendar_buckets().await?;
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

/// Validate the complete callback before writing any operation. Gaps arise
/// naturally from source filters; a marker may never recur after another run.
fn native_event_ranges(
    batch: &RecordBatch,
    retracts: &BooleanArray,
    timestamp: usize,
) -> Result<Vec<std::ops::Range<usize>>> {
    let marker = batch
        .column_by_name(SOURCE_ENVELOPE_FIELD)
        .context("per-event correction requires source envelope")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("source envelope must be UInt64")?;
    ensure!(marker.null_count() == 0, "source envelope contains NULL");
    ensure!(
        batch.column(timestamp).null_count() == 0,
        "source envelope trigger clock contains NULL"
    );
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut previous = None;
    while start < batch.num_rows() {
        let operation = marker.value(start);
        ensure!(
            previous.is_none_or(|last| operation > last),
            "source envelope operations are not strictly ordered"
        );
        let mut end = start + 1;
        while end < batch.num_rows() && marker.value(end) == operation {
            end += 1;
        }
        ensure!(end - start <= 2, "source envelope exceeds two images");
        if end - start == 2 {
            ensure!(
                retracts.value(start) && !retracts.value(start + 1),
                "source envelope pair must retract then append"
            );
            ensure!(
                ScalarValue::try_from_array(batch.column(timestamp), start)?
                    == ScalarValue::try_from_array(batch.column(timestamp), start + 1)?,
                "source envelope images have different trigger clocks"
            );
        }
        ranges.push(start..end);
        previous = Some(operation);
        start = end;
    }
    Ok(ranges)
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
        let emit_per_event = config.emit_per_event;
        let response_root_operator = config.response_root_operator;
        let response_schemas = if response_root_operator.is_some() {
            let input: ArroyoSchema = config
                .input_schema
                .clone()
                .context("response aggregate input schema missing")?
                .try_into()?;
            let output: ArroyoSchema = config
                .final_schema
                .clone()
                .context("response aggregate output schema missing")?
                .try_into()?;
            Some((input, output))
        } else {
            None
        };
        ensure!(
            response_root_operator.is_none()
                || (!emit_per_event
                    && native_config.is_some()
                    && config.retain_indefinitely == Some(true)
                    && config.calendar_aggregates.is_empty()),
            "branch response aggregate requires native state, indefinite retention, no calendar and no accepted-snapshot mode"
        );
        ensure!(
            !emit_per_event
                || (native_config.is_some() && config.retain_indefinitely == Some(true)),
            "per-event aggregate MERGE requires native state and indefinite retention"
        );
        if config.retain_indefinitely == Some(true) && native_config.is_none() {
            bail!("SET updating_ttl = NULL requires worker.aggregate-state native backend limits");
        }
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-updating-aggregate.v2-member-workspace");
        identity.update(&config.aggregate_exec);
        let mut bounds: Vec<_> = config.collection_output_limits.iter().collect();
        bounds.sort_unstable_by_key(|(index, _)| **index);
        for (index, limit) in bounds {
            identity.update(index.to_be_bytes());
            identity.update(limit.to_be_bytes());
        }

        identity.update(&config.metadata_expr);
        if emit_per_event {
            identity.update(b"\0event-results.v1");
        }
        if let Some(root) = response_root_operator {
            identity.update(b"\0event-branch-response.v1");
            identity.update(root.to_be_bytes());
        }
        identity.update(config.ttl_micros.to_be_bytes());
        identity.update([u8::from(config.retain_indefinitely == Some(true))]);
        if let Some(schema) = &config.input_schema {
            identity.update(schema.encode_to_vec());
        }
        if let Some(schema) = &config.final_schema {
            identity.update(schema.encode_to_vec());
        }
        if !config.calendar_aggregates.is_empty() {
            identity.update(b"\0calendar-day-state.v2");
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
        if emit_per_event {
            ensure!(
                native_config.is_some()
                    && final_schema.schema.index_of(UPDATING_META_FIELD).is_err()
                    && (native_append_only
                        || input_schema
                            .schema
                            .field_with_name(SOURCE_ENVELOPE_FIELD)
                            .is_ok_and(|field| field.data_type() == &DataType::UInt64
                                && !field.is_nullable())),
                "per-event aggregate MERGE requires append or complete source-envelope input and accepted snapshot output"
            );
        } else {
            schema_without_metadata.remove(final_schema.schema.index_of(UPDATING_META_FIELD)?);
        }

        let metadata_expr = parse_physical_expr(
            &PhysicalExprNode::decode(&mut config.metadata_expr.as_slice())?,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) =
            aggregate_exec.physical_plan_type.unwrap()
        else {
            bail!("invalid proto -- expected aggregate exec");
        };

        // Physical selector expressions additionally address the canonical arrival
        // column. The graph schema stays raw: the updating owner supplies its
        // persisted per-group ordinal directly to the shared kernel.
        let serialized_expression_schema = aggregate_exec
            .input_schema
            .as_ref()
            .map(Schema::try_from)
            .transpose()?
            .map(Arc::new)
            .unwrap_or_else(|| input_schema.schema.clone());
        // Nonselector plans can omit the metadata-only graph suffix.
        let expression_schema = if serialized_expression_schema
            .index_of(arroyo_planner::grouped_selector::ORDINAL_FIELD)
            .is_ok()
        {
            serialized_expression_schema
        } else {
            ensure!(
                serialized_expression_schema.fields().len() <= input_schema.schema.fields().len()
                    && serialized_expression_schema.fields()[..]
                        == input_schema.schema.fields()
                            [..serialized_expression_schema.fields().len()],
                "aggregate serialized input does not preserve raw input fields"
            );
            input_schema.schema.clone()
        };
        ensure!(
            expression_schema.fields().len() >= input_schema.schema.fields().len()
                && expression_schema.fields()[..input_schema.schema.fields().len()]
                    == input_schema.schema.fields()[..],
            "aggregate physical input does not preserve raw input schema"
        );
        if expression_schema.fields().len() != input_schema.schema.fields().len() {
            ensure!(
                expression_schema.fields().len() == input_schema.schema.fields().len() + 1,
                "aggregate physical input has unexpected extra fields"
            );
            let field = expression_schema.field(input_schema.schema.fields().len());
            ensure!(
                field.name() == arroyo_planner::grouped_selector::ORDINAL_FIELD
                    && field.data_type() == &DataType::UInt64
                    && !field.is_nullable(),
                "aggregate physical input has invalid selector ordinal field"
            );
        }

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

        let mut constructor_workspace = 0usize;
        let aggregates: Vec<_> = aggregate_exec
            .aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .enumerate()
            .map(|(index, (expr, name))| -> DFResult<_> {
                let aggregate = decode_aggregate(&expression_schema, name, expr, registry.as_ref())?;
                let descriptor = if native_config.is_some() && classify(aggregate.fun()).is_none() {
                    aggregate_descriptor(&aggregate, &expression_schema)?
                } else { None };
                // Native SUM persists upstream's retractable [sum,count] state,
                // which differs from its ordinary single-field partial state.
                let descriptor = if aggregate.fun().inner().as_any().is::<datafusion::functions_aggregate::sum::Sum>() && !aggregate.is_distinct() {
                    descriptor.map(|descriptor| descriptor.with_state_layout(&aggregate.sliding_state_fields()?)).transpose()?
                } else { descriptor };
                if let (Some(limits), Some(descriptor)) = (native_config, descriptor) {
                    let floor = descriptor.retained_bound(0,0).ok_or_else(||datafusion::common::DataFusionError::Execution("native accumulator constructor floor overflow".into()))?;
                    constructor_workspace = constructor_workspace.checked_add(floor).ok_or_else(||datafusion::common::DataFusionError::Execution("native accumulator constructor workspace overflow".into()))?;
                    if limits.value_bytes.checked_mul(3).is_none_or(|allowance|constructor_workspace>allowance) {
                        return Err(datafusion::common::DataFusionError::Execution("native accumulator constructors exceed configured decoded-state allowance".into()));
                    }
                }
                Ok((aggregate, filters.get(index).cloned().flatten(), index, descriptor))
            })
            .map_ok(|(agg, filter, _index, descriptor)| {
                let native_state = native_config.is_some();
                let function = agg.fun().name().to_ascii_lowercase();
                let selector = classify(agg.fun()).is_some();
                let retract = if selector { false } else {
                    match agg.create_sliding_accumulator() {
                        Ok(s) => s.supports_retract_batch(),
                        _ => false,
                    }
                };

                // Grouped selectors always use the shared candidate kernel.
                // Other aggregates preserve their established ordinary/sliding
                // state and configured backend admission.
                let verified_fixed = descriptor.is_some_and(|descriptor| matches!(descriptor.capability, AggregateCapability::Fixed { .. }));
                // Sliding extrema hide FIFO history despite scalar state; keep
                // the existing ordered backend index for retractable MIN/MAX.
                let native_sliding = matches!(function.as_str(), "count" | "sum" | "avg")
                    || (verified_fixed && !is_extremum(&agg));
                let append_only_state = native_append_only && !selector
                    && (is_extremum(&agg) || (!retract && verified_fixed));

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
                    descriptor,
                )
            })
            .map_ok(|(agg, t, filter, descriptor)| {
                let is_selector = classify(agg.fun()).is_some();
                if emit_per_event {
                    ensure!(supports_native_result(&agg, &expression_schema)?,
                        "per-event aggregate MERGE requires a verified non-distinct native result capability");
                }
                ensure!(!is_selector || native_config.is_some(),
                    "grouped FIRST_VALUE/LAST_VALUE requires worker.aggregate-state and worker.live-state-resources limits");
                ensure!(!is_selector || native_append_only || agg.order_bys().is_some_and(|order| !order.is_empty()),
                    "native grouped selector requires an explicit ORDER BY for bounded retraction");
                let input_exprs = if is_selector {
                    let mut args = vec![agg.expressions()[0].clone()];
                    if let Some(order) = agg.order_bys() {
                        args.extend(order.iter().map(|sort| sort.expr.clone()));
                    }
                    args
                } else {
                    aggregate_expressions(std::slice::from_ref(&agg), &AggregateMode::Single, 0)?
                        .pop().unwrap()
                };
                let collection = native_config.is_some()
                    && agg
                        .fun()
                        .inner()
                        .as_any()
                        .is::<datafusion::functions_aggregate::array_agg::ArrayAgg>();
                let replay_members = collection || (t == AccumulatorType::Batch && descriptor.is_some() && !is_extremum(&agg));
                if collection {
                    ensure!(agg.expressions().len() == 1, "native ARRAY_AGG requires one value argument");
                }
                if replay_members {
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
                if is_selector {
                    let types = input_exprs.iter().map(|expr| expr.data_type(&input_schema.schema))
                        .collect::<DFResult<Vec<_>>>()?;
                    let limits = native_config.context("grouped selector limits missing")?;
                    SelectorSpec::preflight_types(&types[0], &types[1..], SelectorBudget {
                        key_bytes: limits.key_bytes,
                        value_bytes: limits.value_bytes,
                        decoded_bytes: limits.value_bytes.checked_mul(3)
                            .context("selector decoded allowance overflow")?,
                    })?;
                }
                // Shared selectors own their value codec (including nested Arrow
                // values unsupported by Arrow row ordering). This converter is
                // used only by the other aggregate strategies.
                let row_converter = Arc::new(RowConverter::new(if is_selector { Vec::new() } else {
                    input_exprs
                        .iter()
                        .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                        .collect::<DFResult<_>>()?
                })?);

                let fields = t.state_fields(&agg)?;
                let first_state_col = sliding_state_fields.len();
                sliding_state_fields.extend(fields.into_iter().enumerate().map(|(part, field)| {
                    let field = (*field).clone();
                    if t == AccumulatorType::AppendOnly {
                        // Internal aggregate state fields can repeat names.
                        // Ordinals identify their persisted positions.
                        field.with_name(format!("_native_state_{}", first_state_col + part))
                    } else {
                        field
                    }
                }));
                let state_cols = (first_state_col..sliding_state_fields.len()).collect_vec();

                let (index_converter, index_columns) =
                    if t == AccumulatorType::Batch && native_config.is_some() && !is_selector {
                        let (codec, columns) =
                            native_index_codec(&agg, &input_exprs, &input_schema.schema, replay_members)?;
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
                    (collection, replay_members, descriptor),
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

        if emit_per_event {
            ensure!(
                !key_fields.is_empty()
                    && final_schema.schema.fields().len() == key_fields.len() + aggregates.len()
                    && final_schema.timestamp_index + 1 == final_schema.schema.fields().len(),
                "per-event aggregate MERGE requires keyed verified native result snapshots"
            );
            let (clock, _, _, _, arguments, filter, _, _, _) = aggregates
                .last()
                .context("per-event aggregate has no injected timestamp")?;
            ensure!(
                clock.name() == TIMESTAMP_FIELD
                    && clock.fun().inner().as_any().is::<Max>()
                    && arguments.len() == 1
                    && arguments[0]
                        .as_any()
                        .downcast_ref::<Column>()
                        .is_some_and(|column| column.index() == input_schema.timestamp_index)
                    && filter.is_none(),
                "per-event aggregate timestamp does not match input clock ordinal"
            );
        }

        if response_root_operator.is_some() {
            ensure!(
                !key_fields.is_empty()
                    && final_schema.schema.fields().len()
                        == key_fields.len() + aggregates.len() + 1
                    && final_schema.timestamp_index + 2 == final_schema.schema.fields().len()
                    && final_schema
                        .schema
                        .field(final_schema.schema.fields().len() - 1)
                        == arroyo_rpc::updating_meta_field().as_ref()
                    && aggregates.iter().all(|(agg, kind, _, _, _, _, _, _, _)| {
                        !agg.is_distinct()
                            && matches!(agg.fun().name(), "count" | "max")
                            && agg.order_bys().is_none()
                            && (*kind != AccumulatorType::Batch || !native_append_only)
                    }),
                "branch response aggregate requires keyed native COUNT/MAX changelog output"
            );
            let (clock, _, _, _, arguments, filter, _, _, _) = aggregates
                .last()
                .context("branch response aggregate has no injected timestamp")?;
            ensure!(
                clock.name() == TIMESTAMP_FIELD
                    && clock.fun().inner().as_any().is::<Max>()
                    && arguments.len() == 1
                    && arguments[0]
                        .as_any()
                        .downcast_ref::<Column>()
                        .is_some_and(|column| column.index() == input_schema.timestamp_index)
                    && filter.is_none()
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
                "branch response aggregate timestamp does not match input clock ordinal"
            );
        }

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
            let metadata_suffix = usize::from(!emit_per_event);
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
                        == key_fields.len() + aggregates.len() + metadata_suffix
                    && final_schema.timestamp_index + 1 + metadata_suffix
                        == final_schema.schema.fields().len()
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
            let (agg, _, _, _, _, _, _, _, (collection, _, _)) = aggregates
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
                        (collection, replay_members, descriptor),
                    ),
                )|
                 -> Result<Aggregator> {
                    let member_types = input_exprs
                        .iter()
                        .map(|expr| expr.data_type(&input_schema.schema))
                        .collect::<DFResult<Vec<_>>>()?;
                    let member_decode_base = if replay_members {
                        native_collection_decode_bound(&member_types, 0)?
                    } else {
                        0
                    };
                    Ok(Aggregator {
                        collection_member_bound: if replay_members {
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
                        func: agg.clone(),
                        input_exprs,
                        filter,
                        accumulator_type: t,
                        row_converter,
                        state_cols,
                        index_converter,
                        index_columns,
                        replay_members,
                        array_result: collection,
                        descriptor,
                        member_types,
                        member_decode_base,
                        collection_output_limit: config
                            .collection_output_limits
                            .get(&(index as u32))
                            .filter(|_| native_config.is_some())
                            .map(|limit| usize::try_from(*limit))
                            .transpose()?,
                        injected_timestamp: Some(index) == injected_timestamp_index,
                        selector: if classify(agg.fun()).is_some() {
                            let spec = selector_spec(&agg, &expression_schema)?;
                            let limits =
                                native_config.context("grouped selector state limits missing")?;
                            spec.validate_budget(SelectorBudget {
                                key_bytes: limits.key_bytes,
                                value_bytes: limits.value_bytes,
                                decoded_bytes: limits
                                    .value_bytes
                                    .checked_mul(3)
                                    .context("grouped selector decoded allowance overflow")?,
                            })?;
                            Some(spec)
                        } else {
                            None
                        },
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
        if aggregates
            .iter()
            .any(|aggregate| aggregate.selector.is_some())
        {
            identity.update(b"\0shared-grouped-selector-candidate.v2");
        }
        identity.update(b"\0compact-group-calendar-references.v5");
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

        let mut operator = IncrementalAggregatingFunc {
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
            native_store: None,
            native_codec: None,
            retain_indefinitely: config.retain_indefinitely == Some(true),
            native_schema_identity,
            native_append_only,
            emit_per_event,
            response_root_operator,
            response_previous: None,
            event_output: None,
            response_schemas,
            calendars,
        };
        operator.native_codec = Some(GroupCodec::with_calendar_layout(
            &operator.native_state_types(),
            &operator.native_output_types(),
            &config
                .calendar_aggregates
                .iter()
                .map(|descriptor| descriptor.encode_to_vec())
                .collect::<Vec<_>>(),
        )?);
        Ok(operator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod aggregate_cleanup_tests {
        include!("aggregate_cleanup_tests.rs");
    }
    mod aggregate_duplicate_tests {
        include!("aggregate_duplicate_tests.rs");
    }
    mod aggregate_cleanup_boundary_tests {
        include!("aggregate_cleanup_boundary_tests.rs");
    }
    mod aggregate_response_tests {
        include!("aggregate_response_tests.rs");
    }
    mod aggregate_result_tests {
        include!("aggregate_result_tests.rs");
    }
    use arrow::compute::SortOptions;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arroyo_state::live::{
        Ownership,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
        table::LiveTableManager,
        write::AdmittedWriteBatch,
    };
    use arroyo_types::Watermark;
    use datafusion::execution::FunctionRegistry;
    use datafusion::functions_aggregate::{
        array_agg::array_agg_udaf, count::count_udaf, min_max::max_udaf,
    };
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_proto::physical_plan::to_proto::{
        serialize_physical_aggr_expr, serialize_physical_expr,
    };
    use datafusion_proto::protobuf::{AggregateExecNode, MaybeFilter};

    fn first_value_udaf() -> Arc<datafusion::logical_expr::AggregateUDF> {
        arroyo_planner::grouped_selector_udaf::grouped_selector(
            arroyo_planner::grouped_selector::SelectorKind::First,
            false,
        )
    }
    fn last_value_udaf() -> Arc<datafusion::logical_expr::AggregateUDF> {
        arroyo_planner::grouped_selector_udaf::grouped_selector(
            arroyo_planner::grouped_selector::SelectorKind::Last,
            false,
        )
    }
    fn selector_ordinal_expr() -> Arc<dyn PhysicalExpr> {
        Arc::new(
            datafusion::physical_expr::ScalarFunctionExpr::try_new(
                arroyo_planner::grouped_selector_udaf::ordinal_marker(),
                vec![],
                input_schema().as_ref(),
            )
            .unwrap(),
        )
    }

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
        arroyo_planner::grouped_selector_udaf::register(&mut registry);
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
            let mut builder =
                AggregateExprBuilder::new(function, vec![value.clone(), selector_ordinal_expr()])
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
                    AggregateExprBuilder::new(function, vec![argument, selector_ordinal_expr()])
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
                calendar_aggregates: Vec::new(),
                emit_per_event: false,
                response_root_operator: None,
            },
            Arc::new(registry),
        )
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

    fn native_event_config() -> (UpdatingAggregateOperator, Arc<Registry>) {
        let (mut config, registry) = native_config();
        let mut physical = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut physical.physical_plan_type else {
            panic!("aggregate fixture")
        };
        let selected = [5, 7, 8]; // COUNT, MAX(value), planner-owned MAX(_timestamp).
        aggregate.aggr_expr = selected
            .iter()
            .map(|&i| aggregate.aggr_expr[i].clone())
            .collect();
        aggregate.aggr_expr_name = selected
            .iter()
            .map(|&i| aggregate.aggr_expr_name[i].clone())
            .collect();
        aggregate.filter_expr = selected
            .iter()
            .map(|&i| aggregate.filter_expr[i].clone())
            .collect();
        config.aggregate_exec = physical.encode_to_vec();
        config.input_schema = Some(
            ArroyoSchema::from_schema_keys(input_schema(), vec![0])
                .unwrap()
                .into(),
        );
        let old: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
        let mut fields = vec![input_schema().field(0).clone()];
        fields.extend(selected.iter().map(|&i| old.schema.field(i).clone()));
        config.final_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields)))
                .unwrap()
                .into(),
        );
        config.retain_indefinitely = Some(true);
        config.emit_per_event = true;
        (config, registry)
    }

    async fn native_event_context(config: &UpdatingAggregateOperator) -> OperatorContext {
        let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
        let (control, _rx) = tokio::sync::mpsc::channel(16);
        OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control,
            1,
            vec![Arc::new(input)],
            None,
            HashMap::new(),
        )
        .await
    }

    fn native_correction_config() -> (UpdatingAggregateOperator, Arc<Registry>) {
        let (mut config, registry) = native_event_config();
        let input: ArroyoSchema = config.input_schema.take().unwrap().try_into().unwrap();
        let mut fields = SchemaBuilder::from(input.schema.as_ref().clone());
        fields.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        fields.push(Field::new(SOURCE_ENVELOPE_FIELD, DataType::UInt64, false));
        config.input_schema = Some(
            ArroyoSchema::new(
                Arc::new(fields.finish()),
                input.timestamp_index,
                input.storage_keys().cloned(),
                input.routing_keys().cloned(),
            )
            .into(),
        );
        (config, registry)
    }

    fn correction_batch(
        groups: &[Option<&str>],
        values: &[i64],
        times: &[i64],
        retracts: &[bool],
        ids: &[[u8; 16]],
        markers: &[u64],
    ) -> RecordBatch {
        let input = native_test_changelog_with_ids(&batch_at(groups, values, times), retracts, ids);
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            SOURCE_ENVELOPE_FIELD,
            DataType::UInt64,
            false,
        )));
        let mut columns = input.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from(markers.to_vec())));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    async fn correction_inventory(store: &AggregateStore) -> Vec<(Vec<u8>, Vec<u8>)> {
        let scope = store.begin().await.unwrap();
        let mut entries = Vec::new();
        for prefix in [b"G", b"M", b"R"] {
            let mut after: Option<Vec<u8>> = None;
            while let Some(entry) = scope.first_from(prefix, after.as_deref()).await.unwrap() {
                after = Some(entry.0.clone());
                entries.push(entry);
            }
        }
        entries
    }

    #[test]
    fn per_event_correction_serialized_constructor_validates_injected_clock() {
        let (config, registry) = native_correction_config();
        let decoded = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
        assert!(
            IncrementalAggregatingConstructor::build_with_native_config(
                decoded,
                registry,
                Some(native_test_config())
            )
            .is_ok()
        );
        let (mut config, registry) = native_correction_config();
        let mut physical = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut physical.physical_plan_type else {
            unreachable!()
        };
        aggregate.aggr_expr[2] = aggregate.aggr_expr[1].clone();
        config.aggregate_exec = physical.encode_to_vec();
        let decoded = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
        let error = IncrementalAggregatingConstructor::build_with_native_config(
            decoded,
            registry,
            Some(native_test_config()),
        )
        .err()
        .expect("forged clock must be rejected");
        assert!(
            error
                .to_string()
                .contains("timestamp does not match input clock ordinal"),
            "{error}"
        );
    }

    #[test]
    fn per_event_correction_validates_complete_local_marker_runs() {
        let pair = correction_batch(
            &[Some("a"); 2],
            &[20, 5],
            &[5, 5],
            &[true, false],
            &[[2; 16]; 2],
            &[0, 0],
        );
        assert_eq!(
            native_event_ranges(&pair, native_changelog_columns(&pair).unwrap().0, 3).unwrap(),
            std::iter::once(0..2).collect::<Vec<_>>()
        );
        // Marker zero may be reused by the next complete callback.
        assert_eq!(
            native_event_ranges(&pair, native_changelog_columns(&pair).unwrap().0, 3).unwrap(),
            std::iter::once(0..2).collect::<Vec<_>>()
        );
        for (markers, flags, times) in [
            (vec![0, 0, 0], vec![true, false, false], vec![5, 5, 5]),
            (vec![1, 0], vec![false, false], vec![5, 5]),
            (vec![0, 0], vec![false, true], vec![5, 5]),
            (vec![0, 0], vec![true, false], vec![20, 5]),
            (vec![0, 1, 0], vec![false, false, false], vec![5, 5, 5]),
        ] {
            let len = markers.len();
            let input = correction_batch(
                &vec![Some("a"); len],
                &vec![5; len],
                &times,
                &flags,
                &vec![[2; 16]; len],
                &markers,
            );
            assert!(
                native_event_ranges(&input, native_changelog_columns(&input).unwrap().0, 3)
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn per_event_correction_is_atomic_and_removes_absent_group_state() {
        let (config, registry) = native_correction_config();
        let mut context = native_event_context(&config).await;
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        operator
            .process_batch(
                correction_batch(&[Some("z")], &[99], &[99], &[false], &[[9; 16]], &[0]),
                &mut context,
                &mut AggregateCollector::default(),
            )
            .await
            .unwrap();
        let unrelated = correction_inventory(operator.native_store.as_ref().unwrap()).await;
        assert!(!unrelated.is_empty());
        let mut collector = AggregateCollector::default();
        for input in [
            correction_batch(
                &[Some("a"); 2],
                &[10, 20],
                &[10, 20],
                &[false, false],
                &[[1; 16], [2; 16]],
                &[0, 1],
            ),
            correction_batch(
                &[Some("a"); 2],
                &[20, 5],
                &[5, 5],
                &[true, false],
                &[[2; 16]; 2],
                &[0, 0],
            ),
            correction_batch(&[Some("a")], &[10], &[10], &[true], &[[1; 16]], &[0]),
            correction_batch(&[Some("a")], &[5], &[5], &[true], &[[2; 16]], &[0]),
        ] {
            operator
                .process_batch(input, &mut context, &mut collector)
                .await
                .unwrap();
        }
        let actual = collector
            .batches
            .iter()
            .map(|batch| {
                (0..4)
                    .map(|column| ScalarValue::try_from_array(batch.column(column), 0).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                vec![
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::TimestampNanosecond(Some(10), None)
                ],
                vec![
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::TimestampNanosecond(Some(20), None)
                ],
                vec![
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::TimestampNanosecond(Some(5), None)
                ],
                vec![
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::TimestampNanosecond(Some(10), None)
                ],
            ]
        );
        assert_eq!(
            correction_inventory(operator.native_store.as_ref().unwrap()).await,
            unrelated,
            "cleanup must remove only the absent group and every one of its members"
        );
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        for prefix in [b"D", b"C"] {
            assert!(
                scope.first(prefix).await.unwrap().is_none(),
                "empty group leaked namespace {prefix:?}"
            );
        }
        drop(scope);
        operator
            .process_batch(
                correction_batch(&[Some("a")], &[7], &[7], &[false], &[[2; 16]], &[0]),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(collector.batches.len(), 5);
        assert_eq!(
            ScalarValue::try_from_array(collector.batches[4].column(1), 0).unwrap(),
            ScalarValue::Int64(Some(1))
        );
    }

    #[tokio::test]
    async fn per_event_correction_refuses_two_key_output_without_partial_commit() {
        let (config, registry) = native_correction_config();
        let mut context = native_event_context(&config).await;
        let mut limits = native_test_config();
        limits.max_pending_output_rows = 1;
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(limits),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        let mut collector = AggregateCollector::default();
        operator
            .process_batch(
                correction_batch(
                    &[Some("a"), Some("a"), Some("b")],
                    &[10, 20, 30],
                    &[10, 20, 30],
                    &[false, false, false],
                    &[[1; 16], [2; 16], [3; 16]],
                    &[0, 1, 2],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let before = correction_inventory(operator.native_store.as_ref().unwrap()).await;
        let error = operator
            .process_batch(
                correction_batch(
                    &[Some("a"), Some("b")],
                    &[20, 5],
                    &[5, 5],
                    &[true, false],
                    &[[2; 16]; 2],
                    &[0, 0],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("pending-output row budget"),
            "{error}"
        );
        assert_eq!(collector.batches.len(), 3);
        assert_eq!(
            correction_inventory(operator.native_store.as_ref().unwrap()).await,
            before,
            "refused operation must not change either group's G/M/R entries"
        );
    }

    #[tokio::test]
    async fn per_event_native_caps_rows_and_retains_every_prefix_and_trigger_time() {
        let (config, registry) = native_event_config();
        let mut context = native_event_context(&config).await;
        let mut limits = native_test_config();
        limits.max_pending_output_rows = 2;
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(limits),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        let rows = batch(
            &[Some("k"); 8],
            &[10, 20, 5, 6, 7, 8, 9, 11],
            &[Some(true); 8],
        );
        let mut collector = AggregateCollector::default();
        operator
            .process_batch(rows, &mut context, &mut collector)
            .await
            .unwrap();
        assert_eq!(
            collector
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 2, 2, 2]
        );
        let actual = collector
            .batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |row| {
                    (
                        ScalarValue::try_from_array(batch.column(1), row).unwrap(),
                        ScalarValue::try_from_array(batch.column(3), row).unwrap(),
                    )
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [10, 20, 5, 6, 7, 8, 9, 11]
                .into_iter()
                .enumerate()
                .map(|(row, time)| (
                    ScalarValue::Int64(Some(row as i64 + 1)),
                    ScalarValue::TimestampNanosecond(Some(time), None),
                ))
                .collect::<Vec<_>>()
        );
        let store = operator.native_store.as_ref().unwrap();
        let resources = store.resources().clone();
        let scope = store.begin().await.unwrap();
        assert!(
            scope.first(b"D").await.unwrap().is_none(),
            "event snapshots must not leave ordinary dirty output behind"
        );
        drop(scope);
        operator
            .handle_tick(1, &mut context, &mut collector)
            .await
            .unwrap();
        operator
            .on_close(
                &Some(SignalMessage::EndOfData),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(
            collector.batches.len(),
            4,
            "control messages cannot duplicate event snapshots"
        );
        assert!(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn per_event_native_preserves_delivery_error_domain_and_releases_permits() {
        use arroyo_rpc::errors::{DataflowError, ErrorDomain, RetryHint};
        struct RefusingCollector;
        #[async_trait::async_trait]
        impl Collector for RefusingCollector {
            async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
                Err(DataflowError::ConnectorError {
                    domain: ErrorDomain::User,
                    retry: RetryHint::NoRetry,
                    error: "declared delivery refusal".into(),
                    source: None,
                })
            }
            async fn broadcast_watermark(
                &mut self,
                _: arroyo_types::Watermark,
            ) -> DataflowResult<()> {
                Ok(())
            }
        }
        let (config, registry) = native_event_config();
        let mut context = native_event_context(&config).await;
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.native_store = Some(native_test_store());
        let input = batch(&[Some("k")], &[10], &[Some(true)]);
        let error = operator
            .process_batch(input, &mut context, &mut RefusingCollector)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                DataflowError::ConnectorError {
                    domain: ErrorDomain::User,
                    retry: RetryHint::NoRetry,
                    ..
                }
            ),
            "delivery failure classification changed: {error:?}"
        );
        let resources = operator.native_store.as_ref().unwrap().resources();
        assert!(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .is_ok()
        );
    }

    #[test]
    fn per_event_native_rejects_unsupported_wire_combinations() {
        for unsupported in [
            "finite",
            "changelog",
            "metadata",
            "legacy",
            "calendar",
            "clock",
        ] {
            let (mut config, registry) = native_event_config();
            let mut native = Some(native_test_config());
            match unsupported {
                "finite" => config.retain_indefinitely = None,
                "legacy" => native = None,
                "calendar" => config.calendar_aggregates.push(Default::default()),
                "clock" => {
                    let mut physical =
                        PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
                    let Some(PhysicalPlanType::Aggregate(aggregate)) =
                        &mut physical.physical_plan_type
                    else {
                        unreachable!()
                    };
                    aggregate.aggr_expr[2] = aggregate.aggr_expr[1].clone();
                    config.aggregate_exec = physical.encode_to_vec();
                }
                "changelog" | "metadata" => {
                    let slot = if unsupported == "changelog" {
                        &mut config.input_schema
                    } else {
                        &mut config.final_schema
                    };
                    let old: ArroyoSchema = slot.take().unwrap().try_into().unwrap();
                    let mut fields = SchemaBuilder::from(old.schema.as_ref().clone());
                    fields.push(Field::new(
                        UPDATING_META_FIELD,
                        DataType::Struct(updating_meta_fields()),
                        false,
                    ));
                    *slot = Some(
                        ArroyoSchema::new(
                            Arc::new(fields.finish()),
                            old.timestamp_index,
                            old.storage_keys().cloned(),
                            old.routing_keys().cloned(),
                        )
                        .into(),
                    );
                }
                _ => unreachable!(),
            }
            assert!(
                IncrementalAggregatingConstructor::build_with_native_config(
                    config, registry, native
                )
                .is_err(),
                "admitted {unsupported}"
            );
        }
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
        native_array_operator_with_limits(
            distinct,
            ignore_nulls,
            filtered,
            limit,
            native_test_config(),
        )
    }

    fn native_array_operator_with_limits(
        distinct: bool,
        ignore_nulls: bool,
        filtered: bool,
        limit: Option<u64>,
        native_config: AggregateStateConfig,
    ) -> Result<IncrementalAggregatingFunc> {
        native_array_operator_with_options(
            distinct,
            ignore_nulls,
            filtered,
            limit,
            native_config,
            true,
        )
    }

    fn native_array_operator_with_order(
        distinct: bool,
        ignore_nulls: bool,
        filtered: bool,
        limit: Option<u64>,
        ordered: bool,
    ) -> Result<IncrementalAggregatingFunc> {
        native_array_operator_with_options(
            distinct,
            ignore_nulls,
            filtered,
            limit,
            native_test_config(),
            ordered,
        )
    }

    fn native_array_operator_with_options(
        distinct: bool,
        ignore_nulls: bool,
        filtered: bool,
        limit: Option<u64>,
        native_config: AggregateStateConfig,
        ordered: bool,
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
        } else if ordered {
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
            calendar_aggregates: Vec::new(),
            emit_per_event: false,
            response_root_operator: None,
        };
        IncrementalAggregatingConstructor::build_with_native_config(
            config,
            Arc::new(registry),
            Some(native_config),
        )
    }

    fn native_calendar_operator() -> IncrementalAggregatingFunc {
        native_calendar_operator_with_filters(true)
    }

    fn native_calendar_operator_with_filters(shared: bool) -> IncrementalAggregatingFunc {
        native_calendar_operator_options(shared, false)
    }

    fn native_calendar_operator_options(shared: bool, sum: bool) -> IncrementalAggregatingFunc {
        use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;
        let (mut config, mut registry) = native_config();
        let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else {
            panic!("expected aggregate");
        };
        // Calendar fixtures exercise contribution identities, which may repeat
        // across days. Their unused positions must not impose unrelated live
        // selector row-identity semantics on those inputs.
        for index in 0..5 {
            aggregate.aggr_expr[index] = aggregate.aggr_expr[6].clone();
            aggregate.filter_expr[index].expr = None;
        }
        aggregate.aggr_expr[7] = aggregate.aggr_expr[6].clone();
        aggregate.aggr_expr_name[7] = "selected_week".into();
        aggregate.filter_expr[7] = aggregate.filter_expr[6].clone();
        if sum {
            let function = datafusion::functions_aggregate::sum::sum_udaf();
            Arc::get_mut(&mut registry)
                .unwrap()
                .register_udaf(function.clone())
                .unwrap();
            let expression = Arc::new(
                AggregateExprBuilder::new(
                    function,
                    vec![Arc::new(Column::new("sequence", 1)) as Arc<dyn PhysicalExpr>],
                )
                .schema(input_schema())
                .alias("selected_week")
                .build()
                .unwrap(),
            );
            aggregate.aggr_expr[7] =
                serialize_physical_aggr_expr(expression, &DefaultPhysicalExtensionCodec {})
                    .unwrap();
        }
        if !shared {
            aggregate.filter_expr[7].expr = None;
        }
        let static_filters: Vec<_> = [6, 7]
            .into_iter()
            .map(|index| {
                aggregate.filter_expr[index]
                    .expr
                    .as_ref()
                    .map(|expression| expression.encode_to_vec())
            })
            .collect();
        config.aggregate_exec = plan.encode_to_vec();
        let schema: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
        let mut fields = schema.schema.fields().to_vec();
        for index in 0..5 {
            fields[index] = Arc::new(
                fields[6]
                    .as_ref()
                    .clone()
                    .with_name(format!("unused_count_{index}")),
            );
        }
        fields[7] = Arc::new(fields[6].as_ref().clone().with_name("selected_week"));
        config.final_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields)))
                .unwrap()
                .into(),
        );
        let codec = DefaultPhysicalExtensionCodec {};
        let date: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Date32(Some(200))));
        let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        for (position, (index, horizon)) in [(6, 1), (7, 7)].into_iter().enumerate() {
            config
                .calendar_aggregates
                .push(CalendarAggregateDescriptor {
                    aggregate_index: index,
                    horizon_days: horizon,
                    argument: serialize_physical_expr(
                        &if sum && index == 7 {
                            Arc::new(Column::new("sequence", 1)) as Arc<dyn PhysicalExpr>
                        } else {
                            one.clone()
                        },
                        &codec,
                    )
                    .unwrap()
                    .encode_to_vec(),
                    static_filter: static_filters[position].clone(),
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
        calendar_test_event_for_group(
            operator,
            &GLOBAL_KEY,
            Some(day),
            reference,
            include,
            retract,
            id,
        )
        .await
        .unwrap();
    }

    async fn calendar_test_event_for_group(
        operator: &mut IncrementalAggregatingFunc,
        group: &[u8],
        day: Option<i32>,
        reference: i32,
        include: Option<bool>,
        retract: bool,
        id: &[u8],
    ) -> Result<()> {
        let input = batch(&[Some("value")], &[1], &[include]);
        for calendar in &mut operator.calendars {
            calendar.test_dates(day.unwrap_or(0), reference);
        }
        let mut calendar_inputs = operator.calendar_inputs(&input)?;
        if day.is_none() {
            for input in &mut calendar_inputs {
                input.contribution = arrow_array::Date32Array::from(vec![None::<i32>]);
            }
        }
        operator.validate_calendar_inputs(&calendar_inputs).await?;
        let inputs = operator.compute_inputs(&input)?;
        let store = operator.native_store.as_ref().unwrap();
        let mut scope = store.begin().await?;
        operator
            .native_process_calendar_event(
                &mut scope,
                NativeCalendarEvent {
                    group_key: group,
                    inputs: &inputs,
                    row: 0,
                    retract,
                    row_id: Some(id),
                    calendar_inputs: &calendar_inputs,
                },
            )
            .await?;
        scope.commit().await?;
        Ok(())
    }

    async fn calendar_test_assert_references(
        operator: &IncrementalAggregatingFunc,
        scope: &AggregateScope<'_>,
        key: &[u8],
        reference: i32,
    ) {
        let bytes = scope
            .get(&native_group_key(b'G', key).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = operator
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        for position in 0..operator.calendars.len() {
            assert_eq!(
                group.calendar_references.get(position).unwrap(),
                Some(reference)
            );
        }
    }

    async fn calendar_test_values(operator: &IncrementalAggregatingFunc) -> Vec<ScalarValue> {
        calendar_test_values_for_group(operator, &GLOBAL_KEY).await
    }

    async fn calendar_test_values_for_group(
        operator: &IncrementalAggregatingFunc,
        key: &[u8],
    ) -> Vec<ScalarValue> {
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let bytes = scope
            .get(&native_group_key(b'G', key).unwrap())
            .await
            .unwrap()
            .unwrap();
        let group = operator
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        let mut state = operator.native_accumulators(Some(&group)).unwrap();
        [5, 6, 7]
            .into_iter()
            .map(|index| state[index].evaluate().unwrap())
            .collect()
    }

    async fn calendar_dispatch_context(
        operator: &mut IncrementalAggregatingFunc,
    ) -> OperatorContext {
        // State-only fixtures carry a NULL placeholder; production CDC output
        // requires the typed metadata expression used by the native D drain.
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![
                Arc::new(BooleanArray::from(vec![false])),
                ScalarValue::FixedSizeBinary(16, Some(vec![1; 16]))
                    .to_array()
                    .unwrap(),
            ],
            None,
        );
        operator.metadata_expr = Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
        let mut ctx = native_input_context(input_schema()).await;
        let mut output = SchemaBuilder::from(operator.schema_without_metadata.as_ref().clone());
        output.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        ctx.out_schema = Some(Arc::new(
            ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap(),
        ));
        ctx
    }

    fn calendar_dispatch_watermark(day: i64) -> Watermark {
        Watermark::EventTime(
            arroyo_types::event_time::from_signed_nanos(day * 86_400_000_000_000).unwrap(),
        )
    }

    #[tokio::test]
    async fn calendar_dispatch_finite_watermark_emits_exact_quiet_cdc_and_holds_idle_eof() {
        for day in [200, -2] {
            let mut operator = native_calendar_operator();
            calendar_test_event(&mut operator, day, day, Some(true), false, &[1; 16]).await;
            let mut ctx = calendar_dispatch_context(&mut operator).await;
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
            let scope = operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap();
            assert!(scope.get(b"W").await.unwrap().is_none());
            drop(scope);
            operator
                .handle_watermark(
                    calendar_dispatch_watermark(i64::from(day) + 1),
                    &mut ctx,
                    &mut collector,
                )
                .await
                .unwrap();
            assert_eq!(
                collector.batches.len(),
                2,
                "accepted refresh precedes quiet replacement"
            );
            let refresh = IncrementalAggregatingFunc::get_retracts(&collector.batches[0]).unwrap();
            assert_eq!(refresh.values().iter().collect::<Vec<_>>(), vec![false]);
            let replacement = &collector.batches[1];
            let retracts = IncrementalAggregatingFunc::get_retracts(replacement).unwrap();
            assert_eq!(
                retracts.values().iter().collect::<Vec<_>>(),
                vec![true, false]
            );
            for (row, recent) in [(0, 1), (1, 0)] {
                assert_eq!(
                    ScalarValue::try_from_array(replacement.column(5), row).unwrap(),
                    ScalarValue::Int64(Some(1))
                );
                assert_eq!(
                    ScalarValue::try_from_array(replacement.column(6), row).unwrap(),
                    ScalarValue::Int64(Some(recent))
                );
                assert_eq!(
                    ScalarValue::try_from_array(replacement.column(7), row).unwrap(),
                    ScalarValue::Int64(Some(1))
                );
                assert_eq!(
                    ScalarValue::try_from_array(replacement.column(8), row).unwrap(),
                    ScalarValue::TimestampNanosecond(Some(1), None)
                );
            }
            let emitted = collector.batches.len();
            for watermark in [
                calendar_dispatch_watermark(i64::from(day)),
                calendar_dispatch_watermark(i64::from(day) + 1),
                Watermark::Idle,
                Watermark::EventTime(arroyo_types::from_nanos(u64::MAX as u128)),
            ] {
                operator
                    .handle_watermark(watermark, &mut ctx, &mut collector)
                    .await
                    .unwrap();
            }
            assert_eq!(collector.batches.len(), emitted);
            assert_eq!(
                calendar_test_values(&operator).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1))
                ]
            );
        }
    }

    #[tokio::test]
    async fn calendar_dispatch_cancelled_scope_and_stale_generation_preserve_pending_owner() {
        let mut operator = native_calendar_operator();
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
        operator
            .calendar_watermark(calendar_dispatch_watermark(201))
            .await
            .unwrap();
        let store = operator.native_store.as_ref().unwrap();
        {
            let mut scope = store.begin().await.unwrap();
            operator
                .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 201)
                .await
                .unwrap();
            // Dropping the uncommitted scope is cancellation before publication.
        }
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        {
            let mut scope = store.begin().await.unwrap();
            scope
                .put(
                    &calendar_native::calendar_due_key(199 * 86_400_000_000_000, &GLOBAL_KEY)
                        .unwrap(),
                    &999_u64.to_be_bytes(),
                )
                .unwrap();
            scope.commit().await.unwrap();
        }
        assert!(
            operator.dispatch_calendar_due(None).await.unwrap(),
            "retire stale entry only"
        );
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![ScalarValue::Int64(Some(1)); 3]
        );
        assert!(operator.dispatch_calendar_due(None).await.unwrap());
        assert!(!operator.dispatch_calendar_due(None).await.unwrap());
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
    async fn calendar_dispatch_equal_deadlines_cursor_matches_cold_and_restarts_for_new_keys() {
        for warm in [false, true] {
            let mut operator = native_calendar_operator();
            operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
                page_entries: 1,
                ..native_test_store().limits()
            }));
            let groups: Vec<_> = (1_u8..=24).map(|key| vec![key]).collect();
            for (index, group) in groups.iter().enumerate() {
                calendar_test_event_for_group(
                    &mut operator,
                    group,
                    Some(200),
                    200,
                    Some(true),
                    false,
                    &[index as u8; 16],
                )
                .await
                .unwrap();
            }
            operator
                .calendar_watermark(calendar_dispatch_watermark(201))
                .await
                .unwrap();
            let store = operator.native_store.as_ref().unwrap();
            let mut continuation = if warm {
                Some(
                    store
                        .try_expiry_progress(
                            operator.native_config.unwrap().max_pending_output_bytes,
                        )
                        .unwrap(),
                )
            } else {
                None
            };
            for group in &groups {
                assert!(
                    operator
                        .dispatch_calendar_due(continuation.as_mut())
                        .await
                        .unwrap()
                );
                if let Some(progress) = &continuation {
                    assert_eq!(
                        progress.after.as_ref().unwrap(),
                        &calendar_native::calendar_due_key(201 * 86_400_000_000_000, group)
                            .unwrap()
                    );
                }
                assert_eq!(
                    calendar_test_values_for_group(&operator, group).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(Some(1))
                    ]
                );
            }
            assert!(
                !operator
                    .dispatch_calendar_due(continuation.as_mut())
                    .await
                    .unwrap()
            );
            drop(continuation);
            // A newly inserted smaller key must be visible to the next cold
            // handler even when its day/deadline matches the just-drained set.
            calendar_test_event_for_group(
                &mut operator,
                &[0],
                Some(201),
                201,
                Some(true),
                false,
                &[99; 16],
            )
            .await
            .unwrap();
            operator
                .calendar_watermark(calendar_dispatch_watermark(202))
                .await
                .unwrap();
            assert!(operator.dispatch_calendar_due(None).await.unwrap());
            assert_eq!(
                calendar_test_values_for_group(&operator, &[0]).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1))
                ]
            );
        }
    }

    #[tokio::test]
    async fn calendar_dispatch_one_entry_pages_many_keys_and_deleted_retaining_owner() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root = std::env::temp_dir()
                .join(format!("calendar-dispatch-churn-{}", uuid::Uuid::new_v4()));
            let template = native_test_store();
            let resources = template.resources().clone();
            let limits = AggregateStoreLimits {
                page_entries: 1,
                ..template.limits()
            };
            drop(template);
            let native = if rocks {
                Some(Arc::new(
                    RocksLiveState::open(
                        RocksStateConfig {
                            root: root.clone(),
                            job_id: "calendar-dispatch-churn".into(),
                            operator_id: "aggregate".into(),
                            subtask: 0,
                            generation: 0,
                            attempt: 0,
                        },
                        resources.clone(),
                    )
                    .await
                    .unwrap(),
                ))
            } else {
                None
            };
            let backend: Arc<dyn LiveStateBackend> = if let Some(native) = &native {
                native.clone()
            } else {
                Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap())
            };
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let mut operator = native_calendar_operator();
            let mut fields = operator.schema_without_metadata.fields().to_vec();
            fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
            operator.schema_without_metadata = Arc::new(Schema::new(fields));
            operator.key_converter =
                RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
            let names: Vec<_> = (0..24).map(|index| format!("key{index:02}")).collect();
            let rows = operator
                .key_converter
                .convert_columns(&[Arc::new(StringArray::from(names))])
                .unwrap();
            let groups: Vec<_> = rows.iter().map(|row| row.as_ref().to_vec()).collect();
            operator.native_store = Some(
                AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap(),
            );
            for (index, group) in groups.iter().enumerate() {
                calendar_test_event_for_group(
                    &mut operator,
                    group,
                    Some(200),
                    200,
                    Some(true),
                    false,
                    &[index as u8; 16],
                )
                .await
                .unwrap();
            }
            calendar_test_event_for_group(
                &mut operator,
                &groups[0],
                Some(200),
                200,
                Some(true),
                true,
                &[0; 16],
            )
            .await
            .unwrap();
            let mut ctx = calendar_dispatch_context(&mut operator).await;
            let mut collector = AggregateCollector::default();
            operator
                .handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut collector)
                .await
                .unwrap();
            let scope = operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap();
            assert!(
                scope
                    .get(&native_group_key(b'G', &groups[0]).unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                scope
                    .get(
                        &calendar_native::calendar_due_key(201 * 86_400_000_000_000, &groups[0])
                            .unwrap()
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            for group in &groups[1..] {
                assert!(
                    scope
                        .get(
                            &calendar_native::calendar_due_key(202 * 86_400_000_000_000, group)
                                .unwrap()
                        )
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            drop(scope);
            for group in &groups[1..] {
                assert_eq!(
                    calendar_test_values_for_group(&operator, group).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(Some(1))
                    ]
                );
            }
            let emitted: usize = collector.batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(emitted, 23 * 3);
            operator
                .handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut collector)
                .await
                .unwrap();
            assert_eq!(
                collector
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                emitted
            );
            // Recreate the retired opaque key after progress; stale owner state
            // must not leak into its lifetime or next due boundary.
            calendar_test_event_for_group(
                &mut operator,
                &groups[0],
                Some(201),
                201,
                Some(true),
                false,
                &[0; 16],
            )
            .await
            .unwrap();
            operator
                .handle_watermark(calendar_dispatch_watermark(202), &mut ctx, &mut collector)
                .await
                .unwrap();
            assert_eq!(
                calendar_test_values_for_group(&operator, &groups[0]).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1))
                ]
            );
            assert_eq!(
                collector
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                emitted + 3
            );
            drop(operator);
            drop(manager);
            drop(backend);
            if let Some(native) = native {
                Arc::try_unwrap(native)
                    .unwrap_or_else(|_| panic!("retained calendar Rocks backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn calendar_dispatch_checkpoint_restores_pending_owner_and_output_on_both_backends() {
        use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for (rocks, pending_output) in [(false, false), (false, true), (true, false), (true, true)]
        {
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
            // Checkpoint a real admitted frontier while its quiet H/U owner
            // still awaits recalculation; W/V and pending work restore together.
            operator
                .calendar_watermark(Watermark::EventTime(
                    arroyo_types::event_time::from_signed_nanos(201 * 86_400_000_000_000).unwrap(),
                ))
                .await
                .unwrap();
            let mut ctx = calendar_dispatch_context(&mut operator).await;
            let mut initial = AggregateCollector::default();
            operator
                .drain_native_dirty(&mut ctx, &mut initial)
                .await
                .unwrap();
            assert_eq!(
                initial
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
            operator
                .calendar_watermark(calendar_dispatch_watermark(201))
                .await
                .unwrap();
            if pending_output {
                // Checkpoint after atomic G/H/U/D commit but before sink ACK.
                assert!(operator.dispatch_calendar_due(None).await.unwrap());
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
            let mut fresh = native_calendar_operator();
            fresh.native_store = Some(restored_store);
            let mut ctx = calendar_dispatch_context(&mut fresh).await;
            let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
            let frontier = crate::arrow::aggregate_codec::CalendarProgress::decode(
                &scope.get(b"W").await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(frontier.watermark_nanos, 201 * 86_400_000_000_000);
            assert!(scope.get(b"V").await.unwrap().is_some());
            let due_day = if pending_output { 202 } else { 201 };
            assert!(
                scope
                    .get(
                        &calendar_native::calendar_due_key(
                            due_day * 86_400_000_000_000,
                            &GLOBAL_KEY
                        )
                        .unwrap()
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
            drop(scope);
            let mut output = AggregateCollector::default();
            // A restored, regressing incoming notification must use W's date.
            fresh
                .handle_watermark(calendar_dispatch_watermark(200), &mut ctx, &mut output)
                .await
                .unwrap();
            assert_eq!(output.batches.len(), 1);
            let batch = &output.batches[0];
            assert_eq!(
                IncrementalAggregatingFunc::get_retracts(batch)
                    .unwrap()
                    .values()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![true, false]
            );
            for (row, recent) in [(0, 1), (1, 0)] {
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(5), row).unwrap(),
                    ScalarValue::Int64(Some(1))
                );
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(6), row).unwrap(),
                    ScalarValue::Int64(Some(recent))
                );
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(7), row).unwrap(),
                    ScalarValue::Int64(Some(1))
                );
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(8), row).unwrap(),
                    ScalarValue::TimestampNanosecond(Some(1), None)
                );
            }
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1))
                ]
            );
            fresh
                .handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut output)
                .await
                .unwrap();
            assert_eq!(output.batches.len(), 1);
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

    mod calendar_progress_tests {
        use super::*;
        use arroyo_state::live::{
            self, LastEntryRequest, ReadOptions, ScanPage, ScanRequest, SnapshotReader, StateKey,
            StateSnapshot, WriteBatch,
        };
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        #[derive(Default)]
        struct Trace {
            reads: Mutex<Vec<usize>>,
            commits: AtomicUsize,
            // Arm only input snapshots (validation is always independent).
            // 1 injects real decoded admission failure; 2 suspends for cancellation.
            gate: AtomicUsize,
            faults: AtomicUsize,
        }
        struct Reader {
            inner: StateSnapshot,
            trace: Arc<Trace>,
            index: usize,
            resources: WorkerStateResources,
        }
        #[async_trait::async_trait]
        impl SnapshotReader for Reader {
            async fn get(
                &self,
                key: &StateKey,
                options: ReadOptions,
            ) -> live::Result<Option<Vec<u8>>> {
                self.try_get(key, options).await
            }
            async fn try_get(
                &self,
                key: &StateKey,
                options: ReadOptions,
            ) -> live::Result<Option<Vec<u8>>> {
                if key.key == b"W" {
                    self.trace.reads.lock().unwrap()[self.index] += 1;
                } else if self.index > 0 && self.trace.reads.lock().unwrap()[self.index] > 0 {
                    match self.trace.gate.swap(0, Ordering::SeqCst) {
                        1 => {
                            // W has been successfully loaded; overlay work is
                            // uncommitted. Fail the next read using the actual
                            // resource pool's decoded exhaustion error.
                            assert_eq!(self.trace.commits.load(Ordering::SeqCst), 0);
                            self.trace.faults.fetch_add(1, Ordering::SeqCst);
                            let _permit = self
                                .resources
                                .try_decoded_value(self.resources.config().decoded_value_bytes)?;
                            unreachable!("the input scope owns decoded capacity");
                        }
                        2 => {
                            self.trace.faults.fetch_add(1, Ordering::SeqCst);
                            std::future::pending::<()>().await;
                        }
                        _ => {}
                    }
                }
                self.inner.try_get(key, options).await
            }
            async fn multi_get(
                &self,
                keys: &[StateKey],
                options: ReadOptions,
            ) -> live::Result<Vec<Option<Vec<u8>>>> {
                self.inner.multi_get(keys, options).await
            }
            async fn scan(&self, request: ScanRequest) -> live::Result<ScanPage> {
                self.inner.scan(request).await
            }
            async fn try_scan(&self, request: ScanRequest) -> live::Result<ScanPage> {
                self.inner.try_scan(request).await
            }
            async fn last_in_range(&self, request: LastEntryRequest) -> live::Result<ScanPage> {
                self.inner.last_in_range(request).await
            }
        }
        struct Backend {
            inner: Arc<dyn LiveStateBackend>,
            trace: Arc<Trace>,
            resources: WorkerStateResources,
        }
        #[async_trait::async_trait]
        impl LiveStateBackend for Backend {
            async fn get(
                &self,
                key: &StateKey,
                options: ReadOptions,
            ) -> live::Result<Option<Vec<u8>>> {
                self.inner.get(key, options).await
            }
            async fn try_get(
                &self,
                key: &StateKey,
                options: ReadOptions,
            ) -> live::Result<Option<Vec<u8>>> {
                self.inner.try_get(key, options).await
            }
            async fn multi_get(
                &self,
                keys: &[StateKey],
                options: ReadOptions,
            ) -> live::Result<Vec<Option<Vec<u8>>>> {
                self.inner.multi_get(keys, options).await
            }
            async fn write_batch(&self, batch: WriteBatch) -> live::Result<()> {
                self.inner.write_batch(batch).await
            }
            async fn snapshot(&self) -> live::Result<StateSnapshot> {
                let inner = self.inner.snapshot().await?;
                let mut reads = self.trace.reads.lock().unwrap();
                let index = reads.len();
                reads.push(0);
                Ok(StateSnapshot(Arc::new(Reader {
                    inner,
                    index,
                    trace: self.trace.clone(),
                    resources: self.resources.clone(),
                })))
            }
            fn try_admit_write(
                &self,
                bytes: usize,
                operations: usize,
            ) -> live::Result<AdmittedWriteBatch> {
                self.inner.try_admit_write(bytes, operations)
            }
            async fn admit_write(
                &self,
                bytes: usize,
                operations: usize,
            ) -> live::Result<AdmittedWriteBatch> {
                self.inner.admit_write(bytes, operations).await
            }
            async fn write_admitted(&self, batch: AdmittedWriteBatch) -> live::Result<()> {
                self.inner.write_admitted(batch).await?;
                self.trace.commits.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        async fn setup(
            rocks: bool,
            tight: bool,
        ) -> (
            IncrementalAggregatingFunc,
            Arc<Trace>,
            Arc<dyn LiveStateBackend>,
            std::path::PathBuf,
        ) {
            use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
            let root =
                std::env::temp_dir().join(format!("calendar-progress-{}", uuid::Uuid::new_v4()));
            // This mixed fixture has six indexed Batch aggregates plus
            // calendar allowance: 22 worst-case writes per event. Three rows
            // require 1,098,240 overlay bytes, just over the former 1 MiB.
            let limits = AggregateStoreLimits {
                overlay_bytes: 1280 * 1024,
                write_bytes: 1280 * 1024,
                write_operations: 128,
                ..native_test_store().limits()
            };
            let resources = WorkerStateResources::new(ResourceConfig {
                queued_write_bytes: 8 * 1024 * 1024,
                // Retain room for mandatory backend point reads, while the
                // optional context's conservative output headroom cannot fit.
                decoded_value_bytes: if tight {
                    limits.overlay_bytes + 3 * limits.value_bytes + 128 * 1024
                } else {
                    4 * 1024 * 1024
                },
                ..native_test_store().resources().config().clone()
            })
            .unwrap();
            let inner: Arc<dyn LiveStateBackend> = if rocks {
                Arc::new(
                    RocksLiveState::open(
                        RocksStateConfig {
                            root: root.clone(),
                            job_id: "calendar-progress".into(),
                            operator_id: "aggregate".into(),
                            subtask: 0,
                            generation: 0,
                            attempt: 0,
                        },
                        resources.clone(),
                    )
                    .await
                    .unwrap(),
                )
            } else {
                Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap())
            };
            let trace = Arc::new(Trace::default());
            let backend: Arc<dyn LiveStateBackend> = Arc::new(Backend {
                inner: inner.clone(),
                trace: trace.clone(),
                resources: resources.clone(),
            });
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let mut operator = native_calendar_operator();
            operator.native_store =
                Some(AggregateStore::new(backend, table, resources, limits).unwrap());
            // Guard the fixture's actual production chunk admission, rather
            // than assuming all three rows share one scope from byte limits.
            let fallback = operator
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
                .count();
            let worst_operations = 2 + 2 * fallback + operator.calendar_write_operations();
            let store = operator.native_store.as_ref().unwrap();
            let rows_per_chunk = (limits.write_operations / worst_operations)
                .min(limits.write_bytes / (worst_operations * store.max_encoded_entry_bytes()))
                .min(
                    limits.overlay_bytes
                        / (worst_operations * (limits.key_bytes + limits.value_bytes)),
                );
            assert!(
                rows_per_chunk >= 3,
                "calendar progress fixture needs a three-row chunk; admits {rows_per_chunk}"
            );
            (operator, trace, inner, root)
        }
        fn input() -> RecordBatch {
            native_test_changelog_with_ids(
                &batch(&[Some("value"); 3], &[1, 2, 3], &[Some(true); 3]),
                &[false; 3],
                &[[1; 16], [2; 16], [3; 16]],
            )
        }
        async fn finish(
            operator: IncrementalAggregatingFunc,
            backend: Arc<dyn LiveStateBackend>,
            root: std::path::PathBuf,
        ) {
            drop(operator);
            backend.close().await.unwrap();
            if root.exists() {
                std::fs::remove_dir_all(root).unwrap();
            }
        }

        #[tokio::test]
        async fn calendar_progress_real_chunk_reuse_and_tight_fallback() {
            for rocks in [false, true] {
                for tight in [false, true] {
                    let (mut operator, trace, backend, root) = setup(rocks, tight).await;
                    let input = input();
                    let mut ctx = native_input_context(input.schema()).await;
                    operator
                        .process_native_batch(&input, &mut ctx)
                        .await
                        .unwrap();
                    // Three real rows share one existing chunk/commit. The
                    // independent upfront validation always reads W once.
                    let reads = trace.reads.lock().unwrap().clone();
                    assert_eq!(
                        reads.len(),
                        2,
                        "fixture must use one multirow production chunk"
                    );
                    assert_eq!(reads[0], 1);
                    if tight {
                        assert!(reads[1] > 1);
                    } else {
                        assert_eq!(reads[1], 1);
                    }
                    assert_eq!(trace.commits.load(Ordering::SeqCst), 1);
                    assert_eq!(
                        calendar_test_values(&operator).await,
                        vec![
                            ScalarValue::Int64(Some(3)),
                            ScalarValue::Int64(Some(3)),
                            ScalarValue::Int64(Some(3)),
                        ]
                    );
                    // A later watermark must be observed in a new input scope,
                    // including after this scope cached absence rather than W.
                    operator
                        .calendar_watermark(arroyo_types::Watermark::EventTime(
                            arroyo_types::event_time::from_signed_nanos(201 * 86_400_000_000_000)
                                .unwrap(),
                        ))
                        .await
                        .unwrap();
                    let before = trace.commits.load(Ordering::SeqCst);
                    let error = operator
                        .process_native_batch(&input, &mut ctx)
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("watermark"));
                    assert_eq!(trace.commits.load(Ordering::SeqCst), before);
                    for calendar in &mut operator.calendars {
                        calendar.test_dates(201, 201);
                    }
                    let next = native_test_changelog_with_ids(
                        &batch(&[Some("value"); 3], &[1, 2, 3], &[Some(true); 3]),
                        &[false; 3],
                        &[[4; 16], [5; 16], [6; 16]],
                    );
                    let first = trace.reads.lock().unwrap().len();
                    operator
                        .process_native_batch(&next, &mut ctx)
                        .await
                        .unwrap();
                    let reads = trace.reads.lock().unwrap().clone();
                    assert_eq!(reads[first], 1);
                    if !tight {
                        assert_eq!(reads[first + 1], 1);
                    }
                    // Retractions go through the same actual production chunk.
                    let retract = native_test_changelog_with_ids(
                        &batch(&[Some("value"); 3], &[1, 2, 3], &[Some(true); 3]),
                        &[true; 3],
                        &[[4; 16], [5; 16], [6; 16]],
                    );
                    operator
                        .process_native_batch(&retract, &mut ctx)
                        .await
                        .unwrap();
                    assert_eq!(
                        calendar_test_values(&operator).await,
                        vec![
                            ScalarValue::Int64(Some(3)),
                            ScalarValue::Int64(Some(0)),
                            ScalarValue::Int64(Some(3)),
                        ]
                    );
                    finish(operator, backend, root).await;
                }
            }
        }

        #[tokio::test]
        async fn calendar_progress_loaded_preparation_failure_and_cancellation_retry_cold() {
            for rocks in [false, true] {
                for gate in [1, 2] {
                    let (operator, trace, backend, root) = setup(rocks, false).await;
                    let input = input();
                    let mut ctx = native_input_context(input.schema()).await;
                    trace.gate.store(gate, Ordering::SeqCst);
                    if gate == 2 {
                        let mut processing =
                            Box::pin(operator.process_native_batch(&input, &mut ctx));
                        // RocksDB dispatch is asynchronous, so drive to the
                        // explicit post-W gate rather than assuming one poll.
                        tokio::select! {
                            result = &mut processing => panic!("expected suspended preparation: {result:?}"),
                            _ = async {
                                while trace.faults.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
                            } => {}
                        }
                        assert_eq!(trace.commits.load(Ordering::SeqCst), 0);
                        drop(processing);
                        // Cancellation released all scope and context ownership.
                        let resources = operator.native_store.as_ref().unwrap().resources();
                        assert!(
                            resources
                                .try_decoded_value(resources.config().decoded_value_bytes)
                                .is_ok()
                        );
                    }
                    operator
                        .process_native_batch(&input, &mut ctx)
                        .await
                        .unwrap();
                    assert_eq!(trace.faults.load(Ordering::SeqCst), 1);
                    assert_eq!(trace.commits.load(Ordering::SeqCst), 1);
                    let reads = trace.reads.lock().unwrap().clone();
                    assert_eq!(reads[0], 1);
                    assert_eq!(reads[1], 1, "first preparation loaded W before failure");
                    assert!(
                        reads.len() >= 3,
                        "a fresh snapshot must be opened for retry"
                    );
                    assert!(reads[2] >= 1, "retry must re-read W");
                    assert_eq!(
                        calendar_test_values(&operator).await,
                        vec![
                            ScalarValue::Int64(Some(3)),
                            ScalarValue::Int64(Some(3)),
                            ScalarValue::Int64(Some(3)),
                        ]
                    );
                    finish(operator, backend, root).await;
                }
            }
        }

        #[tokio::test]
        async fn calendar_progress_invalid_upfront_read_never_mutates() {
            for rocks in [false, true] {
                for malformed in [true, false] {
                    let (operator, trace, backend, root) = setup(rocks, false).await;
                    let store = operator.native_store.as_ref().unwrap();
                    let mut scope = store.begin().await.unwrap();
                    let value = if malformed {
                        vec![1, 2]
                    } else {
                        crate::arrow::aggregate_codec::CalendarProgress {
                            watermark_nanos: 200 * 86_400_000_000_000,
                            first_retained_day: 0,
                        }
                        .encode()
                        .to_vec()
                    };
                    scope.put(b"W", &value).unwrap();
                    scope.commit().await.unwrap();
                    let before = trace.commits.load(Ordering::SeqCst);
                    let input = input();
                    let mut ctx = native_input_context(input.schema()).await;
                    let error = operator
                        .process_native_batch(&input, &mut ctx)
                        .await
                        .unwrap_err();
                    assert!(
                        error.to_string().contains(if malformed {
                            "calendar progress"
                        } else {
                            "compiled horizon"
                        }),
                        "{error:#}"
                    );
                    assert_eq!(trace.commits.load(Ordering::SeqCst), before);
                    let scope = store.begin().await.unwrap();
                    assert!(
                        scope
                            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    );
                    drop(scope);
                    finish(operator, backend, root).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn native_cleanup_single_pointer_page() {
        let mut operator = native_calendar_operator();
        operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
            page_entries: 1,
            ..native_test_store().limits()
        }));
        let store = operator.native_store.as_ref().unwrap();
        let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
        let mut pointer = native_generation_prefix(b'U', &GLOBAL_KEY, 0).unwrap();
        pointer.extend_from_slice(&0_u32.to_be_bytes());
        let due = calendar_native::calendar_due_key(17, &GLOBAL_KEY).unwrap();
        let mut scope = store.begin().await.unwrap();
        scope.put(&marker, b"U").unwrap();
        scope.put(&pointer, &17_i64.to_be_bytes()).unwrap();
        scope.put(&due, &0_u64.to_be_bytes()).unwrap();
        scope.commit().await.unwrap();
        let mut progress = NativeCleanupProgress::new(store);
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
        let scope = store.begin().await.unwrap();
        assert_eq!(scope.get(&marker).await.unwrap(), Some(b"U".to_vec()));
        assert!(scope.get(&pointer).await.unwrap().is_none());
        assert!(scope.get(&due).await.unwrap().is_none());
        drop(scope);
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
        let scope = store.begin().await.unwrap();
        assert_eq!(scope.get(&marker).await.unwrap(), Some(b"T".to_vec()));
    }

    /// Actual bounded cleanup calls with finite obsolete/current generations.
    /// Native deleted-key attribution is measured separately in the state test.
    #[tokio::test]
    #[ignore = "opt-in STR-116 finite operator cleanup diagnostic"]
    async fn native_cleanup_generation_pilot() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let repeats = std::env::var("STR116_PILOT_REPEAT")
            .map(|value| vec![value.parse::<usize>().unwrap()])
            .unwrap_or_else(|_| (0..3).collect());
        for repeat in repeats {
            let root = std::env::temp_dir().join(format!("cleanup-pilot-{}", uuid::Uuid::new_v4()));
            let resources = native_test_store().resources().clone();
            let backend = Arc::new(
                RocksLiveState::open(
                    RocksStateConfig {
                        root: root.clone(),
                        job_id: "cleanup-pilot".into(),
                        operator_id: "aggregate".into(),
                        subtask: 0,
                        generation: 0,
                        attempt: 0,
                    },
                    resources.clone(),
                )
                .await
                .unwrap(),
            );
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let store = AggregateStore::new(
                backend.clone(),
                table,
                resources,
                native_test_store().limits(),
            )
            .unwrap();
            let mut operator = native_calendar_operator();
            operator.native_store = Some(store);
            let store = operator.native_store.as_ref().unwrap();
            for group_id in 0_u64..12 {
                let group = group_id.to_be_bytes();
                for generation in 0_u64..4 {
                    if generation < 3 {
                        let mut scope = store.begin().await.unwrap();
                        scope
                            .put(&native_cleanup_key(&group, generation).unwrap(), b"M")
                            .unwrap();
                        scope.commit().await.unwrap();
                    }
                    for family in b"MRBJU" {
                        for row in 0_u64..16 {
                            let mut key =
                                native_generation_prefix(*family, &group, generation).unwrap();
                            key.extend_from_slice(&row.to_be_bytes());
                            let mut scope = store.begin().await.unwrap();
                            scope.put(&key, &row.to_be_bytes()).unwrap();
                            if *family == b'U' {
                                scope
                                    .put(
                                        &calendar_native::calendar_due_key(row as i64, &group)
                                            .unwrap(),
                                        &3_u64.to_be_bytes(),
                                    )
                                    .unwrap();
                            }
                            scope.commit().await.unwrap();
                        }
                    }
                }
            }
            // Keep identical retained history in every run; production adapter
            // pools/pages/native settings remain unchanged.
            let pinned = backend.snapshot().await.unwrap();
            let mut progress = NativeCleanupProgress::new(store);
            assert!(progress._permit.is_some());
            let mut completed = 0_u64;
            let mut pages = 0;
            let mut elapsed = std::time::Duration::ZERO;
            loop {
                let start = std::time::Instant::now();
                let more = operator
                    .cleanup_native_with_progress(&mut progress)
                    .await
                    .unwrap();
                elapsed += start.elapsed();
                if !more {
                    break;
                }
                pages += 1;
                assert!(pages < 600, "finite cleanup segment did not finish");
                // Fixture-only point read counts completed generations outside
                // the timed operation without introducing another C scan.
                let scope = store.begin_point().await.unwrap();
                let marker =
                    native_cleanup_key(&(completed / 3).to_be_bytes(), completed % 3).unwrap();
                let terminal = scope.get(&marker).await.unwrap().is_none();
                drop(scope);
                if terminal {
                    completed += 1;
                    if completed.is_multiple_of(12) {
                        let status = std::fs::read_to_string("/proc/self/status").unwrap();
                        let memory: Vec<_> = status
                            .lines()
                            .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
                            .collect();
                        println!(
                            "STR116_OPERATOR repeat={repeat} completed_generations={completed} completed_members={} completed_markers={completed} pages={pages} seconds={:.9} memory={memory:?}",
                            completed * 5 * 16,
                            elapsed.as_secs_f64()
                        );
                        pages = 0;
                        elapsed = std::time::Duration::ZERO;
                    }
                }
            }
            assert_eq!(completed, 36);
            let scope = store.begin().await.unwrap();
            assert!(scope.first(b"C").await.unwrap().is_none());
            for group_id in 0_u64..12 {
                let group = group_id.to_be_bytes();
                for generation in 0_u64..4 {
                    for family in b"MRBJU" {
                        for row in 0_u64..16 {
                            let mut key =
                                native_generation_prefix(*family, &group, generation).unwrap();
                            key.extend_from_slice(&row.to_be_bytes());
                            assert_eq!(
                                scope.get(&key).await.unwrap(),
                                (generation == 3).then(|| row.to_be_bytes().to_vec())
                            );
                        }
                    }
                }
                for row in 0_i64..16 {
                    assert_eq!(
                        scope
                            .get(&calendar_native::calendar_due_key(row, &group).unwrap())
                            .await
                            .unwrap(),
                        Some(3_u64.to_be_bytes().to_vec())
                    );
                }
            }
            drop(scope);
            drop(progress);
            drop(pinned);
            drop(operator);
            drop(manager);
            Arc::try_unwrap(backend)
                .unwrap_or_else(|_| panic!("retained cleanup backend"))
                .close_and_remove()
                .await
                .unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    /// Finite operator diagnostic, not the four-owner production qualification.
    #[tokio::test]
    #[ignore = "explicit isolated calendar operation-count pilot"]
    async fn calendar_operation_count_pilot() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let root = std::env::temp_dir().join(format!("calendar-pilot-{}", uuid::Uuid::new_v4()));
        let resources = WorkerStateResources::new(ResourceConfig {
            disk_reserve_bytes: 0,
            ..native_test_store().resources().config().clone()
        })
        .unwrap();
        let registry = prometheus::Registry::new();
        resources.register_metrics(&registry).unwrap();
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "calendar-pilot".into(),
                    operator_id: "aggregate".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_calendar_operator();
        operator.native_store = Some(
            AggregateStore::new(
                backend.clone(),
                table,
                resources,
                native_test_store().limits(),
            )
            .unwrap(),
        );
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[255; 16]).await;
        let counts = || {
            registry
                .gather()
                .into_iter()
                .find(|family| family.name() == "arroyo_live_state_operation_latency_seconds")
                .unwrap()
                .get_metric()
                .iter()
                .map(|metric| {
                    let operation = metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == "operation")
                        .unwrap()
                        .value()
                        .to_string();
                    (operation, metric.get_histogram().get_sample_count())
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = counts();
        let start = std::time::Instant::now();
        // Fixed prefix: absent static contribution, old/future contributions,
        // and relevant rows. No synthetic source clock or changed SQL rules.
        for id in 0..128u8 {
            let (day, include) = match id % 4 {
                0 => (200, Some(false)),
                1 => (193, Some(true)),
                2 => (201 + i32::from(id), Some(true)),
                _ => (200, Some(true)),
            };
            calendar_test_event(&mut operator, day, 200, include, false, &[id; 16]).await;
        }
        let seconds = start.elapsed().as_secs_f64();
        let after = counts();
        let deltas: std::collections::BTreeMap<_, _> = after
            .into_iter()
            .map(|(key, count)| {
                let prior = before.get(&key).copied().unwrap_or(0);
                (key, count - prior)
            })
            .collect();
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(129)),
                ScalarValue::Int64(Some(33)),
                ScalarValue::Int64(Some(33))
            ]
        );
        println!("CALENDAR_PILOT events=128 seconds={seconds:.9} operations={deltas:?}");
        let pruning_before = counts();
        let pruning_start = std::time::Instant::now();
        operator
            .calendar_watermark(arroyo_types::Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000).unwrap(),
            ))
            .await
            .unwrap();
        let mut pages = 1;
        loop {
            let scope = operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap();
            let cleanup = crate::arrow::aggregate_codec::CalendarCleanup::decode(
                &scope.get(b"V").await.unwrap().unwrap(),
            )
            .unwrap();
            drop(scope);
            if cleanup.complete {
                break;
            }
            assert!(pages < 64, "finite diagnostic pruning bound");
            operator.prune_calendar_buckets().await.unwrap();
            pages += 1;
        }
        let pruning_seconds = pruning_start.elapsed().as_secs_f64();
        let pruning_deltas: std::collections::BTreeMap<_, _> = counts()
            .into_iter()
            .map(|(key, count)| {
                let prior = pruning_before.get(&key).copied().unwrap_or(0);
                (key, count - prior)
            })
            .collect();
        println!(
            "CALENDAR_PRUNING pages={pages} seconds={pruning_seconds:.9} operations={pruning_deltas:?}"
        );
        // Full bucket oracle after timing/counter capture: G alone would not
        // detect an incorrect fast cleanup deleting retained/future history.
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let mut after = None;
        let mut retained_days = Vec::new();
        while let Some((key, _)) = scope.first_from(b"B", after.as_deref()).await.unwrap() {
            retained_days.push(
                (u32::from_be_bytes(key[key.len() - 4..].try_into().unwrap()) ^ (1 << 31)) as i32,
            );
            after = Some(key);
        }
        let expected_days: Vec<_> = std::iter::once(200)
            .chain((0..128).filter(|id| id % 4 == 2).map(|id| 201 + id))
            .collect();
        assert_eq!(retained_days, expected_days);
        drop(scope);
        drop(operator);
        drop(manager);
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn calendar_fresh_empty_preserves_date32_and_raw_watermark_rejection() {
        let mut operator = native_calendar_operator_options(true, true);
        // Feed the typed Date32 boundary directly: constructing an invalid-date
        // physical Literal formats it through chrono before the handler runs.
        // This helper-level call retains the same raw-progress validation gate.
        let input = batch(&[Some("value")], &[1], &[Some(false)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let calendar_inputs = operator
            .calendars
            .iter()
            .map(|_| CalendarInput {
                contribution: arrow_array::Date32Array::from(vec![None::<i32>]),
                reference: arrow_array::Date32Array::from(vec![i32::MIN]),
            })
            .collect::<Vec<_>>();
        operator
            .validate_calendar_inputs(&calendar_inputs)
            .await
            .unwrap();
        let mut scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let error = operator
            .native_process_calendar_event(
                &mut scope,
                NativeCalendarEvent {
                    group_key: &GLOBAL_KEY,
                    inputs: &inputs,
                    row: 0,
                    retract: false,
                    row_id: Some(&[1; 16]),
                    calendar_inputs: &calendar_inputs,
                },
            )
            .await
            .unwrap_err();
        drop(scope);
        assert!(error.to_string().contains("Date32 range"));
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        assert!(
            scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        drop(scope);
        operator
            .calendar_watermark(arroyo_types::Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000).unwrap(),
            ))
            .await
            .unwrap();
        let error = calendar_test_event_for_group(
            &mut operator,
            &GLOBAL_KEY,
            None,
            199,
            Some(false),
            false,
            &[2; 16],
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("precedes admitted watermark context")
        );
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        assert!(
            scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn calendar_fresh_empty_recreates_with_terminal_cleanup_marker() {
        let mut operator = native_calendar_operator_options(true, true);
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        operator.key_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let keys: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["recreated"]))];
        let rows = operator.key_converter.convert_columns(&keys).unwrap();
        let key = rows.row(0).as_ref().to_vec();
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
        let mut ctx = native_input_context(input_schema()).await;
        ctx.out_schema = Some(Arc::new(
            ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap(),
        ));
        let mut collector = AggregateCollector::default();
        calendar_test_event_for_group(
            &mut operator,
            &key,
            Some(200),
            200,
            Some(true),
            false,
            &[1; 16],
        )
        .await
        .unwrap();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        calendar_test_event_for_group(
            &mut operator,
            &key,
            Some(190),
            200,
            Some(true),
            true,
            &[1; 16],
        )
        .await
        .unwrap();
        operator
            .drain_native_dirty(&mut ctx, &mut collector)
            .await
            .unwrap();
        let store = operator.native_store.as_ref().unwrap();
        let marker = native_cleanup_key(&key, 0).unwrap();
        let group_key = native_group_key(b'G', &key).unwrap();
        let scope = store.begin().await.unwrap();
        let bytes = scope.get(&group_key).await.unwrap().unwrap();
        let group = operator
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        assert_eq!(group.generation, 1);
        for position in 0..operator.calendars.len() {
            assert_eq!(group.calendar_references.get(position).unwrap(), None);
        }
        drop(scope);
        let mut progress = NativeCleanupProgress::new(store);
        let mut reached_terminal = false;
        for _ in 0..64 {
            assert!(
                operator
                    .cleanup_native_with_progress(&mut progress)
                    .await
                    .unwrap()
            );
            let scope = store.begin().await.unwrap();
            if scope.get(&group_key).await.unwrap().is_none() {
                assert_eq!(scope.get(&marker).await.unwrap(), Some(b"T".to_vec()));
                reached_terminal = true;
                break;
            }
        }
        assert!(reached_terminal);
        drop(progress);
        calendar_test_event_for_group(
            &mut operator,
            &key,
            Some(193),
            200,
            Some(true),
            false,
            &[2; 16],
        )
        .await
        .unwrap();
        assert_eq!(
            calendar_test_values_for_group(&operator, &key).await,
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(None)
            ]
        );
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        calendar_test_assert_references(&operator, &scope, &key, 200).await;
        drop(scope);
        // The old terminal marker must not remove the newly live G.
        let mut cleanup = NativeCleanupProgress::new(operator.native_store.as_ref().unwrap());
        operator
            .cleanup_native_with_progress(&mut cleanup)
            .await
            .unwrap();
        drop(cleanup);
        calendar_test_event_for_group(
            &mut operator,
            &key,
            Some(200),
            200,
            Some(true),
            false,
            &[3; 16],
        )
        .await
        .unwrap();
        assert_eq!(
            calendar_test_values_for_group(&operator, &key).await,
            vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(1))
            ]
        );
    }

    #[tokio::test]
    async fn calendar_group_references_keep_independent_contexts_and_expiry_reset() {
        let mut operator = native_calendar_operator();
        for (step, (reference_day, reference_week, day, include, expected)) in [
            (200, 201, 200, true, [1, 1, 1]),
            (199, 201, 200, false, [2, 0, 1]),
            (201, 202, 201, true, [3, 1, 2]),
            (204, 202, 201, false, [4, 0, 2]),
        ]
        .into_iter()
        .enumerate()
        {
            let input = batch(&[Some("value")], &[1], &[Some(include)]);
            operator.calendars[0].test_dates(day, reference_day);
            operator.calendars[1].test_dates(day, reference_week);
            let dates = operator.calendar_inputs(&input).unwrap();
            operator.validate_calendar_inputs(&dates).await.unwrap();
            let inputs = operator.compute_inputs(&input).unwrap();
            let mut scope = operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap();
            operator
                .native_process_calendar_event(
                    &mut scope,
                    NativeCalendarEvent {
                        group_key: &GLOBAL_KEY,
                        inputs: &inputs,
                        row: 0,
                        retract: false,
                        row_id: Some(&[step as u8; 16]),
                        calendar_inputs: &dates,
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                calendar_test_values(&operator).await,
                expected
                    .into_iter()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>()
            );
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
            let group = operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
                .unwrap();
            assert_eq!(
                group.calendar_references.get(0).unwrap(),
                Some(reference_day)
            );
            assert_eq!(
                group.calendar_references.get(1).unwrap(),
                Some(reference_week)
            );
        }
        // The ordinary expiry path must clear the reference proofs with its
        // newly empty accumulators, even for a retained global G.
        let mut operator = native_calendar_operator();
        operator.retain_indefinitely = false;
        operator.ttl = Duration::ZERO;
        calendar_test_event(&mut operator, 200, 200, Some(true), false, &[9; 16]).await;
        assert!(operator.expire_native(None).await.unwrap());
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
        let group = operator
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        assert_eq!(group.generation, 1);
        assert_eq!(group.calendar_references.get(0).unwrap(), None);
        assert_eq!(group.calendar_references.get(1).unwrap(), None);
    }

    #[tokio::test]
    async fn calendar_reuse_matches_full_recalculation_for_original_corrections() {
        let mut operator = native_calendar_operator();
        // Irrelevant and relevant contributions, both horizon edges, reference
        // changes in both directions, and misleading correction envelopes.
        for (id, day, reference, include, retract) in [
            (1, 194, 200, Some(true), false),
            (2, 193, 200, Some(true), false),
            (3, 201, 200, Some(true), false),
            (4, 200, 200, Some(false), false),
            (5, 200, 200, None, false),
            (6, 200, 200, Some(true), false),
            (2, 200, 200, Some(false), true),
            (1, 300, 200, Some(false), true),
            (7, 202, 201, Some(false), false),
            (8, 202, 200, Some(false), false),
            (3, 193, 200, Some(false), true),
            (6, 193, 200, Some(false), true),
        ] {
            calendar_test_event(&mut operator, day, reference, include, retract, &[id; 16]).await;
            let store = operator.native_store.as_ref().unwrap();
            let scope = store.begin().await.unwrap();
            let bytes = scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .unwrap();
            let group = operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
                .unwrap();
            let mut actual = operator.native_accumulators(Some(&group)).unwrap();
            for calendar in &operator.calendars {
                let mut expected = operator
                    .calendar_recalculate(
                        &scope,
                        &GLOBAL_KEY,
                        group.generation,
                        calendar,
                        reference,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    actual[calendar.aggregate_index].evaluate().unwrap(),
                    expected.evaluate().unwrap(),
                    "id={id} retract={retract}"
                );
            }
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
    async fn calendar_pruning_uses_only_real_progress_and_preserves_correction_history() {
        use arroyo_types::{Watermark, event_time::from_signed_nanos};
        let mut operator = native_calendar_operator();
        operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
            page_entries: 1,
            ..native_test_store().limits()
        }));
        for (id, day) in [193, 194, 200, 201].into_iter().enumerate() {
            calendar_test_event(&mut operator, day, 200, Some(true), false, &[id as u8; 16]).await;
        }
        let progress = Watermark::EventTime(from_signed_nanos(200 * 86_400_000_000_000).unwrap());
        operator.calendar_watermark(progress).await.unwrap();
        let store = operator.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let boundary = scope.get(b"W").await.unwrap().unwrap();
        // The seven-day shared family retains D-6 inclusively, plus future D+1.
        let mut after = None;
        let mut days = Vec::new();
        while let Some((key, _)) = scope.first_from(b"B", after.as_deref()).await.unwrap() {
            days.push(
                (u32::from_be_bytes(key[key.len() - 4..].try_into().unwrap()) ^ (1 << 31)) as i32,
            );
            after = Some(key);
        }
        assert_eq!(days, vec![194, 200, 201]);
        assert!(scope.first(b"J").await.unwrap().is_some());
        drop(scope);
        for ignored in [
            Watermark::Idle,
            Watermark::EventTime(arroyo_types::from_nanos(u64::MAX as u128)),
            Watermark::EventTime(from_signed_nanos(199 * 86_400_000_000_000).unwrap()),
        ] {
            operator.calendar_watermark(ignored).await.unwrap();
            let scope = store.begin().await.unwrap();
            assert_eq!(scope.get(b"W").await.unwrap(), Some(boundary.clone()));
        }
        // An admitted correction with current reference removes the original
        // expired day from lifetime without requiring or resurrecting its bucket.
        calendar_test_event(&mut operator, 200, 200, Some(false), true, &[0; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ]
        );
        // Independent old contribution dates remain valid for lifetime, while
        // no recent state is stored for days below the persisted cutoff.
        calendar_test_event(&mut operator, 193, 200, Some(true), false, &[9; 16]).await;
        assert_eq!(
            calendar_test_values(&operator).await,
            vec![
                ScalarValue::Int64(Some(4)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ]
        );
        let store = operator.native_store.as_ref().unwrap();
        let reservation = store
            .resources()
            .try_decoded_value(store.resources().config().decoded_value_bytes)
            .unwrap();
        drop(reservation);
    }

    #[tokio::test]
    async fn calendar_pruning_resumes_persisted_pages_and_uses_family_horizons() {
        use crate::arrow::aggregate_codec::CalendarCleanup;
        let mut operator = native_calendar_operator_with_filters(false);
        operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
            page_entries: 1,
            ..native_test_store().limits()
        }));
        for (id, day) in [193, 194, 199, 200, 201].into_iter().enumerate() {
            calendar_test_event(&mut operator, day, 200, Some(true), false, &[id as u8; 16]).await;
        }
        operator
            .calendar_watermark(arroyo_types::Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000).unwrap(),
            ))
            .await
            .unwrap();
        let store = operator.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let first_page = CalendarCleanup::decode(&scope.get(b"V").await.unwrap().unwrap()).unwrap();
        assert!(!first_page.complete);
        assert!(first_page.after.is_some());
        let mut bucket_count = 0;
        let mut after = None;
        while let Some((key, _)) = scope.first_from(b"B", after.as_deref()).await.unwrap() {
            bucket_count += 1;
            after = Some(key);
        }
        assert_eq!(bucket_count, 9); // one deletion, never an unbounded drain
        drop(scope);
        // A new watermark while cleanup is unfinished replaces the frontier
        // and restarts the cursor for its stricter family boundaries.
        operator
            .calendar_watermark(arroyo_types::Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(201 * 86_400_000_000_000).unwrap(),
            ))
            .await
            .unwrap();
        let scope = store.begin().await.unwrap();
        let second_page =
            CalendarCleanup::decode(&scope.get(b"V").await.unwrap().unwrap()).unwrap();
        assert_eq!(second_page.watermark_day, 201);
        assert!(second_page.after > first_page.after);
        drop(scope);
        // Simulate another owner resuming the same persisted live state.
        let mut resumed = native_calendar_operator_with_filters(false);
        std::mem::swap(&mut resumed.native_store, &mut operator.native_store);
        for _ in 0..12 {
            resumed.prune_calendar_buckets().await.unwrap();
        }
        let store = resumed.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        let complete_bytes = scope.get(b"V").await.unwrap().unwrap();
        assert!(CalendarCleanup::decode(&complete_bytes).unwrap().complete);
        let mut retained = Vec::new();
        let mut after = None;
        while let Some((key, _)) = scope.first_from(b"B", after.as_deref()).await.unwrap() {
            retained.push((
                u32::from_be_bytes(key[key.len() - 8..key.len() - 4].try_into().unwrap()),
                (u32::from_be_bytes(key[key.len() - 4..].try_into().unwrap()) ^ (1 << 31)) as i32,
            ));
            after = Some(key);
        }
        assert_eq!(retained, vec![(6, 201), (7, 199), (7, 200), (7, 201)]);
        drop(scope);
        resumed.prune_calendar_buckets().await.unwrap();
        let mut scope = store.begin().await.unwrap();
        assert_eq!(scope.get(b"V").await.unwrap(), Some(complete_bytes));
        let group_key = native_group_key(b'G', &GLOBAL_KEY).unwrap();
        let group_before = scope.get(&group_key).await.unwrap();
        let error = resumed
            .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 200)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("precedes admitted watermark context")
        );
        assert_eq!(scope.get(&group_key).await.unwrap(), group_before);
        scope.commit().await.unwrap();
        let permit = store
            .resources()
            .try_decoded_value(store.resources().config().decoded_value_bytes)
            .unwrap();
        drop(permit);
    }

    #[tokio::test]
    async fn calendar_pruning_skips_retained_family_and_restores_the_skip_frontier() {
        use crate::arrow::aggregate_codec::CalendarCleanup;
        let mut operator = native_calendar_operator();
        operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
            page_entries: 1,
            ..native_test_store().limits()
        }));
        for (id, day) in [194, 195, 199, 200, 201].into_iter().enumerate() {
            calendar_test_event(&mut operator, day, 200, Some(true), false, &[id as u8; 16]).await;
        }
        operator
            .calendar_watermark(arroyo_types::Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000).unwrap(),
            ))
            .await
            .unwrap();
        let scope = operator
            .native_store
            .as_ref()
            .unwrap()
            .begin()
            .await
            .unwrap();
        let frontier = CalendarCleanup::decode(&scope.get(b"V").await.unwrap().unwrap()).unwrap();
        let cursor = frontier.after.unwrap();
        assert_eq!(&cursor[cursor.len() - 4..], &u32::MAX.to_be_bytes());
        drop(scope);
        // A cold owner uses V, without an in-memory family cache.
        let mut fresh = native_calendar_operator();
        std::mem::swap(&mut fresh.native_store, &mut operator.native_store);
        fresh.prune_calendar_buckets().await.unwrap();
        let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
        assert!(
            CalendarCleanup::decode(&scope.get(b"V").await.unwrap().unwrap())
                .unwrap()
                .complete
        );
        let mut after = None;
        let mut count = 0;
        while let Some((key, _)) = scope.first_from(b"B", after.as_deref()).await.unwrap() {
            count += 1;
            after = Some(key);
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn calendar_pruned_group_move_uses_admitted_envelope_clock_for_both_images() {
        let mut operator = native_calendar_operator();
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        for (step, (group, id, contribution, retract, trigger)) in [
            (&b"A"[..], 1, 194, false, 199),
            (&b"A"[..], 2, 193, false, 199),
            (&b"A"[..], 2, 193, true, 200),
            (&b"B"[..], 2, 200, false, 200),
        ]
        .into_iter()
        .enumerate()
        {
            if step == 2 {
                operator
                    .calendar_watermark(arroyo_types::Watermark::EventTime(
                        arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000)
                            .unwrap(),
                    ))
                    .await
                    .unwrap();
            }
            // The original business FOR field remains old on the before row;
            // common admission supplies current envelope _timestamp on BOTH
            // images. EVENT_DATE binds that internal trigger context.
            let input = batch_at(
                &[Some("original-business-for-day-199")],
                &[1],
                &[i64::from(trigger) * 86_400_000_000_000],
            );
            for calendar in &mut operator.calendars {
                calendar.test_dates(contribution, trigger);
                calendar.test_clock_reference();
            }
            let dates = operator.calendar_inputs(&input).unwrap();
            operator.validate_calendar_inputs(&dates).await.unwrap();
            let inputs = operator.compute_inputs(&input).unwrap();
            let store = operator.native_store.as_ref().unwrap();
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_calendar_event(
                    &mut scope,
                    NativeCalendarEvent {
                        group_key: group,
                        inputs: &inputs,
                        row: 0,
                        retract,
                        row_id: Some(&[id; 16]),
                        calendar_inputs: &dates,
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let store = operator.native_store.as_ref().unwrap();
        let scope = store.begin().await.unwrap();
        for (group, expected) in [(&b"A"[..], [1, 0, 1]), (&b"B"[..], [1, 1, 1])] {
            let bytes = scope
                .get(&native_group_key(b'G', group).unwrap())
                .await
                .unwrap()
                .unwrap();
            let group = operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
                .unwrap();
            let mut state = operator.native_accumulators(Some(&group)).unwrap();
            let actual: Vec<_> = [5, 6, 7]
                .into_iter()
                .map(|index| state[index].evaluate().unwrap())
                .collect();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>()
            );
        }
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
        for (retract, value, expected, prune) in [
            (false, None, (1, 0, None), false),
            (true, Some(99), (0, 0, None), false),
            (false, Some(7), (1, 1, Some(7)), false),
            (true, None, (0, 0, None), false),
            (false, None, (1, 0, None), false),
            (true, Some(99), (0, 0, None), false),
            (false, Some(7), (1, 1, Some(7)), false),
            (true, Some(99), (0, 0, None), true),
        ] {
            if prune {
                operator
                    .calendar_watermark(arroyo_types::Watermark::EventTime(
                        arroyo_types::event_time::from_signed_nanos(207 * 86_400_000_000_000)
                            .unwrap(),
                    ))
                    .await
                    .unwrap();
                for calendar in &mut operator.calendars {
                    calendar.test_dates(207, 207);
                }
                let store = operator.native_store.as_ref().unwrap();
                let mut scope = store.begin().await.unwrap();
                operator
                    .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 207)
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                let scope = store.begin().await.unwrap();
                assert!(scope.first(b"B").await.unwrap().is_none());
                assert!(scope.first(b"J").await.unwrap().is_some());
                let bytes = scope
                    .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                    .await
                    .unwrap()
                    .unwrap();
                let group = operator
                    .native_group_codec()
                    .unwrap()
                    .decode(&bytes, scope.limits().value_bytes)
                    .unwrap();
                let mut state = operator.native_accumulators(Some(&group)).unwrap();
                let actual: Vec<_> = state[..6]
                    .iter_mut()
                    .map(|state| state.evaluate().unwrap())
                    .collect();
                assert_eq!(
                    actual,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(7)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(None),
                    ]
                );
            }
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
            let group = operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
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
            // Compact scalar groups fit the former 256-byte fixture.
            // Keep this an actual encoded-capacity failure.
            value_bytes: 80,
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
        for prefix in [b"G", b"J", b"B", b"H", b"U", b"M", b"R"] {
            assert!(scope.first(prefix).await.unwrap().is_none());
        }
    }

    /// Isolates D enumeration from codec and sink cardinality; not SQL throughput.
    #[tokio::test]
    #[ignore = "opt-in STR123 finite sparse dirty-history diagnostic"]
    async fn native_dirty_history_pilot() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let root = std::env::temp_dir().join(format!("str123-{}", uuid::Uuid::new_v4()));
        let template = native_test_store();
        let resources = template.resources().clone();
        let limits = template.limits();
        drop(template);
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "dirty-pilot".into(),
                    operator_id: "aggregate".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_operator();
        operator.native_store =
            Some(AggregateStore::new(backend.clone(), table, resources, limits).unwrap());
        let store = operator.native_store.as_ref().unwrap();
        let dirty = |i: u64| native_group_key(b'D', &i.to_be_bytes()).unwrap();
        // Pin pre-delete versions; ordinary compaction remains enabled. G absent
        // deliberately isolates the established stale-marker cleanup branch.
        for i in 0..2048_u64 {
            let mut scope = store.begin_point().await.unwrap();
            scope.put(&dirty(i), &[1]).unwrap();
            scope.commit().await.unwrap();
        }
        let history = backend.snapshot().await.unwrap();
        for i in 0..2048_u64 {
            let mut scope = store.begin_point().await.unwrap();
            scope.delete(&dirty(i)).unwrap();
            scope.commit().await.unwrap();
        }
        let mut ctx = native_input_context(input_schema()).await;
        let mut collector = AggregateCollector::default();
        eprintln!(
            "STR123 settings history=2048 cycles=96 dirty_per_cycle=4 pinned_history=true auto_compaction=default limits={limits:?} resources={:?}",
            store.resources().config()
        );
        let mut started = std::time::Instant::now();
        for cycle in 0..96_u64 {
            let mut scope = store.begin_point().await.unwrap();
            // Alternation includes freshly dirtied earlier keys after later keys.
            let keys = if cycle % 2 == 0 {
                [0, 1023, 2047, 3000]
            } else {
                [1, 1024, 2046, 3001]
            };
            for key in keys {
                scope.put(&dirty(key), &[1]).unwrap();
            }
            scope.commit().await.unwrap();
            operator
                .drain_native_dirty(&mut ctx, &mut collector)
                .await
                .unwrap();
            let scope = store.begin_point().await.unwrap();
            for key in keys {
                assert!(scope.get(&dirty(key)).await.unwrap().is_none());
            }
            if (cycle + 1) % 32 == 0 {
                eprintln!(
                    "STR123 completed_cycles={} completed_dirty={} elapsed_ns={}",
                    cycle + 1,
                    (cycle + 1) * 4,
                    started.elapsed().as_nanos()
                );
                for field in ["stat", "status", "io"] {
                    eprintln!(
                        "STR123 process cycle={} field={field} raw={:?}",
                        cycle + 1,
                        std::fs::read_to_string(format!("/proc/self/{field}")).unwrap()
                    );
                }
                started = std::time::Instant::now();
            }
        }
        assert!(collector.batches.is_empty());
        drop(history);
        drop(operator);
        drop(manager);
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn native_dirty_sparse_earlier_key_exact_cdc_and_suppression() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root = std::env::temp_dir().join(format!("dirty-cdc-{}", uuid::Uuid::new_v4()));
            let template = native_test_store();
            let resources = template.resources().clone();
            let limits = template.limits();
            drop(template);
            let backend: Arc<dyn LiveStateBackend> = if rocks {
                Arc::new(
                    RocksLiveState::open(
                        RocksStateConfig {
                            root: root.clone(),
                            job_id: "dirty-cdc".into(),
                            operator_id: "aggregate".into(),
                            subtask: 0,
                            generation: 0,
                            attempt: 0,
                        },
                        resources.clone(),
                    )
                    .await
                    .unwrap(),
                )
            } else {
                Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap())
            };
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let mut operator = ttl_keyed_operator();
            operator.retain_indefinitely = true;
            operator.native_store = Some(
                AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap(),
            );
            let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["a", "z"]))];
            let rows = operator.key_converter.convert_columns(&columns).unwrap();
            let keys: Vec<_> = rows.iter().map(|r| r.as_ref().to_vec()).collect();
            let old = batch(&[Some("a")], &[1], &[Some(true)]);
            let next = batch(&[Some("z")], &[2], &[Some(true)]);
            let mut ctx = native_input_context(old.schema()).await;
            let mut output = SchemaBuilder::from(operator.schema_without_metadata.as_ref().clone());
            output.push(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            ));
            ctx.out_schema = Some(Arc::new(
                ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap(),
            ));
            let mut collector = AggregateCollector::default();
            // Every invocation is cold: a newly dirty earlier key must still be
            // found after the prior invocation finished at z.
            for (group, input, retract) in [
                (0, &old, false),
                (1, &old, false),
                (1, &next, false),
                (0, &next, false),
                (0, &next, true),
                (0, &old, true),
                (0, &old, false),
            ] {
                let store = operator.native_store.as_ref().unwrap();
                let mut scope = store.begin().await.unwrap();
                operator
                    .native_process_event(
                        &mut scope,
                        &keys[group],
                        &operator.compute_inputs(input).unwrap(),
                        0,
                        retract,
                        Some(&test_row_id(input, 0)),
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                operator
                    .drain_native_dirty(&mut ctx, &mut collector)
                    .await
                    .unwrap();
                let emitted = collector
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>();
                // An explicitly dirty unchanged group emits no duplicate CDC.
                let mut scope = store.begin_point().await.unwrap();
                scope
                    .put(&native_group_key(b'D', &keys[group]).unwrap(), &[1])
                    .unwrap();
                scope.commit().await.unwrap();
                operator
                    .drain_native_dirty(&mut ctx, &mut collector)
                    .await
                    .unwrap();
                assert_eq!(
                    collector
                        .batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>(),
                    emitted
                );
            }
            let mut actual = Vec::new();
            for output in &collector.batches {
                let retracts = IncrementalAggregatingFunc::get_retracts(output).unwrap();
                for row in 0..output.num_rows() {
                    actual.push((
                        output.column(0).as_string::<i32>().value(row).to_string(),
                        retracts.value(row),
                        ScalarValue::try_from_array(output.column(6), row).unwrap(),
                        ScalarValue::try_from_array(output.column(1), row).unwrap(),
                        ScalarValue::try_from_array(output.column(2), row).unwrap(),
                        ScalarValue::try_from_array(output.column(9), row).unwrap(),
                    ));
                }
            }
            let value = |key: &str, retract, count, first: &str, last: &str, time| {
                (
                    key.to_owned(),
                    retract,
                    ScalarValue::Int64(Some(count)),
                    ScalarValue::Utf8(Some(first.into())),
                    ScalarValue::Utf8(Some(last.into())),
                    ScalarValue::TimestampNanosecond(Some(time), None),
                )
            };
            assert_eq!(
                actual,
                vec![
                    value("a", false, 1, "a", "a", 1),
                    value("z", false, 1, "a", "a", 1),
                    value("z", true, 1, "a", "a", 1),
                    value("z", false, 2, "a", "z", 2),
                    value("a", true, 1, "a", "a", 1),
                    value("a", false, 2, "a", "z", 2),
                    value("a", true, 2, "a", "z", 2),
                    value("a", false, 1, "a", "a", 1),
                    value("a", true, 1, "a", "a", 1),
                    value("a", false, 1, "a", "a", 1)
                ],
                "rocks={rocks}"
            );
            drop(operator);
            drop(manager);
            backend.close().await.unwrap();
            assert!(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .is_ok()
            );
            if rocks {
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[test]
    fn selector_flush_admission_accounts_for_concurrent_candidate_decode() {
        let frame = 32 * 1024;
        let mut config = native_test_config();
        config.value_bytes = frame;
        config.overlay_bytes = 64 * frame;
        config.max_pending_output_bytes = 24 * frame;
        assert_eq!(
            native_decoded_workspace(config, false).unwrap(),
            139 * frame
        );
        assert_eq!(native_decoded_workspace(config, true).unwrap(), 142 * frame);
        for factor in [139, 142] {
            let resources = WorkerStateResources::new(ResourceConfig {
                block_cache_bytes: 1024 * 1024,
                memtable_bytes: 1024 * 1024,
                queued_write_bytes: 4 * 1024 * 1024,
                decoded_value_bytes: factor * frame,
                scan_page_bytes: 1024 * 1024,
                max_blocking_operations: 2,
                max_snapshots: 2,
                max_open_databases: 1,
                disk_reserve_bytes: 0,
            })
            .unwrap();
            let output = resources
                .try_decoded_value(3 * config.max_pending_output_bytes)
                .unwrap();
            let scope = resources
                .try_decoded_value(config.overlay_bytes + 3 * frame)
                .unwrap();
            let candidate = resources.try_decoded_value(3 * frame);
            assert_eq!(candidate.is_ok(), factor == 142);
            assert_eq!(
                native_decoded_workspace(config, true).unwrap() <= factor * frame,
                factor == 142
            );
            drop(candidate);
            drop(scope);
            drop(output);
            assert!(resources.try_decoded_value(factor * frame).is_ok());
        }
        config.value_bytes = usize::MAX;
        assert!(native_decoded_workspace(config, true).is_err());
    }

    #[tokio::test]
    async fn selector_real_flush_preserves_checkpoint_writer_headroom_and_reloads_both_backends() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let frame = 1024 * 1024;
        let config = AggregateStateConfig {
            key_bytes: 512,
            value_bytes: frame,
            page_bytes: 2 * frame,
            page_entries: 64,
            write_bytes: 64 * frame,
            write_operations: 128,
            overlay_bytes: 64 * frame,
            max_pending_output_rows: 64,
            max_pending_output_bytes: 24 * frame,
            max_resident_bytes: 8 * frame,
        };
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: NATIVE_AGGREGATE_TABLE.as_bytes().to_vec(),
        };
        let encoded_key = aggregate_encoded_key_bound(&namespace, config.key_bytes).unwrap();
        let mut resource_config = ResourceConfig {
            block_cache_bytes: frame,
            memtable_bytes: frame,
            queued_write_bytes: AdmittedWriteBatch::reservation_bytes(
                config.write_bytes,
                config.write_operations,
            )
            .unwrap()
                * 2,
            decoded_value_bytes: 142 * frame,
            scan_page_bytes: 16 * frame,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 2,
            disk_reserve_bytes: 0,
        };
        let writer_bytes = checkpoint_decoded_reservation(&resource_config).unwrap();
        assert_eq!(writer_bytes, 4_374_526);
        let required = aggregate_decoded_foreground(
            native_limits(config),
            config.max_pending_output_bytes,
            true,
            encoded_key,
        )
        .unwrap()
        .checked_add(writer_bytes)
        .unwrap();
        assert_eq!(required, 153_272_318);
        assert!(validate_selector_owner_decoded(config, &resource_config, encoded_key).is_err());
        resource_config.decoded_value_bytes = required - 1;
        assert!(validate_selector_owner_decoded(config, &resource_config, encoded_key).is_err());
        resource_config.decoded_value_bytes = required;
        assert_eq!(
            validate_selector_owner_decoded(config, &resource_config, encoded_key).unwrap(),
            required
        );
        let mut overflow = config;
        overflow.overlay_bytes = usize::MAX;
        assert!(validate_selector_owner_decoded(overflow, &resource_config, encoded_key).is_err());
        for rocks in [false, true] {
            for decoded in [142 * frame, required] {
                let root = std::env::temp_dir()
                    .join(format!("selector-writer-overlap-{}", uuid::Uuid::new_v4()));
                let resources = WorkerStateResources::new(ResourceConfig {
                    decoded_value_bytes: decoded,
                    ..resource_config.clone()
                })
                .unwrap();
                let native = if rocks {
                    Some(Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.clone(),
                                job_id: "selector-writer-overlap".into(),
                                operator_id: "aggregate".into(),
                                subtask: 0,
                                generation: 0,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    ))
                } else {
                    None
                };
                let backend: Arc<dyn LiveStateBackend> = if let Some(native) = &native {
                    native.clone()
                } else {
                    Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * frame).unwrap())
                };
                let mut manager =
                    LiveTableManager::new(backend.clone(), namespace.ownership.clone()).unwrap();
                let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
                let mut store = AggregateStore::new(
                    backend.clone(),
                    table,
                    resources.clone(),
                    native_limits(config),
                )
                .unwrap();
                store.preserve_selector_checkpoint_headroom();
                // At the exact minimum no optional expiry/member key may steal
                // the foreground+writer allowance, even before permits are held.
                assert!(
                    store
                        .try_expiry_progress(config.max_pending_output_bytes)
                        .is_none()
                );
                assert!(
                    store
                        .try_member_progress(config.max_pending_output_bytes)
                        .is_none()
                );
                let mut operator = native_operator();
                operator.native_config = Some(config);
                let input = batch(&[Some("a"), Some("b")], &[1, 2], &[Some(true), Some(true)]);
                let inputs = operator.compute_inputs(&input).unwrap();
                let mut scope = store.begin().await.unwrap();
                for row in 0..2 {
                    operator
                        .native_process_event(
                            &mut scope,
                            &GLOBAL_KEY,
                            &inputs,
                            row,
                            false,
                            Some(&[row as u8 + 1; 16]),
                        )
                        .await
                        .unwrap();
                }
                scope.commit().await.unwrap();
                operator.native_store = Some(store);
                let mut ctx = calendar_dispatch_context(&mut operator).await;
                let mut collector = AggregateCollector::default();
                // Deterministically reproduce the existing asynchronous export
                // lease while executing the actual native dirty drain.
                let writer = resources.try_decoded_value(writer_bytes).unwrap();
                let flushed = operator.drain_native_dirty(&mut ctx, &mut collector).await;
                if decoded == 142 * frame {
                    let error = flushed.unwrap_err();
                    assert!(format!("{error:#}").contains("aggregate scope decoded reservation"));
                    assert!(collector.batches.is_empty());
                    drop(writer);
                    operator
                        .drain_native_dirty(&mut ctx, &mut collector)
                        .await
                        .unwrap();
                } else {
                    flushed.unwrap();
                    drop(writer);
                }
                assert_eq!(
                    collector
                        .batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>(),
                    1
                );
                assert_eq!(
                    collector.batches[0]
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "b"
                );
                // A new owner reconstructs G and its candidate index, then emits
                // the exact before image and the new winner under the same lease.
                if decoded == required {
                    let mut fresh = native_operator();
                    fresh.native_config = Some(config);
                    fresh.native_store = operator.native_store.take();
                    drop(operator);
                    let input = batch(&[Some("c")], &[3], &[Some(true)]);
                    let inputs = fresh.compute_inputs(&input).unwrap();
                    let store = fresh.native_store.as_ref().unwrap();
                    let mut scope = store.begin().await.unwrap();
                    fresh
                        .native_process_event(
                            &mut scope,
                            &GLOBAL_KEY,
                            &inputs,
                            0,
                            false,
                            Some(&[3; 16]),
                        )
                        .await
                        .unwrap();
                    scope.commit().await.unwrap();
                    let mut fresh_ctx = calendar_dispatch_context(&mut fresh).await;
                    let mut next = AggregateCollector::default();
                    let writer = resources.try_decoded_value(writer_bytes).unwrap();
                    fresh
                        .drain_native_dirty(&mut fresh_ctx, &mut next)
                        .await
                        .unwrap();
                    drop(writer);
                    let selected = next
                        .batches
                        .iter()
                        .flat_map(|batch| {
                            let values = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            (0..values.len())
                                .map(|row| values.value(row).to_owned())
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(selected, ["b", "c"]);
                    drop(fresh);
                    drop(fresh_ctx);
                } else {
                    drop(operator);
                }
                drop(ctx);
                drop(collector);
                drop(manager);
                drop(backend);
                assert!(resources.try_decoded_value(decoded).is_ok());
                if let Some(native) = native {
                    Arc::try_unwrap(native)
                        .unwrap_or_else(|_| panic!("retained overlap backend"))
                        .close_and_remove()
                        .await
                        .unwrap();
                    std::fs::remove_dir_all(root).unwrap();
                }
            }
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
        native_test_store_with_resource_budgets(limits, queued_write_bytes, 4 * 1024 * 1024)
    }

    fn native_test_store_with_resource_budgets(
        limits: AggregateStoreLimits,
        queued_write_bytes: usize,
        decoded_value_bytes: usize,
    ) -> AggregateStore {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes,
            decoded_value_bytes,
            scan_page_bytes: (1024_usize * 1024).max(limits.page_bytes.checked_mul(8).unwrap()),
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
        let ids = (0..base.num_rows())
            .map(|row| test_row_id(base, row))
            .collect::<Vec<_>>();
        native_test_changelog_with_ids(base, retracts, &ids)
    }

    fn native_test_changelog_with_ids(
        base: &RecordBatch,
        retracts: &[bool],
        ids: &[[u8; 16]],
    ) -> RecordBatch {
        assert_eq!(base.num_rows(), retracts.len());
        assert_eq!(base.num_rows(), ids.len());
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![
                Arc::new(BooleanArray::from(retracts.to_vec())),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ),
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
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                let mut scope = store.begin_point().await.unwrap();
                let key = native_group_key(b'G', group).unwrap();
                let mut previous = operator
                    .native_group_codec()
                    .unwrap()
                    .decode(
                        &scope.get(&key).await.unwrap().unwrap(),
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
                        &operator
                            .native_group_codec()
                            .unwrap()
                            .encode(&previous, scope.limits().value_bytes)
                            .unwrap(),
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
                            )
                            .await
                            .unwrap();
                    }
                    scope.commit().await.unwrap();
                }
                let scope = store.begin().await.unwrap();
                let current = operator
                    .native_group_codec()
                    .unwrap()
                    .decode(
                        &scope.get(&key).await.unwrap().unwrap(),
                        scope.limits().value_bytes,
                    )
                    .unwrap();
                assert_eq!(current.generation, 1);
                assert_eq!(current.next_ordinal, 5);
                assert_eq!(current.live_rows, 5);
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
    async fn native_input_cancellation_releases_pending_state_scope() {
        let base = batch(&[Some("value")], &[1], &[Some(true)]);
        let input = native_test_changelog(&base, &[true]);
        let seed = native_test_changelog(&base, &[false]);
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
        let store = operator.native_store.as_ref().unwrap();
        let mut ctx = native_input_context(input.schema()).await;
        // A real matching retraction still needs a stable scan. Seed its
        // member before exhausting permits; cancellation must leave it intact.
        operator
            .process_native_batch(&seed, &mut ctx)
            .await
            .unwrap();
        // Exhaust the real backend's snapshot permits so processing suspends
        // after acquiring its state scope.
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
        assert!(store.resources().try_decoded_value(otherwise_free).is_err());
        assert!(store.resources().try_queued_write(1).is_err());
        drop(processing);
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
            let mut operator = native_append_only_operator();
            operator.native_store = Some(native_test_store());
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
            // Established ordering commits the marker and before-value before
            // collector completion. A cold retry must not emit it twice, even
            // when the collector await was cancelled after that commit.
            let scope = operator
                .native_store
                .as_ref()
                .unwrap()
                .begin()
                .await
                .unwrap();
            assert!(
                scope
                    .get(&native_group_key(b'D', &GLOBAL_KEY).unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            let bytes = scope
                .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                .await
                .unwrap()
                .unwrap();
            assert!(
                operator
                    .native_group_codec()
                    .unwrap()
                    .decode(&bytes, scope.limits().value_bytes)
                    .unwrap()
                    .last_emitted
                    .is_some()
            );
            drop(scope);
            let mut retry = AggregateCollector::default();
            operator
                .drain_native_dirty(&mut ctx, &mut retry)
                .await
                .unwrap();
            assert!(retry.batches.is_empty());
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
    async fn calendar_fresh_empty_checkpoint_restores_count_sum_and_reference() {
        use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for (day, include) in [
            (Some(193), Some(true)),
            (Some(201), Some(true)),
            (None, Some(true)),
            (Some(200), Some(false)),
            (Some(200), None),
        ] {
            for rocks in [false, true] {
                let root = std::env::temp_dir()
                    .join(format!("calendar-fresh-empty-{}", uuid::Uuid::new_v4()));
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
                                    job_id: "calendar-fresh-empty".into(),
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
                let limits = AggregateStoreLimits {
                    page_entries: 1,
                    ..native_test_store().limits()
                };
                let store =
                    AggregateStore::new(backends[0].clone(), table, resources.clone(), limits)
                        .unwrap();
                let mut operator = native_calendar_operator_options(true, true);
                operator.native_store = Some(store);
                calendar_test_event_for_group(
                    &mut operator,
                    &GLOBAL_KEY,
                    day,
                    200,
                    include,
                    false,
                    &[1; 16],
                )
                .await
                .unwrap();
                assert_eq!(
                    calendar_test_values(&operator).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(None),
                    ]
                );
                let scope = operator
                    .native_store
                    .as_ref()
                    .unwrap()
                    .begin()
                    .await
                    .unwrap();
                calendar_test_assert_references(&operator, &scope, &GLOBAL_KEY, 200).await;
                drop(scope);
                let snapshot = backends[0].snapshot().await.unwrap();
                let storage = arroyo_state::get_storage_provider(
                    &arroyo_state::StorageProviderFor::Controller {
                        storage_url: Some(format!("file://{}", remote.display())),
                    },
                )
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
                let mut fresh = native_calendar_operator_options(true, true);
                fresh.native_store = Some(restored_store);
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(None),
                    ]
                );
                let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
                calendar_test_assert_references(&fresh, &scope, &GLOBAL_KEY, 200).await;
                drop(scope);
                calendar_test_event(&mut fresh, 200, 200, Some(true), false, &[2; 16]).await;
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(2)),
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(1)),
                    ]
                );
                // Retraction must recover the original calendar day. Other indexed
                // aggregates in this fixture require their truthful before-image filter.
                calendar_test_event(&mut fresh, 190, 200, Some(true), true, &[2; 16]).await;
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(None),
                    ]
                );
                // A fresh restored owner's real-watermark callback updates the
                // reference proofs and accumulators in the same committed G.
                fresh
                    .calendar_watermark(arroyo_types::Watermark::EventTime(
                        arroyo_types::event_time::from_signed_nanos(201 * 86_400_000_000_000)
                            .unwrap(),
                    ))
                    .await
                    .unwrap();
                let mut scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
                fresh
                    .recalculate_calendar_group(&mut scope, &GLOBAL_KEY, 201)
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
                calendar_test_assert_references(&fresh, &scope, &GLOBAL_KEY, 201).await;
                drop(scope);
                // The initially future day201 becomes eligible at this real
                // boundary. Old/null/filtered originals remain ineligible.
                let day_count = i64::from(include == Some(true) && day == Some(201));
                let week_sum = (include == Some(true)
                    && day.is_some_and(|day| (195..=201).contains(&day)))
                .then_some(1);
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(day_count)),
                        ScalarValue::Int64(week_sum),
                    ]
                );
                calendar_test_event(&mut fresh, 201, 201, Some(true), false, &[3; 16]).await;
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(2)),
                        ScalarValue::Int64(Some(day_count + 1)),
                        ScalarValue::Int64(Some(week_sum.unwrap_or(0) + 1)),
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
    }

    #[tokio::test]
    async fn calendar_pruning_checkpoint_restores_boundary_and_original_corrections() {
        use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root =
                std::env::temp_dir().join(format!("calendar-pruning-{}", uuid::Uuid::new_v4()));
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
                                job_id: "calendar-pruning".into(),
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
            let limits = AggregateStoreLimits {
                page_entries: 1,
                ..native_test_store().limits()
            };
            let store =
                AggregateStore::new(backends[0].clone(), table, resources.clone(), limits).unwrap();
            let mut operator = native_calendar_operator();
            operator.native_store = Some(store);
            calendar_test_event(&mut operator, 193, 200, Some(true), false, &[1; 16]).await;
            calendar_test_event(&mut operator, 194, 200, Some(true), false, &[2; 16]).await;
            // Checkpoint after the first committed cleanup page; the pending
            // cursor must survive and resume beyond the already deleted bucket.
            operator
                .calendar_watermark(arroyo_types::Watermark::EventTime(
                    arroyo_types::event_time::from_signed_nanos(200 * 86_400_000_000_000).unwrap(),
                ))
                .await
                .unwrap();
            let store = operator.native_store.as_ref().unwrap();
            let scope = store.begin().await.unwrap();
            let pending = crate::arrow::aggregate_codec::CalendarCleanup::decode(
                &scope.get(b"V").await.unwrap().unwrap(),
            )
            .unwrap();
            assert!(!pending.complete);
            assert!(pending.after.is_some());
            drop(scope);
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
            fresh.prune_calendar_buckets().await.unwrap();
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1)),
                ]
            );
            let restored_store = fresh.native_store.as_ref().unwrap();
            let scope = restored_store.begin().await.unwrap();
            let progress = crate::arrow::aggregate_codec::CalendarProgress::decode(
                &scope.get(b"W").await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(progress.first_retained_day, 194);
            let resumed = crate::arrow::aggregate_codec::CalendarCleanup::decode(
                &scope.get(b"V").await.unwrap().unwrap(),
            )
            .unwrap();
            assert!(resumed.after > pending.after);
            drop(scope);
            fresh.prune_calendar_buckets().await.unwrap();
            calendar_test_event(&mut fresh, 200, 200, Some(false), true, &[1; 16]).await;
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1)),
                ]
            );
            calendar_test_event(&mut fresh, 201, 201, Some(true), false, &[1; 16]).await;
            assert_eq!(
                calendar_test_values(&fresh).await,
                vec![
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(1)),
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
            let limits = AggregateStoreLimits {
                page_entries: 2,
                ..native_test_store().limits()
            };
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
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
            }
            // Persist a mixed List<UTF8>/timestamp last-emitted row before
            // checkpointing, so recovery must retain the exact prior CDC row.
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
            let metadata_expr = operator.metadata_expr.clone();
            let mut ctx = native_input_context(input.schema()).await;
            let mut output = SchemaBuilder::from(operator.schema_without_metadata.as_ref().clone());
            output.push(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            ));
            ctx.out_schema = Some(Arc::new(
                ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap(),
            ));
            let mut before = AggregateCollector::default();
            operator
                .drain_native_dirty(&mut ctx, &mut before)
                .await
                .unwrap();
            assert_eq!(
                before
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
            let prior = before.batches[0]
                .columns()
                .iter()
                .take(2)
                .map(|array| ScalarValue::try_from_array(array, 0).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                prior[0],
                ScalarValue::List(ScalarValue::new_list(
                    &[ScalarValue::Utf8(Some("winner".into()))],
                    &DataType::Utf8,
                    true
                ))
            );
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
            // The fresh owner continues after every original owner/snapshot
            // reference is released; the restored backend has a separate path.
            drop(snapshot);
            drop(operator);
            drop(manager);
            drop(backends.remove(0));
            if rocks {
                Arc::try_unwrap(native.remove(0))
                    .unwrap_or_else(|_| panic!("retained original Rocks backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
            }
            checkpoint::restore(
                backends[0].as_ref(),
                &namespace,
                &config,
                &receipt,
                &storage,
            )
            .await
            .unwrap();
            let mut restored_manager =
                LiveTableManager::new(backends[0].clone(), ownership).unwrap();
            let restored_table = restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let restored_store = AggregateStore::new(
                backends[0].clone(),
                restored_table,
                resources.clone(),
                limits,
            )
            .unwrap();
            let mut fresh = native_array_operator(false, false, false);
            fresh.aggregates[0].collection_output_limit = Some(1);
            fresh.metadata_expr = metadata_expr;
            fresh.native_store = Some(restored_store);
            let restored_store = fresh.native_store.as_ref().unwrap();
            assert_eq!(
                array_members(restored_store, &fresh).await,
                vec![ScalarValue::Utf8(Some("winner".into()))]
            );
            // Recovered full output spans multiple admitted pages, including
            // a null member; bounded selection must retain all of that state.
            fresh.aggregates[0].collection_output_limit = None;
            assert_eq!(
                array_members(restored_store, &fresh).await,
                vec![
                    ScalarValue::Utf8(Some("winner".into())),
                    ScalarValue::Utf8(Some("next".into())),
                    ScalarValue::Utf8(None)
                ]
            );
            fresh.aggregates[0].collection_output_limit = Some(1);
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
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                array_members(restored_store, &fresh).await,
                vec![ScalarValue::Utf8(Some("next".into()))]
            );
            let mut resumed = AggregateCollector::default();
            fresh
                .drain_native_dirty(&mut ctx, &mut resumed)
                .await
                .unwrap();
            let mut changes = Vec::new();
            for batch in &resumed.batches {
                let retracts = IncrementalAggregatingFunc::get_retracts(batch).unwrap();
                for row in 0..batch.num_rows() {
                    changes.push((
                        retracts.value(row),
                        batch
                            .columns()
                            .iter()
                            .take(2)
                            .map(|array| ScalarValue::try_from_array(array, row).unwrap())
                            .collect::<Vec<_>>(),
                    ));
                }
            }
            let next = vec![
                ScalarValue::List(ScalarValue::new_list(
                    &[ScalarValue::Utf8(Some("next".into()))],
                    &DataType::Utf8,
                    true,
                )),
                prior[1].clone(),
            ];
            assert_eq!(changes, vec![(true, prior), (false, next)]);
            let count = resumed.batches.len();
            fresh
                .drain_native_dirty(&mut ctx, &mut resumed)
                .await
                .unwrap();
            assert_eq!(
                resumed.batches.len(),
                count,
                "recovery re-emitted unchanged complex row"
            );
            drop(fresh);
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

    /// Finite real operator-member diagnostic; no SQL/output throughput claim.
    #[tokio::test]
    #[ignore = "opt-in STR119 finite duplicate churn diagnostic"]
    async fn native_duplicate_churn_pilot() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let root = std::env::temp_dir().join(format!("str119-{}", uuid::Uuid::new_v4()));
        let template = native_test_store();
        let resources = template.resources().clone();
        let limits = AggregateStoreLimits {
            page_entries: 1,
            ..template.limits()
        };
        drop(template);
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "duplicate-pilot".into(),
                    operator_id: "aggregate".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_array_operator_with_limit(false, false, false, Some(1)).unwrap();
        operator.native_store =
            Some(AggregateStore::new(backend.clone(), table, resources, limits).unwrap());
        let store = operator.native_store.as_ref().unwrap();
        let input = batch(&[Some("equal"), None], &[1, 1], &[Some(true); 2]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let values = (0..2)
            .map(|row| inputs[0].selected_values(Some(row)).unwrap().unwrap())
            .collect::<Vec<_>>();
        // Every value has equal tuples but interleaved global ordinal gaps.
        for group in 0_u64..3 {
            for ordinal in 0_u64..192 {
                for (row, value) in values.iter().enumerate() {
                    let mut scope = store.begin().await.unwrap();
                    operator
                        .native_member_delta(
                            &mut scope,
                            NativeMemberChange {
                                group: &group.to_be_bytes(),
                                generation: 0,
                                aggregate_index: 0,
                                values: value,
                                ordinal: ordinal * 2 + row as u64,
                                retract: false,
                                row_id: None,
                            },
                        )
                        .await
                        .unwrap();
                    scope.commit().await.unwrap();
                }
            }
        }
        // Pin pre-deletion history identically in both arms; default compaction remains enabled.
        let history = backend.snapshot().await.unwrap();
        eprintln!(
            "STR119 settings groups=3 values=2 occurrences=192 survivors=8 output_limit=1 page_entries=1 pinned_history=true auto_compaction=default limits={limits:?} resources={:?}",
            store.resources().config()
        );
        let mut completed: usize = 0;
        let mut member_progress =
            store.try_member_progress(native_test_config().max_pending_output_bytes);
        let mut started = std::time::Instant::now();
        let block_size = std::env::var("STR119_BLOCK")
            .map(|n| n.parse::<usize>().unwrap())
            .unwrap_or(32);
        assert!(matches!(block_size, 1 | 32));
        for block in 0..184_usize.div_ceil(block_size) {
            for group in 0_u64..3 {
                for value in &values {
                    for _ in 0..block_size.min(184 - block * block_size) {
                        let mut scope = store.begin().await.unwrap();
                        scope.set_member_progress(member_progress.take());
                        operator
                            .native_member_delta(
                                &mut scope,
                                NativeMemberChange {
                                    group: &group.to_be_bytes(),
                                    generation: 0,
                                    aggregate_index: 0,
                                    values: value,
                                    ordinal: 0,
                                    retract: true,
                                    row_id: None,
                                },
                            )
                            .await
                            .unwrap();
                        member_progress = scope.commit_member_progress().await.unwrap();
                        completed += 1;
                    }
                }
            }
            if completed.is_multiple_of(192) || completed == 1104 {
                eprintln!(
                    "STR119 completed={completed} elapsed_ns={} block_size={block_size}",
                    started.elapsed().as_nanos()
                );
                for field in ["stat", "status", "io"] {
                    eprintln!(
                        "STR119 process completed={completed} field={field} raw={:?}",
                        std::fs::read_to_string(format!("/proc/self/{field}")).unwrap()
                    );
                }
                started = std::time::Instant::now();
            }
        }
        assert_eq!(completed, 1104);
        drop(member_progress);
        // Comparison: exact secondary-key gets, mechanism only (not injected-ID operator).
        let started = std::time::Instant::now();
        for _ in 0..23 {
            for group in 0_u64..3 {
                for (row, value) in values.iter().enumerate() {
                    for ordinal in 184_u64..192 {
                        let (_, key, _) = operator
                            .inspect_member_keys(
                                &group.to_be_bytes(),
                                0,
                                0,
                                value,
                                ordinal * 2 + row as u64,
                            )
                            .unwrap();
                        let scope = store.begin_point().await.unwrap();
                        assert!(scope.get(&key).await.unwrap().is_some());
                    }
                }
            }
        }
        eprintln!(
            "STR119 exact_point_reads=1104 elapsed_ns={}",
            started.elapsed().as_nanos()
        );
        // Also exercise the actual injected-event-time explicit-ID member path.
        let explicit = native_operator();
        let explicit_input = batch_at(&[Some("equal")], &[1], &[10]);
        let explicit_values = explicit.compute_inputs(&explicit_input).unwrap()[8]
            .selected_values(Some(0))
            .unwrap()
            .unwrap();
        for ordinal in 0_u64..1104 {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(&ordinal.to_be_bytes());
            let mut scope = store.begin_point().await.unwrap();
            explicit
                .native_member_delta(
                    &mut scope,
                    NativeMemberChange {
                        group: b"explicit",
                        generation: 0,
                        aggregate_index: 8,
                        values: &explicit_values,
                        ordinal,
                        retract: false,
                        row_id: Some(&id),
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let started = std::time::Instant::now();
        for ordinal in 0_u64..1104 {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(&ordinal.to_be_bytes());
            let mut scope = store.begin_point().await.unwrap();
            explicit
                .native_member_delta(
                    &mut scope,
                    NativeMemberChange {
                        group: b"explicit",
                        generation: 0,
                        aggregate_index: 8,
                        values: &explicit_values,
                        ordinal,
                        retract: true,
                        row_id: Some(&id),
                    },
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        eprintln!(
            "STR119 actual_explicit_id_retracts=1104 elapsed_ns={} secondary_scans=0",
            started.elapsed().as_nanos()
        );

        for group in 0_u64..3 {
            let scope = store.begin().await.unwrap();
            assert_eq!(
                CollectionStats::decode(
                    scope
                        .get(&native_collection_totals_key(&group.to_be_bytes(), 0).unwrap())
                        .await
                        .unwrap()
                )
                .unwrap()
                .members,
                16
            );
        }
        // The actual process_native_batch path, including unchanged chunk admission.
        // Its completed denominator is retracted members, not CDC output.
        let mut batched = native_array_operator_with_limit(false, false, false, Some(1)).unwrap();
        batched.native_store = operator.native_store.take();
        let raw = batch(&[Some("batched"); 192], &[1; 192], &[Some(true); 192]);
        let ids = (0_u64..192)
            .map(|number| {
                let mut id = [0_u8; 16];
                id[..8].copy_from_slice(&number.to_be_bytes());
                id
            })
            .collect::<Vec<_>>();
        let seed = native_test_changelog_with_ids(&raw, &[false; 192], &ids);
        let retract = native_test_changelog_with_ids(&raw, &[true; 192], &ids);
        let mut ctx = native_input_context(seed.schema()).await;
        batched.process_native_batch(&seed, &mut ctx).await.unwrap();
        let batch_rows = std::env::var("STR119_BATCH_ROWS")
            .map(|n| n.parse::<usize>().unwrap())
            .unwrap_or(192);
        assert!(matches!(batch_rows, 1 | 192));
        let started = std::time::Instant::now();
        for offset in (0..192).step_by(batch_rows) {
            batched
                .process_native_batch(&retract.slice(offset, batch_rows), &mut ctx)
                .await
                .unwrap();
        }
        eprintln!(
            "STR119 actual_batch_retracts=192 input_batch_rows={batch_rows} elapsed_ns={}",
            started.elapsed().as_nanos()
        );
        assert!(
            array_members(batched.native_store.as_ref().unwrap(), &batched)
                .await
                .is_empty()
        );
        drop(batched);
        drop(history);
        drop(operator);
        drop(manager);
        Arc::try_unwrap(backend)
            .unwrap_or_else(|_| panic!("retained backend"))
            .close_and_remove()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
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
    async fn keyed_membership_overflow_preserves_committed_group() {
        let store = native_test_store();
        let mut operator = native_operator();
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        let group = b"overflow";
        let storage = native_group_key(b'G', group).unwrap();
        let state = EncodedGroup {
            calendar_references: CalendarReferences::default(),
            last_update_nanos: to_nanos(SystemTime::now()) as i64,
            generation: 0,
            next_ordinal: 0,
            live_rows: u64::MAX,
            accumulator_state: operator
                .native_state_values(&mut operator.native_accumulators(None).unwrap())
                .unwrap(),
            last_emitted: None,
        };
        let value = operator
            .native_group_codec()
            .unwrap()
            .encode(&state, store.limits().value_bytes)
            .unwrap();
        let mut scope = store.begin().await.unwrap();
        scope.put(&storage, &value).unwrap();
        scope.commit().await.unwrap();
        let input = batch(&[Some("value")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let mut scope = store.begin().await.unwrap();
        let error = operator
            .native_process_event(
                &mut scope,
                group,
                &inputs,
                0,
                false,
                Some(&test_row_id(&input, 0)),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("live-row count overflow"),
            "{error:#}"
        );
        drop(scope);
        let scope = store.begin().await.unwrap();
        assert_eq!(scope.get(&storage).await.unwrap(), Some(value));
        assert!(
            scope
                .get(&native_group_key(b'D', group).unwrap())
                .await
                .unwrap()
                .is_none()
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
                    Some(&test_row_id(&input, 0))
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
                    Some(&test_row_id(&input, 0))
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("configured key allowance before materialization")
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
                            max_member_workspace: 1,
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
                            max_member_workspace: u64::try_from(
                                native_collection_decode_bound(
                                    std::slice::from_ref(value.data_type()),
                                    stored_args.as_ref().len()
                                )
                                .unwrap()
                            )
                            .unwrap(),
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
            .inspect_member_keys(&GLOBAL_KEY, 0, 8, &timestamp_values, 0)
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
        let encoded = operator
            .native_group_codec()
            .unwrap()
            .encode(
                &EncodedGroup {
                    calendar_references: CalendarReferences::default(),
                    last_update_nanos: to_nanos(SystemTime::now()) as i64,
                    generation: 0,
                    next_ordinal: 1,
                    live_rows: 0,
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

    async fn native_test_evaluate(
        operator: &IncrementalAggregatingFunc,
        scope: &AggregateScope<'_>,
        group: &EncodedGroup,
    ) -> Vec<ScalarValue> {
        let mut states = operator.native_accumulators(Some(group)).unwrap();
        let mut values = Vec::new();
        for (index, state) in states.iter_mut().enumerate() {
            values.push(
                if operator.aggregates[index].accumulator_type == AccumulatorType::Batch {
                    operator
                        .native_fallback_value(scope, &GLOBAL_KEY, group.generation, index)
                        .await
                        .unwrap()
                } else {
                    state.evaluate().unwrap()
                },
            );
        }
        values
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
                "min" | "max"
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None)
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
        let mut members = 0;
        scope
            .visit_prefix(b"M", None, |_, _| {
                members += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            members,
            operator
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.selector.is_some())
                .count()
        );
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
        let group = fresh
            .native_group_codec()
            .unwrap()
            .decode(&encoded, scope.limits().value_bytes)
            .unwrap();
        let actual = native_test_evaluate(&fresh, &scope, &group).await;
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None)
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
        let group = fresh
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        let actual = native_test_evaluate(&fresh, &scope, &group).await;
        // Both non-null rows share the earliest eligible ORDER BY key.
        assert_eq!(actual[0], ScalarValue::Utf8(Some("C".into())));
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
                AccumulatorType::Batch
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None)
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
        let checkpoint_group = operator
            .native_group_codec()
            .unwrap()
            .decode(&checkpoint_bytes, scope.limits().value_bytes)
            .unwrap();
        let checkpoint_state = native_test_evaluate(&operator, &scope, &checkpoint_group).await;
        assert_eq!(checkpoint_state[9], ScalarValue::Int64(Some(100)));
        assert_eq!(checkpoint_state[10], ScalarValue::Int64(Some(50)));
        assert_eq!(checkpoint_state[11], ScalarValue::Utf8(None));
        assert_eq!(checkpoint_state[12], ScalarValue::Utf8(None));
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, None)
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        assert!(scope.first(b"M").await.unwrap().is_some());
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
        let states = native_test_evaluate(&fresh, &scope, &group).await;
        assert_eq!(states[9], ScalarValue::Int64(Some(100)));
        assert_eq!(states[10], ScalarValue::Int64(Some(20)));
        assert_eq!(states[11], ScalarValue::Utf8(Some("end".into())));
        assert_eq!(states[12], ScalarValue::Utf8(None));
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
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, None)
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

    fn event_time_selector_operator(append_only: bool) -> IncrementalAggregatingFunc {
        let (mut config, registry) = native_config();
        use datafusion_proto::protobuf::{PhysicalSortExprNode, physical_expr_node::ExprType};
        let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else {
            panic!("aggregate")
        };
        for (index, node) in aggregate.aggr_expr.iter_mut().take(5).enumerate() {
            let Some(ExprType::AggregateExpr(expression)) = &mut node.expr_type else {
                panic!("expression")
            };
            let kind = if index == 0 || index == 2 {
                arroyo_planner::grouped_selector::SelectorKind::First
            } else {
                arroyo_planner::grouped_selector::SelectorKind::Last
            };
            expression.aggregate_function = Some(datafusion_proto::protobuf::physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(
                arroyo_planner::grouped_selector_udaf::grouped_selector(kind, true).name().to_string()));
            let clock: Arc<dyn PhysicalExpr> = Arc::new(Column::new(TIMESTAMP_FIELD, 3));
            expression.ordering_req.insert(
                0,
                PhysicalSortExprNode {
                    expr: Some(Box::new(
                        serialize_physical_expr(&clock, &DefaultPhysicalExtensionCodec {}).unwrap(),
                    )),
                    asc: true,
                    nulls_first: false,
                },
            );
        }
        config.aggregate_exec = plan.encode_to_vec();
        if !append_only {
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
        }
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.retain_indefinitely = true;
        operator
    }

    #[tokio::test]
    async fn shared_selector_rejects_stale_before_version_without_losing_candidate() {
        let store = native_test_store();
        let operator = event_time_selector_operator(false);
        let original = batch_at(&[Some("kept")], &[20], &[20]);
        let mut scope = store.begin_point().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &operator.compute_inputs(&original).unwrap(),
                0,
                false,
                Some(&[7; 16]),
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        for stale in [
            batch_at(&[Some("stale")], &[20], &[99]),
            batch_at(&[Some("kept")], &[19], &[99]),
            batch(&[Some("kept")], &[20], &[Some(false)]),
        ] {
            let mut scope = store.begin().await.unwrap();
            let error = operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &operator.compute_inputs(&stale).unwrap(),
                    0,
                    true,
                    Some(&[7; 16]),
                )
                .await
                .unwrap_err();
            assert!(error.to_string().contains("before-image"), "{error}");
            drop(scope);
            let scope = store.begin().await.unwrap();
            assert_eq!(
                operator
                    .native_fallback_value(&scope, &GLOBAL_KEY, 0, 1)
                    .await
                    .unwrap(),
                ScalarValue::Utf8(Some("kept".into()))
            );
        }
        // A legitimate before image may carry the new triggering clock while
        // its actual value and explicit secondary key retain the prior version.
        let before = batch_at(&[Some("kept")], &[20], &[99]);
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &operator.compute_inputs(&before).unwrap(),
                0,
                true,
                Some(&[7; 16]),
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 1)
                .await
                .unwrap(),
            ScalarValue::Utf8(None)
        );
    }

    impl IncrementalAggregatingFunc {
        // Inspect production member keys without mutating state; retain the exact
        // codec/bounds path used by changes rather than reproducing key encoding.
        fn inspect_member_keys(
            &self,
            group: &[u8],
            generation: u64,
            aggregate_index: usize,
            values: &[ArrayRef],
            ordinal: u64,
        ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
            let template = native_test_store();
            let limits = self.native_store.as_ref().unwrap_or(&template).limits();
            self.native_member_keys(
                &NativeMemberChange {
                    group,
                    generation,
                    aggregate_index,
                    values,
                    retract: false,
                    ordinal,
                    row_id: None,
                },
                limits,
                None,
                false,
            )
        }
    }

    fn native_mixed_companion_operator() -> IncrementalAggregatingFunc {
        native_companion_operator(false)
    }

    fn native_companion_operator(hll: bool) -> IncrementalAggregatingFunc {
        let (mut config, mut registry) = native_config();
        let codec = DefaultPhysicalExtensionCodec {};
        let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else {
            panic!("aggregate");
        };
        let mut final_fields = ArroyoSchema::try_from(config.final_schema.take().unwrap())
            .unwrap()
            .schema
            .fields()
            .to_vec();
        if hll {
            for index in 1..5 {
                aggregate.aggr_expr[index] = aggregate.aggr_expr[6].clone();
                aggregate.aggr_expr_name[index] = format!("unused_count_{index}");
                aggregate.filter_expr[index].expr = None;
                final_fields[index] = Arc::new(
                    final_fields[6]
                        .as_ref()
                        .clone()
                        .with_name(format!("unused_count_{index}")),
                );
            }
        }
        let functions = [
            if hll {
                datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf()
            } else {
                datafusion::functions_aggregate::median::median_udaf()
            },
            datafusion::functions_aggregate::stddev::stddev_udaf(),
        ];
        for (offset, function) in functions.into_iter().enumerate() {
            Arc::get_mut(&mut registry)
                .unwrap()
                .register_udaf(function.clone())
                .unwrap();
            let argument: Arc<dyn PhysicalExpr> = if offset == 0 && hll {
                Arc::new(Column::new("value", 0))
            } else if offset == 0 {
                Arc::new(datafusion::physical_expr::expressions::CastExpr::new(
                    Arc::new(Column::new("sequence", 1)),
                    DataType::Float64,
                    None,
                ))
            } else {
                Arc::new(Literal::new(ScalarValue::Float64(Some(1.0))))
            };
            let expression = Arc::new(
                AggregateExprBuilder::new(function, vec![argument])
                    .schema(input_schema())
                    .alias(if offset == 0 {
                        "median_priority"
                    } else {
                        "constant_stddev"
                    })
                    .build()
                    .unwrap(),
            );
            let index = 8 + offset;
            aggregate.aggr_expr.insert(
                index,
                serialize_physical_aggr_expr(expression.clone(), &codec).unwrap(),
            );
            aggregate
                .aggr_expr_name
                .insert(index, expression.name().into());
            aggregate.filter_expr.insert(
                index,
                datafusion_proto::protobuf::MaybeFilter {
                    expr: (offset == 0).then(|| {
                        serialize_physical_expr(
                            &(Arc::new(Column::new("include", 2)) as Arc<dyn PhysicalExpr>),
                            &codec,
                        )
                        .unwrap()
                    }),
                },
            );
            final_fields.insert(index, expression.field());
        }
        config.aggregate_exec = plan.encode_to_vec();
        config.final_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(final_fields)))
                .unwrap()
                .into(),
        );
        let mut input = SchemaBuilder::from(input_schema().as_ref().clone());
        input.push(Field::new(
            UPDATING_META_FIELD,
            DataType::Struct(updating_meta_fields()),
            false,
        ));
        config.input_schema = Some(
            ArroyoSchema::from_schema_unkeyed(Arc::new(input.finish()))
                .unwrap()
                .into(),
        );
        let mut limits = native_test_config();
        if hll {
            let proto = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
            let Some(PhysicalPlanType::Aggregate(plan)) = proto.physical_plan_type else {
                panic!("aggregate")
            };
            let floor = plan
                .aggr_expr
                .iter()
                .zip(&plan.aggr_expr_name)
                .map(|(expr, name)| {
                    let aggregate =
                        decode_aggregate(&input_schema(), name, expr, registry.as_ref()).unwrap();
                    aggregate_descriptor(&aggregate, &input_schema())
                        .unwrap()
                        .map_or(0, |descriptor| descriptor.retained_bound(0, 0).unwrap())
                })
                .sum::<usize>();
            // Retain the audited constructor floor plus the existing one-row
            // fixture allowance, rounded to a finite test frame size.
            limits.value_bytes = (floor + native_test_config().value_bytes).next_power_of_two();
            limits.page_bytes = limits.value_bytes.checked_mul(2).unwrap();
            limits.write_bytes = limits.value_bytes.checked_mul(8).unwrap();
            limits.overlay_bytes = limits.write_bytes;
            limits.max_pending_output_bytes = limits.value_bytes.checked_mul(24).unwrap();
        }
        let operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(limits),
        )
        .unwrap();
        if hll {
            // Both fixed-replay fixtures use at most100B per selected value.
            // Check the full shared replay formula, rather than assuming the
            // constructor-floor margin also covers the member temporaries.
            let largest = "s".repeat(100);
            let values = vec![Arc::new(StringArray::from(vec![largest.as_str()])) as ArrayRef];
            let encoded = bounded_row_size(&values, limits.value_bytes).unwrap();
            let aggregate = &operator.aggregates[8];
            let working = aggregate
                .descriptor
                .unwrap()
                .retained_bound(1, 0)
                .unwrap()
                .checked_add(aggregate.member_workspace(encoded, 0, false).unwrap())
                .unwrap();
            assert!(working <= limits.value_bytes);
            assert!(working.checked_mul(8).unwrap() <= limits.max_pending_output_bytes / 3);
        }
        operator
    }

    fn native_companion_store(operator: &IncrementalAggregatingFunc) -> AggregateStore {
        let config = operator.native_config.unwrap();
        let limits = AggregateStoreLimits {
            key_bytes: config.key_bytes,
            value_bytes: config.value_bytes,
            page_bytes: config.page_bytes,
            page_entries: config.page_entries,
            write_bytes: config.write_bytes,
            write_operations: config.write_operations,
            overlay_bytes: config.overlay_bytes,
        };
        let decoded = config
            .overlay_bytes
            .checked_add(config.max_pending_output_bytes.checked_mul(3).unwrap())
            .unwrap()
            .checked_add(config.value_bytes.checked_mul(3).unwrap())
            .unwrap();
        native_test_store_with_resource_budgets(
            limits,
            config.value_bytes.checked_mul(64).unwrap(),
            decoded,
        )
    }

    #[tokio::test]
    async fn mixed_selectors_replay_median_and_fixed_stddev_with_filter_identity_and_reload() {
        let store = native_test_store();
        let operator = native_mixed_companion_operator();
        assert!(operator.aggregates[8].replay_members && !operator.aggregates[8].array_result);
        assert_eq!(
            operator.aggregates[9].accumulator_type,
            AccumulatorType::Sliding
        );
        let input = batch(
            &[Some("a"), Some("b"), Some("c"), Some("d"), Some("excluded")],
            &[1, 5, 9, 9, 1000],
            &[Some(true), Some(true), Some(true), Some(true), Some(false)],
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
                    Some(&[row as u8; 16]),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let fresh = native_mixed_companion_operator();
        let scope = store.begin().await.unwrap();
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::Float64(Some(7.0))
        );
        let group = fresh
            .native_group_codec()
            .unwrap()
            .decode(
                &scope
                    .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                    .await
                    .unwrap()
                    .unwrap(),
                scope.limits().value_bytes,
            )
            .unwrap();
        let mut states = fresh.native_accumulators(Some(&group)).unwrap();
        assert_eq!(
            states[9].evaluate().unwrap(),
            ScalarValue::Float64(Some(0.0))
        );
        drop(scope);
        for (row, expected) in [(3, 5.0), (2, 3.0)] {
            let mut scope = store.begin().await.unwrap();
            fresh
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    true,
                    Some(&[row as u8; 16]),
                )
                .await
                .unwrap();
            assert_eq!(
                fresh
                    .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                    .await
                    .unwrap(),
                ScalarValue::Float64(Some(expected))
            );
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        let stats = CollectionStats::decode(
            scope
                .get(&native_collection_totals_key(&GLOBAL_KEY, 8).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(stats.members, 2);
    }

    #[tokio::test]
    async fn mixed_replay_companion_rejects_over_budget_before_committing_member() {
        let store = native_test_store();
        let operator = native_mixed_companion_operator();
        let input = batch(&[Some("same")], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        let key = native_collection_totals_key(&GLOBAL_KEY, 8).unwrap();
        let mut accepted = 0;
        let mut rejected = false;
        for identity in 0..1000_u64 {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(&identity.to_be_bytes());
            let mut scope = store.begin().await.unwrap();
            match operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, false, Some(&id))
                .await
            {
                Ok(()) => {
                    scope.commit().await.unwrap();
                    accepted += 1;
                }
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("native collection exceeds configured"),
                        "{error}"
                    );
                    drop(scope);
                    rejected = true;
                    break;
                }
            }
        }
        assert!(
            rejected && accepted > 1,
            "finite admitted relation must reject before unbounded accumulator replay"
        );
        let scope = store.begin().await.unwrap();
        assert_eq!(
            CollectionStats::decode(scope.get(&key).await.unwrap())
                .unwrap()
                .members,
            accepted
        );
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::Float64(Some(1.0))
        );
    }

    #[tokio::test]
    async fn replay_companion_rejects_underreported_stats_before_accumulator_update() {
        let store = native_test_store();
        let operator = native_mixed_companion_operator();
        let input = batch(&[Some("kept")], &[7], &[Some(true)]);
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(
                &mut scope,
                &GLOBAL_KEY,
                &operator.compute_inputs(&input).unwrap(),
                0,
                false,
                Some(&[1; 16]),
            )
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let mut scope = store.begin().await.unwrap();
        scope
            .put(
                &native_collection_totals_key(&GLOBAL_KEY, 8).unwrap(),
                &CollectionStats::default().encode(),
            )
            .unwrap();
        let error = operator
            .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("before accumulator update"),
            "{error}"
        );
        drop(scope);
        let scope = store.begin().await.unwrap();
        assert_eq!(
            operator
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::Float64(Some(7.0))
        );
    }

    #[test]
    fn member_workspace_statistics_reject_old_and_inconsistent_frames() {
        assert!(CollectionStats::decode(Some(vec![0; 24])).is_err());
        for stats in [
            CollectionStats {
                members: 1,
                ..CollectionStats::default()
            },
            CollectionStats {
                max_member_workspace: 1,
                ..CollectionStats::default()
            },
        ] {
            assert!(CollectionStats::decode(Some(stats.encode().to_vec())).is_err());
        }
        let stats = CollectionStats {
            members: 2,
            encoded_args_bytes: 17,
            scalar_bytes: 23,
            max_member_workspace: 47,
        };
        let restored = CollectionStats::decode(Some(stats.encode().to_vec())).unwrap();
        assert_eq!(restored.max_member_workspace, 47);
        assert_eq!(restored.members, 2);
    }

    #[tokio::test]
    async fn fixed_replay_restored_maximum_includes_hll_payload_scratch_and_resets_when_empty() {
        let operator = native_companion_operator(true);
        let store = native_companion_store(&operator);
        let large = "x".repeat(100);
        let input = batch(
            &[Some("small"), Some(&large), Some("medium-length")],
            &[1, 2, 3],
            &[Some(true), Some(true), Some(true)],
        );
        let inputs = operator.compute_inputs(&input).unwrap();
        let key = native_collection_totals_key(&GLOBAL_KEY, 8).unwrap();
        for row in 0..3 {
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    row,
                    false,
                    Some(&[row as u8; 16]),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let mut scope = store.begin().await.unwrap();
        let before_nonlargest = CollectionStats::decode(scope.get(&key).await.unwrap()).unwrap();
        operator
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 2, true, Some(&[2; 16]))
            .await
            .unwrap();
        assert_eq!(
            CollectionStats::decode(scope.get(&key).await.unwrap())
                .unwrap()
                .max_member_workspace,
            before_nonlargest.max_member_workspace
        );
        scope.commit().await.unwrap();
        let original = {
            let scope = store.begin().await.unwrap();
            CollectionStats::decode(scope.get(&key).await.unwrap()).unwrap()
        };
        // A corrupted maximum that admits row decoding but excludes HLL's to_string scratch
        // must fail before decoding/updating, and leave the committed relation intact.
        let mut scope = store.begin().await.unwrap();
        let mut corrupt = original;
        corrupt.max_member_workspace -= 1;
        let selected = inputs[8].selected_values(Some(1)).unwrap().unwrap();
        let encoded = bounded_row_size(&selected, scope.limits().value_bytes).unwrap();
        let decode_without_hll_scratch = encoded * 3 + operator.aggregates[8].member_decode_base;
        assert!(corrupt.max_member_workspace >= decode_without_hll_scratch as u64);
        scope.put(&key, &corrupt.encode()).unwrap();
        let error = operator
            .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("workspace exceeds admitted maximum before decode"),
            "{error}"
        );
        drop(scope);
        // Removing the largest member retains a conservative maximum across fresh owners.
        let fresh = native_companion_operator(true);
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 1, true, Some(&[1; 16]))
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        let remaining = CollectionStats::decode(scope.get(&key).await.unwrap()).unwrap();
        assert_eq!(remaining.members, 1);
        assert_eq!(
            remaining.max_member_workspace,
            original.max_member_workspace
        );
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::UInt64(Some(1))
        );
        drop(scope);
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, Some(&[0; 16]))
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        assert!(scope.get(&key).await.unwrap().is_none());
        drop(scope);
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, false, Some(&[9; 16]))
            .await
            .unwrap();
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        let reset = CollectionStats::decode(scope.get(&key).await.unwrap()).unwrap();
        assert!(reset.max_member_workspace < original.max_member_workspace);
    }

    #[tokio::test]
    async fn fixed_replay_streams_history_larger_than_single_materialization_allowance() {
        let operator = native_companion_operator(true);
        let store = native_companion_store(&operator);
        let value = "s".repeat(100);
        let input = batch(&[Some(&value)], &[1], &[Some(true)]);
        let inputs = operator.compute_inputs(&input).unwrap();
        // The materialization budget is one value_bytes allowance. Exceed it
        // with actual selected payload, independently of row-codec overhead.
        let members = (store.limits().value_bytes / value.len() + 1) as u64;
        for identity in 0..members {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&identity.to_be_bytes());
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, false, Some(&id))
                .await
                .unwrap();
            scope.commit().await.unwrap();
        }
        let scope = store.begin().await.unwrap();
        let stats = CollectionStats::decode(
            scope
                .get(&native_collection_totals_key(&GLOBAL_KEY, 8).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(stats.members, members);
        assert!(
            members.checked_mul(value.len() as u64).unwrap() > scope.limits().value_bytes as u64
        );
        assert!(stats.encoded_args_bytes > scope.limits().value_bytes as u64);
        assert!(stats.members > scope.limits().page_entries as u64);
        let fresh = native_companion_operator(true);
        assert_eq!(
            fresh
                .native_fallback_value(&scope, &GLOBAL_KEY, 0, 8)
                .await
                .unwrap(),
            ScalarValue::UInt64(Some(1))
        );
    }

    #[tokio::test]
    async fn actual_sql_grouped_selector_descriptor_constructs_against_physical_input_schema() {
        use arroyo_datastream::logical::OperatorName;
        use arroyo_planner::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
        for declared in [false, true] {
            let watermark = if declared {
                ", WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND"
            } else {
                ""
            };
            let sql = format!(
                "CREATE TABLE events (row_id BIGINT PRIMARY KEY, k TEXT, value TEXT, priority BIGINT, event_time TIMESTAMP NOT NULL{watermark}) WITH (connector='single_file',path='/tmp/grouped-selector-descriptor.jsonl',format='debezium_json',type='source'); SELECT k,FIRST_VALUE(value ORDER BY priority) AS first,LAST_VALUE(value ORDER BY priority DESC) AS last, MEDIAN(CAST(priority AS DOUBLE)) AS median_priority, STDDEV(1.0) AS constant_stddev FROM events GROUP BY k"
            );
            let compiled =
                parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                    .await
                    .unwrap();
            let config = compiled
                .program
                .graph
                .node_weights()
                .flat_map(|node| node.operator_chain.iter())
                .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
                .map(|(operator, _)| {
                    UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
                })
                .unwrap();
            let raw: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
            assert!(
                raw.schema
                    .index_of(arroyo_planner::grouped_selector::ORDINAL_FIELD)
                    .is_err()
            );
            for malformed in [
                "ordinal",
                "ordinal-name",
                "ordinal-type",
                "marker-args",
                "clock",
                "schema",
            ] {
                if malformed == "clock" && !declared {
                    continue;
                }
                let mut bad = config.clone();
                let mut plan = PhysicalPlanNode::decode(bad.aggregate_exec.as_slice()).unwrap();
                let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type
                else {
                    panic!("aggregate");
                };
                if malformed == "schema" {
                    aggregate.input_schema.as_mut().unwrap().columns.pop();
                } else if malformed == "ordinal-type" {
                    let schema = aggregate.input_schema.as_mut().unwrap();
                    let wrong_type = schema.columns[0].arrow_type.clone();
                    schema.columns.last_mut().unwrap().arrow_type = wrong_type;
                } else {
                    let Some(
                        datafusion_proto::protobuf::physical_expr_node::ExprType::AggregateExpr(
                            expression,
                        ),
                    ) = &mut aggregate.aggr_expr[0].expr_type
                    else {
                        panic!("selector");
                    };
                    if malformed == "marker-args" {
                        expression.expr[1].expr_type = Some(
                            datafusion_proto::protobuf::physical_expr_node::ExprType::ScalarUdf(
                                datafusion_proto::protobuf::PhysicalScalarUdfNode {
                                    name: arroyo_planner::grouped_selector::ORDINAL_MARKER.into(),
                                    args: vec![expression.expr[0].clone()],
                                    fun_definition: None,
                                    return_type: None,
                                    nullable: false,
                                },
                            ),
                        );
                    } else if malformed == "ordinal" || malformed == "ordinal-name" {
                        let Some(datafusion_proto::protobuf::physical_expr_node::ExprType::Column(
                            column,
                        )) = &mut expression.expr[1].expr_type
                        else {
                            panic!("ordinal");
                        };
                        if malformed == "ordinal" {
                            column.index = u32::MAX;
                        } else {
                            column.name = "forged_ordinal".into();
                        }
                    } else {
                        expression.ordering_req[0].asc = false;
                    }
                }
                bad.aggregate_exec = plan.encode_to_vec();
                assert!(
                    IncrementalAggregatingConstructor::build_with_native_config(
                        bad,
                        Arc::new(arroyo_planner::physical::new_registry()),
                        Some(native_test_config())
                    )
                    .is_err(),
                    "malformed {malformed} descriptor must return a structured error"
                );
            }
            let operator = IncrementalAggregatingConstructor::build_with_native_config(
                config,
                Arc::new(arroyo_planner::physical::new_registry()),
                Some(native_test_config()),
            )
            .unwrap();
            assert_eq!(
                operator
                    .aggregates
                    .iter()
                    .filter(|aggregate| aggregate.selector.is_some())
                    .count(),
                2
            );
        }
    }

    #[test]
    fn shared_selector_requires_existing_state_limits() {
        let (config, registry) = native_config();
        let error =
            IncrementalAggregatingConstructor::build_with_native_config(config, registry, None)
                .err()
                .expect("grouped selector must not fall back to the old implementation");
        assert!(
            error.to_string().contains("worker.aggregate-state"),
            "{error}"
        );
    }

    // Runtime index regression; the SQL capture matrix separately proves FOR
    // binding and physical roundtrips. Distinct IDs deliberately share args.
    #[tokio::test]
    async fn event_time_selectors_keep_exact_write_identity_after_reload() {
        for append_only in [false, true] {
            let store = native_test_store();
            let operator = event_time_selector_operator(append_only);
            assert_eq!(
                operator.aggregates[1].accumulator_type,
                AccumulatorType::Batch
            );
            let rows = batch(
                &[Some("A"), Some("B"), Some("A")],
                &[20; 3],
                &[Some(true); 3],
            );
            let inputs = operator.compute_inputs(&rows).unwrap();
            let mut scope = store.begin_point().await.unwrap();
            for row in 0..3 {
                let id = [row as u8 + 1; 16];
                operator
                    .native_process_event(
                        &mut scope,
                        &GLOBAL_KEY,
                        &inputs,
                        row,
                        false,
                        (!append_only).then_some(id.as_slice()),
                    )
                    .await
                    .unwrap();
            }
            scope.commit().await.unwrap();
            let fresh = event_time_selector_operator(append_only);
            let scope = store.begin().await.unwrap();
            for (index, expected) in [(0, "A"), (1, "A"), (2, "A"), (3, "A")] {
                assert_eq!(
                    fresh
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, index)
                        .await
                        .unwrap(),
                    ScalarValue::Utf8(Some(expected.into()))
                );
            }
            drop(scope);
            if append_only {
                continue;
            }
            // Remove the newest equal-valued peer first. Deleting the
            // earliest matching args would leave A winning LAST instead of B.
            for (row, id) in [(2, [3; 16]), (0, [1; 16])] {
                let mut scope = store.begin().await.unwrap();
                fresh
                    .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, true, Some(&id))
                    .await
                    .unwrap();
                assert_eq!(
                    fresh
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, 1)
                        .await
                        .unwrap(),
                    ScalarValue::Utf8(Some("B".into()))
                );
                scope.commit().await.unwrap();
            }
            let scope = store.begin().await.unwrap();
            assert_eq!(
                fresh
                    .native_fallback_value(&scope, &GLOBAL_KEY, 0, 0)
                    .await
                    .unwrap(),
                ScalarValue::Utf8(Some("B".into()))
            );
            drop(scope);
            // New write wins the tie; deleting it restores B's original ordinal.
            let replacement = batch(&[Some("C")], &[20], &[Some(true)]);
            let replacement_inputs = fresh.compute_inputs(&replacement).unwrap();
            for retract in [false, true] {
                let mut scope = store.begin().await.unwrap();
                fresh
                    .native_process_event(
                        &mut scope,
                        &GLOBAL_KEY,
                        &replacement_inputs,
                        0,
                        retract,
                        Some(&[4; 16]),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    fresh
                        .native_fallback_value(&scope, &GLOBAL_KEY, 0, 1)
                        .await
                        .unwrap(),
                    ScalarValue::Utf8(Some(if retract { "B" } else { "C" }.into()))
                );
                scope.commit().await.unwrap();
            }
        }
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, Some(id))
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();

        let fresh = native_operator();
        let before = batch_at(&[Some("b")], &[3], &[99]);
        let inputs = fresh.compute_inputs(&before).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, Some(&ids[1]))
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
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, false, Some(&ids[1]))
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, false, Some(&ids[1]))
                .await
                .unwrap_err()
                .to_string()
                .contains("grouped selector appends duplicate live row identity")
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false, Some(id))
                .await
                .unwrap();
        }
        scope.commit().await.unwrap();

        let fresh = native_operator();
        let before_a = batch_at(&[Some("a")], &[1], &[90]);
        let inputs = fresh.compute_inputs(&before_a).unwrap();
        let mut scope = store.begin().await.unwrap();
        fresh
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, Some(&ids[0]))
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
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true, Some(&ids[1]))
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
            .native_process_event(&mut scope, &left, &inputs, 0, false, Some(&id))
            .await
            .unwrap();
        scope.commit().await.unwrap();

        let before = batch_at(&[Some("v")], &[1], &[100]);
        let after = batch_at(&[Some("v")], &[1], &[110]);
        let before_inputs = operator.compute_inputs(&before).unwrap();
        let after_inputs = operator.compute_inputs(&after).unwrap();
        let mut scope = store.begin().await.unwrap();
        operator
            .native_process_event(&mut scope, &left, &before_inputs, 0, true, Some(&id))
            .await
            .unwrap();
        operator
            .native_process_event(&mut scope, &right, &after_inputs, 0, false, Some(&id))
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
        let group = fresh
            .native_group_codec()
            .unwrap()
            .decode(&stored, scope.limits().value_bytes)
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
        let group = operator
            .native_group_codec()
            .unwrap()
            .decode(&stored, scope.limits().value_bytes)
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

    fn ttl_keyed_operator() -> IncrementalAggregatingFunc {
        let mut operator = native_operator();
        operator.retain_indefinitely = false;
        operator.ttl = Duration::from_secs(3600);
        let mut fields = operator.schema_without_metadata.fields().to_vec();
        fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
        operator.schema_without_metadata = Arc::new(Schema::new(fields));
        operator.key_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let metadata = StructArray::new(
            updating_meta_fields(),
            vec![
                Arc::new(BooleanArray::from(vec![false])),
                ScalarValue::FixedSizeBinary(16, None).to_array().unwrap(),
            ],
            None,
        );
        operator.metadata_expr = Arc::new(Literal::new(ScalarValue::Struct(Arc::new(metadata))));
        operator
    }

    async fn ttl_age_group(operator: &IncrementalAggregatingFunc, key: &[u8], deadline: i64) {
        let store = operator.native_store.as_ref().unwrap();
        let mut scope = store.begin_point().await.unwrap();
        let storage_key = native_group_key(b'G', key).unwrap();
        let mut group = operator
            .native_group_codec()
            .unwrap()
            .decode(
                &scope.get(&storage_key).await.unwrap().unwrap(),
                scope.limits().value_bytes,
            )
            .unwrap();
        let ttl = i64::try_from(operator.ttl.as_nanos()).unwrap();
        scope
            .delete(&native_expiry_key(group.last_update_nanos.saturating_add(ttl), key).unwrap())
            .unwrap();
        group.last_update_nanos = deadline - ttl;
        scope
            .put(
                &storage_key,
                &operator
                    .native_group_codec()
                    .unwrap()
                    .encode(&group, scope.limits().value_bytes)
                    .unwrap(),
            )
            .unwrap();
        scope
            .put(&native_expiry_key(deadline, key).unwrap(), &[1])
            .unwrap();
        scope.commit().await.unwrap();
    }

    #[tokio::test]
    async fn finite_ttl_exact_changelog_and_fresh_worker_recovery() {
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            for recover in [false, true] {
                // Existing unit checkpoint fixtures use default global restore
                // admission. Test tight aggregate admission independently;
                // recovery uses the ordinary checkpoint fixture pools.
                for &tight in if recover {
                    &[false][..]
                } else {
                    &[false, true][..]
                } {
                    let root = std::env::temp_dir()
                        .join(format!("str115-recovery-{}", uuid::Uuid::new_v4()));
                    let remote = root.join("checkpoint");
                    std::fs::create_dir_all(&remote).unwrap();
                    // Existing checkpoint regression pools, plus a tighter case.
                    // Its smaller scan pages keep the existing writer inside the
                    // tight decoded budget; these fixture values fit both pages.
                    let original_store = native_test_store();
                    let original_limits = original_store.limits();
                    let mandatory_decoded = original_limits.overlay_bytes
                        + original_limits.value_bytes * 3
                        + native_test_config().max_pending_output_bytes * 3
                        + original_store.max_encoded_entry_bytes() * 2
                        + 128;
                    let resources = WorkerStateResources::new(ResourceConfig {
                        max_open_databases: 2,
                        scan_page_bytes: if tight && rocks {
                            1024 * 1024
                        } else {
                            16 * 1024 * 1024
                        },
                        queued_write_bytes: 16 * 1024 * 1024,
                        decoded_value_bytes: if tight {
                            mandatory_decoded
                        } else {
                            32 * 1024 * 1024
                        },
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
                                        job_id: "ttl-recovery".into(),
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
                                MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024)
                                    .unwrap(),
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
                    let limits = AggregateStoreLimits {
                        page_entries: 2,
                        ..native_test_store().limits()
                    };
                    let mut operator = ttl_keyed_operator();
                    operator.native_store = Some(
                        AggregateStore::new(backends[0].clone(), table, resources.clone(), limits)
                            .unwrap(),
                    );
                    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from_iter_values(
                        (0..7).map(|i| format!("k{i}")),
                    ))];
                    let rows = operator.key_converter.convert_columns(&columns).unwrap();
                    let keys: Vec<_> = rows.iter().map(|r| r.as_ref().to_vec()).collect();
                    let old = batch(&[Some("a")], &[1], &[Some(true)]);
                    let update = batch(&[Some("z")], &[2], &[Some(true)]);
                    for key in &keys {
                        let store = operator.native_store.as_ref().unwrap();
                        let mut scope = store.begin_point().await.unwrap();
                        operator
                            .native_process_event(
                                &mut scope,
                                key,
                                &operator.compute_inputs(&old).unwrap(),
                                0,
                                false,
                                Some(&test_row_id(&old, 0)),
                            )
                            .await
                            .unwrap();
                        scope.commit().await.unwrap();
                    }
                    let mut ctx = native_input_context(old.schema()).await;
                    let mut output =
                        SchemaBuilder::from(operator.schema_without_metadata.as_ref().clone());
                    output.push(Field::new(
                        UPDATING_META_FIELD,
                        DataType::Struct(updating_meta_fields()),
                        false,
                    ));
                    ctx.out_schema = Some(Arc::new(
                        ArroyoSchema::from_schema_unkeyed(Arc::new(output.finish())).unwrap(),
                    ));
                    let mut collector = AggregateCollector::default();
                    operator
                        .drain_native_dirty(&mut ctx, &mut collector)
                        .await
                        .unwrap();
                    // Real interleaved updates delete old E deadlines and install
                    // later deadlines, using the established native event path.
                    for key in &keys[..5] {
                        let mut scope = operator
                            .native_store
                            .as_ref()
                            .unwrap()
                            .begin_point()
                            .await
                            .unwrap();
                        operator
                            .native_process_event(
                                &mut scope,
                                key,
                                &operator.compute_inputs(&update).unwrap(),
                                0,
                                false,
                                Some(&test_row_id(&update, 0)),
                            )
                            .await
                            .unwrap();
                        scope.commit().await.unwrap();
                    }
                    // The sixth key is quiet: expiry must not require new input.
                    for key in &keys[..6] {
                        ttl_age_group(&operator, key, 1_000_000_000).await;
                    }
                    let mut scope = operator
                        .native_store
                        .as_ref()
                        .unwrap()
                        .begin()
                        .await
                        .unwrap();
                    // Obsolete due entry for a non-due key must not reset its state.
                    scope
                        .put(&native_expiry_key(0, &keys[6]).unwrap(), &[1])
                        .unwrap();
                    scope.commit().await.unwrap();
                    if recover {
                        operator
                            .drain_native_dirty(&mut ctx, &mut collector)
                            .await
                            .unwrap();
                        let store = operator.native_store.as_ref().unwrap();
                        let mut progress = store
                            .try_expiry_progress(native_test_config().max_pending_output_bytes);
                        assert_eq!(progress.is_none(), tight);
                        // First chunk removes stale k6 and expires k0 only.
                        assert!(operator.expire_native(progress.as_mut()).await.unwrap());
                        let scope = store.begin().await.unwrap();
                        let bytes = scope
                            .get(&native_group_key(b'G', &keys[0]).unwrap())
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            operator
                                .native_group_codec()
                                .unwrap()
                                .decode(&bytes, scope.limits().value_bytes)
                                .unwrap()
                                .generation,
                            1
                        );
                        drop(scope);
                        operator
                            .drain_native_dirty(&mut ctx, &mut collector)
                            .await
                            .unwrap();
                        let scope = store.begin().await.unwrap();
                        let bytes = scope
                            .get(&native_group_key(b'G', &keys[0]).unwrap())
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            operator
                                .native_group_codec()
                                .unwrap()
                                .decode(&bytes, scope.limits().value_bytes)
                                .unwrap()
                                .generation,
                            2
                        );
                        drop(scope);
                        drop(progress);
                        let snapshot = backends[0].snapshot().await.unwrap();
                        let storage = arroyo_state::get_storage_provider(
                            &arroyo_state::StorageProviderFor::Controller {
                                storage_url: Some(format!("file://{}", remote.display())),
                            },
                        )
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
                        let restored_table =
                            restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
                        let mut fresh = ttl_keyed_operator();
                        fresh.native_store = Some(
                            AggregateStore::new(
                                backends[1].clone(),
                                restored_table,
                                resources.clone(),
                                limits,
                            )
                            .unwrap(),
                        );
                        operator = fresh;
                        let restored_scope = operator
                            .native_store
                            .as_ref()
                            .unwrap()
                            .begin()
                            .await
                            .unwrap();
                        for (key, expected) in [(&keys[0], 0), (&keys[1], 2)] {
                            let bytes = restored_scope
                                .get(&native_group_key(b'G', key).unwrap())
                                .await
                                .unwrap()
                                .unwrap();
                            let restored_group = operator
                                .native_group_codec()
                                .unwrap()
                                .decode(&bytes, limits.value_bytes)
                                .unwrap();
                            assert_eq!(
                                restored_group.live_rows, expected,
                                "restored membership rocks={rocks} tight={tight}"
                            );
                        }
                        drop(restored_scope);
                        // A recovered worker has no retained minimum. A newly
                        // earlier stale entry must be discovered and removed.
                        let mut scope = operator
                            .native_store
                            .as_ref()
                            .unwrap()
                            .begin()
                            .await
                            .unwrap();
                        scope
                            .put(&native_expiry_key(0, &keys[6]).unwrap(), &[1])
                            .unwrap();
                        scope.commit().await.unwrap();
                        drop(snapshot);
                    }
                    operator
                        .flush_native(&mut ctx, &mut collector)
                        .await
                        .unwrap();
                    let mut changes = Vec::new();
                    for output in &collector.batches {
                        let retracts = IncrementalAggregatingFunc::get_retracts(output).unwrap();
                        for row in 0..output.num_rows() {
                            changes.push((
                                output.column(0).as_string::<i32>().value(row).to_string(),
                                retracts.value(row),
                                ScalarValue::try_from_array(output.column(6), row).unwrap(),
                                ScalarValue::try_from_array(output.column(1), row).unwrap(),
                                ScalarValue::try_from_array(output.column(2), row).unwrap(),
                                ScalarValue::try_from_array(output.column(9), row).unwrap(),
                            ));
                        }
                    }
                    assert_eq!(changes.len(), 23);
                    for i in 0..7 {
                        let name = format!("k{i}");
                        let actual: Vec<_> = changes
                            .iter()
                            .filter(|r| r.0 == name)
                            .map(|r| (r.1, r.2.clone(), r.3.clone(), r.4.clone(), r.5.clone()))
                            .collect();
                        let initial = (
                            false,
                            ScalarValue::Int64(Some(1)),
                            ScalarValue::Utf8(Some("a".into())),
                            ScalarValue::Utf8(Some("a".into())),
                            ScalarValue::TimestampNanosecond(Some(1), None),
                        );
                        let mut expected = vec![initial.clone()];
                        if i < 5 {
                            expected.push((
                                true,
                                initial.1.clone(),
                                initial.2.clone(),
                                initial.3.clone(),
                                initial.4.clone(),
                            ));
                            let updated = (
                                false,
                                ScalarValue::Int64(Some(2)),
                                ScalarValue::Utf8(Some("a".into())),
                                ScalarValue::Utf8(Some("z".into())),
                                ScalarValue::TimestampNanosecond(Some(2), None),
                            );
                            expected.push(updated.clone());
                            expected.push((true, updated.1, updated.2, updated.3, updated.4));
                        } else if i == 5 {
                            expected.push((true, initial.1, initial.2, initial.3, initial.4));
                        }
                        assert_eq!(
                            actual, expected,
                            "backend rocks={rocks} recovery={recover} key={name}"
                        );
                    }
                    // All five pending updates precede any TTL retraction.
                    assert!(changes[7..17].iter().all(|r| r.0 != "k5"));
                    assert!(changes[17..].iter().all(|r| r.1));
                    let emitted = collector
                        .batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>();
                    operator
                        .flush_native(&mut ctx, &mut collector)
                        .await
                        .unwrap();
                    assert_eq!(
                        collector
                            .batches
                            .iter()
                            .map(RecordBatch::num_rows)
                            .sum::<usize>(),
                        emitted
                    );
                    let scope = operator
                        .native_store
                        .as_ref()
                        .unwrap()
                        .begin()
                        .await
                        .unwrap();
                    assert!(
                        scope
                            .get(&native_expiry_key(0, &keys[6]).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    );
                    for key in &keys[..6] {
                        assert!(
                            scope
                                .get(&native_group_key(b'G', key).unwrap())
                                .await
                                .unwrap()
                                .is_none()
                        );
                    }
                    let bytes = scope
                        .get(&native_group_key(b'G', &keys[6]).unwrap())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        operator
                            .native_group_codec()
                            .unwrap()
                            .decode(&bytes, scope.limits().value_bytes)
                            .unwrap()
                            .generation,
                        0
                    );
                    drop(scope);
                    drop(operator);
                    drop(manager);
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
        }
    }

    #[tokio::test]
    async fn finite_ttl_cancelled_progress_retry_and_optional_admission() {
        let resources = native_test_store().resources().clone();
        let backend =
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
        let mut operator = native_operator();
        operator.retain_indefinitely = false;
        operator.ttl = Duration::from_secs(1);
        operator.native_store = Some(
            AggregateStore::new(
                backend.clone(),
                table,
                resources.clone(),
                native_test_store().limits(),
            )
            .unwrap(),
        );
        let store = operator.native_store.as_ref().unwrap();
        let state = operator
            .native_state_values(&mut operator.native_accumulators(None).unwrap())
            .unwrap();
        for number in 0..9 {
            let key = format!("k{number}");
            let mut scope = store.begin().await.unwrap();
            let group = EncodedGroup {
                calendar_references: CalendarReferences::default(),
                last_update_nanos: 0,
                generation: 0,
                next_ordinal: 0,
                live_rows: 0,
                accumulator_state: state.clone(),
                last_emitted: None,
            };
            scope
                .put(
                    &native_group_key(b'G', key.as_bytes()).unwrap(),
                    &operator
                        .native_group_codec()
                        .unwrap()
                        .encode(&group, scope.limits().value_bytes)
                        .unwrap(),
                )
                .unwrap();
            scope
                .put(
                    &native_expiry_key(1_000_000_000, key.as_bytes()).unwrap(),
                    &[1],
                )
                .unwrap();
            scope.commit().await.unwrap();
        }
        let mut progress = store
            .try_expiry_progress(native_test_config().max_pending_output_bytes)
            .unwrap();
        assert!(operator.expire_native(Some(&mut progress)).await.unwrap());
        let committed = progress.after.clone();
        let snapshots = [
            backend.snapshot().await.unwrap(),
            backend.snapshot().await.unwrap(),
        ];
        let mut expiring = Box::pin(operator.expire_native(Some(&mut progress)));
        assert!(futures::poll!(&mut expiring).is_pending());
        drop(expiring);
        assert_eq!(progress.after, committed);
        drop(snapshots);
        // A write reservation failure must not publish attempted progress.
        let occupied = resources
            .try_queued_write(resources.config().queued_write_bytes)
            .unwrap();
        assert!(operator.expire_native(Some(&mut progress)).await.is_err());
        assert_eq!(progress.after, committed);
        drop(occupied);
        // Cancellation of the owning flush discards key and permit together.
        drop(progress);
        assert!(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .is_ok()
        );
        // Earlier insertion after that interrupted flush cannot be skipped.
        let mut scope = store.begin().await.unwrap();
        let group = EncodedGroup {
            calendar_references: CalendarReferences::default(),
            last_update_nanos: -1_000_000_000,
            generation: 0,
            next_ordinal: 0,
            live_rows: 0,
            accumulator_state: state,
            last_emitted: None,
        };
        scope
            .put(
                &native_group_key(b'G', b"earlier").unwrap(),
                &operator
                    .native_group_codec()
                    .unwrap()
                    .encode(&group, scope.limits().value_bytes)
                    .unwrap(),
            )
            .unwrap();
        scope
            .put(&native_expiry_key(0, b"earlier").unwrap(), &[1])
            .unwrap();
        scope.commit().await.unwrap();
        let mut retry = store
            .try_expiry_progress(native_test_config().max_pending_output_bytes)
            .unwrap();
        while operator.expire_native(Some(&mut retry)).await.unwrap() {}
        drop(retry);
        let scope = store.begin().await.unwrap();
        for key in std::iter::once("earlier".to_string()).chain((0..9).map(|i| format!("k{i}"))) {
            let bytes = scope
                .get(&native_group_key(b'G', key.as_bytes()).unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                operator
                    .native_group_codec()
                    .unwrap()
                    .decode(&bytes, scope.limits().value_bytes)
                    .unwrap()
                    .generation,
                1
            );
        }
        assert!(scope.first(b"E").await.unwrap().is_none());
        drop(scope);
        // Refusal under live pressure is optional and retains no key/permit.
        let held = resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .unwrap();
        assert!(
            store
                .try_expiry_progress(native_test_config().max_pending_output_bytes)
                .is_none()
        );
        drop(held);
        assert!(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .is_ok()
        );
        // A formerly valid minimum budget still uses the original fresh seeks.
        assert!(
            store
                .try_expiry_progress(resources.config().decoded_value_bytes / 3)
                .is_none()
        );
        let mut scope = store.begin().await.unwrap();
        let bytes = scope
            .get(&native_group_key(b'G', b"earlier").unwrap())
            .await
            .unwrap()
            .unwrap();
        let mut group = operator
            .native_group_codec()
            .unwrap()
            .decode(&bytes, scope.limits().value_bytes)
            .unwrap();
        group.last_update_nanos = 0;
        scope
            .put(
                &native_group_key(b'G', b"fallback").unwrap(),
                &operator
                    .native_group_codec()
                    .unwrap()
                    .encode(&group, scope.limits().value_bytes)
                    .unwrap(),
            )
            .unwrap();
        scope
            .put(
                &native_expiry_key(1_000_000_000, b"fallback").unwrap(),
                &[1],
            )
            .unwrap();
        scope.commit().await.unwrap();
        assert!(operator.expire_native(None).await.unwrap());
        assert!(!operator.expire_native(None).await.unwrap());
        let scope = store.begin().await.unwrap();
        let bytes = scope
            .get(&native_group_key(b'G', b"fallback").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
                .unwrap()
                .generation,
            2
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
            let encoded = operator
                .native_group_codec()
                .unwrap()
                .encode(
                    &EncodedGroup {
                        calendar_references: CalendarReferences::default(),
                        last_update_nanos: 0,
                        generation: 0,
                        next_ordinal: 0,
                        live_rows: 0,
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
        assert!(operator.expire_native(None).await.unwrap());
        assert!(operator.expire_native(None).await.unwrap());
        assert!(!operator.expire_native(None).await.unwrap());
        let scope = store.begin().await.unwrap();
        for number in 0..5 {
            let group = format!("k{number}");
            let bytes = scope
                .get(&native_group_key(b'G', group.as_bytes()).unwrap())
                .await
                .unwrap()
                .unwrap();
            let restored = operator
                .native_group_codec()
                .unwrap()
                .decode(&bytes, scope.limits().value_bytes)
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

    fn checkpoint_rows(schema: &ArroyoSchema, columns: Vec<ArrayRef>) -> RecordBatch {
        let mut fields = schema.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("generation", DataType::UInt64, false)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
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
    /// Opt-in end-to-end fallback diagnostic; wrapper counts actual admitted
    /// backend pages, including entries the caller fetched but did not select.
    #[tokio::test]
    #[ignore = "opt-in STR-121 finite collection page utilisation diagnostic"]
    async fn native_collection_page_utilisation_pilot() {
        use arroyo_state::live::{
            self, LastEntryRequest, ReadOptions, ScanPage, ScanRequest, SnapshotReader, StateKey,
            StateSnapshot, WriteBatch, lifecycle::RocksStateConfig, rocks::RocksLiveState,
            write::AdmittedWriteBatch,
        };
        use std::sync::atomic::{AtomicU64, Ordering};
        #[derive(Default)]
        struct Counts {
            pages: AtomicU64,
            entries: AtomicU64,
            bytes: AtomicU64,
        }
        struct Reader {
            inner: StateSnapshot,
            counts: Arc<Counts>,
        }
        #[async_trait::async_trait]
        impl SnapshotReader for Reader {
            async fn get(&self, k: &StateKey, o: ReadOptions) -> live::Result<Option<Vec<u8>>> {
                self.inner.get(k, o).await
            }
            async fn try_get(&self, k: &StateKey, o: ReadOptions) -> live::Result<Option<Vec<u8>>> {
                self.inner.try_get(k, o).await
            }
            async fn multi_get(
                &self,
                k: &[StateKey],
                o: ReadOptions,
            ) -> live::Result<Vec<Option<Vec<u8>>>> {
                self.inner.multi_get(k, o).await
            }
            async fn scan(&self, r: ScanRequest) -> live::Result<ScanPage> {
                self.try_scan(r).await
            }
            async fn try_scan(&self, r: ScanRequest) -> live::Result<ScanPage> {
                let p = self.inner.try_scan(r).await?;
                self.counts.pages.fetch_add(1, Ordering::Relaxed);
                self.counts
                    .entries
                    .fetch_add(p.entries.len() as u64, Ordering::Relaxed);
                self.counts.bytes.fetch_add(
                    p.entries
                        .iter()
                        .map(|e| (e.key.key.len() + e.value.len()) as u64)
                        .sum::<u64>(),
                    Ordering::Relaxed,
                );
                Ok(p)
            }
            async fn last_in_range(&self, r: LastEntryRequest) -> live::Result<ScanPage> {
                self.inner.last_in_range(r).await
            }
        }
        struct Backend {
            inner: Arc<RocksLiveState>,
            counts: Arc<Counts>,
        }
        #[async_trait::async_trait]
        impl LiveStateBackend for Backend {
            async fn get(&self, k: &StateKey, o: ReadOptions) -> live::Result<Option<Vec<u8>>> {
                self.inner.get(k, o).await
            }
            async fn try_get(&self, k: &StateKey, o: ReadOptions) -> live::Result<Option<Vec<u8>>> {
                self.inner.try_get(k, o).await
            }
            async fn multi_get(
                &self,
                k: &[StateKey],
                o: ReadOptions,
            ) -> live::Result<Vec<Option<Vec<u8>>>> {
                self.inner.multi_get(k, o).await
            }
            async fn write_batch(&self, b: WriteBatch) -> live::Result<()> {
                self.inner.write_batch(b).await
            }
            async fn snapshot(&self) -> live::Result<StateSnapshot> {
                Ok(StateSnapshot(Arc::new(Reader {
                    inner: self.inner.snapshot().await?,
                    counts: self.counts.clone(),
                })))
            }
            fn try_admit_write(&self, b: usize, n: usize) -> live::Result<AdmittedWriteBatch> {
                self.inner.try_admit_write(b, n)
            }
            async fn admit_write(&self, b: usize, n: usize) -> live::Result<AdmittedWriteBatch> {
                self.inner.admit_write(b, n).await
            }
            async fn write_admitted(&self, b: AdmittedWriteBatch) -> live::Result<()> {
                self.inner.write_admitted(b).await
            }
        }
        let repeats = std::env::var("STR121_PILOT_REPEAT")
            .map(|s| vec![s.parse::<usize>().unwrap()])
            .unwrap_or_else(|_| (0..3).collect());
        for repeat in repeats {
            // DISTINCT gives the supported unordered ARRAY_AGG path. Ordinary
            // ordered output additionally exercises duplicate multiplicity.
            for (scenario, distinct, value_len, page_entries, limit) in [
                ("ordered", false, 24, 16, None),
                ("unordered_distinct", true, 24, 16, None),
                ("bounded", false, 24, 16, Some(7)),
                ("zero", false, 24, 16, Some(0)),
                ("byte_limited", false, 384, 128, Some(1)),
            ] {
                let root = std::env::temp_dir().join(format!("str121-{}", uuid::Uuid::new_v4()));
                let template = native_test_store();
                let resources = template.resources().clone();
                let mut limits = template.limits();
                limits.page_entries = page_entries;
                if scenario == "byte_limited" {
                    // Existing documented aggregate key/value/output settings;
                    // tighter page bytes exercise byte rather than entry limits.
                    limits.key_bytes = 512;
                    limits.value_bytes = 32768;
                    limits.page_bytes = 35 * 1024;
                }
                drop(template);
                let rocks = Arc::new(
                    RocksLiveState::open(
                        RocksStateConfig {
                            root: root.clone(),
                            job_id: "str121".into(),
                            operator_id: "collection".into(),
                            subtask: 0,
                            generation: 0,
                            attempt: 0,
                        },
                        resources.clone(),
                    )
                    .await
                    .unwrap(),
                );
                let counts = Arc::new(Counts::default());
                let backend: Arc<dyn LiveStateBackend> = Arc::new(Backend {
                    inner: rocks.clone(),
                    counts: counts.clone(),
                });
                let mut manager = LiveTableManager::new(
                    backend.clone(),
                    Ownership::PartitionLocal {
                        subtask: 0,
                        parallelism: 1,
                    },
                )
                .unwrap();
                let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
                let namespace = table.namespace().clone();
                let store = AggregateStore::new(backend.clone(), table, resources, limits).unwrap();
                let mut config = native_test_config();
                if scenario == "byte_limited" {
                    config.key_bytes = limits.key_bytes;
                    config.value_bytes = limits.value_bytes;
                    config.max_pending_output_bytes = 524288;
                }
                let mut operator =
                    native_array_operator_with_limits(distinct, false, false, limit, config)
                        .unwrap();
                operator.native_store = Some(store);
                let store = operator.native_store.as_ref().unwrap();
                eprintln!(
                    "STR121_SETTINGS scenario={scenario} key_bytes={} value_bytes={} output_bytes={} member_bound={} limit={limit:?}",
                    limits.key_bytes,
                    limits.value_bytes,
                    config.max_pending_output_bytes,
                    operator.aggregates[0].collection_member_bound
                );
                let mut expected = Vec::<(i64, Option<String>)>::new();
                // Full collections retain 8/16/24 rows under unchanged fixture
                // output admission. Bounded longer-value state retains 48/96/144.
                // Every phase retracts/reinserts a duplicate-containing prefix.
                let growth = if scenario == "byte_limited" { 48 } else { 8 };
                for third in 0..3 {
                    let values = (0..growth)
                        .map(|i| {
                            if i % 11 == 0 {
                                None
                            } else {
                                Some(format!(
                                    "{:03}-{}",
                                    third * growth + i / 2,
                                    "x".repeat(value_len)
                                ))
                            }
                        })
                        .collect::<Vec<_>>();
                    let refs = values.iter().map(|v| v.as_deref()).collect::<Vec<_>>();
                    let orders = (0..growth)
                        .map(|i| (third * growth + i) as i64)
                        .collect::<Vec<_>>();
                    let input = batch(&refs, &orders, &vec![Some(true); growth]);
                    let inputs = operator.compute_inputs(&input).unwrap();
                    for row in 0..growth {
                        let mut scope = store.begin().await.unwrap();
                        operator
                            .native_process_event(
                                &mut scope,
                                &GLOBAL_KEY,
                                &inputs,
                                row,
                                false,
                                Some(&test_row_id(&input, row)),
                            )
                            .await
                            .unwrap();
                        scope.commit().await.unwrap();
                        expected.push((orders[row], values[row].clone()));
                    }
                    for retract in [true, false] {
                        for row in 0..8 {
                            let mut scope = store.begin().await.unwrap();
                            operator
                                .native_process_event(
                                    &mut scope,
                                    &GLOBAL_KEY,
                                    &inputs,
                                    row,
                                    retract,
                                    Some(&test_row_id(&input, row)),
                                )
                                .await
                                .unwrap();
                            scope.commit().await.unwrap();
                        }
                    }
                    let mut wanted = expected.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>();
                    if distinct {
                        wanted.sort();
                        wanted.dedup();
                    }
                    if let Some(limit) = limit {
                        wanted.truncate(limit as usize);
                    }
                    if scenario == "byte_limited" && third > 0 {
                        let snapshot = backend.snapshot().await.unwrap();
                        let page = snapshot
                            .try_scan(ScanRequest {
                                range: live::ScanRange {
                                    namespace: namespace.clone(),
                                    prefix: Some(native_member_prefix(&GLOBAL_KEY, 0, 0).unwrap()),
                                    start: None,
                                    end: None,
                                },
                                max_entries: limits.page_entries,
                                max_bytes: limits.page_bytes,
                                cursor: None,
                            })
                            .await
                            .unwrap();
                        assert!(page.entries.len() < limits.page_entries);
                        assert!(
                            page.next_cursor.is_some(),
                            "byte cutoff must leave retained members"
                        );
                        eprintln!(
                            "STR121_BYTE_PAGE repeat={repeat} third={third} entries={} configured_entries={}",
                            page.entries.len(),
                            limits.page_entries
                        );
                    }
                    counts.pages.store(0, Ordering::Relaxed);
                    counts.entries.store(0, Ordering::Relaxed);
                    counts.bytes.store(0, Ordering::Relaxed);
                    eprintln!(
                        "STR121_PHASE repeat={repeat} scenario={scenario} third={third} start"
                    );
                    for file in ["stat", "io"] {
                        let contents =
                            std::fs::read_to_string(format!("/proc/self/{file}")).unwrap();
                        eprintln!(
                            "STR121_PROCESS initial_{file}={}",
                            contents.replace('\n', ";")
                        );
                    }
                    let start = std::time::Instant::now();
                    let actual = array_members(store, &operator).await;
                    let elapsed = start.elapsed();
                    let mut actual = actual
                        .into_iter()
                        .map(|v| match v {
                            ScalarValue::Utf8(v) => v,
                            other => panic!("unexpected {other:?}"),
                        })
                        .collect::<Vec<_>>();
                    if distinct {
                        actual.sort();
                    }
                    assert_eq!(actual, wanted);
                    eprintln!(
                        "STR121 repeat={repeat} scenario={scenario} third={third} retained={} selected={} completed_outputs=1 elapsed_ns={} fetched_pages={} fetched_entries={} fetched_payload_bytes={} page_entries={} page_bytes={}",
                        expected.len(),
                        actual.len(),
                        elapsed.as_nanos(),
                        counts.pages.load(Ordering::Relaxed),
                        counts.entries.load(Ordering::Relaxed),
                        counts.bytes.load(Ordering::Relaxed),
                        limits.page_entries,
                        limits.page_bytes
                    );
                    for file in ["status", "stat", "io"] {
                        let contents =
                            std::fs::read_to_string(format!("/proc/self/{file}")).unwrap();
                        eprintln!("STR121_PROCESS {file}={}", contents.replace('\n', ";"));
                    }
                    eprintln!("STR121_PHASE repeat={repeat} scenario={scenario} third={third} end");
                }
                drop(operator);
                drop(manager);
                drop(backend);
                Arc::try_unwrap(rocks)
                    .unwrap_or_else(|_| panic!("retained collection backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
    #[tokio::test]
    async fn native_unordered_array_preserves_multiplicity_nulls_and_retraction() {
        for page_entries in [1, 3] {
            let mut operator =
                native_array_operator_with_order(false, false, false, None, false).unwrap();
            assert!(operator.aggregates[0].func.order_bys().is_none());
            let mut limits = native_test_store().limits();
            limits.page_entries = page_entries;
            operator.native_store = Some(native_test_store_with_limits(limits));
            let store = operator.native_store.as_ref().unwrap();
            let input = batch(
                &[Some("b"), Some("a"), Some("b"), None],
                &[1, 2, 3, 4],
                &[Some(true); 4],
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
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
            }
            let sorted = |values: Vec<ScalarValue>| {
                let mut values = values
                    .into_iter()
                    .map(|v| match v {
                        ScalarValue::Utf8(value) => value,
                        other => panic!("unexpected unordered ARRAY_AGG member {other:?}"),
                    })
                    .collect::<Vec<_>>();
                values.sort();
                values
            };
            assert_eq!(
                sorted(array_members(store, &operator).await),
                vec![None, Some("a".into()), Some("b".into()), Some("b".into())]
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
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                sorted(array_members(store, &operator).await),
                vec![None, Some("a".into()), Some("b".into())]
            );
            let mut scope = store.begin().await.unwrap();
            operator
                .native_process_event(
                    &mut scope,
                    &GLOBAL_KEY,
                    &inputs,
                    3,
                    true,
                    Some(&test_row_id(&input, 3)),
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                sorted(array_members(store, &operator).await),
                vec![Some("a".into()), Some("b".into())]
            );
        }
    }
    #[tokio::test]
    async fn native_collection_pages_decode_selected_wide_values_on_both_backends() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for rocks in [false, true] {
            let root =
                std::env::temp_dir().join(format!("str121-selected-wide-{}", uuid::Uuid::new_v4()));
            let resources = native_test_store().resources().clone();
            let backend: Arc<dyn LiveStateBackend> = if rocks {
                Arc::new(
                    RocksLiveState::open(
                        RocksStateConfig {
                            root: root.clone(),
                            job_id: "wide-array".into(),
                            operator_id: "aggregate".into(),
                            subtask: 0,
                            generation: 0,
                            attempt: 0,
                        },
                        resources.clone(),
                    )
                    .await
                    .unwrap(),
                )
            } else {
                Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap())
            };
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let limits = AggregateStoreLimits {
                key_bytes: 512,
                page_entries: 1,
                ..native_test_store().limits()
            };
            let mut operator = native_array_operator_with_limits(
                false,
                false,
                false,
                None,
                AggregateStateConfig {
                    key_bytes: 512,
                    ..native_test_config()
                },
            )
            .unwrap();
            operator.native_store =
                Some(AggregateStore::new(backend.clone(), table, resources, limits).unwrap());
            let store = operator.native_store.as_ref().unwrap();
            let wide = "x".repeat(384);
            let input = batch(
                &[Some(wide.as_str()), Some("next")],
                &[1, 2],
                &[Some(true); 2],
            );
            let inputs = operator.compute_inputs(&input).unwrap();
            for row in 0..2 {
                let mut scope = store.begin_point().await.unwrap();
                operator
                    .native_process_event(
                        &mut scope,
                        &GLOBAL_KEY,
                        &inputs,
                        row,
                        false,
                        Some(&test_row_id(&input, row)),
                    )
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
            }
            assert_eq!(
                array_members(store, &operator).await,
                vec![
                    ScalarValue::Utf8(Some(wide.clone())),
                    ScalarValue::Utf8(Some("next".into()))
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
                )
                .await
                .unwrap();
            scope.commit().await.unwrap();
            assert_eq!(
                array_members(store, &operator).await,
                vec![ScalarValue::Utf8(Some("next".into()))]
            );
            drop(operator);
            drop(manager);
            if rocks {
                backend.close().await.unwrap();
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
    mod calendar_merge_tests {
        include!("calendar_merge_tests.rs");
    }
}
