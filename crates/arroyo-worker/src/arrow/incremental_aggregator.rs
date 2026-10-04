use crate::arrow::aggregate_codec::{EncodedGroup, decode_group, encode_group};
use crate::arrow::aggregate_store::{AggregateScope, AggregateStore, AggregateStoreLimits};
use crate::arrow::decode_aggregate;
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
    Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StructArray, UInt32Array, UInt64Array,
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
use arroyo_types::{CheckpointBarrier, SignalMessage, to_nanos};
use datafusion::common::{Result as DFResult, ScalarValue};
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

/// One indexed aggregate member change within an admitted state scope.
struct NativeMemberChange<'a> {
    group: &'a [u8],
    generation: u64,
    aggregate_index: usize,
    values: &'a [ArrayRef],
    retract: bool,
    ordinal: u64,
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
    Batch,
}

impl AccumulatorType {
    fn state_fields(&self, agg: &AggregateFunctionExpr) -> DFResult<Vec<FieldRef>> {
        Ok(match self {
            AccumulatorType::Sliding => agg.sliding_state_fields()?,
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
    native_max_input_batch_bytes: Option<usize>,
    native_store: Option<AggregateStore>,
    retain_indefinitely: bool,
    native_schema_identity: Vec<u8>,
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

impl IncrementalAggregatingFunc {
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
        if change.values[0].is_null(0)
            && (matches!(name.as_str(), "min" | "max") || aggregate.func.ignore_nulls())
        {
            return Ok(());
        }
        let (primary, secondary, args) = self.native_member_keys(
            change.group,
            change.generation,
            change.aggregate_index,
            change.values,
            change.ordinal,
        )?;
        if change.retract {
            let prefix = &secondary[..secondary.len() - 8];
            let Some((secondary_key, primary_key)) = scope.first(prefix).await? else {
                // An unmatched retract preserves the old aggregate behavior.
                return Ok(());
            };
            ensure!(
                scope.get(&primary_key).await?.as_deref() == Some(args.as_slice()),
                "aggregate member indexes disagree"
            );
            scope.delete(&secondary_key)?;
            scope.delete(&primary_key)?;
        } else {
            scope.put(&primary, &args)?;
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
        if let Some((_, args)) = scope.first(&prefix).await? {
            let parser = aggregate.row_converter.parser();
            let columns = aggregate
                .row_converter
                .convert_rows(std::iter::once(parser.parse(&args)))?;
            accumulator.update_batch(&columns)?;
        }
        Ok(accumulator.evaluate_mut()?)
    }

    async fn native_process_event(
        &self,
        scope: &mut AggregateScope<'_>,
        group_key: &[u8],
        inputs: &[AggregateInput],
        row: usize,
        retract: bool,
    ) -> Result<()> {
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
        let now = to_nanos(SystemTime::now()) as i64;
        let ttl_nanos = i64::try_from(self.ttl.as_nanos())?;
        let expired = !self.retain_indefinitely
            && previous
                .as_ref()
                .is_some_and(|group| now.saturating_sub(group.last_update_nanos) >= ttl_nanos);
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
        if let Some(group) = &previous {
            if !self.retain_indefinitely {
                let old_deadline = group.last_update_nanos.saturating_add(ttl_nanos);
                scope.delete(&native_expiry_key(old_deadline, group_key)?)?;
            }
            if expired {
                scope.put(&native_cleanup_key(group_key, group.generation)?, b"M")?;
            }
        }
        for (index, (input, state)) in inputs.iter().zip(accumulators.iter_mut()).enumerate() {
            let Some(values) = input.selected_values(Some(row))? else {
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
                        },
                    )
                    .await?;
                }
            }
        }
        let next = EncodedGroup {
            last_update_nanos: now,
            generation,
            next_ordinal,
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
        let max_input_batch_bytes = self.native_max_input_batch_bytes.ok_or_else(|| {
            anyhow!("native aggregate requires worker.execution-resources.max-batch-bytes")
        })?;
        ensure!(
            batch.get_array_memory_size() <= max_input_batch_bytes,
            "native aggregate input exceeds configured max-batch-bytes"
        );
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
        let input_working_bytes = input_working_bytes
            .and_then(|total| {
                keys.iter()
                    .try_fold(total, |total, key| total.checked_add(key.len()))
            })
            .ok_or_else(|| anyhow!("native aggregate input working-set size overflow"))?;
        ensure!(
            input_working_bytes <= max_input_batch_bytes,
            "native aggregate expressions exceed configured max-batch-bytes"
        );
        let retracts = Self::get_retracts(batch);
        let limits = store.limits();
        let fallback = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.accumulator_type == AccumulatorType::Batch)
            .count();
        let worst_operations = 2usize
            .checked_add(fallback.saturating_mul(2))
            .and_then(|value| value.checked_add(3 * usize::from(!self.retain_indefinitely)))
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
            let mut scope = store.begin().await?;
            for (row, key) in keys.iter().enumerate().take(end).skip(start) {
                let retract = retracts.is_some_and(|flags| flags.value(row));
                self.native_process_event(&mut scope, key, &inputs, row, retract)
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
        let rows_per_chunk = (store.limits().write_operations / 4)
            .min(store.limits().write_bytes / (4 * store.max_encoded_entry_bytes()))
            .min(
                store.limits().overlay_bytes
                    / (4 * (store.limits().key_bytes + store.limits().value_bytes)),
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
                    group.accumulator_state =
                        self.native_state_values(&mut self.native_accumulators(None)?)?;
                    group.last_update_nanos = now;
                    scope.put(
                        &storage_key,
                        &encode_group(&group, scope.limits().value_bytes)?,
                    )?;
                    scope.put(&native_group_key(b'D', group_key)?, &[1])?;
                    scope.put(&native_cleanup_key(group_key, old_generation)?, b"M")?;
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

    async fn cleanup_native(&self) -> Result<()> {
        let store = self
            .native_store
            .as_ref()
            .ok_or_else(|| anyhow!("native aggregate store missing"))?;
        let mut scope = store.begin().await?;
        let Some((cleanup_key, phase)) = scope.first(b"C").await? else {
            return Ok(());
        };
        ensure!(cleanup_key.len() >= 13, "invalid aggregate cleanup key");
        let group_end = cleanup_key.len() - 8;
        let group = &cleanup_key[5..group_end];
        let generation = u64::from_be_bytes(cleanup_key[group_end..].try_into()?);
        ensure!(
            phase == b"M" || phase == b"R",
            "invalid aggregate cleanup phase"
        );
        let prefix = native_generation_prefix(phase[0], group, generation)?;
        let mut after = None;
        let mut exhausted = false;
        let max_entries = store
            .limits()
            .page_entries
            .min(store.limits().write_operations.saturating_sub(1))
            .min((store.limits().write_bytes / store.max_encoded_entry_bytes()).saturating_sub(1))
            .min(
                (store.limits().overlay_bytes
                    / (store.limits().key_bytes + store.limits().value_bytes))
                    .saturating_sub(1),
            );
        ensure!(
            max_entries > 0,
            "native aggregate cleanup requires two write operations"
        );
        for _ in 0..max_entries {
            let Some((key, _)) = scope.first_from(&prefix, after.as_deref()).await? else {
                exhausted = true;
                break;
            };
            scope.delete(&key)?;
            after = Some(key);
        }
        if exhausted {
            if phase == b"M" {
                scope.put(&cleanup_key, b"R")?;
            } else {
                scope.delete(&cleanup_key)?;
            }
        }
        scope.commit().await?;
        Ok(())
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
            self.cleanup_native().await?;
            tokio::task::yield_now().await;
        }
        self.cleanup_native().await?;
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
        let max_dirty_groups = (configured.max_pending_output_rows / 2)
            .min(store_limits.write_operations / 2)
            .min(store_limits.write_bytes / store.max_encoded_entry_bytes().saturating_mul(2))
            .min(
                store_limits.overlay_bytes
                    / store_limits
                        .key_bytes
                        .saturating_add(store_limits.value_bytes)
                        .saturating_mul(2),
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
                    let unchanged = group.last_emitted.as_ref().is_some_and(|old| {
                        old.iter()
                            .zip(next.iter())
                            .take(old.len().saturating_sub(1))
                            .all(|(old, new)| old == new)
                    });
                    if !unchanged {
                        let append = !next.last().is_some_and(ScalarValue::is_null);
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
        let required_operations = 2usize
            .checked_add(
                indexed
                    .checked_mul(2)
                    .context("aggregate index count overflow")?,
            )
            .and_then(|count| count.checked_add(3 * usize::from(!self.retain_indefinitely)))
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
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-updating-aggregate.v1");
        identity.update(&config.aggregate_exec);
        identity.update(&config.metadata_expr);
        identity.update(config.ttl_micros.to_be_bytes());
        identity.update([u8::from(config.retain_indefinitely == Some(true))]);
        if let Some(schema) = &config.input_schema {
            identity.update(schema.encode_to_vec());
        }
        if let Some(schema) = &config.final_schema {
            identity.update(schema.encode_to_vec());
        }
        let native_schema_identity = identity.finalize().to_vec();
        let ttl = Duration::from_micros(if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        });

        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let final_schema: ArroyoSchema = config.final_schema.unwrap().try_into()?;
        let mut schema_without_metadata = SchemaBuilder::from((*final_schema.schema).clone());
        schema_without_metadata.remove(final_schema.schema.index_of(UPDATING_META_FIELD).unwrap());

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
            .map(|(index, (expr, name))| {
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

                // Only these sliding accumulators have fixed-size,
                // reconstructible state on the native path. DataFusion's
                // moving MIN/MAX retains FIFO history that its state() omits;
                // FIRST/LAST likewise need the persisted member index. Any
                // other native aggregate must have an explicit index codec or
                // fail at construction rather than retain hidden history.
                let native_sliding = matches!(function.as_str(), "count" | "sum" | "avg");

                (
                    agg,
                    if retract && (!native_state || native_sliding) {
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
                let row_converter = Arc::new(RowConverter::new(
                    input_exprs
                        .iter()
                        .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                        .collect::<DFResult<_>>()?,
                )?);

                let fields = t.state_fields(&agg)?;

                let field_names = fields.iter().map(|f| f.name().to_string()).collect_vec();
                sliding_state_fields.extend(fields.into_iter().map(|f| (*f).clone()));

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
                    field_names,
                    input_exprs,
                    filter,
                    index_converter,
                    index_columns,
                ))
            })
            .flatten_ok()
            .collect::<Result<_>>()?;

        let state_schema = Schema::new(sliding_state_fields);

        let versioned_inputs = aggregates
            .iter()
            .any(|(agg, _, _, _, _, filter, _, _)| filter.is_some() || agg.order_bys().is_some());
        let aggregates = aggregates
            .into_iter()
            .map(
                |(
                    agg,
                    t,
                    row_converter,
                    field_names,
                    input_exprs,
                    filter,
                    index_converter,
                    index_columns,
                )| Aggregator {
                    func: agg,
                    input_exprs,
                    filter,
                    accumulator_type: t,
                    row_converter,
                    state_cols: field_names
                        .iter()
                        .map(|f| state_schema.index_of(f).unwrap())
                        .collect(),
                    index_converter,
                    index_columns,
                },
            )
            .collect();

        // ensure the last field (timestamp) has the expected name before creating the arroyo schema
        let mut state_fields = state_schema.fields().to_vec();
        let timestamp_field = state_fields.pop().unwrap();
        state_fields.push(Arc::new(versioned_state_timestamp(
            (*timestamp_field).clone().with_name(TIMESTAMP_FIELD),
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
            native_max_input_batch_bytes: arroyo_rpc::config::config()
                .worker
                .execution_resources
                .as_ref()
                .map(|resources| resources.max_batch_bytes),
            native_store: None,
            retain_indefinitely: config.retain_indefinitely == Some(true),
            native_schema_identity,
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
    };
    use datafusion::execution::FunctionRegistry;
    use datafusion::functions_aggregate::{
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
            },
            Arc::new(registry),
        )
    }

    fn operator() -> IncrementalAggregatingFunc {
        let (config, registry) = native_config();
        IncrementalAggregatingConstructor::build(config, registry).unwrap()
    }

    fn native_operator() -> IncrementalAggregatingFunc {
        let (config, registry) = native_config();
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native_test_config()),
        )
        .unwrap();
        operator.retain_indefinitely = true;
        operator
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
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 4 * 1024 * 1024,
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false)
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
            .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, 0, true)
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false)
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
                .native_process_event(&mut scope, &GLOBAL_KEY, &inputs, row, false)
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
