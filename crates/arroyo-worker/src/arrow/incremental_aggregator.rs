use crate::arrow::decode_aggregate;
use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{Result, anyhow, bail};
use arrow::compute::{filter, max_array};
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
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_fields};
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
}

const GLOBAL_KEY: Vec<u8> = vec![];

impl IncrementalAggregatingFunc {
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
        if let Some(batch) = self.flush(ctx).await? {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
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
        if let Some(SignalMessage::EndOfData) = final_message
            && let Some(batch) = self.flush(ctx).await?
        {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.initialize(ctx).await?;
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
                let retract = match agg.create_sliding_accumulator() {
                    Ok(s) => s.supports_retract_batch(),
                    _ => false,
                };

                (
                    agg,
                    if retract {
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

                Ok::<_, anyhow::Error>((agg, t, row_converter, field_names, input_exprs, filter))
            })
            .flatten_ok()
            .collect::<Result<_>>()?;

        let state_schema = Schema::new(sliding_state_fields);

        let versioned_inputs = aggregates
            .iter()
            .any(|(agg, _, _, _, _, filter)| filter.is_some() || agg.order_bys().is_some());
        let aggregates = aggregates
            .into_iter()
            .map(
                |(agg, t, row_converter, field_names, input_exprs, filter)| Aggregator {
                    func: agg,
                    input_exprs,
                    filter,
                    accumulator_type: t,
                    row_converter,
                    state_cols: field_names
                        .iter()
                        .map(|f| state_schema.index_of(f).unwrap())
                        .collect(),
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::SortOptions;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
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
            expressions.push(Arc::new(
                AggregateExprBuilder::new(function, vec![value.clone()])
                    .schema(schema.clone())
                    .alias(name)
                    .order_by(LexOrdering::new(vec![PhysicalSortExpr::new(
                        sequence.clone(),
                        SortOptions {
                            descending,
                            nulls_first: false,
                        },
                    )]))
                    .build()
                    .unwrap(),
            ));
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
            },
            Arc::new(registry),
        )
    }

    fn operator() -> IncrementalAggregatingFunc {
        let (config, registry) = native_config();
        IncrementalAggregatingConstructor::build(config, registry).unwrap()
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
        aggregate.aggr_expr.drain(..5);
        aggregate.aggr_expr_name.drain(..5);
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
