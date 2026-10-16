//! Fixed-state SESSION execution using the original DataFusion aggregate expressions.
use anyhow::{Context, Result, ensure};
use arrow::ipc::writer::StreamWriter;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use arroyo_rpc::config::WindowStateConfig;
use datafusion::{
    common::{ScalarValue, utils::expr::COUNT_STAR_EXPANSION},
    logical_expr::Accumulator,
    physical_expr::{
        aggregate::AggregateFunctionExpr,
        expressions::{Column, Literal},
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateMode},
    },
};
use sha2::{Digest, Sha256};
use std::{io::Write, sync::Arc};

#[derive(Clone)]
pub(super) struct SessionStateLayout {
    pub schema: SchemaRef,
    /// Maximum state IPC stream, excluding the store's S metadata header.
    pub max_encoded_bytes: usize,
    /// Target accumulators, source arrays, scalar vectors and state construction.
    pub decoded_working_bytes: usize,
    pub identity: Vec<u8>,
}

pub(super) trait SessionStateUpdate: Send {
    fn merge(&mut self, source: &RecordBatch) -> Result<()>;
    fn state(&mut self) -> Result<RecordBatch>;
}

struct CompiledAggregate {
    expression: Arc<AggregateFunctionExpr>,
    argument: Argument,
    filter: Option<usize>,
    state_index: usize,
    count: bool,
}
#[derive(Clone, Copy)]
enum Argument {
    Column(usize),
    CountStar,
}

pub(super) struct SessionAccumulatorPlan {
    aggregates: Vec<CompiledAggregate>,
    pub(super) layout: SessionStateLayout,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
}

struct CappedBytes {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for CappedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max)
        {
            return Err(std::io::Error::other(
                "SESSION aggregate IPC exceeds configured bound",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SessionAccumulatorPlan {
    pub(super) fn try_new(
        finish: &Arc<dyn ExecutionPlan>,
        input: &SchemaRef,
        limits: WindowStateConfig,
    ) -> Result<Option<Self>> {
        let Some(final_aggregate) = finish.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };
        if !final_aggregate.group_expr().expr().is_empty() {
            return Ok(None);
        }
        let aggregate = match final_aggregate.mode() {
            AggregateMode::Single => final_aggregate,
            AggregateMode::Final => {
                let Some(partial) = final_aggregate
                    .input()
                    .as_any()
                    .downcast_ref::<AggregateExec>()
                else {
                    return Ok(None);
                };
                if *partial.mode() != AggregateMode::Partial
                    || !partial.group_expr().expr().is_empty()
                    || partial.aggr_expr().len() != final_aggregate.aggr_expr().len()
                {
                    return Ok(None);
                }
                // The planner's actual final expressions must pair with these partials.
                for (p, f) in partial.aggr_expr().iter().zip(final_aggregate.aggr_expr()) {
                    if p.fun() != f.fun()
                        || p.is_distinct() != f.is_distinct()
                        || p.field() != f.field()
                    {
                        return Ok(None);
                    }
                }
                partial
            }
            _ => return Ok(None),
        };
        if aggregate.input().name() != "bounded_reader"
            || !aggregate.input().children().is_empty()
            || aggregate.input().schema() != *input
        {
            return Ok(None);
        }
        let count = aggregate.aggr_expr().len();
        // Bound compilation/container work before constructing per-expression state.
        if count == 0
            || count
                .checked_mul(4096)
                .is_none_or(|n| n > limits.max_resident_bytes || n > limits.partial_bytes)
        {
            return Ok(None);
        }
        let mut compiled = Vec::with_capacity(count);
        let mut fields = Vec::with_capacity(count);
        for (index, expression) in aggregate.aggr_expr().iter().enumerate() {
            if expression.is_distinct() || expression.order_bys().is_some_and(|x| !x.is_empty()) {
                return Ok(None);
            }
            let fun = expression.fun().inner().as_any();
            let is_count = fun.is::<datafusion::functions_aggregate::count::Count>();
            let eligible_integer = fun.is::<datafusion::functions_aggregate::sum::Sum>()
                || fun.is::<datafusion::functions_aggregate::min_max::Min>()
                || fun.is::<datafusion::functions_aggregate::min_max::Max>();
            if !is_count && !eligible_integer {
                return Ok(None);
            }
            let arguments = expression.expressions();
            if arguments.len() != 1 {
                return Ok(None);
            }
            let argument = if let Some(column) = arguments[0].as_any().downcast_ref::<Column>() {
                let Some(field) = input.fields().get(column.index()) else {
                    return Ok(None);
                };
                if eligible_integer
                    && !matches!(field.data_type(), DataType::Int64 | DataType::UInt64)
                {
                    return Ok(None);
                }
                if eligible_integer && expression.field().data_type() != field.data_type() {
                    return Ok(None);
                }
                Argument::Column(column.index())
            } else if is_count
                && arguments[0]
                    .as_any()
                    .downcast_ref::<Literal>()
                    .is_some_and(|literal| literal.value() == &COUNT_STAR_EXPANSION)
            {
                Argument::CountStar
            } else {
                return Ok(None);
            };
            let filter = match &aggregate.filter_expr()[index] {
                None => None,
                Some(filter) => {
                    let Some(column) = filter.as_any().downcast_ref::<Column>() else {
                        return Ok(None);
                    };
                    if input
                        .fields()
                        .get(column.index())
                        .is_none_or(|f| f.data_type() != &DataType::Boolean)
                    {
                        return Ok(None);
                    }
                    Some(column.index())
                }
            };
            let state_fields = expression.state_fields()?;
            if state_fields.len() != 1
                || !matches!(
                    state_fields[0].data_type(),
                    DataType::Int64 | DataType::UInt64
                )
                || state_fields[0].data_type() != expression.field().data_type()
            {
                return Ok(None);
            }
            fields.extend(state_fields);
            compiled.push(CompiledAggregate {
                expression: expression.clone(),
                argument,
                filter,
                state_index: index,
                count: is_count,
            });
        }
        let schema = Arc::new(Schema::new(fields));
        let columns = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int64 => ScalarValue::Int64(Some(0)).to_array_of_size(1),
                DataType::UInt64 => ScalarValue::UInt64(Some(0)).to_array_of_size(1),
                _ => unreachable!("classified fixed state"),
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let template = RecordBatch::try_new(schema.clone(), columns)?;
        let mut bytes = CappedBytes {
            bytes: Vec::new(),
            max: limits.partial_bytes,
        };
        let encoded = (|| -> Result<()> {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
            writer.write(&template)?;
            writer.finish()?;
            Ok(())
        })();
        if encoded.is_err() {
            return Ok(None);
        }
        // IPC is fixed-layout for one integer cell. A nullable field can add one
        // alignment-sized validity buffer absent from the nonnull template.
        let Some(max_encoded_bytes) = count
            .checked_mul(64)
            .and_then(|n| n.checked_add(bytes.bytes.len()))
        else {
            return Ok(None);
        };
        if max_encoded_bytes
            .checked_add(10)
            .is_none_or(|n| n > limits.partial_bytes)
        {
            return Ok(None);
        }
        let Some(decoded_working_bytes) = count.checked_mul(4096).and_then(|n| {
            max_encoded_bytes
                .checked_mul(3)
                .and_then(|b| n.checked_add(b))
        }) else {
            return Ok(None);
        };
        if decoded_working_bytes > limits.max_resident_bytes {
            return Ok(None);
        }
        let mut identity = Sha256::new();
        identity.update(b"streamr.session-accumulator.v1");
        // Reuse the already bounded schema/state template; avoid an additional
        // escaped-JSON allocation for caller-defined field names.
        identity.update(&bytes.bytes);
        Ok(Some(Self {
            aggregates: compiled,
            layout: SessionStateLayout {
                schema,
                max_encoded_bytes,
                decoded_working_bytes,
                identity: identity.finalize().to_vec(),
            },
            output_schema: finish.schema(),
            input_schema: input.clone(),
        }))
    }

    pub(super) fn updater(&self, row: &RecordBatch) -> Result<SessionAccumulatorUpdate<'_>> {
        ensure!(
            row.schema() == self.input_schema && row.num_rows() == 1,
            "SESSION aggregate input schema/cardinality changed"
        );
        let mut update = self.empty_update()?;
        for (compiled, accumulator) in self.aggregates.iter().zip(&mut update.accumulators) {
            if let Some(index) = compiled.filter {
                let filter = row
                    .column(index)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("SESSION FILTER type changed")?;
                if filter.is_null(0) || !filter.value(0) {
                    continue;
                }
            }
            let value: ArrayRef = match compiled.argument {
                Argument::Column(index) => row.column(index).clone(),
                Argument::CountStar => COUNT_STAR_EXPANSION.to_array_of_size(1)?,
            };
            accumulator.update_batch(&[value])?;
        }
        update.check_size()?;
        Ok(update)
    }

    fn empty_update(&self) -> Result<SessionAccumulatorUpdate<'_>> {
        let accumulators = self
            .aggregates
            .iter()
            .map(|a| a.expression.create_accumulator())
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let update = SessionAccumulatorUpdate {
            plan: self,
            accumulators,
        };
        update.check_size()?;
        Ok(update)
    }

    pub(super) fn evaluate(&self, state: &RecordBatch) -> Result<RecordBatch> {
        let mut update = self.empty_update()?;
        update.merge(state)?;
        let values = update
            .accumulators
            .iter_mut()
            .map(|a| a.evaluate_mut()?.to_array_of_size(1))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let result = RecordBatch::try_new(self.output_schema.clone(), values)?;
        ensure!(
            result.get_array_memory_size() <= self.layout.decoded_working_bytes,
            "SESSION aggregate output exceeds decoded bound"
        );
        Ok(result)
    }
}

pub(super) struct SessionAccumulatorUpdate<'a> {
    plan: &'a SessionAccumulatorPlan,
    accumulators: Vec<Box<dyn Accumulator>>,
}
impl SessionAccumulatorUpdate<'_> {
    fn check_size(&self) -> Result<()> {
        let bytes = self.accumulators.iter().try_fold(
            self.accumulators.capacity() * std::mem::size_of::<Box<dyn Accumulator>>(),
            |n, a| {
                n.checked_add(a.size())
                    .context("SESSION accumulator size overflow")
            },
        )?;
        ensure!(
            bytes <= self.plan.layout.decoded_working_bytes,
            "SESSION accumulator exceeds compiled decoded bound"
        );
        Ok(())
    }
}
impl SessionStateUpdate for SessionAccumulatorUpdate<'_> {
    fn merge(&mut self, source: &RecordBatch) -> Result<()> {
        ensure!(
            source.schema() == self.plan.layout.schema && source.num_rows() == 1,
            "SESSION aggregate state schema/cardinality changed"
        );
        for (compiled, accumulator) in self.plan.aggregates.iter().zip(&mut self.accumulators) {
            if compiled.count {
                let counts = source
                    .column(compiled.state_index)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .context("SESSION COUNT state type changed")?;
                ensure!(
                    !counts.is_null(0) && counts.value(0) >= 0,
                    "SESSION COUNT state is null or negative"
                );
            }
            accumulator.merge_batch(&[source.column(compiled.state_index).clone()])?;
        }
        self.check_size()
    }
    fn state(&mut self) -> Result<RecordBatch> {
        let columns = self
            .accumulators
            .iter_mut()
            .map(|a| {
                let values = a.state()?;
                ensure!(values.len() == 1, "SESSION aggregate state arity changed");
                Ok(values[0].to_array_of_size(1)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.plan.layout.schema.clone(), columns)?;
        ensure!(
            batch.get_array_memory_size() <= self.plan.layout.decoded_working_bytes,
            "SESSION state exceeds decoded bound"
        );
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext, new_registry};
    use arroyo_rpc::{df::ArroyoSchema, grpc::api};
    use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionContext};
    use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
    use futures::StreamExt;
    use prost::Message;
    use std::sync::RwLock;
    use tokio::sync::mpsc::{Receiver, channel};

    struct Fixture {
        finish: Arc<dyn ExecutionPlan>,
        input: SchemaRef,
        receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    }
    fn limits() -> WindowStateConfig {
        WindowStateConfig {
            key_bytes: 512,
            partial_bytes: 32768,
            page_bytes: 131072,
            page_entries: 64,
            write_bytes: 524288,
            write_operations: 64,
            max_resident_bytes: 8 * 1024 * 1024,
        }
    }
    async fn compile(expressions: &str) -> Fixture {
        compile_typed(expressions, "BIGINT").await
    }
    async fn compile_typed(expressions: &str, amount_type: &str) -> Fixture {
        let query = format!(
            "CREATE TABLE session_input (k TEXT, amount {amount_type}, selected BOOLEAN, timestamp TIMESTAMP NOT NULL, WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute') WITH (connector='single_file',path='/tmp/session-accumulator-input.jsonl',format='json',type='source'); SELECT k, SESSION(INTERVAL '10 seconds') AS window, {expressions} FROM session_input GROUP BY k, window"
        );
        let compiled = arroyo_planner::parse_and_get_program(
            &query,
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
            .flat_map(|n| n.operator_chain.iter())
            .map(|(o, _)| o)
            .find(|o| {
                o.operator_name == arroyo_datastream::logical::OperatorName::SessionWindowAggregate
            })
            .unwrap();
        let config =
            api::SessionWindowAggregateOperator::decode(operator.operator_config.as_slice())
                .unwrap();
        let input: ArroyoSchema = config.input_schema.unwrap().try_into().unwrap();
        let receiver = Arc::new(RwLock::new(None));
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::BoundedBatchStream(receiver.clone()),
        };
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let finish = PhysicalPlanNode::decode(config.final_aggregation_plan.as_slice())
            .unwrap()
            .try_into_physical_plan(&new_registry(), &runtime, &codec)
            .unwrap();
        Fixture {
            finish,
            input: input.schema,
            receiver,
        }
    }
    fn row(schema: &SchemaRef, amount: Option<i64>, selected: Option<bool>) -> RecordBatch {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let scalar = match field.data_type() {
                    DataType::Int64 => ScalarValue::Int64(amount),
                    DataType::UInt64 => ScalarValue::UInt64(amount.map(|x| x as u64)),
                    DataType::Boolean => ScalarValue::Boolean(selected),
                    DataType::Utf8 => ScalarValue::Utf8(Some("key".into())),
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, zone) => {
                        ScalarValue::TimestampNanosecond(Some(0), zone.clone())
                    }
                    other => panic!("unexpected planned test field: {other:?}"),
                };
                scalar.to_array_of_size(1).unwrap()
            })
            .collect();
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }
    fn values(batch: &RecordBatch) -> Vec<ScalarValue> {
        batch
            .columns()
            .iter()
            .map(|a| ScalarValue::try_from_array(a, 0).unwrap())
            .collect()
    }
    async fn raw(fixture: &Fixture, rows: Vec<RecordBatch>) -> RecordBatch {
        let (sender, receiver) = channel(1);
        *fixture.receiver.write().unwrap() = Some(receiver);
        fixture.finish.reset().unwrap();
        let mut stream = fixture
            .finish
            .execute(0, SessionContext::new().task_ctx())
            .unwrap();
        let producer = async move {
            for row in rows {
                sender.send(row).await.unwrap();
            }
        };
        let consumer = async {
            let mut output = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                if batch.num_rows() != 0 {
                    output.push(batch);
                }
            }
            assert_eq!(output.len(), 1);
            output.remove(0)
        };
        let (_, output) = futures::join!(producer, consumer);
        output
    }

    #[tokio::test]
    async fn session_accumulator_real_sql_partial_filters_nulls_and_integer_merges() {
        let fixture = compile("COUNT(*) AS n, COUNT(amount) FILTER (WHERE selected) AS chosen, SUM(amount) FILTER (WHERE selected) AS total, MIN(amount) AS lo, MAX(amount) AS hi").await;
        let plan = SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, limits())
            .unwrap()
            .expect("actual SQL must select fixed state");
        let rows = vec![
            row(&fixture.input, Some(i64::MAX), Some(true)),
            row(&fixture.input, Some(1), Some(true)),
            row(&fixture.input, Some(-5), None),
            row(&fixture.input, None, Some(true)),
            row(&fixture.input, Some(7), Some(false)),
        ];
        let expected = raw(&fixture, rows.clone()).await;
        let mut merged = plan.updater(&rows[4]).unwrap();
        // Exercise connected-session merge order independently of arrival order.
        for index in [1, 3, 0, 2] {
            let mut update = plan.updater(&rows[index]).unwrap();
            merged.merge(&update.state().unwrap()).unwrap();
        }
        let state = merged.state().unwrap();
        assert_eq!(values(&plan.evaluate(&state).unwrap()), values(&expected));
        assert_eq!(
            values(&expected),
            vec![
                ScalarValue::Int64(Some(5)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(i64::MIN)),
                ScalarValue::Int64(Some(-5)),
                ScalarValue::Int64(Some(i64::MAX))
            ]
        );
        assert!(state.get_array_memory_size() <= plan.layout.decoded_working_bytes);
        let empty_rows = vec![
            row(&fixture.input, None, None),
            row(&fixture.input, None, Some(false)),
        ];
        let expected = raw(&fixture, empty_rows.clone()).await;
        let mut update = plan.updater(&empty_rows[0]).unwrap();
        update
            .merge(&plan.updater(&empty_rows[1]).unwrap().state().unwrap())
            .unwrap();
        assert_eq!(
            values(&plan.evaluate(&update.state().unwrap()).unwrap()),
            values(&expected)
        );
    }

    #[tokio::test]
    async fn session_accumulator_whole_query_fallback_and_benchmark_route() {
        for expressions in [
            "COUNT(*) AS n, SUM(amount) AS total",
            "MIN(amount) AS lo, MAX(amount) AS hi",
        ] {
            let fixture = compile(expressions).await;
            assert!(
                SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, limits())
                    .unwrap()
                    .is_some()
            );
        }
        for expressions in [
            "COUNT(*) AS n, AVG(amount) AS mean",
            "SUM(CAST(amount AS DOUBLE)) AS total",
            "SUM(amount + 1) AS total",
            "SUM(amount) FILTER (WHERE amount > 0) AS total",
            "SUM(DISTINCT amount) AS total",
            "ARRAY_AGG(amount ORDER BY timestamp) AS items",
        ] {
            let fixture = compile(expressions).await;
            assert!(
                SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, limits())
                    .unwrap()
                    .is_none(),
                "unexpected eligibility: {expressions}"
            );
        }
        let fixture = compile("COUNT(*) AS n, SUM(amount) AS total").await;
        let mut tight = limits();
        tight.partial_bytes = 4096;
        assert!(
            SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, tight)
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn session_accumulator_unsigned_state_and_count_representable_boundary() {
        let fixture = compile_typed(
            "COUNT(amount) AS n, SUM(amount) AS total, MIN(amount) AS lo, MAX(amount) AS hi",
            "BIGINT UNSIGNED",
        )
        .await;
        let plan = SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, limits())
            .unwrap()
            .unwrap();
        let make_row = |value| {
            RecordBatch::try_new(
                fixture.input.clone(),
                fixture
                    .input
                    .fields()
                    .iter()
                    .map(|field| {
                        if field.data_type() == &DataType::UInt64 {
                            ScalarValue::UInt64(Some(value))
                                .to_array_of_size(1)
                                .unwrap()
                        } else {
                            row(&fixture.input, Some(0), Some(true))
                                .column(fixture.input.index_of(field.name()).unwrap())
                                .clone()
                        }
                    })
                    .collect(),
            )
            .unwrap()
        };
        let first = make_row(u64::MAX);
        let second = make_row(1);
        let expected = raw(&fixture, vec![first.clone(), second.clone()]).await;
        let mut update = plan.updater(&first).unwrap();
        update
            .merge(&plan.updater(&second).unwrap().state().unwrap())
            .unwrap();
        assert_eq!(
            values(&plan.evaluate(&update.state().unwrap()).unwrap()),
            values(&expected)
        );
        assert_eq!(values(&expected)[1], ScalarValue::UInt64(Some(0)));
        let fixture = compile("COUNT(*) AS n").await;
        let plan = SessionAccumulatorPlan::try_new(&fixture.finish, &fixture.input, limits())
            .unwrap()
            .unwrap();
        let mut update = plan.updater(&row(&fixture.input, None, None)).unwrap();
        let state = RecordBatch::try_new(
            plan.layout.schema.clone(),
            vec![
                ScalarValue::Int64(Some(i64::MAX - 1))
                    .to_array_of_size(1)
                    .unwrap(),
            ],
        )
        .unwrap();
        update.merge(&state).unwrap();
        assert_eq!(
            values(&plan.evaluate(&update.state().unwrap()).unwrap()),
            vec![ScalarValue::Int64(Some(i64::MAX))]
        );
    }
}
