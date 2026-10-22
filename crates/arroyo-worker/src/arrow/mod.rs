use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, EventResponseRole,
    OperatorConstructor, Registry,
};
use arroyo_planner::physical::ArroyoPhysicalExtensionCodec;
use arroyo_planner::physical::DecodingContext;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_rpc::grpc::api;
use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::EventResponse;
use datafusion::common::Result as DFResult;
use datafusion::common::internal_err;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::{parse_physical_expr, parse_physical_sort_expr};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf::physical_aggregate_expr_node::AggregateFunction;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode, proto_error};
use futures::StreamExt;
use itertools::Itertools;
use prost::Message as ProstMessage;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

mod aggregate_codec;
mod aggregate_store;
pub mod async_udf;
pub mod incremental_aggregator;
pub mod instant_join;
pub mod join_with_expiration;
pub mod lookup_join;
mod session_accumulator;
pub mod session_aggregating_window;
mod session_native;
mod session_store;
pub mod sliding_aggregating_window;
pub mod state_table;
mod state_table_concat;
pub mod state_table_owner;
pub mod state_table_runtime;
pub(crate) mod sync;
pub mod tumbling_aggregating_window;
mod updating_cache;
pub mod updating_join;
pub mod watermark_generator;
pub mod window_fn;
mod window_native;
mod window_store;

pub struct ValueExecutionOperator {
    event_output: Option<Arc<EventEdgeSpec>>,
    name: String,
    executor: StatelessPhysicalExecutor,
}

pub struct ValueExecutionConstructor;
impl OperatorConstructor for ValueExecutionConstructor {
    type ConfigT = api::ValuePlanOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;
        Ok(ConstructedOperator::from_operator(Box::new(
            ValueExecutionOperator {
                event_output: None,
                name: config.name,
                executor,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ValueExecutionOperator {
    fn event_response_role(&self) -> EventResponseRole {
        EventResponseRole::Scalar
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
        if inputs.len() != 1 || output.schema != self.executor.plan.schema() {
            return Err(DataflowError::ArgumentError(
                "scalar event binding schema/input mismatch".into(),
            ));
        }
        validate_response_physical(self.executor.plan.as_ref(), false)?;
        let next = Arc::new(EventEdgeSpec {
            schema: output.schema.clone(),
            ..inputs[0].as_ref().clone()
        });
        self.register_response_output(next.clone())
            .map_err(|error| DataflowError::ArgumentError(error.to_string()))?;
        Ok(Some(next))
    }
    fn validate_event_source_plumbing(&self) -> DataflowResult<()> {
        validate_response_physical(self.executor.plan.as_ref(), true)
    }

    async fn process_response_index(
        &mut self,
        _index: usize,
        _partitions: usize,
        response: EventResponse,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let spec = self.event_output.as_deref().ok_or_else(|| {
            DataflowError::ArgumentError(
                "scalar response output descriptor was not explicitly registered".into(),
            )
        })?;
        let output = execute_scalar_response(&mut self.executor, &response, spec).await?;
        drop(response);
        collector.collect_response(output).await
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: (&self.name).into(),
            fields: vec![("plan", (&*self.executor.plan).into())],
        }
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let mut records = self.executor.process_batch(record_batch).await;
        while let Some(batch) = records.next().await {
            let batch = batch?;
            collector.collect(batch).await?;
        }
        Ok(())
    }
}

pub struct ProjectionOperator {
    event_output: Option<Arc<EventEdgeSpec>>,
    name: String,
    output_schema: ArroyoSchema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

pub struct ProjectionConstructor;
impl OperatorConstructor for ProjectionConstructor {
    type ConfigT = api::ProjectionOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let output_schema: ArroyoSchema = config.output_schema.unwrap().try_into()?;

        let exprs: anyhow::Result<_> = config
            .exprs
            .iter()
            .map(|expr| {
                Ok(parse_physical_expr(
                    &PhysicalExprNode::decode(&mut expr.as_slice())?,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )?)
            })
            .collect();

        Ok(ConstructedOperator::from_operator(Box::new(
            ProjectionOperator {
                event_output: None,
                name: config.name,
                output_schema,
                exprs: exprs?,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ProjectionOperator {
    fn event_response_role(&self) -> EventResponseRole {
        EventResponseRole::Scalar
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
        if inputs.len() != 1 || output.schema != self.output_schema.schema.clone() {
            return Err(DataflowError::ArgumentError(
                "scalar event binding schema/input mismatch".into(),
            ));
        }

        let next = Arc::new(EventEdgeSpec {
            schema: output.schema.clone(),
            ..inputs[0].as_ref().clone()
        });
        self.register_response_output(next.clone())
            .map_err(|error| DataflowError::ArgumentError(error.to_string()))?;
        Ok(Some(next))
    }
    fn validate_event_source_plumbing(&self) -> DataflowResult<()> {
        Ok(())
    }

    async fn process_response_index(
        &mut self,
        _index: usize,
        _partitions: usize,
        response: EventResponse,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let spec = self.event_output.as_deref().ok_or_else(|| {
            DataflowError::ArgumentError(
                "scalar response output descriptor was not explicitly registered".into(),
            )
        })?;
        let rows = response
            .rows
            .as_ref()
            .map(|input| -> DataflowResult<RecordBatch> {
                validate_scalar_packet_rows(input.num_rows())?;
                let arrays = self
                    .exprs
                    .iter()
                    .map(|expression| expression.evaluate(input)?.into_array(input.num_rows()))
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(RecordBatch::try_new(
                    self.output_schema.schema.clone(),
                    arrays,
                )?)
            })
            .transpose()?;
        let output = crate::event_response_codec::rebind_response_rows(&response, rows, spec)
            .map_err(|error| DataflowError::ArgumentError(error.to_string()))?;
        drop(response);
        collector.collect_response(output).await
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: (&self.name).into(),
            fields: vec![(
                "exprs",
                AsDisplayable::List(self.exprs.iter().map(|e| e.to_string()).collect()),
            )],
        }
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let outputs = self
            .exprs
            .iter()
            .map(|e| {
                e.evaluate(&record_batch)
                    .and_then(|f| f.into_array(record_batch.num_rows()))
            })
            .try_collect()?;

        let output = RecordBatch::try_new(self.output_schema.schema.clone(), outputs)?;
        collector.collect(output).await
    }
}

pub struct KeyExecutionOperator {
    event_output: Option<Arc<EventEdgeSpec>>,
    name: String,
    executor: StatelessPhysicalExecutor,
    #[allow(unused)]
    key_fields: Vec<usize>,
}

pub struct KeyExecutionConstructor;

impl OperatorConstructor for KeyExecutionConstructor {
    type ConfigT = api::KeyPlanOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;

        Ok(ConstructedOperator::from_operator(Box::new(
            KeyExecutionOperator {
                event_output: None,
                name: config.name,
                executor,
                key_fields: config
                    .key_fields
                    .into_iter()
                    .map(|field| field as usize)
                    .collect(),
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for KeyExecutionOperator {
    fn event_response_role(&self) -> EventResponseRole {
        EventResponseRole::Scalar
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
        if inputs.len() != 1 || output.schema != self.executor.plan.schema() {
            return Err(DataflowError::ArgumentError(
                "scalar event binding schema/input mismatch".into(),
            ));
        }
        validate_response_physical(self.executor.plan.as_ref(), false)?;
        let next = Arc::new(EventEdgeSpec {
            schema: output.schema.clone(),
            ..inputs[0].as_ref().clone()
        });
        self.register_response_output(next.clone())
            .map_err(|error| DataflowError::ArgumentError(error.to_string()))?;
        Ok(Some(next))
    }
    fn validate_event_source_plumbing(&self) -> DataflowResult<()> {
        validate_response_physical(self.executor.plan.as_ref(), true)
    }

    async fn process_response_index(
        &mut self,
        _index: usize,
        _partitions: usize,
        response: EventResponse,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let spec = self.event_output.as_deref().ok_or_else(|| {
            DataflowError::ArgumentError(
                "scalar response output descriptor was not explicitly registered".into(),
            )
        })?;
        let output = execute_scalar_response(&mut self.executor, &response, spec).await?;
        drop(response);
        collector.collect_response(output).await
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed(&self.name),
            fields: vec![
                ("key_fields", AsDisplayable::Debug(&self.key_fields)),
                ("plan", AsDisplayable::Plan(self.executor.plan.as_ref())),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let mut records = self.executor.process_batch(batch).await;
        while let Some(batch) = records.next().await {
            //TODO: sort by the key
            //info!("batch {:?}", batch);
            collector.collect(batch?).await?;
        }
        Ok(())
    }
}

pub struct StatelessPhysicalExecutor {
    batch: Arc<RwLock<Option<RecordBatch>>>,
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    active: Arc<AtomicBool>,
}

struct ExecutionInputGuard {
    batch: Arc<RwLock<Option<RecordBatch>>>,
    active: Arc<AtomicBool>,
}

impl Drop for ExecutionInputGuard {
    fn drop(&mut self) {
        self.batch.write().unwrap().take();
        self.active.store(false, Ordering::Release);
    }
}

fn failed_execution_stream(
    schema: SchemaRef,
    error: datafusion::common::DataFusionError,
) -> SendableRecordBatchStream {
    Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Err(error) }),
        ),
    )
}

impl StatelessPhysicalExecutor {
    pub fn new(mut proto: &[u8], registry: &Registry) -> anyhow::Result<Self> {
        let batch = Arc::new(RwLock::default());

        let plan = PhysicalPlanNode::decode(&mut proto)?;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(batch.clone()),
        };

        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let plan = plan.try_into_physical_plan(registry, &runtime, &codec)?;
        arroyo_planner::physical::validate_json_table_execution_plan(plan.as_ref())?;
        let task_context =
            SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();

        Ok(Self {
            batch,
            plan,
            task_context,
            active: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn process_batch(&mut self, batch: RecordBatch) -> SendableRecordBatchStream {
        if self.active.swap(true, Ordering::AcqRel) {
            return failed_execution_stream(self.plan.schema(), datafusion::common::DataFusionError::Execution(
                "stateless executor requires the previous stream to finish or be dropped before another input".into(),
            ));
        }
        let input_guard = ExecutionInputGuard {
            batch: self.batch.clone(),
            active: self.active.clone(),
        };
        {
            let mut writer = self.batch.write().unwrap();
            *writer = Some(batch);
        }
        let result = self
            .plan
            .reset()
            .and_then(|_| {
                if arroyo_planner::local_physical::plan_contains_local(self.plan.as_ref()) {
                    arroyo_planner::local_physical::wrap_local_execution(self.plan.clone())
                } else {
                    Ok(self.plan.clone())
                }
            })
            .and_then(|plan| plan.execute(0, self.task_context.clone()));
        let stream = match result {
            Ok(stream) => stream,
            Err(error) => return failed_execution_stream(self.plan.schema(), error),
        };
        let schema = stream.schema();
        let held_input = async_stream::try_stream! {
            let _input_guard = input_guard;
            let mut stream = stream;
            while let Some(batch) = stream.next().await {
                yield batch?;
            }
        };
        Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, held_input),
        )
    }

    /// The returned batch is caller-owned after the stream has drained.
    pub async fn process_single(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let mut stream = self.process_batch(batch).await;
        let result = stream.next().await.transpose()?.ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "expected one output batch, received none".into(),
            )
        })?;
        if stream.next().await.transpose()?.is_some() {
            return Err(datafusion::common::DataFusionError::Execution(
                "expected one output batch, received more than one".into(),
            ));
        }
        Ok(result)
    }
}

pub fn decode_aggregate(
    schema: &SchemaRef,
    name: &str,
    expr: &PhysicalExprNode,
    registry: &dyn FunctionRegistry,
) -> DFResult<Arc<AggregateFunctionExpr>> {
    let codec = &DefaultPhysicalExtensionCodec {};
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty aggregate physical expression"))?;

    match expr_type {
        ExprType::AggregateExpr(agg_node) => {
            // Validate lifecycle bindings before DataFusion's builder asks a
            // Column for its datatype (which indexes the schema directly).
            if let Some(AggregateFunction::UserDefinedAggrFunction(name)) =
                &agg_node.aggregate_function
            {
                let descriptor = registry.udaf(name).ok().and_then(|function| {
                    arroyo_planner::grouped_selector_udaf::classify(&function)
                });
                if let Some((_, declared)) = descriptor {
                    use arroyo_planner::grouped_selector::{ORDINAL_FIELD, ORDINAL_MARKER};
                    let valid_ordinal = agg_node.expr.len() == 2
                        && match agg_node.expr[1].expr_type.as_ref() {
                            Some(ExprType::Column(column)) => {
                                column.name == ORDINAL_FIELD
                                    && schema.index_of(ORDINAL_FIELD).ok()
                                        == Some(column.index as usize)
                                    && schema.fields().get(column.index as usize).is_some_and(
                                        |field| {
                                            field.name() == ORDINAL_FIELD
                                                && field.data_type()
                                                    == &arrow_schema::DataType::UInt64
                                                && !field.is_nullable()
                                        },
                                    )
                            }
                            Some(ExprType::ScalarUdf(function)) => {
                                function.name == ORDINAL_MARKER && function.args.is_empty()
                            }
                            _ => false,
                        };
                    if !valid_ordinal {
                        return Err(proto_error("invalid grouped selector lifecycle ordinal"));
                    }
                    if declared {
                        let valid_clock = agg_node.ordering_req.first().is_some_and(|sort| {
                            sort.asc
                                && !sort.nulls_first
                                && sort.expr.as_ref().is_some_and(|expr| {
                                    match expr.expr_type.as_ref() {
                                        Some(ExprType::Column(column)) => {
                                            column.name == arroyo_rpc::TIMESTAMP_FIELD
                                                && schema.index_of(arroyo_rpc::TIMESTAMP_FIELD).ok()
                                                    == Some(column.index as usize)
                                                && schema
                                                    .fields()
                                                    .get(column.index as usize)
                                                    .is_some_and(|field| {
                                                        field.name() == arroyo_rpc::TIMESTAMP_FIELD
                                                            && matches!(
                                                                field.data_type(),
                                                                arrow_schema::DataType::Timestamp(
                                                                    _,
                                                                    _
                                                                )
                                                            )
                                                    })
                                        }
                                        _ => false,
                                    }
                                })
                        });
                        if !valid_clock {
                            return Err(proto_error(
                                "invalid grouped selector raw FOR primary order",
                            ));
                        }
                    }
                }
            }
            let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node
                .expr
                .iter()
                .map(|e| parse_physical_expr(e, registry, schema, codec))
                .collect::<DFResult<Vec<_>>>()?;
            let ordering_req: LexOrdering = agg_node
                .ordering_req
                .iter()
                .map(|e| parse_physical_sort_expr(e, registry, schema, codec))
                .collect::<DFResult<LexOrdering>>()?;
            agg_node
                .aggregate_function
                .as_ref()
                .map(|func| match func {
                    AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
                        let agg_udf = match &agg_node.fun_definition {
                            Some(buf) => codec.try_decode_udaf(udaf_name, buf)?,
                            None => registry.udaf(udaf_name)?,
                        };

                        AggregateExprBuilder::new(agg_udf, input_phy_expr)
                            .schema(Arc::clone(schema))
                            .alias(name)
                            .with_ignore_nulls(agg_node.ignore_nulls)
                            .with_distinct(agg_node.distinct)
                            .order_by(ordering_req)
                            .build()
                            .map(Arc::new)
                    }
                })
                .transpose()?
                .ok_or_else(|| proto_error("Invalid AggregateExpr, missing aggregate_function"))
        }
        _ => internal_err!("Invalid aggregate expression for AggregateExec"),
    }
}

#[cfg(test)]
mod json_table_tests;

/// The response carrier admits <=2 raw images or <=4 native changes. Scalar
/// plans may filter/project those rows, never expand an original event here.
fn validate_scalar_packet_rows(rows: usize) -> DataflowResult<()> {
    if !(1..=4).contains(&rows) {
        return Err(DataflowError::ArgumentError(
            "scalar response requires one admitted bounded event packet".into(),
        ));
    }
    Ok(())
}

async fn execute_scalar_response(
    executor: &mut StatelessPhysicalExecutor,
    response: &EventResponse,
    next: &EventEdgeSpec,
) -> DataflowResult<EventResponse> {
    let rows = if let Some(input) = &response.rows {
        validate_scalar_packet_rows(input.num_rows())?;
        let limit = input.num_rows();
        let mut stream = executor.process_batch(input.clone()).await;
        let mut output: Option<RecordBatch> = None;
        let mut count = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            count = count.checked_add(batch.num_rows()).ok_or_else(|| {
                DataflowError::ArgumentError("scalar response row count overflow".into())
            })?;
            if count > limit {
                return Err(DataflowError::ArgumentError(
                    "scalar response plan expanded an original event packet".into(),
                ));
            }
            output = Some(match output {
                None => batch,
                Some(previous) => {
                    let schema = batch.schema();
                    arrow::compute::concat_batches(&schema, &[previous, batch])?
                }
            });
        }
        // Release the executor input/active guard before public output awaits.
        drop(stream);
        output
    } else {
        // Empty completion is NOT a SQL NULL row; no physical execution.
        None
    };
    crate::event_response_codec::rebind_response_rows(response, rows, next)
        .map_err(|error| DataflowError::ArgumentError(error.to_string()))
}

// Engine passes the same constructed descriptor to scalars/collector/wire.
fn register_scalar_response_output(
    slot: &mut Option<Arc<EventEdgeSpec>>,
    spec: Arc<EventEdgeSpec>,
    schema: &SchemaRef,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        slot.is_none(),
        "duplicate scalar response output registration"
    );
    anyhow::ensure!(
        spec.schema == *schema,
        "scalar response output schema mismatch"
    );
    crate::event_response_codec::ResponseSpec::from_edge(&spec)?;
    *slot = Some(spec);
    Ok(())
}
impl ValueExecutionOperator {
    fn register_response_output(&mut self, spec: Arc<EventEdgeSpec>) -> anyhow::Result<()> {
        register_scalar_response_output(&mut self.event_output, spec, &self.executor.plan.schema())
    }
}
impl KeyExecutionOperator {
    fn register_response_output(&mut self, spec: Arc<EventEdgeSpec>) -> anyhow::Result<()> {
        register_scalar_response_output(&mut self.event_output, spec, &self.executor.plan.schema())
    }
}
impl ProjectionOperator {
    fn register_response_output(&mut self, spec: Arc<EventEdgeSpec>) -> anyhow::Result<()> {
        register_scalar_response_output(&mut self.event_output, spec, &self.output_schema.schema)
    }
}

fn validate_response_physical(plan: &dyn ExecutionPlan, source: bool) -> DataflowResult<()> {
    fn visit(plan: &dyn ExecutionPlan, source: bool, leaves: &mut usize) -> DataflowResult<()> {
        if plan
            .as_any()
            .is::<arroyo_planner::physical::ArroyoMemExec>()
            || arroyo_planner::physical::is_single_locked_batch_reader(plan)
        {
            if !plan.children().is_empty() {
                return Err(DataflowError::ArgumentError(
                    "event scalar reader has children".into(),
                ));
            }
            *leaves += 1;
            return Ok(());
        }
        let allowed = plan
            .as_any()
            .is::<datafusion::physical_plan::projection::ProjectionExec>()
            || (!source
                && plan
                    .as_any()
                    .is::<datafusion::physical_plan::filter::FilterExec>())
            || (source
                && plan
                    .as_any()
                    .is::<arroyo_planner::physical::DebeziumUnrollingExec>());
        if !allowed || plan.children().len() != 1 {
            return Err(DataflowError::ArgumentError(format!(
                "event route has unsupported physical scalar/source plumbing: node={}, source={}, children={}",
                plan.name(),
                source,
                plan.children().len(),
            )));
        }
        visit(plan.children()[0].as_ref(), source, leaves)
    }
    let mut leaves = 0;
    visit(plan, source, &mut leaves)?;
    if leaves != 1 {
        return Err(DataflowError::ArgumentError(
            "event scalar requires one source reader".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod scalar_response_tests;
