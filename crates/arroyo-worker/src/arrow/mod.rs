use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_planner::physical::ArroyoPhysicalExtensionCodec;
use arroyo_planner::physical::DecodingContext;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api;
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
mod execution;
pub mod incremental_aggregator;
pub mod instant_join;
pub mod join_with_expiration;
pub mod lookup_join;
pub mod session_aggregating_window;
mod session_native;
mod session_store;
pub mod sliding_aggregating_window;
pub mod state_table;
pub mod state_table_owner;
pub mod state_table_runtime;
pub mod stateful_processor;
pub(crate) mod sync;
pub mod tumbling_aggregating_window;
mod updating_cache;
pub mod watermark_generator;
pub mod window_fn;
mod window_native;
mod window_store;

pub struct ValueExecutionOperator {
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
                name: config.name,
                executor,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ValueExecutionOperator {
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
    name: String,
    output_schema: ArroyoSchema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    resources: Option<Arc<execution::ExecutionResources>>,
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
                name: config.name,
                output_schema,
                exprs: exprs?,
                resources: execution::configured_execution_resources()?,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ProjectionOperator {
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
        let _input_reservation = self
            .resources
            .as_ref()
            .map(|resources| resources.reserve_batch("Streamr projection input", &record_batch))
            .transpose()?;
        let outputs = self
            .exprs
            .iter()
            .map(|e| {
                e.evaluate(&record_batch)
                    .and_then(|f| f.into_array(record_batch.num_rows()))
            })
            .try_collect()?;

        let output = RecordBatch::try_new(self.output_schema.schema.clone(), outputs)?;
        let _output_reservation = self
            .resources
            .as_ref()
            .map(|resources| resources.reserve_batch("Streamr projection output", &output))
            .transpose()?;
        collector.collect(output).await
    }
}

pub struct KeyExecutionOperator {
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
    resources: Option<Arc<execution::ExecutionResources>>,
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
    pub fn new(proto: &[u8], registry: &Registry) -> anyhow::Result<Self> {
        Self::new_with_resources(
            proto,
            registry,
            execution::configured_execution_resources()?,
        )
    }

    fn new_with_resources(
        mut proto: &[u8],
        registry: &Registry,
        resources: Option<Arc<execution::ExecutionResources>>,
    ) -> anyhow::Result<Self> {
        let batch = Arc::new(RwLock::default());

        let plan = PhysicalPlanNode::decode(&mut proto)?;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(batch.clone()),
        };

        let runtime = match &resources {
            Some(resources) => resources.runtime.clone(),
            None => RuntimeEnvBuilder::new().build_arc()?,
        };
        let plan = plan.try_into_physical_plan(registry, &runtime, &codec)?;
        let task_context = match &resources {
            Some(resources) => resources.task_context(),
            None => SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx(),
        };

        Ok(Self {
            batch,
            plan,
            task_context,
            resources,
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
        let input_reservation = match self
            .resources
            .as_ref()
            .map(|resources| resources.reserve_batch("execution input", &batch))
            .transpose()
        {
            Ok(reservation) => reservation,
            Err(error) => return failed_execution_stream(self.plan.schema(), error),
        };
        {
            let mut writer = self.batch.write().unwrap();
            *writer = Some(batch);
        }
        let result = self
            .plan
            .reset()
            .and_then(|_| self.plan.execute(0, self.task_context.clone()));
        let stream = match result {
            Ok(stream) => stream,
            Err(error) => return failed_execution_stream(self.plan.schema(), error),
        };
        let schema = stream.schema();
        let runtime = self
            .resources
            .as_ref()
            .map(|resources| resources.runtime.clone());
        let held_input = async_stream::try_stream! {
            let _input_guard = input_guard;
            let _runtime = runtime;
            let _reservation = input_reservation;
            let mut stream = stream;
            while let Some(batch) = stream.next().await {
                yield batch?;
            }
        };
        let held_input = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, held_input),
        );
        match &self.resources {
            Some(resources) => sync::streams::bounded_output_stream(
                held_input,
                resources.runtime.memory_pool.clone(),
                resources.limits.max_batch_bytes,
            ),
            None => held_input,
        }
    }

    /// The returned batch is caller-owned after the stream has drained; callers
    /// retaining it must supply their own output reservation.
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
