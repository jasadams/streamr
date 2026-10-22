use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::datatypes::IntervalMonthDayNanoType;
use arroyo_datastream::logical::{LogicalEdge, LogicalGraph, LogicalNode};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, Spans, TableReference, plan_err,
};
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions::datetime::date_bin;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use petgraph::graph::{DiGraph, NodeIndex};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

use crate::ArroyoSchemaProvider;
use crate::extension::debezium::{
    DEBEZIUM_UNROLLING_EXTENSION_NAME, DebeziumUnrollingExtension, TO_DEBEZIUM_EXTENSION_NAME,
};
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::physical::{
    ArroyoMemExec, ArroyoPhysicalExtensionCodec, DebeziumUnrollingExec, DecodingContext,
    ToDebeziumExec,
};
use crate::schemas::add_timestamp_field_arrow;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{AggregateMode, physical_plan_node::PhysicalPlanType},
};
use prost::Message;

pub(crate) struct PlanToGraphVisitor<'a> {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    output_schemas: HashMap<NodeIndex, ArroyoSchemaRef>,
    named_nodes: HashMap<NamedNode, NodeIndex>,
    // each node that needs to know its inputs should push an empty vec in pre_visit.
    // In post_visit each node should clean up its vec and push its index to the last vec, if present.
    traversal: Vec<Vec<NodeIndex>>,
    planner: Planner<'a>,
}

impl<'a> PlanToGraphVisitor<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider, session_state: &'a SessionState) -> Self {
        Self {
            graph: Default::default(),
            output_schemas: Default::default(),
            named_nodes: Default::default(),
            traversal: vec![],
            planner: Planner::new(schema_provider, session_state),
        }
    }
}

pub(crate) struct Planner<'a> {
    schema_provider: &'a ArroyoSchemaProvider,
    planner: DefaultPhysicalPlanner,
    session_state: &'a SessionState,
}

impl<'a> Planner<'a> {
    pub(crate) fn new(
        schema_provider: &'a ArroyoSchemaProvider,
        session_state: &'a SessionState,
    ) -> Self {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ArroyoExtensionPlanner {},
        )]);
        Self {
            schema_provider,
            planner,
            session_state,
        }
    }

    pub(crate) fn sync_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        // This copy is physical-plan local: the streaming edge carries raw
        // rows, and its lifecycle owner appends the persisted write ordinal.
        let bound = bind_grouped_selector_ordinals(plan.clone())?;
        let fut = self
            .planner
            .create_physical_plan(&bound, self.session_state);
        let (tx, mut rx) = oneshot::channel();
        thread::scope(|s| {
            let _handle = tokio::runtime::Handle::current();
            let builder = thread::Builder::new();
            let builder = if cfg!(debug_assertions) {
                // Debug modes can end up with stack overflows because DataFusion is stack heavy.
                builder.stack_size(10_000_000)
            } else {
                builder
            };
            builder
                .spawn_scoped(s, move || {
                    let rt = Builder::new_current_thread().enable_all().build().unwrap();
                    rt.block_on(async {
                        let plan = fut.await;
                        tx.send(plan).unwrap();
                    });
                })
                .unwrap();
        });

        rx.try_recv().unwrap()
    }

    pub(crate) fn sync_local_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.repartition_aggregations = false;
        config.options_mut().optimizer.repartition_joins = false;
        config.options_mut().optimizer.repartition_sorts = false;
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        crate::local_physical::normalize_finite_partitions(
            Planner::new(self.schema_provider, &state).sync_plan(plan)?,
        )
    }
    pub(crate) fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = crate::local_planning::lower_expression(expr.clone(), input_dfschema, self)?;
        self.planner
            .create_physical_expr(&expr, input_dfschema, self.session_state)
    }

    pub(crate) fn serialize_as_physical_expr(
        &self,
        expr: &Expr,
        schema: &DFSchema,
    ) -> Result<Vec<u8>> {
        let physical = self.create_physical_expr(expr, schema)?;
        let proto = serialize_physical_expr(&physical, &ArroyoPhysicalExtensionCodec::default())?;
        Ok(proto.encode_to_vec())
    }

    // This splits aggregates into two parts, the partial aggregation and the final aggregation.
    // This needs to be done in physical space as that's the only point at which this split is realized.
    pub(crate) fn split_physical_plan(
        &self,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
        add_timestamp_field: bool,
    ) -> Result<SplitPlanOutput> {
        let physical_plan = self.sync_plan(aggregate)?;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let mut physical_plan_node =
            PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;
        let PhysicalPlanType::Aggregate(mut final_aggregate_proto) = physical_plan_node
            .physical_plan_type
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing physical plan type".to_string()))?
        else {
            return plan_err!("unexpected physical plan type");
        };
        let AggregateMode::Final = final_aggregate_proto.mode() else {
            return plan_err!("unexpected physical plan type");
        };
        // pull out the partial aggregation, so we can checkpoint it.
        let partial_aggregation_plan = *final_aggregate_proto
            .input
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing input".to_string()))?;

        // need to convert to ExecutionPlan to get the partial schema.
        let partial_aggregation_exec_plan = partial_aggregation_plan.try_into_physical_plan(
            self.schema_provider,
            &RuntimeEnvBuilder::new().build().unwrap(),
            &codec,
        )?;

        let partial_schema = partial_aggregation_exec_plan.schema();
        let final_input_table_provider =
            ArroyoMemExec::new("partial".into(), partial_schema.clone());

        final_aggregate_proto.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(final_input_table_provider),
            &codec,
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(final_aggregate_proto)),
        };

        let (partial_schema, timestamp_index) = if add_timestamp_field {
            (
                add_timestamp_field_arrow((*partial_schema).clone()),
                partial_schema.fields().len(),
            )
        } else {
            (partial_schema.clone(), partial_schema.fields().len() - 1)
        };

        let partial_schema = ArroyoSchema::new_keyed(partial_schema, timestamp_index, key_indices);

        Ok(SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        })
    }

    pub fn binning_function_proto(
        &self,
        width: Duration,
        input_schema: DFSchemaRef,
    ) -> Result<PhysicalExprNode> {
        let date_bin = date_bin().call(vec![
            Expr::Literal(
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNanoType::make_value(
                    0,
                    0,
                    width.as_nanos() as i64,
                ))),
                None,
            ),
            Expr::Column(datafusion::common::Column {
                relation: None,
                name: "_timestamp".into(),
                spans: Spans::new(),
            }),
        ]);

        let binning_function = self.create_physical_expr(&date_bin, &input_schema)?;
        serialize_physical_expr(&binning_function, &ArroyoPhysicalExtensionCodec::default())
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) enum NamedNode {
    Source(TableReference),
    Watermark(TableReference),
    EventResponseRoot(u32),
    RemoteTable(TableReference),
    EventResponseRemoteTable(u32, TableReference),
    Sink(TableReference),
    StateTableMerge(TableReference),
}

/// Add the internal ordinal only to the physical aggregate reader. Never add
/// it to the graph edge schema or let the volatile marker execute as a UDF.
fn bind_grouped_selector_ordinals(plan: LogicalPlan) -> Result<LogicalPlan> {
    use crate::grouped_selector::{ORDINAL_FIELD, ORDINAL_MARKER};
    use datafusion::common::tree_node::Transformed;
    use datafusion::logical_expr::Aggregate;
    plan.transform_up(|node| {
        let LogicalPlan::Aggregate(mut aggregate) = node else { return Ok(Transformed::no(node)) };
        let mut marker_found = false;
        for expression in &aggregate.aggr_expr {
            expression.apply(|expr| {
                marker_found |= matches!(expr, Expr::ScalarFunction(function) if function.name() == ORDINAL_MARKER);
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        if !marker_found { return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate))) }
        let LogicalPlan::Extension(extension) = aggregate.input.as_ref() else {
            return plan_err!("grouped selector requires its native keyed input reader");
        };
        let Some(input) = extension.node.as_any().downcast_ref::<KeyCalculationExtension>() else {
            return plan_err!("grouped selector requires a native key-calculation reader");
        };
        if input.schema.has_column_with_unqualified_name(ORDINAL_FIELD) {
            return plan_err!("reserved grouped selector ordinal field collides with input schema");
        }
        let mut input = input.clone();
        let mut fields = crate::fields_with_qualifiers(&input.schema);
        fields.push(crate::DFField::new(None, ORDINAL_FIELD, arrow_schema::DataType::UInt64, false));
        input.schema = Arc::new(crate::schema_from_df_fields_with_metadata(&fields, input.schema.metadata().clone())?);
        aggregate.input = Arc::new(LogicalPlan::Extension(Extension { node: Arc::new(input) }));
        aggregate.aggr_expr = aggregate.aggr_expr.into_iter().map(|expr| {
            Ok(expr.transform_up(|expr| {
                if matches!(&expr, Expr::ScalarFunction(function) if function.name() == ORDINAL_MARKER) {
                    Ok(Transformed::yes(Expr::Column(datafusion::common::Column::new_unqualified(ORDINAL_FIELD))))
                } else { Ok(Transformed::no(expr)) }
            })?.data)
        }).collect::<Result<Vec<_>>>()?;
        let aggregate = Aggregate::try_new_with_schema(aggregate.input, aggregate.group_expr, aggregate.aggr_expr, aggregate.schema)?;
        Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
    }).map(|result| result.data)
}

struct ArroyoExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for ArroyoExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let schema = node.schema().as_ref().into();
        if let Ok::<&dyn ArroyoExtension, _>(arroyo_extension) = node.try_into()
            && arroyo_extension.transparent()
        {
            match node.name() {
                DEBEZIUM_UNROLLING_EXTENSION_NAME => {
                    let node = node
                        .as_any()
                        .downcast_ref::<DebeziumUnrollingExtension>()
                        .unwrap();
                    let input = physical_inputs[0].clone();
                    return Ok(Some(Arc::new(DebeziumUnrollingExec::try_new(
                        input,
                        node.primary_keys.clone(),
                    )?)));
                }
                TO_DEBEZIUM_EXTENSION_NAME => {
                    let input = physical_inputs[0].clone();
                    return Ok(Some(Arc::new(ToDebeziumExec::try_new(
                        input,
                        Arc::new(schema),
                    )?)));
                }
                _ => return Ok(None),
            }
        };
        let name =
            if let Some(key_extension) = node.as_any().downcast_ref::<KeyCalculationExtension>() {
                key_extension.name.clone()
            } else {
                None
            };
        Ok(Some(Arc::new(ArroyoMemExec::new(
            name.unwrap_or("memory".to_string()),
            Arc::new(schema),
        ))))
    }
}

impl PlanToGraphVisitor<'_> {
    fn add_index_to_traversal(&mut self, index: NodeIndex) {
        if let Some(last) = self.traversal.last_mut() {
            last.push(index);
        }
    }

    pub(crate) fn add_plan(&mut self, plan: LogicalPlan) -> Result<()> {
        self.traversal.clear();
        plan.visit(self)?;
        Ok(())
    }

    pub fn into_graph(self) -> LogicalGraph {
        self.graph
    }

    pub fn build_extension(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        extension: &dyn ArroyoExtension,
    ) -> Result<()> {
        extension.validate_response_scope()?;

        if let Some(node_name) = extension.node_name()
            && self.named_nodes.contains_key(&node_name)
        {
            // we should've short circuited
            return plan_err!(
                "extension {:?} has already been planned, shouldn't try again.",
                node_name
            );
        }

        let input_schemas = input_nodes
            .iter()
            .map(|index| {
                Ok(self
                    .output_schemas
                    .get(index)
                    .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?
                    .clone())
            })
            .collect::<Result<Vec<_>>>()?;

        // This visitor owns the graph and its memoization map. Resolve slots
        // from the already-planned root payload, never from NodeIndex or names.
        let bound = if let Some(slot) = extension.response_root_slot() {
            let root_index = self
                .named_nodes
                .get(&NamedNode::EventResponseRoot(slot))
                .ok_or_else(|| {
                    datafusion::common::plan_datafusion_err!(
                        "response root was not planned before its consumer"
                    )
                })?;
            let root = &self.graph[*root_index];
            if root.parallelism != 1 {
                return plan_err!("response root requires singleton execution");
            }
            extension.bind_response_root(root.node_id)?
        } else {
            None
        };
        let extension = bound.as_deref().unwrap_or(extension);
        let NodeWithIncomingEdges { node, edges } = extension
            .plan_node(&self.planner, self.graph.node_count(), input_schemas)
            .map_err(|e| e.context(format!("planning operator {extension:?}")))?;

        let node_index = self.graph.add_node(node);
        self.add_index_to_traversal(node_index);

        for (source, edge) in input_nodes.into_iter().zip(edges) {
            self.graph.add_edge(source, node_index, edge);
        }

        self.output_schemas
            .insert(node_index, extension.output_schema().into());

        if let Some(node_name) = extension.node_name() {
            self.named_nodes.insert(node_name, node_index);
        }
        Ok(())
    }
}

impl TreeNodeVisitor<'_> for PlanToGraphVisitor<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        arroyo_extension.validate_response_scope()?;

        if arroyo_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }

        if let Some(name) = arroyo_extension.node_name()
            && let Some(node_index) = self.named_nodes.get(&name)
        {
            self.add_index_to_traversal(*node_index);
            return Ok(TreeNodeRecursion::Jump);
        }

        if !node.inputs().is_empty() {
            self.traversal.push(vec![]);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    // most of the work sits in post visit so that we can have the inputs of each node
    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("planning extension"))?;

        if arroyo_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }

        if let Some(name) = arroyo_extension.node_name()
            && self.named_nodes.contains_key(&name)
        {
            return Ok(TreeNodeRecursion::Continue);
        }

        let input_nodes = if !node.inputs().is_empty() {
            self.traversal.pop().unwrap_or_default()
        } else {
            vec![]
        };
        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        self.build_extension(input_nodes, arroyo_extension)?;

        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) struct SplitPlanOutput {
    pub(crate) partial_aggregation_plan: PhysicalPlanNode,
    pub(crate) partial_schema: ArroyoSchema,
    pub(crate) finish_plan: PhysicalPlanNode,
}
