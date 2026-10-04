//! Structural proof and graph reconstruction for a singleton state-table event
//! owner. The SQL planner lowers each proven region to one fused runtime owner
//! with captured exit branches.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use arrow_schema::{DataType, Schema, SchemaRef};
use arroyo_datastream::logical::{
    ChainedLogicalOperator, LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName,
};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{
    FusedStateTableOperator, FusedStateTableStep, ProjectionOperator, StateTableCaptureOperator,
    StateTableDefinition, StateTableOperator, ValuePlanOperator,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{ExecutionPlan, filter::FilterExec, projection::ProjectionExec};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use petgraph::Direction::{Incoming, Outgoing};
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use prost::Message;

fn reject(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("state-table fusion: {}", message.into()))
}

/// The caller must inspect the *physical* ArrowValue plan and prove that it
/// contains only bounded projection/filter operations over one input event.
/// The sole admitted volatile value function is the concrete, zero-argument
/// DataFusion UUID v4 function as a direct projected field. Predicates, nested
/// expressions, and state ownership keys cannot depend on it. Aggregate, limit, sort,
/// union, join, repartition, table scan, arbitrary UDF, and row-expanding
/// nodes remain unsupported. An operator name alone cannot prove this.
pub(crate) trait ScalarPlanAdmission {
    fn admit_projection(
        &self,
        plan: &ProjectionOperator,
        input: &ArroyoSchemaRef,
        output: &ArroyoSchemaRef,
    ) -> Result<()>;
    fn admit_scalar_plan(
        &self,
        plan: &ValuePlanOperator,
        input: &ArroyoSchemaRef,
        output: &ArroyoSchemaRef,
    ) -> Result<()>;
}

pub(crate) struct BoundedScalarAdmission<'a> {
    pub registry: &'a crate::ArroyoSchemaProvider,
}

fn admit_expression(expr: &dyn PhysicalExpr, uuid_value_root: bool) -> Result<()> {
    if let Some(function) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()
    {
        let implementation = function.fun().inner();
        // Match implementations, not a user UDF's possibly identical name.
        // DataFusion's UUID v4 returns one 36-character UTF8 value per input
        // row. It is admitted only as a materialized value, never a filter or
        // ownership key; the owner evaluates one event at a time.
        let bounded_uuid = uuid_value_root
            && implementation
                .as_any()
                .is::<datafusion_functions::string::uuid::UuidFunc>()
            && function.args().is_empty()
            && function.return_type() == &DataType::Utf8;
        if !bounded_uuid
            && !implementation
                .as_any()
                .is::<datafusion_functions::core::getfield::GetFieldFunc>()
            && !implementation
                .as_any()
                .is::<datafusion_functions::core::coalesce::CoalesceFunc>()
            && !implementation
                .as_any()
                .is::<datafusion_functions::core::nullif::NullIfFunc>()
        {
            return Err(reject(format!(
                "scalar function {} has unqualified purity or allocation bounds",
                function.name()
            )));
        }
    }
    if let Some(binary) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::BinaryExpr>()
        && *binary.op() == datafusion::logical_expr::Operator::StringConcat
    {
        return Err(reject(
            "string concatenation needs an explicit expansion bound",
        ));
    }
    for child in expr.children() {
        admit_expression(child.as_ref(), false)?;
    }
    Ok(())
}

#[cfg(test)]
mod uuid_admission_tests {
    use super::admit_expression;
    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::{Volatility, create_udf};
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion_functions::string::uuid::UuidFunc;
    use std::sync::Arc;

    #[test]
    fn only_concrete_uuid_values_are_admitted() {
        let field = Arc::new(Field::new("candidate", DataType::Utf8, false));
        let built_in = ScalarFunctionExpr::new(
            "uuid",
            Arc::new(UuidFunc::new().into()),
            vec![],
            field.clone(),
        );
        admit_expression(&built_in, true).unwrap();
        assert!(admit_expression(&built_in, false).is_err());

        let spoof = create_udf(
            "uuid",
            vec![],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(|_| unreachable!("admission must not evaluate a UDF")),
        );
        let spoof_expr = ScalarFunctionExpr::new("uuid", Arc::new(spoof), vec![], field);
        assert!(admit_expression(&spoof_expr, true).is_err());
    }
}

fn admit_plan(
    plan: &dyn ExecutionPlan,
    input_count: &mut usize,
    expected_input: &SchemaRef,
) -> Result<()> {
    if plan.as_any().is::<crate::physical::ArroyoMemExec>() {
        if plan.schema().as_ref() != expected_input.as_ref() {
            return Err(reject(
                "scalar physical input schema differs from graph edge",
            ));
        }
        *input_count += 1;
        return Ok(());
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        for (expr, _) in projection.expr() {
            admit_expression(expr.as_ref(), true)?;
        }
    } else if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        admit_expression(filter.predicate().as_ref(), false)?;
    } else {
        return Err(reject(format!(
            "physical plan {} can expand, reorder, or retain input events",
            plan.name()
        )));
    }
    let children = plan.children();
    if children.len() != 1 {
        return Err(reject("scalar physical plan does not have one input"));
    }
    admit_plan(children[0].as_ref(), input_count, expected_input)
}

impl ScalarPlanAdmission for BoundedScalarAdmission<'_> {
    fn admit_projection(
        &self,
        plan: &ProjectionOperator,
        expected_input: &ArroyoSchemaRef,
        expected_output: &ArroyoSchemaRef,
    ) -> Result<()> {
        let input: ArroyoSchema = plan
            .input_schema
            .clone()
            .ok_or_else(|| reject("projection lacks input schema"))?
            .try_into()?;
        let output: ArroyoSchema = plan
            .output_schema
            .clone()
            .ok_or_else(|| reject("projection lacks output schema"))?
            .try_into()?;
        if input != **expected_input || output != **expected_output {
            return Err(reject("projection config schema differs from graph edge"));
        }
        if plan.exprs.len() != output.schema.fields().len() {
            return Err(reject(
                "projection expression count differs from output schema",
            ));
        }
        for bytes in &plan.exprs {
            let proto = PhysicalExprNode::decode(bytes.as_slice())
                .map_err(|error| reject(format!("invalid projection expression: {error}")))?;
            let expr = parse_physical_expr(
                &proto,
                self.registry,
                input.schema.as_ref(),
                &DefaultPhysicalExtensionCodec {},
            )?;
            admit_expression(expr.as_ref(), true)?;
        }
        Ok(())
    }

    fn admit_scalar_plan(
        &self,
        plan: &ValuePlanOperator,
        input: &ArroyoSchemaRef,
        output: &ArroyoSchemaRef,
    ) -> Result<()> {
        let proto = PhysicalPlanNode::decode(plan.physical_plan.as_slice())
            .map_err(|error| reject(format!("invalid scalar physical plan: {error}")))?;
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let physical = proto.try_into_physical_plan(
            self.registry,
            &runtime,
            &crate::physical::ArroyoPhysicalExtensionCodec {
                context: crate::physical::DecodingContext::Planning,
            },
        )?;
        let mut inputs = 0;
        if physical.schema().as_ref() != output.schema.as_ref() {
            return Err(reject(
                "scalar physical output schema differs from graph edge",
            ));
        }
        admit_plan(physical.as_ref(), &mut inputs, &input.schema)?;
        if inputs != 1 {
            return Err(reject(
                "scalar physical plan does not have exactly one event input",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FusionKind {
    Projection,
    ScalarFilter,
    StateAccess,
}

#[derive(Clone, Debug)]
pub(crate) struct FusionStep {
    pub node: NodeIndex,
    pub parent_step: Option<usize>,
    pub kind: FusionKind,
    pub operator: ChainedLogicalOperator,
    pub output_schema: ArroyoSchemaRef,
    /// Exit branches are captured immediately after this step; later state
    /// writes cannot alter their source/old/new/action values.
    pub captures: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct FusionCapture {
    pub producer: NodeIndex,
    pub consumers: Vec<NodeIndex>,
    pub schema: ArroyoSchemaRef,
}

#[derive(Clone, Debug)]
pub(crate) struct FusionRegion {
    pub source: NodeIndex,
    pub event_scope_id: String,
    pub steps: Vec<FusionStep>,
    pub captures: Vec<FusionCapture>,
}

fn single_operator(graph: &LogicalGraph, node: NodeIndex) -> Result<&ChainedLogicalOperator> {
    let weight = &graph[node];
    if weight.operator_chain.len() != 1 {
        return Err(reject(format!(
            "node {} is already chained; fuse before chain optimization",
            weight.node_id
        )));
    }
    Ok(weight.operator_chain.first())
}

fn only_parent(graph: &LogicalGraph, node: NodeIndex) -> Result<(NodeIndex, LogicalEdge)> {
    let incoming = graph.edges_directed(node, Incoming).collect::<Vec<_>>();
    if incoming.len() != 1 {
        return Err(reject(format!(
            "node {} has {} inputs; a fused event needs one source lineage",
            graph[node].node_id,
            incoming.len()
        )));
    }
    let edge = incoming[0];
    if edge.weight().edge_type != LogicalEdgeType::Forward {
        return Err(reject(format!(
            "node {} has a shuffle or join edge inside the event scope",
            graph[node].node_id
        )));
    }
    Ok((edge.source(), edge.weight().clone()))
}

fn classify(
    graph: &LogicalGraph,
    index: NodeIndex,
    admission: &dyn ScalarPlanAdmission,
) -> Result<FusionKind> {
    let node = &graph[index];
    if node.parallelism != 1 {
        return Err(reject(format!(
            "node {} has parallelism {}; state-table event ownership is singleton",
            node.node_id, node.parallelism
        )));
    }
    let operator = single_operator(graph, index)?;
    match operator.operator_name {
        OperatorName::StateTable => {
            let access = StateTableOperator::decode(operator.operator_config.as_slice())
                .map_err(|error| reject(format!("invalid state access plan: {error}")))?;
            if !access.requires_fused_serial_owner || access.event_scope_id.is_empty() {
                return Err(reject("state access has no serial event ownership marker"));
            }
            Ok(FusionKind::StateAccess)
        }
        OperatorName::Projection => {
            let projection = ProjectionOperator::decode(operator.operator_config.as_slice())
                .map_err(|error| reject(format!("invalid projection plan: {error}")))?;
            let (_, incoming) = only_parent(graph, index)?;
            admission.admit_projection(
                &projection,
                &incoming.schema,
                &step_output_schema(graph, index)?,
            )?;
            Ok(FusionKind::Projection)
        }
        OperatorName::ArrowValue => {
            let value = ValuePlanOperator::decode(operator.operator_config.as_slice())
                .map_err(|error| reject(format!("invalid scalar plan: {error}")))?;
            let (_, incoming) = only_parent(graph, index)?;
            admission.admit_scalar_plan(
                &value,
                &incoming.schema,
                &step_output_schema(graph, index)?,
            )?;
            Ok(FusionKind::ScalarFilter)
        }
        other => Err(reject(format!(
            "node {} uses unsupported {:?} between related state accesses",
            node.node_id, other
        ))),
    }
}

fn state_access(graph: &LogicalGraph, index: NodeIndex) -> Result<StateTableOperator> {
    StateTableOperator::decode(single_operator(graph, index)?.operator_config.as_slice())
        .map_err(|error| reject(format!("invalid state access plan: {error}")))
}

fn step_output_schema(graph: &LogicalGraph, node: NodeIndex) -> Result<ArroyoSchemaRef> {
    let mut outgoing = graph.edges_directed(node, Outgoing);
    if let Some(first) = outgoing.next() {
        if outgoing.any(|edge| edge.weight().schema != first.weight().schema) {
            return Err(reject(format!(
                "node {} has incompatible outgoing event schemas",
                graph[node].node_id
            )));
        }
        return Ok(first.weight().schema.clone());
    }
    let operation = single_operator(graph, node)?;
    let encoded = match operation.operator_name {
        OperatorName::StateTable => state_access(graph, node)?.output_schema,
        OperatorName::Projection => {
            ProjectionOperator::decode(operation.operator_config.as_slice())
                .map_err(|error| reject(format!("invalid terminal projection: {error}")))?
                .output_schema
        }
        _ => None,
    }
    .ok_or_else(|| reject("terminal scalar node lacks output schema"))?;
    let schema: ArroyoSchema = encoded.try_into()?;
    Ok(std::sync::Arc::new(schema))
}

fn ownership_signature(access: &StateTableOperator) -> Result<(Vec<String>, Vec<DataType>)> {
    let table = access
        .table
        .as_ref()
        .ok_or_else(|| reject("state access lacks a table descriptor"))?;
    if table.parallelism != 1
        || table.partition_key.is_empty()
        || table.partition_key.len() != access.ownership_bindings.len()
        || access.ownership_bindings.iter().any(String::is_empty)
    {
        return Err(reject(
            "state access has incompatible singleton partition ownership metadata",
        ));
    }
    let schema: Schema = serde_json::from_str(&table.schema_json)
        .map_err(|error| reject(format!("invalid state-table descriptor schema: {error}")))?;
    let mut types = Vec::with_capacity(table.partition_key.len());
    for &position in &table.partition_key {
        let position = usize::try_from(position)
            .map_err(|_| reject("partition key index exceeds platform range"))?;
        let field = schema
            .fields()
            .get(position)
            .ok_or_else(|| reject("partition key index exceeds table schema"))?;
        if !table.primary_key.contains(&(position as u64)) {
            return Err(reject("partition key is not part of the primary key"));
        }
        types.push(field.data_type().clone());
    }
    Ok((access.ownership_bindings.clone(), types))
}

fn is_valid_input_boundary(
    graph: &LogicalGraph,
    boundary: NodeIndex,
    admission: &dyn ScalarPlanAdmission,
) -> Result<bool> {
    let operator = single_operator(graph, boundary)?;
    let mut current = match operator.operator_name {
        OperatorName::ConnectorSource => boundary,
        OperatorName::ExpressionWatermark => only_parent(graph, boundary)?.0,
        _ => return Ok(false),
    };
    if graph[boundary].parallelism != 1 {
        return Err(reject(
            "state-table input boundary requires parallelism one",
        ));
    }
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(current.index()) {
            return Err(reject("cycle in state-table connector lineage"));
        }
        let operation = single_operator(graph, current)?;
        if operation.operator_name == OperatorName::ConnectorSource {
            if graph[current].parallelism != 1
                || graph.edges_directed(current, Incoming).next().is_some()
            {
                return Err(reject(
                    "state-table connector source needs singleton root ownership",
                ));
            }
            return Ok(true);
        }
        if !matches!(
            operation.operator_name,
            OperatorName::ArrowValue | OperatorName::Projection
        ) {
            return Err(reject(format!(
                "unsupported {:?} before state-table watermark boundary",
                operation.operator_name
            )));
        }
        classify(graph, current, admission)?;
        current = only_parent(graph, current)?.0;
    }
}

/// Discover one fused region per source. Unrelated branches of the same source
/// remain in the graph; every related state operation and compatible scalar
/// path is executed by one owner in deterministic dependency order.
pub(crate) fn analyze(
    graph: &LogicalGraph,
    admission: &dyn ScalarPlanAdmission,
) -> Result<Vec<FusionRegion>> {
    let mut by_source: BTreeMap<usize, (String, BTreeSet<usize>)> = BTreeMap::new();
    for index in graph.node_indices() {
        if single_operator(graph, index)?.operator_name != OperatorName::StateTable {
            continue;
        }
        let access = state_access(graph, index)?;
        let mut path = BTreeSet::new();
        let mut current = index;
        loop {
            classify(graph, current, admission)?;
            path.insert(current.index());
            let (parent, _) = only_parent(graph, current)?;
            if is_valid_input_boundary(graph, parent, admission)? {
                let entry = by_source
                    .entry(parent.index())
                    .or_insert_with(|| (access.event_scope_id.clone(), BTreeSet::new()));
                if entry.0 != access.event_scope_id {
                    return Err(reject(format!(
                        "source {} has incompatible event-scope identities",
                        graph[parent].node_id
                    )));
                }
                entry.1.extend(path);
                break;
            }
            if !path.insert(parent.index()) {
                return Err(reject("cycle in state-table input lineage"));
            }
            current = parent;
        }
    }

    let mut regions = Vec::new();
    for (source_index, (scope, mut selected)) in by_source {
        let source = NodeIndex::new(source_index);
        // Sibling accesses do not see each other in the logical-plan ancestor
        // check. Recheck ownership here, across the complete source region.
        let mut ownership = None;
        let mut table_descriptors: HashMap<String, StateTableDefinition> = HashMap::new();
        for &index in &selected {
            let node = NodeIndex::new(index);
            if single_operator(graph, node)?.operator_name != OperatorName::StateTable {
                continue;
            }
            let access = state_access(graph, node)?;
            let signature = ownership_signature(&access)?;
            if ownership
                .as_ref()
                .is_some_and(|existing| existing != &signature)
            {
                return Err(reject(
                    "related state accesses have incompatible partition ownership bindings or field types",
                ));
            }
            ownership = Some(signature);
            let descriptor = access.table.expect("ownership signature required a table");
            if let Some(prior) =
                table_descriptors.insert(descriptor.table_identity.clone(), descriptor.clone())
                && prior != descriptor
            {
                return Err(reject(
                    "same state table identity has conflicting descriptors",
                ));
            }
        }
        // Add downstream scalar work and related accesses. Do not visit other
        // source branches: they can keep their existing graph path.
        let mut pending = selected.clone();
        while let Some(index) = pending.pop_first() {
            let node = NodeIndex::new(index);
            for edge in graph.edges_directed(node, Outgoing) {
                if edge.weight().edge_type != LogicalEdgeType::Forward {
                    return Err(reject(format!(
                        "node {} has a shuffle or join edge in the fused event lineage",
                        graph[node].node_id
                    )));
                }
                let target = edge.target();
                let op = single_operator(graph, target)?;
                // Every state access was walked backwards above. An unsupported
                // operator on a path to another access would already have been
                // rejected there; an unselected consumer is a downstream exit.
                // Keep its original edge/schema for the capture extractor.
                if !selected.contains(&target.index())
                    && !matches!(
                        op.operator_name,
                        OperatorName::StateTable
                            | OperatorName::Projection
                            | OperatorName::ArrowValue
                    )
                {
                    continue;
                }
                classify(graph, target, admission)?;
                let (parent, _) = only_parent(graph, target)?;
                if parent != node {
                    return Err(reject(
                        "state-table scalar branch has multiple input lineages",
                    ));
                }
                if op.operator_name == OperatorName::StateTable
                    && state_access(graph, target)?.event_scope_id != scope
                {
                    return Err(reject("related state accesses have different event scopes"));
                }
                if selected.insert(target.index()) {
                    pending.insert(target.index());
                }
            }
        }

        let mut indegree = BTreeMap::new();
        for &index in &selected {
            let (parent, _) = only_parent(graph, NodeIndex::new(index))?;
            if parent != source && !selected.contains(&parent.index()) {
                return Err(reject("state-table region has an unowned input lineage"));
            }
            indegree.insert(index, usize::from(parent != source));
        }
        let mut ready = indegree
            .iter()
            .filter_map(|(&index, &degree)| (degree == 0).then_some(index))
            .collect::<BTreeSet<_>>();
        let mut ordered = Vec::with_capacity(selected.len());
        while let Some(index) = ready.pop_first() {
            ordered.push(NodeIndex::new(index));
            for edge in graph.edges_directed(NodeIndex::new(index), Outgoing) {
                let child = edge.target().index();
                if let Some(degree) = indegree.get_mut(&child) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.insert(child);
                    }
                }
            }
        }
        if ordered.len() != selected.len() {
            return Err(reject("cycle in related state-table graph"));
        }
        let step_index = ordered
            .iter()
            .enumerate()
            .map(|(step, node)| (node.index(), step))
            .collect::<HashMap<_, _>>();
        let mut captures = Vec::new();
        let mut steps = Vec::with_capacity(ordered.len());
        let mut prior_accesses_by_table: HashMap<String, Vec<(NodeIndex, bool)>> = HashMap::new();
        for node in ordered {
            let (parent, _) = only_parent(graph, node)?;
            let operator = single_operator(graph, node)?.clone();
            let kind = classify(graph, node, admission)?;
            if kind == FusionKind::StateAccess {
                let access = state_access(graph, node)?;
                let mutation = access.lookup_join_type.is_none();
                let table = access
                    .table
                    .ok_or_else(|| reject("state access lacks a table descriptor"))?
                    .table_identity;
                let prior = prior_accesses_by_table.entry(table.clone()).or_default();
                for &(earlier, earlier_mutation) in prior.iter() {
                    if (mutation || earlier_mutation)
                        && !is_ancestor(graph, earlier, node, &selected)?
                    {
                        return Err(reject(format!(
                            "unordered read/write effects on state table {table}; add an explicit dependency"
                        )));
                    }
                }
                prior.push((node, mutation));
            }
            let mut exits: BTreeMap<usize, (NodeIndex, ArroyoSchemaRef)> = BTreeMap::new();
            for edge in graph.edges_directed(node, Outgoing) {
                if !selected.contains(&edge.target().index())
                    && exits
                        .insert(
                            edge.target().index(),
                            (edge.target(), edge.weight().schema.clone()),
                        )
                        .is_some()
                {
                    return Err(reject(
                        "duplicate outgoing branch edge in state-table event scope",
                    ));
                }
            }
            let mut branch_captures = Vec::new();
            if !exits.is_empty() {
                let schema = exits.values().next().unwrap().1.clone();
                if exits.values().any(|(_, candidate)| *candidate != schema) {
                    return Err(reject(format!(
                        "node {} fans out with incompatible schemas",
                        graph[node].node_id
                    )));
                }
                branch_captures.push(captures.len());
                captures.push(FusionCapture {
                    producer: node,
                    consumers: exits.values().map(|(target, _)| *target).collect(),
                    schema,
                });
            }
            steps.push(FusionStep {
                node,
                parent_step: (parent != source).then(|| step_index[&parent.index()]),
                kind,
                operator,
                output_schema: step_output_schema(graph, node)?,
                captures: branch_captures,
            });
        }
        regions.push(FusionRegion {
            source,
            event_scope_id: scope,
            steps,
            captures,
        });
    }
    let mut table_owners = HashMap::<String, NodeIndex>::new();
    for region in &regions {
        for step in &region.steps {
            if step.kind != FusionKind::StateAccess {
                continue;
            }
            let table = state_access(graph, step.node)?
                .table
                .ok_or_else(|| reject("state access lacks table definition"))?
                .table_identity;
            if let Some(prior_source) = table_owners.insert(table.clone(), region.source)
                && prior_source != region.source
            {
                return Err(reject(format!(
                    "state table {table} is accessed from multiple source owners; use one ordered event source"
                )));
            }
        }
    }
    Ok(regions)
}

fn is_ancestor(
    graph: &LogicalGraph,
    ancestor: NodeIndex,
    mut node: NodeIndex,
    selected: &BTreeSet<usize>,
) -> Result<bool> {
    while selected.contains(&node.index()) {
        let (parent, _) = only_parent(graph, node)?;
        if parent == ancestor {
            return Ok(true);
        }
        node = parent;
    }
    Ok(false)
}

/// The wire/runtime integration creates an owner and a stateless extractor for
/// each capture. `owner_edge` carries the typed capture envelope; original
/// outgoing edge schemas are preserved from each extractor to its consumers.
/// The worker must use the dedicated fused-owner and capture-extractor wire
/// contracts; standalone state accesses cannot be scheduled from this graph.
pub(crate) struct FusionLowering {
    pub owner: LogicalNode,
    pub owner_edge: LogicalEdge,
    pub extractors: Vec<LogicalNode>,
}

pub(crate) fn lower_region(
    region: &FusionRegion,
    source_edge: &LogicalEdge,
    next_node_id: &mut u32,
) -> Result<FusionLowering> {
    let mut tables = BTreeMap::new();
    let steps = region
        .steps
        .iter()
        .map(|step| {
            if step.kind == FusionKind::StateAccess {
                let access =
                    StateTableOperator::decode(step.operator.operator_config.as_slice())
                        .map_err(|error| reject(format!("invalid state access plan: {error}")))?;
                let table = access
                    .table
                    .ok_or_else(|| reject("state access lacks table descriptor"))?;
                tables.insert(table.table_identity.clone(), table);
            }
            Ok(FusedStateTableStep {
                parent_step: step
                    .parent_step
                    .map(|index| u32::try_from(index).map_err(|_| reject("too many fused steps")))
                    .transpose()?,
                kind: match step.kind {
                    FusionKind::Projection => "projection",
                    FusionKind::ScalarFilter => "value",
                    FusionKind::StateAccess => "state_access",
                }
                .into(),
                operator_config: step.operator.operator_config.clone(),
                output_schema: Some(step.output_schema.as_ref().clone().into()),
                captures: step
                    .captures
                    .iter()
                    .map(|&index| u32::try_from(index).map_err(|_| reject("too many captures")))
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let source_timestamp = source_edge
        .schema
        .schema
        .fields()
        .get(source_edge.schema.timestamp_index)
        .ok_or_else(|| reject("source event has no timestamp field"))?;
    let mut envelope_fields = region
        .captures
        .iter()
        .enumerate()
        .map(|(index, capture)| {
            arrow_schema::Field::new(
                format!("capture_{index}"),
                DataType::Struct(capture.schema.schema.fields().clone()),
                true,
            )
        })
        .collect::<Vec<_>>();
    envelope_fields.push(source_timestamp.as_ref().clone());
    let envelope = ArroyoSchema::new_unkeyed(
        std::sync::Arc::new(Schema::new(envelope_fields)),
        region.captures.len(),
    );
    let owner_config = FusedStateTableOperator {
        event_scope_id: region.event_scope_id.clone(),
        input_schema: Some(source_edge.schema.as_ref().clone().into()),
        output_schema: Some(envelope.clone().into()),
        steps,
        tables: tables.into_values().collect(),
        capture_schemas: region
            .captures
            .iter()
            .map(|capture| capture.schema.as_ref().clone().into())
            .collect(),
    };
    let owner_id = *next_node_id;
    *next_node_id = next_node_id
        .checked_add(1)
        .ok_or_else(|| reject("fused node ID overflow"))?;
    let owner = LogicalNode::single(
        owner_id,
        format!("fused_state_table_{owner_id}"),
        OperatorName::FusedStateTable,
        owner_config.encode_to_vec(),
        format!("fused state-table event owner {}", region.event_scope_id),
        1,
    );
    let extractors = region
        .captures
        .iter()
        .enumerate()
        .map(|(index, capture)| {
            let id = *next_node_id;
            *next_node_id = next_node_id
                .checked_add(1)
                .ok_or_else(|| reject("capture extractor node ID overflow"))?;
            let config = StateTableCaptureOperator {
                capture_index: u32::try_from(index)
                    .map_err(|_| reject("too many capture branches"))?,
                input_schema: Some(envelope.clone().into()),
                output_schema: Some(capture.schema.as_ref().clone().into()),
            };
            Ok(LogicalNode::single(
                id,
                format!("state_table_capture_{id}"),
                OperatorName::StateTableCapture,
                config.encode_to_vec(),
                format!("state-table capture {index}"),
                1,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FusionLowering {
        owner,
        owner_edge: LogicalEdge::project_all(LogicalEdgeType::Forward, envelope),
        extractors,
    })
}

fn validate_lowering(
    region: &FusionRegion,
    source_edge: &LogicalEdge,
    lowered: &FusionLowering,
) -> Result<()> {
    if lowered.owner.parallelism != 1
        || lowered.owner.operator_chain.len() != 1
        || lowered.owner.operator_chain.first().operator_name != OperatorName::FusedStateTable
    {
        return Err(reject(
            "fused owner needs one configured singleton operator",
        ));
    }
    let owner_config = FusedStateTableOperator::decode(
        lowered
            .owner
            .operator_chain
            .first()
            .operator_config
            .as_slice(),
    )
    .map_err(|error| reject(format!("invalid fused owner config: {error}")))?;
    let owner_input: ArroyoSchema = owner_config
        .input_schema
        .ok_or_else(|| reject("fused owner lacks input schema"))?
        .try_into()?;
    let owner_output: ArroyoSchema = owner_config
        .output_schema
        .ok_or_else(|| reject("fused owner lacks output schema"))?
        .try_into()?;
    if owner_config.event_scope_id != region.event_scope_id
        || owner_config.steps.len() != region.steps.len()
        || &owner_input != source_edge.schema.as_ref()
        || &owner_output != lowered.owner_edge.schema.as_ref()
    {
        return Err(reject(
            "fused owner wire plan differs from proven graph region",
        ));
    }
    if lowered.extractors.len() != region.captures.len() {
        return Err(reject("lowering omitted a captured branch extractor"));
    }
    let envelope = &lowered.owner_edge.schema;
    if lowered.owner_edge.edge_type != LogicalEdgeType::Forward
        || envelope.timestamp_index != region.captures.len()
        || envelope.schema.fields().len() != region.captures.len() + 1
    {
        return Err(reject("fused owner output is not a typed capture envelope"));
    }
    let source_timestamp = source_edge
        .schema
        .schema
        .fields()
        .get(source_edge.schema.timestamp_index)
        .ok_or_else(|| reject("source event has no planned timestamp"))?;
    if envelope.schema.field(envelope.timestamp_index) != source_timestamp.as_ref() {
        return Err(reject("fused envelope changed the event timestamp field"));
    }
    for (index, (capture, extractor)) in region.captures.iter().zip(&lowered.extractors).enumerate()
    {
        let field = envelope.schema.field(index);
        if field.name() != &format!("capture_{index}")
            || !field.is_nullable()
            || field.data_type() != &DataType::Struct(capture.schema.schema.fields().clone())
        {
            return Err(reject(format!(
                "capture {index} does not preserve its typed branch schema and presence bit"
            )));
        }
        if extractor.parallelism != 1 || extractor.operator_chain.len() != 1 {
            return Err(reject(
                "capture extractor must be a singleton stateless operator",
            ));
        }
        let operation = extractor.operator_chain.first();
        match operation.operator_name {
            OperatorName::StateTableCapture => {
                let config =
                    StateTableCaptureOperator::decode(operation.operator_config.as_slice())
                        .map_err(|error| reject(format!("invalid capture extractor: {error}")))?;
                let input: ArroyoSchema = config
                    .input_schema
                    .ok_or_else(|| reject("capture extractor lacks input schema"))?
                    .try_into()
                    .map_err(|error| reject(format!("invalid capture input schema: {error}")))?;
                let output: ArroyoSchema = config
                    .output_schema
                    .ok_or_else(|| reject("capture extractor lacks output schema"))?
                    .try_into()
                    .map_err(|error| reject(format!("invalid capture output schema: {error}")))?;
                if config.capture_index as usize != index
                    || &input != envelope.as_ref()
                    || &output != capture.schema.as_ref()
                {
                    return Err(reject(
                        "capture extractor index/input/output schema mismatch",
                    ));
                }
            }
            _ => return Err(reject("capture extractor has an unsupported operator type")),
        }
    }
    Ok(())
}

/// Rebuilds instead of removing nodes from a `DiGraph`, whose swap-removal
/// changes NodeIndex values and can silently miswire a later region.
pub(crate) fn rebuild(
    graph: &LogicalGraph,
    regions: &[FusionRegion],
    mut lower: impl FnMut(&FusionRegion, &LogicalEdge) -> Result<FusionLowering>,
) -> Result<LogicalGraph> {
    let mut removed = HashSet::new();
    for region in regions {
        for step in &region.steps {
            if !removed.insert(step.node.index()) {
                return Err(reject("state-table fusion regions overlap"));
            }
        }
    }
    let mut result = LogicalGraph::new();
    let mut retained = HashMap::new();
    for old in graph.node_indices() {
        if !removed.contains(&old.index()) {
            retained.insert(old.index(), result.add_node(graph[old].clone()));
        }
    }
    for edge in graph.edge_references() {
        if let (Some(&source), Some(&target)) = (
            retained.get(&edge.source().index()),
            retained.get(&edge.target().index()),
        ) {
            result.add_edge(source, target, edge.weight().clone());
        }
    }
    for region in regions {
        let source = retained
            .get(&region.source.index())
            .copied()
            .ok_or_else(|| reject("fused source was removed by another region"))?;
        let first = region
            .steps
            .iter()
            .find(|step| step.parent_step.is_none())
            .ok_or_else(|| reject("fused region has no source child"))?;
        let (_, source_edge) = only_parent(graph, first.node)?;
        for step in region
            .steps
            .iter()
            .filter(|step| step.parent_step.is_none())
        {
            let (_, edge) = only_parent(graph, step.node)?;
            if edge.schema != source_edge.schema {
                return Err(reject("source fanout has incompatible event schemas"));
            }
        }
        let lowered = lower(region, &source_edge)?;
        validate_lowering(region, &source_edge, &lowered)?;
        let owner = result.add_node(lowered.owner);
        result.add_edge(source, owner, source_edge);
        for (capture, extractor) in region.captures.iter().zip(lowered.extractors) {
            let output = result.add_node(extractor);
            result.add_edge(owner, output, lowered.owner_edge.clone());
            for &consumer in &capture.consumers {
                let target = retained
                    .get(&consumer.index())
                    .copied()
                    .ok_or_else(|| reject("capture consumer is inside another fused region"))?;
                let edge = graph
                    .edges_connecting(capture.producer, consumer)
                    .next()
                    .ok_or_else(|| reject("captured branch lost its original edge"))?
                    .weight()
                    .clone();
                result.add_edge(output, target, edge);
            }
        }
    }
    let mut ids = HashSet::new();
    if result.node_weights().any(|node| !ids.insert(node.node_id)) {
        return Err(reject("fused lowering produced duplicate logical node IDs"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_schema::{Field, TimeUnit};

    struct AdmitTestScalar;
    impl ScalarPlanAdmission for AdmitTestScalar {
        fn admit_projection(
            &self,
            _: &ProjectionOperator,
            _: &ArroyoSchemaRef,
            _: &ArroyoSchemaRef,
        ) -> Result<()> {
            Ok(())
        }
        fn admit_scalar_plan(
            &self,
            _: &ValuePlanOperator,
            _: &ArroyoSchemaRef,
            _: &ArroyoSchemaRef,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn schema() -> ArroyoSchema {
        ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),
            1,
        )
    }

    fn node(id: u32, name: OperatorName, config: Vec<u8>) -> LogicalNode {
        LogicalNode::single(
            id,
            format!("op_{id}"),
            name,
            config,
            format!("node {id}"),
            1,
        )
    }

    fn access(id: u32, table: &str, lookup: bool) -> LogicalNode {
        let config = StateTableOperator {
            table: Some(StateTableDefinition {
                name: table.into(),
                table_identity: table.into(),
                schema_identity: format!("{table}-v1"),
                schema_json: serde_json::to_string(schema().schema.as_ref()).unwrap(),
                primary_key: vec![0],
                partition_key: vec![0],
                parallelism: 1,
            }),
            event_scope_id: "one-source".into(),
            requires_fused_serial_owner: true,
            lookup_join_type: lookup.then_some("left".into()),
            ownership_bindings: vec!["event.key".into()],
            ..Default::default()
        };
        node(id, OperatorName::StateTable, config.encode_to_vec())
    }

    fn projection(id: u32) -> LogicalNode {
        let config = ProjectionOperator {
            name: format!("projection_{id}"),
            input_schema: Some(schema().into()),
            output_schema: Some(schema().into()),
            exprs: vec![],
        };
        node(id, OperatorName::Projection, config.encode_to_vec())
    }

    fn forward(graph: &mut LogicalGraph, source: NodeIndex, target: NodeIndex) {
        graph.add_edge(
            source,
            target,
            LogicalEdge::project_all(LogicalEdgeType::Forward, schema()),
        );
    }

    #[test]
    fn structural_capture_fanout_rebuilds_without_index_swaps() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", false));
        let projection = graph.add_node(projection(2));
        let second = graph.add_node(access(3, "ledger", false));
        let first_sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        let second_sink = graph.add_node(node(5, OperatorName::ConnectorSink, vec![]));
        let third_sink = graph.add_node(node(6, OperatorName::ConnectorSink, vec![]));
        let unrelated_sink = graph.add_node(node(7, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, first);
        forward(&mut graph, first, projection);
        forward(&mut graph, projection, second);
        forward(&mut graph, first, first_sink);
        forward(&mut graph, first, second_sink);
        forward(&mut graph, second, third_sink);
        forward(&mut graph, source, unrelated_sink);

        let regions = analyze(&graph, &AdmitTestScalar).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].steps.len(), 3);
        assert_eq!(regions[0].captures.len(), 2);
        assert_eq!(regions[0].steps[0].captures, vec![0]);
        assert_eq!(regions[0].captures[0].consumers.len(), 2);
        assert_eq!(regions[0].steps[2].captures, vec![1]);
        let mut next_id = 8;
        let rebuilt = rebuild(&graph, &regions, |region, edge| {
            lower_region(region, edge, &mut next_id)
        })
        .unwrap();
        assert_eq!(rebuilt.node_count(), 8); // source, four sinks, owner, two extractors
        assert_eq!(rebuilt.edge_count(), 7); // source fanout, owner branches, three sink links
        assert!(!rebuilt.node_weights().any(|node| node.node_id == 1));
        assert!(rebuilt.node_weights().any(|node| node.node_id == 7));
    }

    #[test]
    fn downstream_aggregate_exit_preserves_direct_fanout_and_dependent_access() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", false));
        let projection = graph.add_node(projection(2));
        let second = graph.add_node(access(3, "ledger", false));
        let direct_sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        let key = graph.add_node(node(5, OperatorName::ArrowKey, vec![]));
        let aggregate = graph.add_node(node(6, OperatorName::UpdatingAggregate, vec![]));
        let aggregate_sink = graph.add_node(node(7, OperatorName::ConnectorSink, vec![]));
        let dependent_sink = graph.add_node(node(8, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, first);
        forward(&mut graph, first, projection);
        forward(&mut graph, first, direct_sink);
        forward(&mut graph, projection, second);
        forward(&mut graph, projection, key);
        forward(&mut graph, key, aggregate);
        forward(&mut graph, aggregate, aggregate_sink);
        forward(&mut graph, second, dependent_sink);

        let regions = analyze(&graph, &AdmitTestScalar).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].steps.len(), 3);
        assert_eq!(regions[0].captures.len(), 3);
        assert_eq!(regions[0].captures[0].consumers, vec![direct_sink]);
        assert_eq!(regions[0].captures[1].consumers, vec![key]);
        assert_eq!(regions[0].captures[2].consumers, vec![dependent_sink]);
        let mut next_id = 9;
        let rebuilt = rebuild(&graph, &regions, |region, edge| {
            lower_region(region, edge, &mut next_id)
        })
        .unwrap();
        assert_eq!(rebuilt.node_count(), 10);
        assert_eq!(rebuilt.edge_count(), 9);
        assert!(rebuilt.node_weights().any(|node| node.node_id == 5));
        assert!(rebuilt.node_weights().any(|node| node.node_id == 6));
        assert!(!rebuilt.node_weights().any(|node| node.node_id == 1));
        assert!(!rebuilt.node_weights().any(|node| node.node_id == 2));
        assert!(!rebuilt.node_weights().any(|node| node.node_id == 3));
    }

    #[test]
    fn unsupported_keying_between_related_accesses_still_rejects() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", false));
        let key = graph.add_node(node(2, OperatorName::ArrowKey, vec![]));
        let second = graph.add_node(access(3, "ledger", false));
        let sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, first);
        forward(&mut graph, first, key);
        forward(&mut graph, key, second);
        forward(&mut graph, second, sink);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("unsupported ArrowKey between related state accesses")
        );
    }

    #[test]
    fn unordered_same_table_effects_are_rejected() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", false));
        let second = graph.add_node(access(2, "inventory", true));
        let first_sink = graph.add_node(node(3, OperatorName::ConnectorSink, vec![]));
        let second_sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, first);
        forward(&mut graph, source, second);
        forward(&mut graph, first, first_sink);
        forward(&mut graph, second, second_sink);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("unordered read/write effects")
        );
    }

    #[test]
    fn same_table_from_two_sources_is_rejected_before_backend_registration() {
        let mut graph = LogicalGraph::new();
        let first_source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let second_source = graph.add_node(node(1, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(2, "inventory", false));
        let second = graph.add_node(access(3, "inventory", true));
        let first_sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        let second_sink = graph.add_node(node(5, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, first_source, first);
        forward(&mut graph, second_source, second);
        forward(&mut graph, first, first_sink);
        forward(&mut graph, second, second_sink);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("multiple source owners")
        );
    }

    #[test]
    fn source_watermark_remains_upstream_of_the_serial_owner() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let source_projection = graph.add_node(node(
            1,
            OperatorName::ArrowValue,
            ValuePlanOperator::default().encode_to_vec(),
        ));
        let watermark = graph.add_node(node(2, OperatorName::ExpressionWatermark, vec![1]));
        let state = graph.add_node(access(3, "inventory", false));
        let sink = graph.add_node(node(4, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, source_projection);
        forward(&mut graph, source_projection, watermark);
        forward(&mut graph, watermark, state);
        forward(&mut graph, state, sink);
        let regions = analyze(&graph, &AdmitTestScalar).unwrap();
        assert_eq!(regions[0].source, watermark);
        let mut next_id = 5;
        let rebuilt = rebuild(&graph, &regions, |region, edge| {
            lower_region(region, edge, &mut next_id)
        })
        .unwrap();
        assert!(rebuilt.node_weights().any(|node| node.node_id == 1));
        assert!(rebuilt.node_weights().any(|node| node.node_id == 2));
        assert!(!rebuilt.node_weights().any(|node| node.node_id == 3));
    }

    #[test]
    fn shuffle_inside_scope_is_rejected() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let state = graph.add_node(access(1, "inventory", false));
        let sink = graph.add_node(node(2, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, state);
        graph.add_edge(
            state,
            sink,
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, schema()),
        );
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("shuffle or join edge")
        );
    }

    #[test]
    fn sibling_accesses_require_the_same_partition_ownership() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", true));
        let mut incompatible = access(2, "ledger", true);
        let mut config = StateTableOperator::decode(
            incompatible
                .operator_chain
                .first()
                .operator_config
                .as_slice(),
        )
        .unwrap();
        config.ownership_bindings = vec!["event.other_key".into()];
        incompatible
            .operator_chain
            .iter_mut()
            .next()
            .unwrap()
            .0
            .operator_config = config.encode_to_vec();
        let second = graph.add_node(incompatible);
        forward(&mut graph, source, first);
        forward(&mut graph, source, second);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("incompatible partition ownership")
        );
    }

    #[test]
    fn repeated_table_identity_requires_one_descriptor() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let first = graph.add_node(access(1, "inventory", true));
        let mut incompatible = access(2, "inventory", true);
        let mut config = StateTableOperator::decode(
            incompatible
                .operator_chain
                .first()
                .operator_config
                .as_slice(),
        )
        .unwrap();
        config.table.as_mut().unwrap().schema_identity = "different-version".into();
        incompatible
            .operator_chain
            .iter_mut()
            .next()
            .unwrap()
            .0
            .operator_config = config.encode_to_vec();
        let second = graph.add_node(incompatible);
        forward(&mut graph, source, first);
        forward(&mut graph, source, second);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("conflicting descriptors")
        );
    }

    #[test]
    fn source_with_an_upstream_edge_is_not_a_source_lineage() {
        let mut graph = LogicalGraph::new();
        let upstream = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let source = graph.add_node(node(1, OperatorName::ConnectorSource, vec![]));
        let state = graph.add_node(access(2, "inventory", true));
        forward(&mut graph, upstream, source);
        forward(&mut graph, source, state);
        assert!(
            analyze(&graph, &AdmitTestScalar)
                .unwrap_err()
                .to_string()
                .contains("connector source needs singleton root ownership")
        );
    }

    #[test]
    fn lowering_rejects_an_inconsistent_owner_envelope() {
        let mut graph = LogicalGraph::new();
        let source = graph.add_node(node(0, OperatorName::ConnectorSource, vec![]));
        let state = graph.add_node(access(1, "inventory", false));
        let sink = graph.add_node(node(2, OperatorName::ConnectorSink, vec![]));
        forward(&mut graph, source, state);
        forward(&mut graph, state, sink);
        let regions = analyze(&graph, &AdmitTestScalar).unwrap();
        let error = rebuild(&graph, &regions, |region, source_edge| {
            let mut lowered = lower_region(region, source_edge, &mut 3)?;
            lowered.owner_edge = source_edge.clone();
            Ok(lowered)
        })
        .unwrap_err();
        assert!(error.to_string().contains("wire plan differs"), "{error}");
    }
}
