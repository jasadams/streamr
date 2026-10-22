//! Private eligibility proof for one admitted source reconverging at a native LEFT JOIN.
use std::sync::Arc;

use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, JoinType, LogicalPlan, UserDefinedLogicalNodeCore,
};

use crate::extension::join::JoinExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::extension::watermark_node::WatermarkNode;

/// The slot has query-local memoization meaning only. The builder resolves it
/// through the actual graph payload before any response config is serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct ResponseRoute {
    pub slot: u32,
    pub operator: Option<u32>,
}

pub(super) fn column_at(plan: &LogicalPlan, index: usize) -> Column {
    let (qualifier, field) = plan.schema().qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

pub(super) fn bare_column(mut expression: &Expr) -> Result<&Column> {
    while let Expr::Alias(alias) = expression {
        expression = &alias.expr;
    }
    match expression {
        Expr::Column(column) => Ok(column),
        _ => plan_err!("response grouping and JOIN keys require direct source columns"),
    }
}

pub(super) fn scalar_input(plan: &LogicalPlan) -> Result<&LogicalPlan> {
    match plan {
        LogicalPlan::Projection(projection) => Ok(&projection.input),
        LogicalPlan::Filter(filter) => Ok(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => Ok(&alias.input),
        LogicalPlan::Extension(extension) => {
            if let Some(remote) = extension
                .node
                .as_any()
                .downcast_ref::<RemoteTableExtension>()
            {
                return Ok(&remote.input);
            }
            if let Some(watermark) = extension.node.as_any().downcast_ref::<WatermarkNode>() {
                return Ok(&watermark.input);
            }
            if let Some(key) = extension
                .node
                .as_any()
                .downcast_ref::<KeyCalculationExtension>()
            {
                return Ok(&key.input);
            }
            plan_err!("response branch requires scalar work around exactly one aggregate")
        }
        _ => plan_err!("response branch requires scalar work around exactly one aggregate"),
    }
}

pub(super) fn aggregate(plan: &LogicalPlan) -> Result<&UpdatingAggregateExtension> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(aggregate) = extension
            .node
            .as_any()
            .downcast_ref::<UpdatingAggregateExtension>()
    {
        return Ok(aggregate);
    }
    validate_scalar_step(plan)?;
    aggregate(scalar_input(plan)?)
}

pub(super) fn validate_scalar_step(plan: &LogicalPlan) -> Result<()> {
    if let LogicalPlan::Projection(projection) = plan {
        for (index, expression) in projection.expr.iter().enumerate() {
            if projection.schema.field(index).name() == TIMESTAMP_FIELD {
                if bare_column(expression)?.name != TIMESTAMP_FIELD {
                    return plan_err!("response branch cannot overwrite the original event clock");
                }
            } else {
                crate::continuous_merge::event_source::validate_repeatable(
                    expression,
                    projection.input.schema(),
                )?;
            }
        }
    } else if let LogicalPlan::Filter(filter) = plan {
        // HAVING and postaggregate WHERE use the existing native delta filter:
        // both old/new images must see the same immutable predicate. No clock
        // or volatile scalar may recompute a different old contribution.
        crate::continuous_merge::event_source::validate_repeatable(
            &filter.predicate,
            filter.input.schema(),
        )?;
    }
    Ok(())
}

/// Follow only identity/alias projections. Computed, cast and reordered key
/// equivalences are deliberately outside this first admitted shape.
pub(super) fn trace_column(
    plan: &LogicalPlan,
    column: &Column,
    stop: &LogicalPlan,
) -> Result<usize> {
    let index = plan.schema().index_of_column(column)?;
    if plan == stop {
        return Ok(index);
    }
    match plan {
        LogicalPlan::Projection(projection) => trace_column(
            &projection.input,
            bare_column(&projection.expr[index])?,
            stop,
        ),
        LogicalPlan::SubqueryAlias(alias) => {
            trace_column(&alias.input, &column_at(&alias.input, index), stop)
        }
        _ => {
            let input = scalar_input(plan)?;
            let input_column = if input.schema().index_of_column(column).is_ok() {
                column.clone()
            } else {
                return plan_err!(
                    "response key lineage is not an unchanged qualified source column"
                );
            };
            trace_column(input, &input_column, stop)
        }
    }
}

pub(super) fn validate_scalars(plan: &LogicalPlan) -> Result<()> {
    if crate::event_response_root::source_projection(plan).is_some() {
        return Ok(());
    }
    // Source admission expressions have their own canonical physical proof;
    // everything above that boundary must be replayable branch-local scalar work.
    if let LogicalPlan::Projection(projection) = plan {
        for (index, expression) in projection.expr.iter().enumerate() {
            if projection.schema.field(index).name() == TIMESTAMP_FIELD {
                if bare_column(expression)?.name != TIMESTAMP_FIELD {
                    return plan_err!("response branch cannot overwrite the original event clock");
                }
            } else {
                crate::continuous_merge::event_source::validate_repeatable(
                    expression,
                    projection.input.schema(),
                )?;
            }
        }
    } else if let LogicalPlan::Filter(filter) = plan {
        crate::continuous_merge::event_source::validate_repeatable(
            &filter.predicate,
            filter.input.schema(),
        )?;
    }
    validate_scalars(scalar_input(plan)?)
}

fn grouped_source_columns(aggregate: &UpdatingAggregateExtension) -> Result<Vec<usize>> {
    // Direct calendar maintenance has unbounded quiet-key fanout across bounded
    // Data batches. It is not the two-key/four-delta-row response branch mode.
    if !aggregate.calendar_aggregates.is_empty() {
        return plan_err!("response branch requires no calendar aggregates");
    }
    crate::continuous_merge::event_source::validate(aggregate)?;
    let LogicalPlan::Aggregate(plan) = &aggregate.aggregate else {
        return plan_err!("response branch lacks its native aggregate");
    };
    for (aggregate_index, expression) in plan.aggr_expr.iter().enumerate() {
        let mut expression = expression;
        while let Expr::Alias(alias) = expression {
            expression = &alias.expr;
        }
        let Expr::AggregateFunction(function) = expression else {
            return plan_err!("response branches support COUNT/MAX only");
        };
        if !matches!(function.func.name(), "count" | "max")
            || function.params.distinct
            || function.params.order_by.is_some()
        {
            return plan_err!("response branches support native non-distinct COUNT/MAX only");
        }
        let injected_clock = aggregate_index + 1 == plan.aggr_expr.len()
            && function.func.name() == "max"
            && function.params.args.len() == 1
            && bare_column(&function.params.args[0])
                .is_ok_and(|column| column.name == TIMESTAMP_FIELD)
            && plan
                .schema
                .field(plan.group_expr.len() + aggregate_index)
                .name()
                == TIMESTAMP_FIELD;
        if !injected_clock {
            for argument in &function.params.args {
                crate::continuous_merge::event_source::validate_repeatable(
                    argument,
                    plan.input.schema(),
                )?;
            }
        }
        if let Some(filter) = &function.params.filter {
            crate::continuous_merge::event_source::validate_repeatable(
                filter,
                plan.input.schema(),
            )?;
        }
    }
    validate_scalars(&plan.input)?;
    let source = crate::event_response_root::find_source(&plan.input)?.0;
    let source_plan = LogicalPlan::Projection(source.clone());
    plan.group_expr
        .iter()
        .map(|expression| {
            let index = trace_column(&plan.input, bare_column(expression)?, &source_plan)?;
            let field = source.schema.field(index);
            if field.is_nullable()
                || field.name() == TIMESTAMP_FIELD
                || field.name() == UPDATING_META_FIELD
            {
                return plan_err!(
                    "response grouping requires non-null direct public source columns"
                );
            }
            let source_column = bare_column(&source.expr[index])?;
            let (_, source_node) = crate::event_response_root::find_source(&plan.input)?;
            source_node
                .table
                .fields
                .iter()
                .position(|field| field.field().name() == &source_column.name)
                .ok_or_else(|| {
                    datafusion::common::plan_datafusion_err!(
                        "response key is not a declared source column"
                    )
                })
        })
        .collect()
}

/// Memoization namespace only, derived from explicit validated route markers.
/// This is never a runtime root identifier or a test of relation-name equality.
pub(crate) fn memo_slot(plan: &LogicalPlan) -> Result<Option<u32>> {
    let mut slots = std::collections::BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node {
            // The public accepted JOIN output and mutation/state boundaries
            // retain their own existing capture identity. Never namespace an
            // unrelated ancestor from private work on their input side.
            if extension.node.as_any().is::<JoinExtension>()
                || extension
                    .node
                    .as_any()
                    .is::<crate::extension::state_table::StateTableAccess>()
                || extension
                    .node
                    .as_any()
                    .is::<crate::extension::state_table::StateTableScan>()
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(watermark) = extension.node.as_any().downcast_ref::<WatermarkNode>() {
                slots.extend(watermark.response_root_slot);
            }
            if let Some(aggregate) = extension
                .node
                .as_any()
                .downcast_ref::<UpdatingAggregateExtension>()
            {
                slots.extend(aggregate.response_route.map(|route| route.slot));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if slots.len() > 1 {
        return plan_err!("named scalar capture contains ambiguous response roots");
    }
    Ok(slots.pop_first())
}

pub(super) fn replace_aggregate(
    plan: LogicalPlan,
    old: &UpdatingAggregateExtension,
    input: LogicalPlan,
    slot: u32,
) -> Result<LogicalPlan> {
    let LogicalPlan::Aggregate(native) = &old.aggregate else {
        return plan_err!("missing native aggregate");
    };
    let replacement = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
        Arc::new(input),
        native.group_expr.clone(),
        native.aggr_expr.clone(),
        native.schema.clone(),
    )?);
    let mut next = old.with_exprs_and_inputs(vec![], vec![replacement])?;
    next.response_route = Some(ResponseRoute {
        slot,
        operator: None,
    });
    // Response mode retains native changelog schemas and native retract kernels.
    if next.emit_per_event {
        return plan_err!("response branch cannot also use direct append snapshot mode");
    }
    plan.transform_down(|node| {
        if let LogicalPlan::Extension(extension) = &node
            && extension
                .node
                .as_any()
                .downcast_ref::<UpdatingAggregateExtension>()
                == Some(old)
        {
            return Ok(Transformed::new(
                LogicalPlan::Extension(Extension {
                    node: Arc::new(next.clone()),
                }),
                true,
                TreeNodeRecursion::Jump,
            ));
        }
        Ok(Transformed::no(node))
    })
    .map(|result| result.data)
}

pub(crate) fn resolve_join(join: &JoinExtension, slot: u32) -> Result<JoinExtension> {
    if !join.updating || !join.left_outer || join.is_instant || join.ttl.is_some() {
        return plan_err!(
            "response reconvergence requires an indefinite native updating LEFT JOIN"
        );
    }
    let LogicalPlan::Projection(projection) = &join.rewritten_join else {
        return plan_err!("response JOIN requires the native timestamp projection");
    };
    let LogicalPlan::Join(native) = projection.input.as_ref() else {
        return plan_err!("response JOIN requires exactly two native aggregate branches");
    };
    if native.join_type != JoinType::Left || native.filter.is_some() || native.null_equals_null {
        return plan_err!(
            "response JOIN requires LEFT equality conditions without residual filters"
        );
    }
    let left = aggregate(&native.left)?;
    let right = aggregate(&native.right)?;
    let left_keys = grouped_source_columns(left)?;
    let right_keys = grouped_source_columns(right)?;
    if left_keys.is_empty() || left_keys != right_keys || native.on.len() != left_keys.len() {
        return plan_err!(
            "response branches and JOIN must use the complete same ordered grouping tuple"
        );
    }
    let left_stop = LogicalPlan::Extension(Extension {
        node: Arc::new(left.clone()),
    });
    let right_stop = LogicalPlan::Extension(Extension {
        node: Arc::new(right.clone()),
    });
    for (index, (left_on, right_on)) in native.on.iter().enumerate() {
        if trace_column(&native.left, bare_column(left_on)?, &left_stop)? != index
            || trace_column(&native.right, bare_column(right_on)?, &right_stop)? != index
            || left.schema().field(index).data_type() != right.schema().field(index).data_type()
        {
            return plan_err!(
                "response JOIN equality must bind every grouping column in its declared order and type"
            );
        }
    }
    let LogicalPlan::Aggregate(left_native) = &left.aggregate else {
        return plan_err!("missing left native aggregate");
    };
    let LogicalPlan::Aggregate(right_native) = &right.aggregate else {
        return plan_err!("missing right native aggregate");
    };
    let shared = crate::event_response_root::extract_shared_source_root(
        [left_native.input.as_ref(), right_native.input.as_ref()],
        slot,
    )?;
    if !matches!(&shared.root, LogicalPlan::Extension(extension)
        if extension.node.as_any().downcast_ref::<WatermarkNode>()
            .is_some_and(|watermark| watermark.response_root_slot == Some(slot)))
    {
        return plan_err!("response extraction did not retain the planned root descriptor");
    }
    // Retain and check the union projection, including source clock dependencies.
    if shared.source_columns.is_empty() {
        return plan_err!("response root has no admitted source columns");
    }
    let mut rewritten = native.clone();
    rewritten.left = Arc::new(replace_aggregate(
        native.left.as_ref().clone(),
        left,
        shared.branches[0].clone(),
        slot,
    )?);
    rewritten.right = Arc::new(replace_aggregate(
        native.right.as_ref().clone(),
        right,
        shared.branches[1].clone(),
        slot,
    )?);
    let mut next = join.clone();
    next.rewritten_join =
        LogicalPlan::Projection(datafusion::logical_expr::Projection::try_new_with_schema(
            projection.expr.clone(),
            Arc::new(LogicalPlan::Join(rewritten)),
            projection.schema.clone(),
        )?);
    next.response_route = Some(ResponseRoute {
        slot,
        operator: None,
    });
    next.accepted_schema = Some(without_meta(join.schema())?);
    Ok(next)
}

pub(super) fn branch_admission(plan: &LogicalPlan, slot: u32) -> Result<&WatermarkNode> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(root) = extension.node.as_any().downcast_ref::<WatermarkNode>()
    {
        if root.response_root_slot != Some(slot) {
            return plan_err!("accepted JOIN branches require the same private response root slot");
        }
        return Ok(root);
    }
    branch_admission(scalar_input(plan)?, slot)
}

pub(crate) fn common_admission_source(
    left: &WatermarkNode,
    right: &WatermarkNode,
    slot: u32,
) -> Result<datafusion::common::TableReference> {
    if left.response_root_slot != Some(slot)
        || right.response_root_slot != Some(slot)
        || left != right
    {
        return plan_err!(
            "accepted JOIN branches require one identical canonical response admission"
        );
    }
    let source = crate::event_response_root::source_for_response_admission(left)?;
    let right_source = crate::event_response_root::source_for_response_admission(right)?;
    if source != right_source {
        return plan_err!("accepted JOIN branches have different input source definitions");
    }
    Ok(source)
}

/// A logical DAG repeats the same root under both native branches. Collapse
/// those leaves only at a completely validated accepted boundary; ordinary
/// joins/unions retain StateTableAccess's existing one-source traversal.
pub(crate) fn accepted_join_source(
    join: &JoinExtension,
) -> Result<datafusion::common::TableReference> {
    let route = join.response_route.ok_or_else(|| {
        datafusion::common::plan_datafusion_err!("accepted JOIN has no private response route")
    })?;
    if !join.updating
        || !join.left_outer
        || join.is_instant
        || join.ttl.is_some()
        || join.accepted_schema.as_ref() != Some(&without_meta(join.rewritten_join.schema())?)
    {
        return plan_err!("accepted JOIN source boundary has an invalid native or public schema");
    }
    let LogicalPlan::Projection(projection) = &join.rewritten_join else {
        return plan_err!("accepted JOIN source boundary lacks its native projection");
    };
    let LogicalPlan::Join(native) = projection.input.as_ref() else {
        return plan_err!("accepted JOIN source boundary lacks its two native inputs");
    };
    if native.join_type != JoinType::Left || native.filter.is_some() || native.null_equals_null {
        return plan_err!("accepted JOIN source boundary requires native LEFT equality semantics");
    }
    let left = aggregate(&native.left)?;
    let right = aggregate(&native.right)?;
    if left.response_route != Some(route)
        || right.response_route != Some(route)
        || left.emit_per_event
        || right.emit_per_event
    {
        return plan_err!("accepted JOIN branches do not belong to the same native response route");
    }
    let left_keys = grouped_source_columns(left)?;
    let right_keys = grouped_source_columns(right)?;
    if left_keys.is_empty() || left_keys != right_keys || native.on.len() != left_keys.len() {
        return plan_err!(
            "accepted JOIN source boundary requires the complete common grouping tuple"
        );
    }
    let left_stop = LogicalPlan::Extension(Extension {
        node: Arc::new(left.clone()),
    });
    let right_stop = LogicalPlan::Extension(Extension {
        node: Arc::new(right.clone()),
    });
    for (index, (left_key, right_key)) in native.on.iter().enumerate() {
        if trace_column(&native.left, bare_column(left_key)?, &left_stop)? != index
            || trace_column(&native.right, bare_column(right_key)?, &right_stop)? != index
            || left.schema().field(index).data_type() != right.schema().field(index).data_type()
        {
            return plan_err!(
                "accepted JOIN source boundary requires the full ordered grouping equality"
            );
        }
    }
    let LogicalPlan::Aggregate(left_native) = &left.aggregate else {
        return plan_err!("accepted JOIN lacks left native aggregate");
    };
    let LogicalPlan::Aggregate(right_native) = &right.aggregate else {
        return plan_err!("accepted JOIN lacks right native aggregate");
    };
    common_admission_source(
        branch_admission(&left_native.input, route.slot)?,
        branch_admission(&right_native.input, route.slot)?,
        route.slot,
    )
}

pub(super) fn without_meta(schema: &DFSchemaRef) -> Result<DFSchemaRef> {
    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field.name() != UPDATING_META_FIELD).then_some(index))
        .collect::<Vec<_>>();
    if indices.len() + 1 != schema.fields().len() {
        return plan_err!("response JOIN requires exactly one native changelog metadata field");
    }
    let fields = crate::fields_with_qualifiers(schema);
    let kept = indices
        .into_iter()
        .map(|index| fields[index].clone())
        .collect::<Vec<_>>();
    Ok(Arc::new(crate::schema_from_df_fields_with_metadata(
        &kept,
        schema.metadata().clone(),
    )?))
}

pub(crate) fn resolve(input: LogicalPlan, slot: u32) -> Result<LogicalPlan> {
    let mut joins = 0;
    input.apply(|node| {
        if let LogicalPlan::Extension(extension) = node
            && extension.node.as_any().is::<JoinExtension>()
        {
            joins += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if joins == 0 {
        return Ok(input);
    }
    if joins != 1 {
        return plan_err!("response MERGE requires exactly one native reconvergence JOIN");
    }
    struct Rewrite {
        slot: u32,
    }
    impl datafusion::common::tree_node::TreeNodeRewriter for Rewrite {
        type Node = LogicalPlan;
        fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
            if let LogicalPlan::Extension(extension) = &node
                && let Some(join) = extension.node.as_any().downcast_ref::<JoinExtension>()
            {
                return Ok(Transformed::new(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(resolve_join(join, self.slot)?),
                    }),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            match &node {
                LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_) => {}
                LogicalPlan::Extension(extension)
                    if extension.node.as_any().is::<RemoteTableExtension>() => {}
                _ => {
                    return plan_err!("response JOIN result permits only scalar MERGE source work");
                }
            }
            Ok(Transformed::no(node))
        }
        fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
            datafusion::common::tree_node::TreeNodeRewriter::f_up(
                &mut crate::continuous_merge::event_source::EventSourceRewriter::default(),
                node,
            )
        }
    }
    Ok(input.rewrite(&mut Rewrite { slot })?.data)
}

/// Reject unsupported role cardinalities before a public Program can reach
/// worker construction. Ordinary aggregates and joins are deliberately ignored.
pub(crate) fn validate_graph(graph: &arroyo_datastream::logical::LogicalGraph) -> Result<()> {
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::ExpressionWatermarkConfig;
    use prost::Message;
    // Called before chaining: the numeric descriptor is bound to this actual
    // logical payload, never to a containing physical chain head or a name.
    for node in graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name == OperatorName::ExpressionWatermark {
                let config = ExpressionWatermarkConfig::decode(operator.operator_config.as_slice())
                    .map_err(|error| {
                        datafusion::common::plan_datafusion_err!(
                            "invalid response root config: {error}"
                        )
                    })?;
                if config
                    .response_root_operator
                    .is_some_and(|root| root != node.node_id)
                {
                    return plan_err!(
                        "response root descriptor differs from its planned logical payload"
                    );
                }
            }
        }
    }
    validate_graph_configs(graph)
}

// Config/schema and role-cardinality invariants survive chaining unchanged.
// Keep the prechaining numeric payload binding in validate_graph above.
fn validate_graph_configs(graph: &arroyo_datastream::logical::LogicalGraph) -> Result<()> {
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::{
        ExpressionWatermarkConfig, JoinOperator, UpdatingAggregateOperator,
    };
    use prost::Message;
    let mut roots = Vec::new();
    let mut aggregates = Vec::new();
    let mut joins = Vec::new();
    let mut results = Vec::new();
    let mut unions = 0usize;
    for node in graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            let root = match operator.operator_name {
                OperatorName::ExpressionWatermark => {
                    let config =
                        ExpressionWatermarkConfig::decode(operator.operator_config.as_slice())
                            .map_err(|error| {
                                datafusion::common::plan_datafusion_err!(
                                    "invalid response root config: {error}"
                                )
                            })?;
                    if let Some(root) = config.response_root_operator {
                        roots.push(root);
                    }
                    config.response_root_operator
                }
                OperatorName::UpdatingAggregate => {
                    let config =
                        UpdatingAggregateOperator::decode(operator.operator_config.as_slice())
                            .map_err(|error| {
                                datafusion::common::plan_datafusion_err!(
                                    "invalid response aggregate config: {error}"
                                )
                            })?;
                    if let Some(root) = config.response_root_operator {
                        if config.emit_per_event
                            || config.retain_indefinitely != Some(true)
                            || config.ttl_micros != 0
                        {
                            return plan_err!(
                                "response aggregate must retain its indefinite native changelog mode"
                            );
                        }
                        if config.accepted_output_schema.is_some() {
                            results.push(root);
                        } else {
                            aggregates.push(root);
                        }
                    }
                    config.response_root_operator
                }
                OperatorName::Join => {
                    let config = JoinOperator::decode(operator.operator_config.as_slice())
                        .map_err(|error| {
                            datafusion::common::plan_datafusion_err!(
                                "invalid response JOIN config: {error}"
                            )
                        })?;
                    if let Some(root) = config.response_root_operator {
                        if !config.updating || !config.left_outer || config.ttl_micros.is_some() {
                            return plan_err!(
                                "response JOIN must retain the native indefinite LEFT changelog kernel"
                            );
                        }
                        let native = config.output_schema.clone().ok_or_else(|| {
                            datafusion::common::plan_datafusion_err!(
                                "response JOIN has no native output schema"
                            )
                        })?;
                        let accepted = config.accepted_output_schema.clone().ok_or_else(|| {
                            datafusion::common::plan_datafusion_err!(
                                "response JOIN has no accepted output schema"
                            )
                        })?;
                        let native = arroyo_rpc::df::ArroyoSchema::try_from(native)?;
                        let accepted = arroyo_rpc::df::ArroyoSchema::try_from(accepted)?;
                        let fields = native
                            .schema
                            .fields()
                            .iter()
                            .filter(|field| field.name() != UPDATING_META_FIELD)
                            .cloned()
                            .collect::<Vec<_>>();
                        if fields.len() + 1 != native.schema.fields().len() {
                            return plan_err!(
                                "response JOIN native output requires one metadata field"
                            );
                        }
                        let expected = arroyo_rpc::df::ArroyoSchema::from_schema_unkeyed(
                            Arc::new(arrow_schema::Schema::new_with_metadata(
                                fields,
                                native.schema.metadata().clone(),
                            )),
                        )?;
                        if accepted != expected {
                            return plan_err!(
                                "response JOIN accepted schema must remove only native metadata"
                            );
                        }
                        joins.push(root);
                    }
                    config.response_root_operator
                }
                OperatorName::ArrowValue => {
                    let config = arroyo_rpc::grpc::api::ValuePlanOperator::decode(
                        operator.operator_config.as_slice(),
                    )
                    .map_err(|error| {
                        datafusion::common::plan_datafusion_err!(
                            "invalid native SELECT config: {error}"
                        )
                    })?;
                    if config.response_union_inputs != 0 {
                        if !(2..=3).contains(&config.response_union_inputs) || node.parallelism != 1
                        {
                            return plan_err!(
                                "finite response UNION requires two or three singleton branch inputs"
                            );
                        }
                        unions += 1;
                    }
                    config
                        .accepted_result
                        .map(|boundary| boundary.root_operator)
                }
                _ => None,
            };
            if root.is_some() && node.parallelism != 1 {
                return plan_err!("response roles require singleton execution");
            }
        }
    }
    if roots.is_empty()
        && aggregates.is_empty()
        && joins.is_empty()
        && results.is_empty()
        && unions == 0
    {
        return Ok(());
    }
    if roots.len() > 1 {
        return plan_err!("only one private response route per job is currently supported");
    }
    if roots.len() != 1
        || !((aggregates.len() == 2 && joins.len() == 1 && results.is_empty() && unions == 0)
            || ((1..=3).contains(&aggregates.len())
                && joins.is_empty()
                && results.len() == 1
                && unions == usize::from(aggregates.len() > 1)))
        || aggregates
            .iter()
            .chain(joins.iter())
            .chain(results.iter())
            .any(|root| *root != roots[0])
    {
        return plan_err!(
            "response route requires one root, two aggregate branches and one matching native JOIN"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::{
        ExpressionWatermarkConfig, JoinOperator, UpdatingAggregateOperator,
    };
    use prost::Message;
    use std::collections::BTreeSet;

    fn query(left_where: &str, right_where: &str) -> String {
        format!("SET updating_ttl = NULL;
        CREATE TABLE events (k TEXT NOT NULL, v BIGINT, eligible BOOLEAN NOT NULL,
            event_time TIMESTAMP NOT NULL,
            WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
            WITH(connector='single_file',path='/tmp/str64-planner-input.jsonl',format='json',type='source');
        CREATE VIEW counts AS SELECT k,COUNT(*) AS total FROM events {left_where} GROUP BY k;
        CREATE VIEW peaks AS SELECT k,MAX(v) AS peak FROM events {right_where} GROUP BY k;
        CREATE VIEW coherent AS SELECT counts.k,counts.total,peaks.peak
            FROM counts LEFT JOIN peaks ON counts.k=peaks.k;
        CREATE STATE TABLE totals(k TEXT PRIMARY KEY,total BIGINT,peak BIGINT) PARTITION BY k;
        CREATE VIEW captured AS MERGE INTO totals AS previous USING coherent AS incoming
            ON previous.k=incoming.k
            WHEN MATCHED THEN UPDATE SET total=incoming.total,peak=incoming.peak
            WHEN NOT MATCHED THEN INSERT(k,total,peak) VALUES(incoming.k,incoming.total,incoming.peak)
            RETURNING source AS source,old AS old,new AS new,action AS action;
        SELECT source.k,source.total,source.peak,EVENT_TIMESTAMP() AS trigger_time FROM captured;
        SELECT source.k,new.total,new.peak,EVENT_TIMESTAMP() AS trigger_time FROM captured;")
    }

    #[tokio::test]
    async fn ordinary_unmarked_join_keeps_single_source_scope_guard() {
        let sql = query("", "WHERE eligible");
        let declarations = sql.split("CREATE STATE TABLE totals").next().unwrap();
        let ordinary = format!(
            "{declarations}
            CREATE STATE TABLE extra(k TEXT PRIMARY KEY,allowed BOOLEAN) PARTITION BY k;
            SELECT coherent.k,extra.allowed FROM coherent LEFT JOIN extra ON coherent.k=extra.k;"
        );
        reject(
            &ordinary,
            "state-table access requires one input event source",
        )
        .await;
    }

    async fn compile(sql: &str) -> crate::CompiledSql {
        parse_and_get_program(
            sql,
            ArroyoSchemaProvider::new(),
            SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{error}\n{sql}"))
    }

    async fn reject(sql: &str, fragment: &str) {
        let error = parse_and_get_program(
            sql,
            ArroyoSchemaProvider::new(),
            SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains(fragment),
            "expected {fragment:?}, got {error}"
        );
    }

    fn verify(compiled: &crate::CompiledSql, expected_routes: usize) {
        let graph = &compiled.program.graph;
        let root_payloads = graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter().map(|(operator, _)| operator))
            .filter(|operator| operator.operator_name == OperatorName::ExpressionWatermark)
            .filter_map(|operator| {
                ExpressionWatermarkConfig::decode(operator.operator_config.as_slice())
                    .unwrap()
                    .response_root_operator
            })
            .collect::<Vec<_>>();
        // A chained node may contain the generator after another operator;
        // count actual generator payloads, including duplicate descriptors.
        assert_eq!(root_payloads.len(), expected_routes);
        let roots = root_payloads.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(roots.len(), expected_routes);
        let mut aggregate_roots = Vec::new();
        let mut join_roots = Vec::new();
        let mut owners = 0;
        for node in graph.node_weights() {
            for (operator, _) in node.operator_chain.iter() {
                match operator.operator_name {
                    OperatorName::UpdatingAggregate => {
                        let config =
                            UpdatingAggregateOperator::decode(operator.operator_config.as_slice())
                                .unwrap();
                        assert!(!config.emit_per_event);
                        let root = config.response_root_operator.unwrap();
                        assert!(roots.contains(&root));
                        let schema: arrow_schema::Schema =
                            serde_json::from_str(&config.final_schema.unwrap().arrow_schema)
                                .unwrap();
                        assert!(schema.index_of(UPDATING_META_FIELD).is_ok());
                        aggregate_roots.push(root);
                    }
                    OperatorName::Join => {
                        let config =
                            JoinOperator::decode(operator.operator_config.as_slice()).unwrap();
                        let root = config.response_root_operator.unwrap();
                        assert!(roots.contains(&root));
                        assert!(config.updating && config.left_outer);
                        let native: arrow_schema::Schema =
                            serde_json::from_str(&config.output_schema.unwrap().arrow_schema)
                                .unwrap();
                        let accepted_timestamp = config
                            .accepted_output_schema
                            .as_ref()
                            .unwrap()
                            .timestamp_index;
                        let accepted: arrow_schema::Schema = serde_json::from_str(
                            &config.accepted_output_schema.unwrap().arrow_schema,
                        )
                        .unwrap();
                        assert!(native.index_of(UPDATING_META_FIELD).is_ok());
                        assert!(accepted.index_of(UPDATING_META_FIELD).is_err());
                        assert_eq!(
                            accepted_timestamp as usize,
                            accepted.index_of(TIMESTAMP_FIELD).unwrap()
                        );
                        let expected = native
                            .fields()
                            .iter()
                            .filter(|field| field.name() != UPDATING_META_FIELD)
                            .cloned()
                            .collect::<Vec<_>>();
                        assert_eq!(accepted.fields(), &arrow_schema::Fields::from(expected));
                        assert_eq!(accepted.metadata(), native.metadata());
                        assert!(!config.join_plan.is_empty());
                        join_roots.push(root);
                    }
                    OperatorName::FusedStateTable => owners += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(aggregate_roots.len(), 2 * expected_routes);
        assert_eq!(join_roots.len(), expected_routes);
        assert_eq!(owners, expected_routes);
        for root in roots {
            assert_eq!(
                aggregate_roots.iter().filter(|item| **item == root).count(),
                2
            );
            assert_eq!(join_roots.iter().filter(|item| **item == root).count(), 1);
        }
        let external = graph
            .externals(petgraph::Direction::Incoming)
            .collect::<Vec<_>>();
        assert_eq!(external.len(), 1);
        assert!(graph[external[0]].operator_chain.is_source());
        let mut dfs = petgraph::visit::Dfs::new(graph, external[0]);
        let mut count = 0;
        while dfs.next(graph).is_some() {
            count += 1;
        }
        assert_eq!(count, graph.node_count());
    }

    #[tokio::test]
    async fn shared_source_two_native_branches_have_one_actual_response_root() {
        verify(&compile(&query("", "WHERE eligible")).await, 1);
    }

    #[tokio::test]
    async fn first_left_empty_and_differing_source_columns_keep_the_root_before_filters() {
        let mut compiled = compile(&query("WHERE v>=0", "WHERE eligible")).await;
        verify(&compiled, 1);
        compiled
            .program
            .optimize(&arroyo_datastream::optimizers::ChainingOptimizer {});
        // Payload root IDs survive physical chaining; names and physical head IDs
        // are intentionally not used as an identity oracle after optimization.
        let ids = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .map(|(operator, _)| {
                UpdatingAggregateOperator::decode(operator.operator_config.as_slice())
                    .unwrap()
                    .response_root_operator
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
    }

    #[tokio::test]
    async fn reordered_named_aggregate_side_preserves_native_join_key_mapping() {
        let sql = query("", "WHERE eligible").replace(
            "CREATE VIEW counts AS SELECT k,COUNT(*) AS total FROM events  GROUP BY k;",
            "CREATE VIEW counts_native AS SELECT k,COUNT(*) AS total FROM events GROUP BY k; CREATE VIEW counts AS SELECT total,k FROM counts_native;",
        );
        assert!(sql.contains("CREATE VIEW counts AS SELECT total,k FROM counts_native"));
        let compiled = compile(&sql).await;
        verify(&compiled, 1);
        let absolute = sql.replace(
            "SELECT total,k FROM counts_native",
            "SELECT abs(total) AS total,k FROM counts_native",
        );
        let compiled = compile(&absolute).await;
        verify(&compiled, 1);
    }

    #[tokio::test]
    async fn different_named_merges_are_rejected_before_unsupported_worker_construction() {
        let sql = query("", "WHERE eligible");
        let start = sql.find("CREATE STATE TABLE totals").unwrap();
        let tail = sql[start..]
            .replace("totals", "totals_second")
            .replace("captured", "captured_second");
        reject(
            &format!("{sql}\n{tail}"),
            "only one private response route per job",
        )
        .await;
    }

    #[tokio::test]
    async fn nullable_and_computed_grouping_keys_are_not_inferred_equal() {
        reject(
            &query("", "WHERE eligible").replace("k TEXT NOT NULL", "k TEXT"),
            "non-null direct public source columns",
        )
        .await;
        reject(
            &query("", "WHERE eligible")
                .replace("SELECT k,MAX(v)", "SELECT upper(k) AS k,MAX(v)")
                .replace(
                    "WHERE eligible GROUP BY k",
                    "WHERE eligible GROUP BY upper(k)",
                ),
            "per-event aggregate MERGE requires keyed input",
        )
        .await;
    }

    #[tokio::test]
    async fn branch_selector_calendar_and_volatile_contributions_remain_excluded() {
        reject(
            &query("", "WHERE eligible").replace("MAX(v)", "LAST_VALUE(v)"),
            "COUNT/MAX only",
        )
        .await;
        reject(
            &query("WHERE random()>0.5", "WHERE eligible"),
            "repeatable source expressions",
        )
        .await;
        reject(
            &query("", "WHERE eligible").replace(
                "COUNT(*)",
                "COUNT(*) FILTER (WHERE CAST(event_time AS DATE)=EVENT_DATE())",
            ),
            "no calendar aggregates",
        )
        .await;
        reject(
            &query("", "WHERE eligible").replace("SET updating_ttl = NULL;", ""),
            "updating_ttl = NULL",
        )
        .await;
    }

    #[tokio::test]
    async fn partial_or_non_key_join_cannot_masquerade_as_event_reconvergence() {
        reject(
            &query("", "WHERE eligible").replace("counts.k=peaks.k", "counts.total=peaks.peak"),
            "every grouping column",
        )
        .await;
        reject(
            &query("", "WHERE eligible").replace("LEFT JOIN", "INNER JOIN"),
            "indefinite native updating LEFT JOIN",
        )
        .await;
    }
    #[tokio::test]
    async fn different_physical_sources_and_group_tuples_are_rejected_before_workers() {
        let sql = query("", "WHERE eligible");
        let other = "CREATE TABLE other(k TEXT NOT NULL,v BIGINT,eligible BOOLEAN NOT NULL,event_time TIMESTAMP NOT NULL,WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY) WITH(connector='single_file',path='/tmp/str64-other.jsonl',format='json',type='source');";
        reject(
            &sql.replace("CREATE VIEW counts", &format!("{other} CREATE VIEW counts"))
                .replace("MAX(v) AS peak FROM events", "MAX(v) AS peak FROM other"),
            "same physical source definition",
        )
        .await;
        reject(
            &sql.replace(
                "k TEXT NOT NULL, v BIGINT",
                "k TEXT NOT NULL, alt TEXT NOT NULL, v BIGINT",
            )
            .replace("SELECT k,MAX(v)", "SELECT alt AS k,MAX(v)")
            .replace("WHERE eligible GROUP BY k", "WHERE eligible GROUP BY alt"),
            "complete same ordered grouping tuple",
        )
        .await;
    }

    #[tokio::test]
    async fn ordinary_readers_keep_native_aggregate_ownership_separate_from_response_branches() {
        let compiled = compile(&format!(
            "{} SELECT k,total FROM counts;",
            query("", "WHERE eligible")
        ))
        .await;
        let configs = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .map(|(operator, _)| {
                UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(configs.len(), 3);
        assert_eq!(
            configs
                .iter()
                .filter(|config| config.response_root_operator.is_some())
                .count(),
            2
        );
        let ordinary = configs
            .iter()
            .find(|config| config.response_root_operator.is_none())
            .unwrap();
        assert!(!ordinary.emit_per_event);
        let schema: arrow_schema::Schema =
            serde_json::from_str(&ordinary.final_schema.as_ref().unwrap().arrow_schema).unwrap();
        assert!(schema.index_of(UPDATING_META_FIELD).is_ok());
    }
    #[tokio::test]
    async fn active_joined_merge_preserves_one_shared_named_scalar_capture_per_route() {
        let sql=query("", "WHERE eligible")
            .replace("CREATE VIEW counts", "CREATE VIEW shared_values AS SELECT k,v+1 AS shifted,eligible FROM events; CREATE VIEW counts")
            .replace("COUNT(*) AS total FROM events", "COUNT(*) AS total FROM shared_values")
            .replace("MAX(v) AS peak FROM events", "MAX(shifted) AS peak FROM shared_values");
        let compiled = compile(&sql).await;
        verify(&compiled, 1);
        let names = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::ArrowValue)
            .map(|(operator, _)| {
                arroyo_rpc::grpc::api::ValuePlanOperator::decode(
                    operator.operator_config.as_slice(),
                )
                .unwrap()
                .name
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names
                .iter()
                .filter(|name| name.as_str() == "value_calculation(shared_values)")
                .count(),
            1
        );
    }
    #[test]
    fn prechain_root_binding_rejects_wrong_logical_payload() {
        let mut graph = arroyo_datastream::logical::LogicalGraph::new();
        graph.add_node(arroyo_datastream::logical::LogicalNode::single(
            3,
            "root-operator".into(),
            OperatorName::ExpressionWatermark,
            ExpressionWatermarkConfig {
                response_root_operator: Some(7),
                ..Default::default()
            }
            .encode_to_vec(),
            "root payload".into(),
            1,
        ));
        assert!(
            validate_graph(&graph)
                .expect_err("wrong prechain root must reject")
                .to_string()
                .contains("descriptor differs from its planned logical payload")
        );
    }

    #[tokio::test]
    async fn final_graph_rejects_missing_accepted_schema_and_mismatched_private_owner() {
        let compiled = compile(&query("", "WHERE eligible")).await;
        let mut missing_schema = compiled.program.graph.clone();
        for node in missing_schema.node_weights_mut() {
            for (operator, _) in node.operator_chain.iter_mut() {
                if operator.operator_name == OperatorName::Join {
                    let mut config =
                        JoinOperator::decode(operator.operator_config.as_slice()).unwrap();
                    config.accepted_output_schema = None;
                    operator.operator_config = config.encode_to_vec();
                }
            }
        }
        assert!(
            validate_graph_configs(&missing_schema)
                .unwrap_err()
                .to_string()
                .contains("no accepted output schema")
        );
        let mut wrong_owner = compiled.program.graph.clone();
        for node in wrong_owner.node_weights_mut() {
            for (operator, _) in node.operator_chain.iter_mut() {
                if operator.operator_name == OperatorName::UpdatingAggregate {
                    let mut config =
                        UpdatingAggregateOperator::decode(operator.operator_config.as_slice())
                            .unwrap();
                    config.response_root_operator = Some(u32::MAX);
                    operator.operator_config = config.encode_to_vec();
                }
            }
        }
        assert!(
            validate_graph_configs(&wrong_owner)
                .unwrap_err()
                .to_string()
                .contains("matching native JOIN")
        );
    }
    #[tokio::test]
    async fn state_read_below_a_branch_is_not_a_proven_single_source_path() {
        let sql=query("", "WHERE eligible")
            .replace("CREATE VIEW counts", "CREATE STATE TABLE extra(k TEXT PRIMARY KEY,allowed BOOLEAN) PARTITION BY k; CREATE VIEW enriched AS SELECT events.k,events.v,events.eligible FROM events LEFT JOIN extra ON events.k=extra.k; CREATE VIEW counts")
            .replace("COUNT(*) AS total FROM events", "COUNT(*) AS total FROM enriched")
            .replace("MAX(v) AS peak FROM events", "MAX(v) AS peak FROM enriched");
        reject(
            &sql,
            "one direct source with scalar projections and filters",
        )
        .await;
    }
    #[tokio::test]
    async fn postaggregate_projection_and_predicate_must_recompute_repeatably() {
        let sql = query("", "WHERE eligible");
        let native =
            "CREATE VIEW counts_native AS SELECT k,COUNT(*) AS total FROM events GROUP BY k;";
        let original = "CREATE VIEW counts AS SELECT k,COUNT(*) AS total FROM events  GROUP BY k;";
        assert!(sql.contains(original));
        reject(&sql.replace(original,&format!("{native} CREATE VIEW counts AS SELECT k,total+CAST(random()*1000 AS BIGINT) AS total FROM counts_native;")),
            "repeatable source expressions").await;
        reject(&sql.replace(original,&format!("{native} CREATE VIEW counts AS SELECT k,total FROM counts_native WHERE random()>0.5;")),
            "repeatable source expressions").await;
        reject(
            &sql.replace(
                "FROM events  GROUP BY k",
                "FROM events  GROUP BY k HAVING COUNT(*)>random()*10",
            ),
            "repeatable source expressions",
        )
        .await;
        reject(
            &sql.replace(
                "FROM events  GROUP BY k",
                "FROM events  GROUP BY k HAVING COUNT(*)>EXTRACT(EPOCH FROM EVENT_TIMESTAMP())",
            ),
            "repeatable source expressions",
        )
        .await;
    }

    #[tokio::test]
    async fn immutable_having_between_native_aggregate_and_join_remains_admitted() {
        let sql = query("", "WHERE eligible").replace(
            "FROM events  GROUP BY k",
            "FROM events  GROUP BY k HAVING COUNT(*)>=1",
        );
        verify(&compile(&sql).await, 1);
        let max_having = query("", "WHERE eligible").replace(
            "FROM events WHERE eligible GROUP BY k",
            "FROM events WHERE eligible GROUP BY k HAVING MAX(v)>0",
        );
        verify(&compile(&max_having).await, 1);
    }

    #[tokio::test]
    async fn calendar_branch_rejects_unbounded_quiet_fanout_before_capped_response_mode() {
        reject(
            &query("", "WHERE eligible").replace(
                "COUNT(*)",
                "COUNT(*) FILTER (WHERE CAST(event_time AS DATE)=EVENT_DATE())",
            ),
            "response branch requires no calendar aggregates",
        )
        .await;
    }
}
