//! Route-local accepted snapshots for a direct aggregate feeding MERGE.
use std::sync::Arc;

use arroyo_rpc::{SOURCE_ENVELOPE_FIELD, TIMESTAMP_FIELD, UPDATING_META_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DFSchema, Result, plan_err};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::{
    Expr, Extension, Filter, LogicalPlan, Projection, SubqueryAlias, Volatility,
};

use crate::builder::NamedNode;
use crate::extension::ArroyoExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::extension::watermark_node::WatermarkNode;
use crate::rewriters::depends_on_event_clock;

/// Before images recompute contributions; they have no stored scalar history.
/// The operation's trigger clock can also differ from the original row's clock.
fn validate_repeatable(expression: &Expr, schema: &DFSchema) -> Result<()> {
    if depends_on_event_clock(expression, schema) {
        return plan_err!("per-event correction requires repeatable source expressions");
    }
    expression.apply(|expression| {
        if matches!(expression, Expr::ScalarFunction(function)
            if function.func.signature().volatility != Volatility::Immutable)
            || matches!(expression, Expr::Column(column) if column.name == TIMESTAMP_FIELD)
        {
            return plan_err!("per-event correction requires repeatable source expressions");
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn is_timestamp_column(mut expression: &Expr) -> bool {
    while let Expr::Alias(alias) = expression {
        expression = &alias.expr;
    }
    matches!(expression, Expr::Column(column) if column.name == TIMESTAMP_FIELD)
}

fn validate(aggregate: &UpdatingAggregateExtension) -> Result<()> {
    if aggregate.ttl.is_some() || !aggregate.calendar_aggregates.is_empty() {
        return plan_err!(
            "per-event aggregate MERGE requires SET updating_ttl = NULL and no calendar aggregates"
        );
    }
    let LogicalPlan::Aggregate(plan) = &aggregate.aggregate else {
        return plan_err!("per-event aggregate MERGE requires a grouped aggregate");
    };
    if plan.group_expr.is_empty()
        || plan
            .group_expr
            .iter()
            .any(|expression| !matches!(expression, Expr::Column(_)))
    {
        return plan_err!("per-event aggregate MERGE requires keyed input");
    }
    let corrections = plan
        .input
        .schema()
        .has_column_with_unqualified_name(UPDATING_META_FIELD);
    if corrections {
        for expression in &plan.group_expr {
            validate_repeatable(expression, plan.input.schema())?;
        }
    }
    for (index, expression) in plan.aggr_expr.iter().enumerate() {
        let output = expression;
        let mut expression = expression;
        while let Expr::Alias(alias) = expression {
            expression = &alias.expr;
        }
        let Expr::AggregateFunction(function) = expression else {
            return plan_err!("per-event aggregate MERGE supports native COUNT/MAX only");
        };
        let selector = crate::grouped_selector_udaf::classify(&function.func);
        if function.params.distinct
            || (selector.is_none()
                && (!matches!(function.func.name(), "count" | "max")
                    || function.params.order_by.is_some()))
        {
            return plan_err!(
                "per-event aggregate MERGE supports native non-distinct COUNT/MAX and grouped selectors only"
            );
        }
        if let Some((_, declared)) = selector {
            let args = &function.params.args;
            if args.len() != 2
                || !matches!(&args[1], Expr::ScalarFunction(marker)
                    if marker.name() == crate::grouped_selector::ORDINAL_MARKER
                        && marker.args.is_empty()
                        && args[1].get_type(plan.input.schema())? == arrow_schema::DataType::UInt64)
            {
                return plan_err!("per-event selector lacks its native write ordinal");
            }
            let orders = function.params.order_by.as_deref().unwrap_or_default();
            if declared
                && !orders.first().is_some_and(|order| {
                    is_timestamp_column(&order.expr)
                        && order.asc
                        && !order.nulls_first
                        && matches!(
                            order.expr.get_type(plan.input.schema()),
                            Ok(arrow_schema::DataType::Timestamp(_, _))
                        )
                })
            {
                return plan_err!("per-event selector requires raw FOR timestamp ASC NULLS LAST");
            }
            if corrections {
                // Only generated lifecycle inputs are exempt: user values,
                // secondary sort expressions and FILTER must be replayable.
                validate_repeatable(&args[0], plan.input.schema())?;
                for order in orders.iter().skip(usize::from(declared)) {
                    validate_repeatable(&order.expr, plan.input.schema())?;
                }
                if let Some(filter) = &function.params.filter {
                    validate_repeatable(filter, plan.input.schema())?;
                }
            }
        }
        if corrections {
            // plan/aggregate appends this hidden clock MAX, whose retraction uses
            // row identity rather than a recomputed clock value. The worker also
            // validates its exact function/type/input/output ordinals on decode.
            let injected_clock = index + 1 == plan.aggr_expr.len()
                && matches!(output, Expr::Alias(alias) if alias.name == TIMESTAMP_FIELD)
                && function.func.name() == "max"
                && function.params.args.len() == 1
                && is_timestamp_column(&function.params.args[0])
                && function.params.filter.is_none();
            if !injected_clock && selector.is_none() {
                validate_repeatable(output, plan.input.schema())?;
            }
        }
    }
    let mut sources = 0;
    plan.input.apply(|node| {
        match node {
            LogicalPlan::Projection(projection) => {
                if corrections {
                    let source_projection = matches!(projection.input.as_ref(), LogicalPlan::Extension(extension)
                        if extension.node.name() == "DebeziumUnrollingExtension");
                    for (expression, field) in projection.expr.iter().zip(projection.schema.fields()) {
                        // The source's FOR expression establishes the operation
                        // clock; later hidden clock columns only forward it.
                        // Neither is a user aggregate contribution or predicate.
                        if field.name() == TIMESTAMP_FIELD {
                            if source_projection || is_timestamp_column(expression) {
                                continue;
                            }
                            return plan_err!(
                                "per-event correction must preserve the source trigger timestamp"
                            );
                        }
                        validate_repeatable(expression, projection.input.schema())?;
                    }
                }
            }
            LogicalPlan::Filter(filter) => {
                if corrections {
                    validate_repeatable(&filter.predicate, filter.input.schema())?;
                }
            }
            LogicalPlan::SubqueryAlias(_) => {}
            LogicalPlan::Extension(extension) => {
                let name = extension.node.name();
                let extension: &dyn ArroyoExtension = extension.node.as_ref().try_into()?;
                match extension.node_name() {
                    Some(NamedNode::Source(_)) => sources += 1,
                    _ if matches!(name, "KeyCalculationExtension" | "RemoteTableExtension" | "WatermarkNode" | "DebeziumUnrollingExtension") => {}
                    _ => return plan_err!("per-event aggregate MERGE requires one direct source with scalar projections and filters"),
                }
            }
            _ => return plan_err!("per-event aggregate MERGE does not support joins, unions or nested maintained dependencies"),
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if sources != 1 {
        return plan_err!("per-event aggregate MERGE requires exactly one direct source");
    }
    Ok(())
}

/// Private direct CDC lineage. Each physical segment is checked separately at
/// aggregate graph construction; this rewrite only retains the existing marker.
struct PreserveEnvelope;
impl TreeNodeRewriter for PreserveEnvelope {
    type Node = LogicalPlan;
    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let next = match node {
            LogicalPlan::Projection(projection) => {
                let mut expr = projection.expr;
                if let Some(index) = projection
                    .input
                    .schema()
                    .index_of_column_by_name(None, SOURCE_ENVELOPE_FIELD)
                {
                    if let Some(output) = projection
                        .schema
                        .index_of_column_by_name(None, SOURCE_ENVELOPE_FIELD)
                    {
                        let expected = Expr::Column(
                            crate::fields_with_qualifiers(projection.input.schema())[index]
                                .qualified_column(),
                        );
                        if expr[output] != expected {
                            return plan_err!(
                                "per-event correction source envelope must be an unchanged column"
                            );
                        }
                    } else {
                        expr.push(Expr::Column(
                            crate::fields_with_qualifiers(projection.input.schema())[index]
                                .qualified_column(),
                        ));
                    }
                }
                LogicalPlan::Projection(Projection::try_new(expr, projection.input)?)
            }
            LogicalPlan::SubqueryAlias(alias) => {
                LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(alias.input, alias.alias)?)
            }
            LogicalPlan::Extension(extension) => {
                if let Some(watermark) = extension.node.as_any().downcast_ref::<WatermarkNode>() {
                    let preserve = watermark
                        .input
                        .schema()
                        .has_column_with_unqualified_name(SOURCE_ENVELOPE_FIELD);
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(WatermarkNode::with_source_envelope(
                            watermark.input.clone(),
                            watermark.qualifier.clone(),
                            watermark.watermark_expression.clone(),
                            preserve,
                        )?),
                    })
                } else if let Some(remote) = extension
                    .node
                    .as_any()
                    .downcast_ref::<RemoteTableExtension>()
                {
                    let mut remote = remote.clone();
                    remote.schema = remote.input.schema().clone();
                    remote.materialize = false;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(remote),
                    })
                } else if let Some(key) = extension
                    .node
                    .as_any()
                    .downcast_ref::<KeyCalculationExtension>()
                {
                    let mut key = key.clone();
                    key.schema = key.input.schema().clone();
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(key),
                    })
                } else {
                    LogicalPlan::Extension(extension)
                }
            }
            other => other,
        };
        Ok(Transformed::yes(next))
    }
}

#[derive(Default)]
struct EventSourceRewriter {
    aggregates: usize,
}

impl TreeNodeRewriter for EventSourceRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Extension(extension) = &node
            && let Some(aggregate) = extension
                .node
                .as_any()
                .downcast_ref::<UpdatingAggregateExtension>()
        {
            validate(aggregate)?;
            self.aggregates += 1;
            let mut aggregate = aggregate.clone();
            aggregate.emit_per_event = true;
            if let LogicalPlan::Aggregate(plan) = &aggregate.aggregate
                && plan
                    .input
                    .schema()
                    .has_column_with_unqualified_name(UPDATING_META_FIELD)
            {
                let input = plan
                    .input
                    .as_ref()
                    .clone()
                    .rewrite(&mut PreserveEnvelope)?
                    .data;
                // Only the input gains an envelope column. Keep the original
                // result schema, including the planner-qualified trigger clock;
                // inferring MAX(...).alias("_timestamp") would lose its qualifier.
                aggregate.aggregate = LogicalPlan::Aggregate(
                    datafusion::logical_expr::Aggregate::try_new_with_schema(
                        Arc::new(input),
                        plan.group_expr.clone(),
                        plan.aggr_expr.clone(),
                        plan.schema.clone(),
                    )?,
                );
            }
            aggregate.final_calculation = aggregate.aggregate.clone();
            // The aggregate's internal logical plan is already rewritten.
            return Ok(Transformed::new(
                LogicalPlan::Extension(Extension {
                    node: Arc::new(aggregate),
                }),
                true,
                TreeNodeRecursion::Jump,
            ));
        }
        match &node {
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
            }
            LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<RemoteTableExtension>() => {}
            _ => {
                return plan_err!(
                    "per-event aggregate MERGE requires a direct aggregate result with scalar projections and filters"
                );
            }
        }
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let next = match node {
            LogicalPlan::Projection(projection) => {
                let expressions = projection
                    .expr
                    .into_iter()
                    .zip(projection.schema.fields())
                    .filter_map(|(expression, field)| {
                        (field.name() != UPDATING_META_FIELD).then_some(expression)
                    })
                    .collect();
                LogicalPlan::Projection(Projection::try_new(expressions, projection.input)?)
            }
            LogicalPlan::Filter(filter) => {
                LogicalPlan::Filter(Filter::try_new(filter.predicate, filter.input)?)
            }
            LogicalPlan::SubqueryAlias(alias) => {
                LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(alias.input, alias.alias)?)
            }
            LogicalPlan::Extension(extension) => {
                if let Some(remote) = extension
                    .node
                    .as_any()
                    .downcast_ref::<RemoteTableExtension>()
                {
                    let mut remote = remote.clone();
                    remote.schema = remote.input.schema().clone();
                    // An ordinary reader may still use the named, coalescing
                    // producer. This route has a different emission contract.
                    remote.materialize = false;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(remote),
                    })
                } else {
                    LogicalPlan::Extension(extension)
                }
            }
            _ => unreachable!("event-source descent validated each node"),
        };
        Ok(Transformed::yes(next))
    }
}

pub(super) fn resolve(input: LogicalPlan) -> Result<LogicalPlan> {
    if !input
        .schema()
        .has_column_with_unqualified_name(UPDATING_META_FIELD)
    {
        return Ok(input);
    }
    let mut rewriter = EventSourceRewriter::default();
    let result = input.rewrite(&mut rewriter)?.data;
    if rewriter.aggregates != 1 {
        return plan_err!("per-event aggregate MERGE requires exactly one maintained aggregate");
    }
    Ok(result)
}
