//! Route-local accepted snapshots for an append aggregate feeding MERGE.
use std::sync::Arc;

use arroyo_rpc::UPDATING_META_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, Extension, Filter, LogicalPlan, Projection, SubqueryAlias};

use crate::builder::NamedNode;
use crate::extension::ArroyoExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;

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
        || plan
            .input
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD)
    {
        return plan_err!("per-event aggregate MERGE requires keyed append input");
    }
    for expression in &plan.aggr_expr {
        let mut expression = expression;
        while let Expr::Alias(alias) = expression {
            expression = &alias.expr;
        }
        let Expr::AggregateFunction(function) = expression else {
            return plan_err!("per-event aggregate MERGE supports native COUNT/MAX only");
        };
        if !matches!(function.func.name(), "count" | "max")
            || function.params.distinct
            || function.params.order_by.is_some()
        {
            return plan_err!(
                "per-event aggregate MERGE supports native non-distinct COUNT/MAX only"
            );
        }
    }
    let mut sources = 0;
    plan.input.apply(|node| {
        match node {
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {}
            LogicalPlan::Extension(extension) => {
                let name = extension.node.name();
                let extension: &dyn ArroyoExtension = extension.node.as_ref().try_into()?;
                match extension.node_name() {
                    Some(NamedNode::Source(_)) => sources += 1,
                    _ if matches!(name, "KeyCalculationExtension" | "RemoteTableExtension" | "WatermarkNode") => {}
                    _ => return plan_err!("per-event aggregate MERGE requires one append source with scalar projections and filters"),
                }
            }
            _ => return plan_err!("per-event aggregate MERGE does not support joins, unions or nested maintained dependencies"),
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if sources != 1 {
        return plan_err!("per-event aggregate MERGE requires exactly one append source");
    }
    Ok(())
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
