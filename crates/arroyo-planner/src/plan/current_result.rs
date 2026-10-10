//! Scope watermark expiry to a complete retaining-key composition. Analyze all
//! consumers without changing shared views: ordinary LAST_VALUE is historical
//! state, even when its input happens to contain finalized windows. A proven
//! composition receives private current-result ownership over shared windows.

use super::aggregate::current_result_expiry;
use crate::WindowBehavior;
use crate::extension::aggregate::AggregateExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::{CurrentResultExpiry, UpdatingAggregateExtension};
use datafusion::common::{
    Result, ScalarValue, TableReference,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan, Projection, Union};
use std::{collections::HashSet, sync::Arc};

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

#[derive(Clone, Copy)]
enum Slot<'a> {
    Null,
    Aggregate(&'a UpdatingAggregateExtension, usize),
    Window(&'a AggregateExtension, usize),
    Union(&'a Union, usize),
}

/// Only direct field renames/materialization and NULL casts preserve a slot.
/// Expressions, joins and branch filters are deliberately not inferred.
fn expression_slot<'a>(plan: &'a LogicalPlan, expr: &Expr) -> Option<Slot<'a>> {
    match unalias(expr) {
        Expr::Column(column) => slot(plan, plan.schema().index_of_column(column).ok()?),
        Expr::Literal(value, _) if value.is_null() => Some(Slot::Null),
        Expr::Cast(cast) => match unalias(&cast.expr) {
            Expr::Literal(value, _) if value.is_null() => Some(Slot::Null),
            _ => None,
        },
        _ => None,
    }
}

fn slot(plan: &LogicalPlan, index: usize) -> Option<Slot<'_>> {
    match plan {
        LogicalPlan::Projection(projection) => {
            expression_slot(&projection.input, projection.expr.get(index)?)
        }
        LogicalPlan::SubqueryAlias(alias) => slot(&alias.input, index),
        LogicalPlan::Union(union) => Some(Slot::Union(union, index)),
        LogicalPlan::Extension(extension) => {
            let node = &extension.node;
            if let Some(remote) = node.as_any().downcast_ref::<RemoteTableExtension>() {
                slot(&remote.input, index)
            } else if let Some(keys) = node.as_any().downcast_ref::<KeyCalculationExtension>() {
                // Updating aggregates use an untrimmed key projection.
                if keys.schema != *keys.input.schema() {
                    return None;
                }
                slot(&keys.input, index)
            } else if let Some(aggregate) =
                node.as_any().downcast_ref::<UpdatingAggregateExtension>()
            {
                Some(Slot::Aggregate(aggregate, index))
            } else {
                node.as_any()
                    .downcast_ref::<AggregateExtension>()
                    .map(|aggregate| Slot::Window(aggregate, index))
            }
        }
        _ => None,
    }
}

fn logical_aggregate(node: &UpdatingAggregateExtension) -> Option<&Aggregate> {
    let LogicalPlan::Aggregate(aggregate) = &node.aggregate else {
        return None;
    };
    Some(aggregate)
}

fn plain_function<'a>(expr: &'a Expr, name: &str) -> Option<&'a [Expr]> {
    let Expr::AggregateFunction(function) = unalias(expr) else {
        return None;
    };
    let params = &function.params;
    if !function.func.name().eq_ignore_ascii_case(name)
        || params.distinct
        || params.filter.is_some()
        || params
            .order_by
            .as_ref()
            .is_some_and(|order| !order.is_empty())
        || params.null_treatment.is_some()
    {
        return None;
    }
    Some(&params.args)
}

fn max_slot(node: &UpdatingAggregateExtension, index: usize) -> Option<Slot<'_>> {
    let aggregate = logical_aggregate(node)?;
    let expr = aggregate
        .aggr_expr
        .get(index.checked_sub(aggregate.group_expr.len())?)?;
    let [argument] = plain_function(expr, "max")? else {
        return None;
    };
    expression_slot(&aggregate.input, argument)
}

fn union_column(slot: Slot<'_>, union: &Union) -> Option<usize> {
    match slot {
        Slot::Union(found, index) if std::ptr::eq(found, union) => Some(index),
        _ => None,
    }
}

fn same_group_keys(
    outer: &Aggregate,
    union: &Union,
    branch: &LogicalPlan,
    contributor: &UpdatingAggregateExtension,
) -> Option<()> {
    let contributor = logical_aggregate(contributor)?;
    if outer.group_expr.is_empty() || outer.group_expr.len() != contributor.group_expr.len() {
        return None;
    }
    let mut matched = vec![false; contributor.group_expr.len()];
    for key in &outer.group_expr {
        let index = union_column(expression_slot(&outer.input, key)?, union)?;
        let Slot::Aggregate(found, key_index) = slot(branch, index)? else {
            return None;
        };
        if logical_aggregate(found)? != contributor
            || key_index >= matched.len()
            || matched[key_index]
        {
            return None;
        }
        matched[key_index] = true;
    }
    Some(())
}

fn candidate_expiry(node: &UpdatingAggregateExtension) -> Option<CurrentResultExpiry> {
    let aggregate = logical_aggregate(node)?;
    // One user aggregate plus the engine's appended event timestamp MAX.
    if aggregate.aggr_expr.len() != 2 || !node.calendar_aggregates.is_empty() {
        return None;
    }
    let LogicalPlan::Extension(key_extension) = aggregate.input.as_ref() else {
        return None;
    };
    let keys = key_extension
        .node
        .as_any()
        .downcast_ref::<KeyCalculationExtension>()?;
    let LogicalPlan::Projection(projection) = &keys.input else {
        return None;
    };
    current_result_expiry(&projection.input, &aggregate.aggr_expr[..1]).ok()?
}

/// The scalar selected by LAST_VALUE must be a finalized COUNT, with exactly
/// the same key partition as the current-result stage. A timestamp lineage on
/// its own cannot prove either the value or partition.
fn current_count(node: &UpdatingAggregateExtension, index: usize) -> Option<()> {
    let aggregate = logical_aggregate(node)?;
    if index != aggregate.group_expr.len() {
        return None;
    }
    candidate_expiry(node)?;
    let Expr::AggregateFunction(function) = unalias(&aggregate.aggr_expr[0]) else {
        return None;
    };
    let Slot::Window(window, value_index) =
        expression_slot(&aggregate.input, &function.params.args[0])?
    else {
        return None;
    };
    let LogicalPlan::Aggregate(window_aggregate) = &window.aggregate else {
        return None;
    };
    let WindowBehavior::FromOperator {
        window_index,
        is_nested: false,
        ..
    } = &window.window_behavior
    else {
        return None;
    };
    let internal_index = value_index.checked_sub(usize::from(value_index > *window_index))?;
    if value_index == *window_index {
        return None;
    }
    let count_index = internal_index.checked_sub(window_aggregate.group_expr.len())?;
    plain_function(window_aggregate.aggr_expr.get(count_index)?, "count")?;
    if aggregate.group_expr.len() != window.key_fields.len() {
        return None;
    }
    let mut matched = vec![false; window.key_fields.len()];
    for key in &aggregate.group_expr {
        let Slot::Window(found, key_index) = expression_slot(&aggregate.input, key)? else {
            return None;
        };
        if found != window || key_index == *window_index {
            return None;
        }
        let key_index = key_index.checked_sub(usize::from(key_index > *window_index))?;
        if key_index >= matched.len() || matched[key_index] {
            return None;
        }
        matched[key_index] = true;
    }
    Some(())
}

fn retaining_count(node: &UpdatingAggregateExtension, index: usize) -> Option<()> {
    let aggregate = logical_aggregate(node)?;
    if index != aggregate.group_expr.len()
        || aggregate.aggr_expr.len() != 2
        || !node.calendar_aggregates.is_empty()
    {
        return None;
    }
    plain_function(&aggregate.aggr_expr[0], "count")?;
    Some(())
}

fn zero(expr: &Expr) -> bool {
    matches!(
        unalias(expr),
        Expr::Literal(
            ScalarValue::Int64(Some(0))
                | ScalarValue::Int32(Some(0))
                | ScalarValue::UInt64(Some(0))
                | ScalarValue::UInt32(Some(0)),
            _
        )
    )
}

/// Exact existing-SQL contract: a two-branch UNION contributes retaining COUNT
/// and latest finalized COUNT in complementary slots, the outer MAX reduces
/// each slot, COALESCE supplies zero, and HAVING owns the key through retaining
/// COUNT. Extra aggregates or alternate grouping/order/filter semantics fail
/// closed rather than resetting unrelated accumulators on a watermark.
fn composition(projection: &Projection) -> Option<&UpdatingAggregateExtension> {
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        return None;
    };
    let Expr::IsNotNull(owner) = unalias(&filter.predicate) else {
        return None;
    };
    let Slot::Aggregate(outer, owner_index) = expression_slot(&filter.input, owner)? else {
        return None;
    };
    let aggregate = logical_aggregate(outer)?;
    if aggregate.aggr_expr.len() != 3 || !outer.calendar_aggregates.is_empty() {
        return None;
    }
    let Slot::Union(union, retaining_index) = max_slot(outer, owner_index)? else {
        return None;
    };
    let [first, second] = union.inputs.as_slice() else {
        return None;
    };
    let mut selected = None;
    for expr in &projection.expr {
        let Expr::ScalarFunction(function) = unalias(expr) else {
            continue;
        };
        if function.func.name() != "coalesce" {
            continue;
        }
        let [recent, fallback] = function.args.as_slice() else {
            return None;
        };
        if !zero(fallback) {
            return None;
        }
        let Slot::Aggregate(found, recent_index) = expression_slot(&filter.input, recent)? else {
            return None;
        };
        if found != outer || recent_index == owner_index {
            return None;
        }
        let recent_index = union_column(max_slot(outer, recent_index)?, union)?;
        if recent_index == retaining_index {
            return None;
        }
        let (retaining_branch, recent_branch) = match (
            slot(first, retaining_index)?,
            slot(second, retaining_index)?,
        ) {
            (Slot::Aggregate(_, _), Slot::Null) => (first, second),
            (Slot::Null, Slot::Aggregate(_, _)) => (second, first),
            _ => return None,
        };
        let Slot::Aggregate(retaining, retained_value) = slot(retaining_branch, retaining_index)?
        else {
            return None;
        };
        let Slot::Aggregate(current, current_value) = slot(recent_branch, recent_index)? else {
            return None;
        };
        if !matches!(slot(retaining_branch, recent_index), Some(Slot::Null)) {
            return None;
        }
        retaining_count(retaining, retained_value)?;
        current_count(current, current_value)?;
        same_group_keys(aggregate, union, retaining_branch, retaining)?;
        same_group_keys(aggregate, union, recent_branch, current)?;
        if selected.replace(current).is_some() {
            return None;
        }
    }
    selected
}

fn contains_current(plan: &LogicalPlan, current: &UpdatingAggregateExtension) -> bool {
    if let LogicalPlan::Extension(extension) = plan {
        if extension
            .node
            .as_any()
            .downcast_ref::<UpdatingAggregateExtension>()
            == Some(current)
        {
            return true;
        }
    }
    plan.inputs()
        .into_iter()
        .any(|input| contains_current(input, current))
}

fn remote_names(plan: &LogicalPlan, names: &mut HashSet<TableReference>) {
    if let LogicalPlan::Extension(extension) = plan {
        if let Some(remote) = extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
        {
            names.insert(remote.name.clone());
        }
    }
    for input in plan.inputs() {
        remote_names(input, names);
    }
}

/// Named remote nodes are the graph's sharing boundary. Give the selected
/// current-result path private names, including any intermediate UNION/view
/// materialization, while keeping its finalized window input shared. Reusing
/// the original names would let whichever sink is visited first choose expiry
/// semantics for an ordinary consumer of the same LAST_VALUE view.
fn specialize(
    plan: LogicalPlan,
    current: &UpdatingAggregateExtension,
    names: &mut HashSet<TableReference>,
    next_name: &mut usize,
) -> Result<LogicalPlan> {
    plan.transform_down(|plan| {
        let LogicalPlan::Extension(extension) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if let Some(node) = extension
            .node
            .as_any()
            .downcast_ref::<UpdatingAggregateExtension>()
        {
            if node == current {
                let mut node = node.clone();
                node.event_time_expiry = candidate_expiry(&node);
                return Ok(Transformed::new(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(node),
                    }),
                    true,
                    TreeNodeRecursion::Continue,
                ));
            }
        }
        if let Some(remote) = extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
        {
            if remote.materialize && contains_current(&remote.input, current) {
                let mut remote = remote.clone();
                loop {
                    let name =
                        TableReference::bare(format!("__arroyo_current_result_{}", *next_name));
                    *next_name += 1;
                    if names.insert(name.clone()) {
                        remote.name = name;
                        break;
                    }
                }
                return Ok(Transformed::new(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(remote),
                    }),
                    true,
                    TreeNodeRecursion::Continue,
                ));
            }
        }
        Ok(Transformed::no(plan))
    })
    .map(|transformed| transformed.data)
}

pub(crate) fn attach_expiry(
    plans: Vec<LogicalPlan>,
    retained_mutations: &[LogicalPlan],
) -> Result<Vec<LogicalPlan>> {
    let mut names = HashSet::new();
    for plan in plans.iter().chain(retained_mutations) {
        remote_names(plan, &mut names);
    }
    let mut next_name = 0;
    plans
        .into_iter()
        .map(|plan| {
            plan.transform_down(|plan| {
                let LogicalPlan::Projection(projection) = &plan else {
                    return Ok(Transformed::no(plan));
                };
                let Some(current) = composition(projection).cloned() else {
                    return Ok(Transformed::no(plan));
                };
                let plan = specialize(plan, &current, &mut names, &mut next_name)?;
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Continue))
            })
            .map(|transformed| transformed.data)
        })
        .collect()
}
