//! Native aggregate composition: complete internal changes, one accepted SELECT cut.
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::merge_event_branches::{self as branches, ResponseRoute};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Result, TableReference, plan_err};
use datafusion::logical_expr::{Aggregate, Extension, LogicalPlan};
use std::sync::Arc;

fn as_aggregate(plan: &LogicalPlan) -> Option<&UpdatingAggregateExtension> {
    if let LogicalPlan::Extension(extension) = plan {
        extension
            .node
            .as_any()
            .downcast_ref::<UpdatingAggregateExtension>()
    } else {
        None
    }
}
fn terminal(plan: &LogicalPlan) -> Result<&UpdatingAggregateExtension> {
    if let Some(aggregate) = as_aggregate(plan) {
        return Ok(aggregate);
    }
    if let LogicalPlan::Projection(projection) = plan {
        for (expression, field) in projection.expr.iter().zip(projection.schema.fields()) {
            if field.name() == arroyo_rpc::TIMESTAMP_FIELD
                && branches::bare_column(expression)?.name != arroyo_rpc::TIMESTAMP_FIELD
            {
                return plan_err!("accepted native SELECT must preserve the producer clock");
            }
        }
    }
    terminal(branches::scalar_input(plan)?)
}

/// Follow only copied grouping fields through branch SELECT and UNION ordinals.
/// Extra grouping columns may be nullable; the common outer tuple must be a
/// non-null declared source tuple. No expression equivalence is guessed.
fn grouped_inputs<'a>(
    plan: &'a LogicalPlan,
    indices: &[usize],
    out: &mut Vec<(&'a UpdatingAggregateExtension, Vec<usize>)>,
) -> Result<()> {
    if let Some(aggregate) = as_aggregate(plan) {
        let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
            return plan_err!("native result aggregate plan missing");
        };
        if indices
            .iter()
            .any(|index| *index >= native.group_expr.len())
        {
            return plan_err!("nested result grouping must copy native grouping columns");
        }
        out.push((aggregate, indices.to_vec()));
        return Ok(());
    }
    if let LogicalPlan::Union(union) = plan {
        for input in &union.inputs {
            grouped_inputs(input, indices, out)?;
        }
        return Ok(());
    }
    branches::validate_scalar_step(plan)?;
    let input = branches::scalar_input(plan)?;
    let next = indices
        .iter()
        .map(|index| {
            let column = match plan {
                LogicalPlan::Projection(projection) => {
                    branches::bare_column(&projection.expr[*index])?.clone()
                }
                _ => {
                    let column = branches::column_at(plan, *index);
                    if input.schema().index_of_column(&column).is_ok() {
                        column
                    } else {
                        branches::column_at(input, *index)
                    }
                }
            };
            input.schema().index_of_column(&column)
        })
        .collect::<Result<Vec<_>>>()?;
    grouped_inputs(input, &next, out)
}
fn source_keys(aggregate: &UpdatingAggregateExtension, fields: &[usize]) -> Result<Vec<usize>> {
    crate::continuous_merge::event_source::validate(aggregate)?;
    let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
        return plan_err!("native result input missing");
    };
    branches::validate_scalars(&native.input)?;
    let (source, definition) = crate::event_response_root::find_source(&native.input)?;
    let stop = LogicalPlan::Projection(source.clone());
    fields
        .iter()
        .map(|field| {
            let column = branches::bare_column(&native.group_expr[*field])?;
            let index = branches::trace_column(&native.input, column, &stop)?;
            if source.schema.field(index).is_nullable() {
                return plan_err!("nested result outer grouping requires non-null source keys");
            }
            let source_column = branches::bare_column(&source.expr[index])?;
            definition
                .table
                .fields
                .iter()
                .position(|field| field.field().name() == &source_column.name)
                .ok_or_else(|| {
                    datafusion::common::plan_datafusion_err!(
                        "nested result grouping is not a declared source field"
                    )
                })
        })
        .collect()
}

fn replace_terminal(
    plan: LogicalPlan,
    terminal: &UpdatingAggregateExtension,
    replacement: &UpdatingAggregateExtension,
) -> Result<LogicalPlan> {
    struct Rewrite<'a> {
        old: &'a UpdatingAggregateExtension,
        next: &'a UpdatingAggregateExtension,
    }
    impl TreeNodeRewriter for Rewrite<'_> {
        type Node = LogicalPlan;
        fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
            if as_aggregate(&node) == Some(self.old) {
                return Ok(Transformed::new(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(self.next.clone()),
                    }),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            branches::scalar_input(&node)?;
            Ok(Transformed::no(node))
        }
        fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
            TreeNodeRewriter::f_up(
                &mut crate::continuous_merge::event_source::EventSourceRewriter::default(),
                node,
            )
        }
    }
    Ok(plan
        .rewrite(&mut Rewrite {
            old: terminal,
            next: replacement,
        })?
        .data)
}

pub(crate) fn resolve(input: &LogicalPlan, slot: u32) -> Result<Option<LogicalPlan>> {
    // Existing JOIN/direct routes keep their established validation and kernels.
    let Ok(outer) = terminal(input) else {
        return Ok(None);
    };
    let LogicalPlan::Aggregate(native) = &outer.aggregate else {
        return plan_err!("terminal native result plan missing");
    };
    let mut count = 0;
    native.input.apply(|node| {
        if as_aggregate(node).is_some() {
            count += 1;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if count == 0 {
        return Ok(None);
    }
    if outer.ttl.is_some() || !outer.calendar_aggregates.is_empty() || native.group_expr.is_empty()
    {
        return plan_err!(
            "terminal composed result requires indefinite keyed native aggregation; calendar maintenance belongs to its source branch"
        );
    }
    crate::continuous_merge::event_source::validate_aggregate(outer)?;
    let indices = native
        .group_expr
        .iter()
        .map(|expression| {
            native
                .input
                .schema()
                .index_of_column(branches::bare_column(expression)?)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut leaves = Vec::new();
    grouped_inputs(&native.input, &indices, &mut leaves)?;
    // This initial proved topology is finite native aggregate branches feeding
    // one terminal owner. It is not an SQL function/arithmetic limitation.
    if !(1..=3).contains(&leaves.len()) {
        return plan_err!(
            "composed result supports one to three registered native aggregate branches"
        );
    }
    let expected_keys = source_keys(leaves[0].0, &leaves[0].1)?;
    let mut maintenance = 0;
    for (aggregate, fields) in &leaves {
        if source_keys(aggregate, fields)? != expected_keys {
            return plan_err!("composed branches require the same copied source grouping tuple");
        }
        if !aggregate.calendar_aggregates.is_empty() {
            maintenance += 1;
            let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
                unreachable!()
            };
            if fields.len() != native.group_expr.len() {
                return plan_err!(
                    "maintenance branch grouping must map bijectively to the terminal tuple"
                );
            }
        }
    }
    if maintenance > 1 {
        return plan_err!("composed result supports one native maintenance producer");
    }
    let source_inputs = leaves
        .iter()
        .map(|(aggregate, _)| {
            let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
                unreachable!()
            };
            native.input.as_ref()
        })
        .collect::<Vec<_>>();
    let shared = crate::event_response_root::extract_shared_source_root(source_inputs, slot)?;
    let mut rewritten = native.input.as_ref().clone();
    for ((old, key_fields), source_input) in leaves.iter().zip(shared.branches) {
        let LogicalPlan::Aggregate(old_native) = &old.aggregate else {
            unreachable!()
        };
        let mut next = (*old).clone();
        next.aggregate = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
            Arc::new(source_input),
            old_native.group_expr.clone(),
            old_native.aggr_expr.clone(),
            old_native.schema.clone(),
        )?);
        next.final_calculation = UpdatingAggregateExtension::new(
            next.aggregate.clone(),
            next.key_fields.clone(),
            next.timestamp_qualifier.clone(),
            next.ttl,
            next.calendar_aggregates.clone(),
        )?
        .final_calculation;
        next.response_route = Some(ResponseRoute {
            slot,
            operator: None,
        });
        next.response_key_fields = key_fields.clone();
        rewritten = rewritten
            .transform_down(|node| {
                if as_aggregate(&node) == Some(*old) {
                    Ok(Transformed::new(
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(next.clone()),
                        }),
                        true,
                        TreeNodeRecursion::Jump,
                    ))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
    }
    let mut next = outer.clone();
    next.aggregate = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
        Arc::new(rewritten),
        native.group_expr.clone(),
        native.aggr_expr.clone(),
        native.schema.clone(),
    )?);
    next.final_calculation = UpdatingAggregateExtension::new(
        next.aggregate.clone(),
        next.key_fields.clone(),
        next.timestamp_qualifier.clone(),
        next.ttl,
        vec![],
    )?
    .final_calculation;
    next.response_route = Some(ResponseRoute {
        slot,
        operator: None,
    });
    next.accepted_schema = Some(branches::without_meta(next.final_calculation.schema())?);
    let result = replace_terminal(input.clone(), outer, &next)?;
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(RemoteTableExtension {
            schema: result.schema().clone(),
            input: result,
            name: TableReference::bare("accepted_native_result"),
            materialize: false,
            union_schema: false,
            accepted_result: Some(ResponseRoute {
                slot,
                operator: None,
            }),
        }),
    })))
}

/// Logical cut proof, invoked again at source-scope counting and serialization.
/// Stop only at the configured terminal producer; never hide unrelated state.
pub(crate) fn accepted_source(plan: &LogicalPlan, slot: u32) -> Result<TableReference> {
    let outer = terminal(plan)?;
    if outer.response_route.map(|route| route.slot) != Some(slot)
        || outer.accepted_schema.as_ref()
            != Some(&branches::without_meta(outer.final_calculation.schema())?)
    {
        return plan_err!("accepted native result has an invalid producer/root/schema");
    }
    let LogicalPlan::Aggregate(native) = &outer.aggregate else {
        return plan_err!("accepted native result plan missing");
    };
    let mut sources = Vec::new();
    native.input.apply(|node| {
        if let Some(aggregate) = as_aggregate(node) {
            if aggregate.response_route.map(|route| route.slot) != Some(slot)
                || aggregate.accepted_schema.is_some()
                || aggregate.emit_per_event
            {
                return plan_err!("accepted native result hides an unrelated native owner");
            }
            let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
                return plan_err!("native branch plan missing");
            };
            let root = branches::branch_admission(&native.input, slot)?;
            sources.push(crate::event_response_root::source_for_response_admission(
                root,
            )?);
            return Ok(TreeNodeRecursion::Jump);
        }
        match node {
            LogicalPlan::Union(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_) => {}
            LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<RemoteTableExtension>()
                    || extension
                        .node
                        .as_any()
                        .is::<crate::extension::key_calculation::KeyCalculationExtension>() => {}
            _ => return plan_err!("accepted native result has unsupported producer ancestry"),
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if sources.is_empty() || sources.iter().any(|name| name != &sources[0]) {
        return plan_err!("accepted result branches require one physical source");
    }
    Ok(sources.remove(0))
}
