//! Bind finite SQL rowsets to immutable expression arguments before streaming rewrites.
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};

use crate::ArroyoSchemaProvider;
use crate::builder::Planner;
use crate::local_physical::{BoundValueUdf, LocalQueryUdf};

pub(crate) fn lower_expression(
    expr: Expr,
    outer: &DFSchema,
    planner: &Planner<'_>,
) -> Result<Expr> {
    expr.transform_down(|expr| {
        let (subquery, exists, negated, array) = match &expr {
            Expr::ScalarSubquery(query) => (query, false, false, false),
            Expr::Exists(query) => (&query.subquery, true, query.negated, false),
            Expr::ScalarFunction(function) if function.func.inner().as_any().is::<crate::local_physical::ArrayQueryMarker>() => {
                let Expr::ScalarSubquery(query) = &function.args[0] else { return datafusion::common::plan_err!("ARRAY(query) marker requires a query"); };
                (query, false, false, true)
            },
            _ => return Ok(Transformed::no(expr)),
        };
        crate::local_sql::validate_finite_plan(&subquery.subquery, &[Arc::new(outer.clone())])?;
        let mut columns = Vec::new();
        let mut bindings: Vec<Arc<BoundValueUdf>> = Vec::new();
        let plan = subquery.subquery.as_ref().clone().transform_up(|node| {
            let node = map_preserving_schema(node, |expr| {
                let result = expr.transform_up(|expr| {
                    if let Expr::OuterReferenceColumn(data_type, column) = expr {
                        let index = match columns.iter().position(|c| c == &column) {
                            Some(index) => index,
                            None => {
                                outer.index_of_column(&column)?;
                                let index = columns.len();
                                columns.push(column);
                                bindings.push(Arc::new(BoundValueUdf::new(index, data_type)));
                                index
                            }
                        };
                        return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                            Arc::new(ScalarUDF::new_from_shared_impl(bindings[index].clone())), vec![],
                        ))));
                    }
                    Ok(Transformed::no(expr))
                })?;
                Ok(result.data)
            })?;
            // Nested local queries bind to their own relational input. Ancestor
            // bindings are already immutable value kernels in this rowset.
            let schema = node.inputs().iter().fold(DFSchema::empty(), |mut schema, input| {
                schema.merge(input.schema());
                schema
            });
            let node = map_preserving_schema(node, |expr| lower_expression(expr, &schema, planner))?;
            Ok(Transformed::yes(normalize_local_inputs(node)?))
        })?.data;
        let result_type = if exists { arrow_schema::DataType::Boolean }
            else if array { arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new("item", subquery.subquery.schema().field(0).data_type().clone(), true))) }
            else { subquery.subquery.schema().field(0).data_type().clone() };
        let plan = plan.transform_up(expand_finite_values)?.data;
        let plan = datafusion::optimizer::Optimizer::with_rules(vec![
            Arc::new(datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate::new()),
            Arc::new(datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate::new()),
        ]).optimize(plan, &datafusion::optimizer::OptimizerContext::default(), |_, _| {})?;
        let plan = plan.rewrite_with_subqueries(&mut crate::rewriters::UnnestRewriter {})?.data;
        let plan = planner.sync_local_plan(&plan)?;
        let function = Arc::new(ScalarUDF::from(LocalQueryUdf::new(plan, bindings, result_type, exists, array)));
        let mut replacement = Expr::ScalarFunction(ScalarFunction::new_udf(function,
            columns.into_iter().map(Expr::Column).collect()));
        if negated { replacement = Expr::Not(Box::new(replacement)); }
        Ok(Transformed::yes(replacement))
    }).map(|result| result.data)
}

pub(crate) fn lower_plan(
    plan: LogicalPlan,
    provider: &ArroyoSchemaProvider,
) -> Result<LogicalPlan> {
    if !contains_queries(&plan)? {
        return Ok(plan);
    }
    let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
    config.options_mut().optimizer.repartition_aggregations = false;
    config.options_mut().optimizer.repartition_joins = false;
    config.options_mut().optimizer.repartition_sorts = false;
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let planner = Planner::new(provider, &state);
    plan.transform_up(|node| {
        let node = expand_finite_values(node)?.data;
        let schema = node
            .inputs()
            .iter()
            .fold(DFSchema::empty(), |mut schema, input| {
                schema.merge(input.schema());
                schema
            });
        let node = map_preserving_schema(node, |expr| lower_expression(expr, &schema, &planner))?;
        Ok(Transformed::yes(normalize_local_inputs(node)?))
    })
    .map(|result| result.data)
}

// Native aggregate and sort kernels are synchronous. Materialize their local
// query arguments in an ordinary input projection, whose async evaluation
// finishes before the native kernel observes the row.
fn normalize_local_inputs(node: LogicalPlan) -> Result<LogicalPlan> {
    if matches!(node, LogicalPlan::Join(_)) {
        for expression in node.expressions() {
            let mut has_local = false;
            expression.apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && function.func.inner().as_any().is::<LocalQueryUdf>()
                {
                    has_local = true;
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })?;
            if has_local {
                return datafusion::common::plan_err!(
                    "finite local subqueries in JOIN ON predicates are unsupported; use local queries in projections, filters, aggregate arguments or sort keys"
                );
            }
        }
    }
    if !matches!(node, LogicalPlan::Aggregate(_) | LogicalPlan::Sort(_)) {
        return Ok(node);
    }
    let input = node.inputs()[0].clone();
    let original_schema = node.schema().clone();
    let mut projected = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let original_count = projected.len();
    let mut expressions = Vec::new();
    for expression in node.expressions() {
        let lowered = expression
            .transform_down(|expr| {
                if let Expr::ScalarFunction(function) = &expr
                    && function.func.inner().as_any().is::<LocalQueryUdf>()
                {
                    let mut name = format!("__local_value_{}", projected.len());
                    while input.schema().field_with_unqualified_name(&name).is_ok() {
                        name.push('_');
                    }
                    projected.push(expr.alias(&name));
                    return Ok(Transformed::yes(Expr::Column(
                        datafusion::common::Column::from_name(name),
                    )));
                }
                Ok(Transformed::no(expr))
            })?
            .data;
        expressions.push(lowered);
    }
    if projected.len() == original_count {
        return Ok(node);
    }
    let projected = datafusion::logical_expr::LogicalPlanBuilder::from(input)
        .project(projected)?
        .build()?;
    let sorted = matches!(node, LogicalPlan::Sort(_));
    let node = if let LogicalPlan::Aggregate(aggregate) = node {
        let mut expressions = expressions.into_iter();
        let groups = expressions
            .by_ref()
            .take(aggregate.group_expr.len())
            .collect::<Vec<_>>();
        let aggr = expressions
            .enumerate()
            .map(|(index, expr)| {
                aggregate_alias(expr, aggregate.schema.field(groups.len() + index).name())
            })
            .collect();
        LogicalPlan::Aggregate(datafusion::logical_expr::Aggregate::try_new_with_schema(
            Arc::new(projected),
            groups,
            aggr,
            aggregate.schema,
        )?)
    } else {
        node.with_new_exprs(expressions, vec![projected])?
    };
    if sorted {
        datafusion::logical_expr::LogicalPlanBuilder::from(node)
            .project(original_schema.columns().into_iter().map(Expr::Column))?
            .build()
    } else {
        Ok(node)
    }
}

fn expand_finite_values(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Subquery(query) = node {
        return Ok(Transformed::yes(query.subquery.as_ref().clone()));
    }
    let LogicalPlan::Values(values) = node else {
        return Ok(Transformed::no(node));
    };
    let mut rows = values.values.into_iter();
    let projection = |row: Vec<Expr>| -> Result<LogicalPlan> {
        Ok(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new_with_schema(
                row.into_iter()
                    .enumerate()
                    .map(|(index, expr)| expr.alias(values.schema.field(index).name()))
                    .collect(),
                Arc::new(datafusion::logical_expr::LogicalPlanBuilder::empty(true).build()?),
                values.schema.clone(),
            )?,
        ))
    };
    let mut plan = projection(rows.next().expect("SQL VALUES has at least one row"))?;
    for row in rows {
        plan = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .union(projection(row)?)?
            .build()?;
    }
    Ok(Transformed::yes(plan))
}

fn map_preserving_schema<F>(node: LogicalPlan, mut f: F) -> Result<LogicalPlan>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    match node {
        LogicalPlan::Projection(projection) => Ok(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new_with_schema(
                projection
                    .expr
                    .into_iter()
                    .map(&mut f)
                    .collect::<Result<_>>()?,
                projection.input,
                projection.schema,
            )?,
        )),
        LogicalPlan::Aggregate(aggregate) => {
            let group_count = aggregate.group_expr.len();
            Ok(LogicalPlan::Aggregate(
                datafusion::logical_expr::Aggregate::try_new_with_schema(
                    aggregate.input,
                    aggregate
                        .group_expr
                        .into_iter()
                        .map(&mut f)
                        .collect::<Result<_>>()?,
                    aggregate
                        .aggr_expr
                        .into_iter()
                        .enumerate()
                        .map(|(index, expr)| {
                            f(expr).map(|expr| {
                                aggregate_alias(
                                    expr,
                                    aggregate.schema.field(group_count + index).name(),
                                )
                            })
                        })
                        .collect::<Result<_>>()?,
                    aggregate.schema,
                )?,
            ))
        }
        node => node
            .map_expressions(|expr| Ok(Transformed::yes(f(expr)?)))
            .map(|result| result.data),
    }
}

fn aggregate_alias(mut expr: Expr, name: &str) -> Expr {
    while let Expr::Alias(alias) = expr {
        expr = *alias.expr;
    }
    expr.alias(name)
}

pub(crate) fn contains_queries(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        for expr in node.expressions() {
            if expr.exists(|expr| Ok(matches!(expr, Expr::ScalarSubquery(_) | Expr::Exists(_)) || matches!(expr, Expr::ScalarFunction(function) if function.func.inner().as_any().is::<crate::local_physical::ArrayQueryMarker>())))? {return Ok(true);}
        }
        Ok(false)
    })
}
