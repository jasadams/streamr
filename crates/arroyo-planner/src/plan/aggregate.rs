use crate::extension::aggregate::AggregateExtension;
use crate::extension::key_calculation::{KeyCalculationExtension, KeysOrExprs};
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::plan::WindowDetectingVisitor;
use crate::{
    ArroyoSchemaProvider, DFField, WindowBehavior, fields_with_qualifiers, find_window,
    schema_from_df_fields_with_metadata,
};
use arroyo_rpc::TIMESTAMP_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DFSchema, DataFusionError, Result, not_impl_err, plan_err};
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr;
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan};
use datafusion::prelude::col;
use itertools::Itertools;
use std::sync::Arc;
use tracing::debug;

/// Logical contract for an aggregate whose FILTER moves with the trigger date.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct CalendarAggregate {
    pub aggregate_index: usize,
    pub argument: Expr,
    pub static_filter: Option<Expr>,
    pub contribution_date: Expr,
    pub reference_date: Expr,
    pub horizon_days: u32,
}

fn unalias(mut expression: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expression {
        expression = alias.expr.as_ref();
    }
    expression
}

/// Grouped selectors use the raw declared FOR clock, not watermark progress.
/// Bind through the same lineage checks as EVENT_TIMESTAMP(), including views
/// and retained lookups. Inputs without a declaration retain DataFusion ordering.
fn order_event_time_selectors(
    expressions: &mut [Expr],
    input: &LogicalPlan,
    schema: &Arc<DFSchema>,
    provider: &ArroyoSchemaProvider,
) -> Result<Arc<DFSchema>> {
    let mut selectors = Vec::new();
    for (index, expression) in expressions.iter().enumerate() {
        if matches!(unalias(expression), Expr::AggregateFunction(function)
            if matches!(function.func.name(), "first_value" | "last_value"))
        {
            selectors.push(index);
        }
    }
    if selectors.is_empty() {
        return Ok(schema.clone());
    }
    let mut declared = false;
    input.apply(|plan| {
        // A prior aggregate owns output time, not a triggering row's raw FOR
        // clock. Do not infer a declaration from sources behind that boundary.
        if matches!(plan, LogicalPlan::Aggregate(_)) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if let LogicalPlan::Extension(extension) = plan {
            if extension.node.as_any().is::<AggregateExtension>()
                || extension.node.as_any().is::<UpdatingAggregateExtension>()
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            declared |= extension
                .node
                .as_any()
                .downcast_ref::<crate::extension::table_source::TableSourceExtension>()
                .is_some_and(|source| source.table.event_time_field.is_some());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let marker = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        provider.functions["event_timestamp"].clone(),
        vec![],
    ));
    let timestamp = if declared {
        let binding = marker
            .rewrite(&mut crate::rewriters::EventClockRewriter { input })?
            .data;
        Some(unalias(&binding).clone())
    } else {
        None
    };
    for index in selectors {
        let original_name = expressions[index].schema_name().to_string();
        let had_alias = matches!(expressions[index], Expr::Alias(_));
        let mut expression = &mut expressions[index];
        while let Expr::Alias(alias) = expression {
            expression = alias.expr.as_mut();
        }
        let Expr::AggregateFunction(function) = expression else {
            unreachable!()
        };
        let kind = if function.func.name() == "first_value" {
            crate::grouped_selector::SelectorKind::First
        } else {
            crate::grouped_selector::SelectorKind::Last
        };
        if let Some(timestamp) = &timestamp {
            function
                .params
                .order_by
                .get_or_insert_with(Vec::new)
                .insert(0, timestamp.clone().sort(true, false));
        }
        function.func = crate::grouped_selector_udaf::grouped_selector(kind, declared);
        function.params.args.push(Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                crate::grouped_selector_udaf::ordinal_marker(),
                vec![],
            ),
        ));
        if !had_alias {
            expressions[index] = expressions[index].clone().alias(original_name);
        }
    }
    // Internal provenance comes from the UDAF descriptor, never inherited
    // aggregate schema metadata whose ordinals can describe a prior stage.
    let mut metadata = schema.metadata().clone();
    metadata.retain(|key, _| !key.starts_with("streamr.event_time_selector."));
    Ok(Arc::new(schema_from_df_fields_with_metadata(
        &fields_with_qualifiers(schema),
        metadata,
    )?))
}

/// Materialize variable-width companion arguments once, using the existing key
/// projection. Native admission can then inspect the exact stored value before
/// invoking the upstream accumulator, without evaluating a volatile expression
/// again. FILTER stays on the aggregate, as in DataFusion's argument-before-
/// filter evaluation; separate aggregate occurrences never share a new field.
fn project_companion_arguments(
    aggregates: &mut [Expr],
    input: &LogicalPlan,
    projection: &mut Vec<Expr>,
    fields: &mut Vec<DFField>,
) -> Result<()> {
    use arrow_schema::DataType;
    use datafusion::functions_aggregate::{approx_distinct::ApproxDistinct, count::Count};
    use datafusion::logical_expr::ExprSchemable;
    for (aggregate_index, aggregate) in aggregates.iter_mut().enumerate() {
        let original_name = aggregate.schema_name().to_string();
        let had_alias = matches!(aggregate, Expr::Alias(_));
        let mut expression = &mut *aggregate;
        while let Expr::Alias(alias) = expression {
            expression = alias.expr.as_mut();
        }
        let Expr::AggregateFunction(function) = expression else {
            continue;
        };
        let builtin = function.func.inner().as_any();
        let hll = builtin.is::<ApproxDistinct>() && !function.params.distinct;
        let distinct_count = builtin.is::<Count>() && function.params.distinct;
        if (!hll && !distinct_count) || function.params.args.len() != 1 {
            continue;
        }
        let argument = &function.params.args[0];
        if matches!(unalias(argument), Expr::Column(_)) {
            continue;
        }
        let kind = argument.get_type(input.schema().as_ref())?;
        let variable = matches!(
            kind,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
        ) || (distinct_count && kind == DataType::BinaryView);
        if !variable {
            continue;
        }
        let base = format!("__streamr_companion_arg_v1_{aggregate_index}_0");
        let mut name = base.clone();
        let mut suffix = 0usize;
        // Check every unqualified name, including qualified source columns: a
        // new unqualified reference must never become ambiguous with either.
        while fields.iter().any(|field| field.name() == &name) {
            suffix += 1;
            name = format!("{base}_{suffix}");
        }
        let projected = argument.clone().alias(name);
        let field: DFField = projected.to_field(input.schema().as_ref())?.into();
        function.params.args[0] = Expr::Column(field.qualified_column());
        projection.push(projected);
        fields.push(field);
        if !had_alias {
            *aggregate = aggregate.clone().alias(original_name);
        }
    }
    Ok(())
}

fn event_date(expression: &Expr) -> bool {
    matches!(unalias(expression), Expr::ScalarFunction(function)
        if function.name() == "event_date" && function.args.is_empty())
}

fn horizon_lower(expression: &Expr) -> Option<u32> {
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;
    let Expr::BinaryExpr(binary) = unalias(expression) else {
        return None;
    };
    if binary.op != Operator::Minus || !event_date(&binary.left) {
        return None;
    }
    let days = match unalias(&binary.right) {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(value)), _) if value.milliseconds == 0 => {
            value.days
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(value)), _)
            if value.months == 0 && value.nanoseconds == 0 =>
        {
            value.days
        }
        _ => return None,
    };
    u32::try_from(days).ok()?.checked_add(1)
}

fn temporal_clause(expression: &Expr) -> Option<(Expr, u32)> {
    use datafusion::logical_expr::Operator;
    match unalias(expression) {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if event_date(&binary.right) {
                Some((*binary.left.clone(), 1))
            } else if event_date(&binary.left) {
                Some((*binary.right.clone(), 1))
            } else {
                None
            }
        }
        Expr::Between(between) if !between.negated && event_date(&between.high) => {
            Some((*between.expr.clone(), horizon_lower(&between.low)?))
        }
        _ => None,
    }
}

fn split_conjunction(expression: &Expr, clauses: &mut Vec<Expr>) {
    if let Expr::BinaryExpr(binary) = unalias(expression)
        && binary.op == datafusion::logical_expr::Operator::And
    {
        split_conjunction(&binary.left, clauses);
        split_conjunction(&binary.right, clauses);
        return;
    }
    clauses.push(expression.clone());
}

fn calendar_filters(
    expressions: &mut [Expr],
    input: &LogicalPlan,
) -> Result<Vec<CalendarAggregate>> {
    use crate::rewriters::{EventClockRewriter, depends_on_event_clock};
    use datafusion::logical_expr::ExprSchemable;
    let mut calendars = Vec::new();
    for (aggregate_index, expression) in expressions.iter_mut().enumerate() {
        if !depends_on_event_clock(expression, input.schema()) {
            continue;
        }
        let mut inner = expression;
        while let Expr::Alias(alias) = inner {
            inner = alias.expr.as_mut();
        }
        let Expr::AggregateFunction(function) = inner else {
            return plan_err!(
                "unsupported clock-dependent aggregate: expected COUNT/SUM calendar FILTER"
            );
        };
        if !matches!(function.func.name(), "count" | "sum")
            || function.params.distinct
            || function.params.order_by.is_some()
            || function.params.args.len() != 1
            || function
                .params
                .args
                .iter()
                .any(|argument| depends_on_event_clock(argument, input.schema()))
        {
            return plan_err!(
                "unsupported clock-dependent aggregate: calendar FILTER requires non-distinct COUNT/SUM with one clock-independent argument"
            );
        }
        let Some(filter) = &function.params.filter else {
            return plan_err!("unsupported clock-dependent aggregate: expected calendar FILTER");
        };
        let mut clauses = Vec::new();
        split_conjunction(filter, &mut clauses);
        let mut temporal = None;
        let mut static_clauses = Vec::new();
        for clause in clauses {
            if depends_on_event_clock(&clause, input.schema()) {
                let Some(candidate) = temporal_clause(&clause) else {
                    return plan_err!(
                        "unsupported clock-dependent FILTER: expected DATE = EVENT_DATE() or DATE BETWEEN EVENT_DATE() - constant DAY AND EVENT_DATE(); retained contribution time is a distinct input"
                    );
                };
                if temporal.replace(candidate).is_some() {
                    return plan_err!(
                        "unsupported clock-dependent FILTER: exactly one calendar clause is required"
                    );
                }
            } else {
                static_clauses.push(clause);
            }
        }
        let Some((contribution_date, horizon_days)) = temporal else {
            return plan_err!("unsupported clock-dependent FILTER: missing calendar clause");
        };
        if depends_on_event_clock(&contribution_date, input.schema())
            || contribution_date.get_type(input.schema().as_ref())?
                != arrow::datatypes::DataType::Date32
        {
            return plan_err!(
                "unsupported clock-dependent FILTER: contribution must be a clock-independent DATE expression"
            );
        }
        let mut reference = None;
        filter.apply(|expression| {
            if event_date(expression) {
                reference = Some(unalias(expression).clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let reference_date = reference
            .expect("temporal clause has reference date")
            .rewrite(&mut EventClockRewriter { input })?
            .data;
        let static_filter = static_clauses
            .into_iter()
            .reduce(|left, right| left.and(right));
        function.params.filter = static_filter.clone().map(Box::new);
        calendars.push(CalendarAggregate {
            aggregate_index,
            argument: function.params.args[0].clone(),
            static_filter,
            contribution_date,
            reference_date,
            horizon_days,
        });
    }
    Ok(calendars)
}

pub struct AggregateRewriter<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
}

impl AggregateRewriter<'_> {
    pub fn rewrite_non_windowed_aggregate(
        input: Arc<LogicalPlan>,
        mut key_fields: Vec<DFField>,
        group_expr: Vec<Expr>,
        mut aggr_expr: Vec<Expr>,
        schema: Arc<DFSchema>,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Transformed<LogicalPlan>> {
        let calendar_aggregates = calendar_filters(&mut aggr_expr, &input)?;
        let key_count = key_fields.len();
        key_fields.extend(fields_with_qualifiers(input.schema()));

        let mut key_projection_expressions = group_expr
            .iter()
            .zip(key_fields.iter())
            .map(|(expr, f)| expr.clone().alias(f.name().to_string()))
            .collect_vec();

        key_projection_expressions.extend(
            fields_with_qualifiers(input.schema())
                .iter()
                .map(|field| Expr::Column(field.qualified_column())),
        );

        project_companion_arguments(
            &mut aggr_expr,
            &input,
            &mut key_projection_expressions,
            &mut key_fields,
        )?;
        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);
        let key_projection =
            LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        debug!(
            "key projection fields: {:?}",
            key_projection
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        let key_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KeyCalculationExtension::new(
                key_projection,
                KeysOrExprs::Keys((0..key_count).collect()),
            )),
        });
        let Ok(timestamp_field) = key_plan
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
        else {
            return plan_err!("no timestamp field found in schema");
        };

        let timestamp_field: DFField = timestamp_field.into();
        let column = timestamp_field.qualified_column();
        aggr_expr.push(max(col(column.clone())).alias("_timestamp"));

        let mut output_schema_fields = fields_with_qualifiers(&schema);
        output_schema_fields.push(timestamp_field.clone());
        let output_schema = Arc::new(schema_from_df_fields_with_metadata(
            &output_schema_fields,
            schema.metadata().clone(),
        )?);
        let aggregate = Aggregate::try_new_with_schema(
            Arc::new(key_plan),
            group_expr,
            aggr_expr,
            output_schema,
        )?;
        debug!(
            "aggregate field names: {:?}",
            aggregate
                .schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        let updating_aggregate_extension = UpdatingAggregateExtension::new(
            LogicalPlan::Aggregate(aggregate),
            (0..key_count).collect(),
            column.relation,
            schema_provider.planning_options.ttl,
            calendar_aggregates,
        )?;
        let final_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(updating_aggregate_extension),
        });
        Ok(Transformed::yes(final_plan))
    }
}

impl TreeNodeRewriter for AggregateRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Aggregate(Aggregate {
            input,
            mut group_expr,
            mut aggr_expr,
            schema,
            ..
        }) = node
        else {
            return Ok(Transformed::no(node));
        };
        let schema =
            order_event_time_selectors(&mut aggr_expr, &input, &schema, self.schema_provider)?;
        if group_expr
            .iter()
            .any(|expression| crate::rewriters::depends_on_event_clock(expression, input.schema()))
        {
            return plan_err!(
                "unsupported clock-dependent GROUP BY: calendar FILTER requires clock-independent grouping keys"
            );
        }
        let mut window_group_expr: Vec<_> = group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| {
                find_window(expr)
                    .map(|option| option.map(|inner| (i, inner)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        if window_group_expr.len() > 1 {
            return not_impl_err!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            );
        }

        let mut key_fields: Vec<DFField> = fields_with_qualifiers(&schema)
            .iter()
            .take(group_expr.len())
            .map(|field| {
                DFField::new(
                    field.qualifier().cloned(),
                    format!("_key_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();

        let mut window_detecting_visitor = WindowDetectingVisitor::default();
        input.visit_with_subqueries(&mut window_detecting_visitor)?;

        let window = window_detecting_visitor.window;
        let window_behavior = match (window.is_some(), !window_group_expr.is_empty()) {
            (true, true) => {
                let input_window = window.unwrap();
                let (window_index, group_by_window_type) = window_group_expr.pop().unwrap();
                if group_by_window_type != input_window {
                    return Err(DataFusionError::NotImplemented(
                        "window in group by does not match input window".to_string(),
                    ));
                }
                let matching_field = window_detecting_visitor.fields.iter().next();
                match matching_field {
                    Some(field) => {
                        group_expr[window_index] = Expr::Column(field.qualified_column());
                        WindowBehavior::InData
                    }
                    None => {
                        if matches!(input_window, arroyo_datastream::WindowType::Session { .. }) {
                            return plan_err!(
                                "can't reinvoke session window in nested aggregates. Need to pass the window struct up from the source query."
                            );
                        }
                        group_expr.remove(window_index);
                        key_fields.remove(window_index);
                        let window_field = schema.qualified_field(window_index).into();
                        WindowBehavior::FromOperator {
                            window: input_window,
                            window_field,
                            window_index,
                            is_nested: true,
                        }
                    }
                }
            }
            (true, false) => WindowBehavior::InData,
            (false, true) => {
                // strip out window from group by, will be handled by operator.
                let (window_index, window_type) = window_group_expr.pop().unwrap();
                group_expr.remove(window_index);
                key_fields.remove(window_index);
                let window_field = schema.qualified_field(window_index).into();
                WindowBehavior::FromOperator {
                    window: window_type,
                    window_field,
                    window_index,
                    is_nested: false,
                }
            }
            (false, false) => {
                return Self::rewrite_non_windowed_aggregate(
                    input,
                    key_fields,
                    group_expr,
                    aggr_expr,
                    schema,
                    self.schema_provider,
                );
            }
        };

        if aggr_expr
            .iter()
            .any(|expression| crate::rewriters::depends_on_event_clock(expression, input.schema()))
        {
            return plan_err!(
                "unsupported clock-dependent window aggregate: calendar FILTER requires a maintained non-windowed aggregate"
            );
        }
        let key_count = key_fields.len();
        key_fields.extend(fields_with_qualifiers(input.schema()));

        let mut key_projection_expressions = group_expr
            .iter()
            .zip(key_fields.iter())
            .map(|(expr, f)| expr.clone().alias(f.name().to_string()))
            .collect_vec();

        key_projection_expressions.extend(
            fields_with_qualifiers(input.schema())
                .iter()
                .map(|field| Expr::Column(field.qualified_column())),
        );

        project_companion_arguments(
            &mut aggr_expr,
            &input,
            &mut key_projection_expressions,
            &mut key_fields,
        )?;
        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);
        let key_projection =
            LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        let key_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KeyCalculationExtension::new(
                key_projection,
                KeysOrExprs::Keys((0..key_count).collect()),
            )),
        });
        let mut aggregate_schema_fields = fields_with_qualifiers(&schema);
        if let WindowBehavior::FromOperator {
            window: _,
            window_field: _,
            window_index,
            is_nested: _,
        } = &window_behavior
        {
            aggregate_schema_fields.remove(*window_index);
        }
        let internal_schema = Arc::new(schema_from_df_fields_with_metadata(
            &aggregate_schema_fields,
            schema.metadata().clone(),
        )?);

        let rewritten_aggregate = Aggregate::try_new_with_schema(
            Arc::new(key_plan),
            group_expr,
            aggr_expr,
            internal_schema,
        )?;

        let aggregate_extension = AggregateExtension::new(
            window_behavior,
            LogicalPlan::Aggregate(rewritten_aggregate),
            (0..key_count).collect(),
        );
        let final_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(aggregate_extension),
        });
        // check that the windowing is correct
        WindowDetectingVisitor::get_window(&final_plan)?;
        Ok(Transformed::yes(final_plan))
    }
}

// Internal planner metadata, consumed when serializing UpdatingAggregateOperator.
// This is an output bound only: all members remain available for CDC retractions.
pub(crate) const COLLECTION_LIMIT_PREFIX: &str = "streamr.collection_output_limit.";

fn literal_index(expr: &Expr) -> Option<i64> {
    use datafusion::common::ScalarValue;
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        _ => None,
    }
}

fn prefix_slice(expr: &Expr) -> Option<(&datafusion::common::Column, u64)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.name() != "array_slice" || !(3..=4).contains(&function.args.len()) {
        return None;
    }
    let Expr::Column(column) = &function.args[0] else {
        return None;
    };
    // DuckDB/DataFusion slices use inclusive, one-based endpoints; zero is
    // also a valid beginning. Negative endpoints depend on the full length.
    if !matches!(literal_index(&function.args[1]), Some(0 | 1)) {
        return None;
    }
    if function.args.len() == 4 && literal_index(&function.args[3]) != Some(1) {
        return None;
    }
    let end = literal_index(&function.args[2])?;
    // List offsets are i32. Preserve DataFusion's invalid-index error for
    // endpoints which cannot be represented, instead of truncating first.
    (0..=i64::from(i32::MAX))
        .contains(&end)
        .then_some((column, end as u64))
}

/// Match before the bottom-up Arroyo aggregate rewrite hides the Aggregate.
/// Only direct projections qualify; other shapes retain normal materialization.
pub(crate) fn bound_collection_outputs(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(mut projection) = node else {
        return Ok(Transformed::no(node));
    };
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    };
    for expression in &aggregate.group_expr {
        if find_window(expression)?.is_some() {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        }
    }
    let mut bounds = std::collections::HashMap::new();
    let mut unbounded = std::collections::HashSet::new();
    for expression in &projection.expr {
        expression.apply(|expr| {
            if let Some((column, limit)) = prefix_slice(expr) {
                if let Ok(index) = aggregate.schema.index_of_column(column)
                    && let Some(previous) = bounds.insert(index, limit)
                    && previous != limit
                {
                    unbounded.insert(index);
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Expr::Column(column) = expr
                && let Ok(index) = aggregate.schema.index_of_column(column)
            {
                unbounded.insert(index);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let mut metadata = aggregate.schema.metadata().clone();
    let mut changed = false;
    for (index, limit) in bounds {
        if unbounded.contains(&index) || index < aggregate.group_expr.len() {
            continue;
        }
        let ordinal = index - aggregate.group_expr.len();
        let mut expr = &aggregate.aggr_expr[ordinal];
        while let Expr::Alias(alias) = expr {
            expr = alias.expr.as_ref();
        }
        let Expr::AggregateFunction(function) = expr else {
            continue;
        };
        if function.func.name() != "array_agg"
            || function.params.distinct
            || function.params.order_by.as_ref().is_none_or(Vec::is_empty)
        {
            continue;
        }
        metadata.insert(
            format!("{COLLECTION_LIMIT_PREFIX}{ordinal}"),
            limit.to_string(),
        );
        changed = true;
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    }
    let mut aggregate = aggregate.clone();
    aggregate.schema = Arc::new(schema_from_df_fields_with_metadata(
        &fields_with_qualifiers(&aggregate.schema),
        metadata,
    )?);
    projection.input = Arc::new(LogicalPlan::Aggregate(aggregate));
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

#[cfg(test)]
mod bounded_collection_tests {
    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    use prost::Message;

    async fn limits(select: &str) -> std::collections::HashMap<u32, u64> {
        let sql = format!(
            "CREATE TABLE src WITH (connector = 'impulse', event_rate = '1'); \
             SELECT {select} FROM src GROUP BY counter"
        );
        let compiled =
            parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                .await
                .unwrap_or_else(|error| panic!("{error}: {sql}"));
        let operator = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .unwrap();
        UpdatingAggregateOperator::decode(operator.0.operator_config.as_slice())
            .unwrap()
            .collection_output_limits
    }

    #[tokio::test]
    async fn counts_by_member_struct_slice_serializes_only_the_ranking_bound() {
        use datafusion_proto::protobuf::{PhysicalPlanNode, physical_plan_node::PhysicalPlanType};
        let aggregate =
            "ARRAY_AGG(named_struct('item_key', item_key, 'n', n) ORDER BY n DESC, item_key ASC)";
        for (projection, bounded) in [
            (format!("array_slice({aggregate}, 1, 5) AS top_items"), true),
            (format!("{aggregate} AS top_items"), false),
            (
                format!("array_slice({aggregate}, 1, 5) AS top_items, {aggregate} AS all_items"),
                false,
            ),
            (
                format!(
                    "array_slice({aggregate}, 1, 5) AS top_items, array_slice({aggregate}, 1, 3) AS other_items"
                ),
                false,
            ),
        ] {
            // The generic two-stage SQL from native_updating_top5_cdc.py:
            // nullable members, COUNT by member, typed records, exact tie order.
            let sql = format!(
                "SET updating_ttl = NULL;
                CREATE TABLE ranking_input (
                    row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, v TEXT, position BIGINT NOT NULL
                ) WITH (connector = 'single_file', path = '/tmp/top-k-input.jsonl',
                    format = 'debezium_json', type = 'source', wait_for_control = 'true');
                CREATE VIEW member_counts AS SELECT k, v AS item_key, COUNT(*) AS n
                    FROM ranking_input WHERE v IS NOT NULL GROUP BY k, v;
                SELECT k, {projection} FROM member_counts GROUP BY k;"
            );
            let compiled =
                parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                    .await
                    .unwrap_or_else(|error| panic!("{error}: {sql}"));
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
            assert_eq!(
                configs.len(),
                2,
                "COUNT and ranking must use native aggregate boundaries"
            );
            let selected = configs
                .iter()
                .filter(|config| !config.collection_output_limits.is_empty())
                .collect::<Vec<_>>();
            assert_eq!(selected.len(), usize::from(bounded), "{projection}");
            if let Some(config) = selected.first() {
                assert_eq!(
                    config.collection_output_limits,
                    [(0, 5)].into_iter().collect()
                );
                let PhysicalPlanType::Aggregate(plan) =
                    PhysicalPlanNode::decode(config.aggregate_exec.as_slice())
                        .unwrap()
                        .physical_plan_type
                        .unwrap()
                else {
                    panic!("not an aggregate physical plan");
                };
                assert!(
                    plan.aggr_expr_name[0]
                        .to_ascii_lowercase()
                        .contains("array_agg")
                );
            }
        }
    }

    #[tokio::test]
    async fn finite_prefix_limits_reach_operator_config() {
        for k in [0, 1, 5] {
            let select =
                format!("array_slice(array_agg(counter ORDER BY counter DESC), 1, {k}) AS ranked");
            assert_eq!(limits(&select).await, [(0, k)].into_iter().collect());
        }
        assert_eq!(
            limits("array_slice(array_agg(counter ORDER BY counter), 0, 5, 1)").await,
            [(0, 5)].into_iter().collect()
        );
        assert_eq!(
            limits(
                "array_slice(array_agg(counter ORDER BY counter) FILTER (WHERE counter > 0), 1, 5)"
            )
            .await,
            [(0, 5)].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn incompatible_consumers_preserve_full_array_semantics() {
        for select in [
            "array_agg(counter ORDER BY counter)",
            "array_slice(array_agg(counter), 1, 5)",
            "array_slice(array_agg(DISTINCT counter ORDER BY counter), 1, 5)",
            "array_slice(array_agg(counter ORDER BY counter), 2, 5)",
            "array_slice(array_agg(counter ORDER BY counter), 1, -1)",
            "array_slice(array_agg(counter ORDER BY counter), 1, CAST(NULL AS BIGINT))",
            "array_slice(array_agg(counter ORDER BY counter), 1, 2147483648)",
            "array_slice(array_agg(counter ORDER BY counter), 1, 5, 2)",
            "array_slice(array_agg(counter ORDER BY counter), 1, counter)",
            "array_slice(array_agg(counter ORDER BY counter), 1, 5), array_agg(counter ORDER BY counter)",
            "array_slice(array_agg(counter ORDER BY counter), 1, 5), array_slice(array_agg(counter ORDER BY counter), 1, 3)",
        ] {
            assert!(limits(select).await.is_empty(), "{select}");
        }
    }
}

#[cfg(test)]
mod event_time_selector_tests {
    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    use datafusion_proto::protobuf::{
        PhysicalPlanNode, physical_expr_node::ExprType, physical_plan_node::PhysicalPlanType,
    };
    use prost::Message;

    async fn aggregate(
        declared: bool,
        select: &str,
        view: &str,
    ) -> datafusion_proto::protobuf::AggregateExecNode {
        let watermark = if declared {
            ", WATERMARK FOR event_time AS completeness - INTERVAL '5' SECOND"
        } else {
            ""
        };
        let sql = format!("CREATE TABLE events (id BIGINT PRIMARY KEY, event_time TIMESTAMP NOT NULL,
            completeness TIMESTAMP NOT NULL, value TEXT, priority BIGINT{watermark})
            WITH (connector='single_file', path='/tmp/selectors.jsonl', format='debezium_json', type='source');
            {view} SELECT id, {select} FROM selected GROUP BY id");
        let compiled =
            parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                .await
                .unwrap_or_else(|e| panic!("{e}: {sql}"));
        let config = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .map(|(operator, _)| {
                UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
            })
            .unwrap();
        let raw_schema: arroyo_rpc::df::ArroyoSchema =
            config.input_schema.clone().unwrap().try_into().unwrap();
        assert!(
            raw_schema
                .schema
                .index_of(crate::grouped_selector::ORDINAL_FIELD)
                .is_err()
        );
        let wire = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        assert!(!format!("{wire:?}").contains(crate::grouped_selector::ORDINAL_MARKER));
        let provider = ArroyoSchemaProvider::new();
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build()
            .unwrap();
        let codec = crate::physical::ArroyoPhysicalExtensionCodec {
            context: crate::physical::DecodingContext::Planning,
        };
        use datafusion_proto::physical_plan::AsExecutionPlan;
        let decoded = wire
            .clone()
            .try_into_physical_plan(&provider, &runtime, &codec)
            .unwrap();
        let decoded = decoded
            .as_any()
            .downcast_ref::<datafusion::physical_plan::aggregates::AggregateExec>()
            .unwrap();
        for expression in decoded.aggr_expr() {
            if crate::grouped_selector_udaf::classify(expression.fun()).is_some() {
                crate::grouped_selector_udaf::selector_spec(expression, &decoded.input_schema())
                    .unwrap();
            }
        }
        let Some(PhysicalPlanType::Aggregate(aggregate)) =
            PhysicalPlanNode::decode(config.aggregate_exec.as_slice())
                .unwrap()
                .physical_plan_type
        else {
            panic!("expected aggregate")
        };
        *aggregate
    }

    #[tokio::test]
    async fn declared_selector_order_survives_view_filter_and_serialization() {
        let plan = aggregate(true,
            "FIRST_VALUE(value) AS earliest, LAST_VALUE(NULLIF(value, '') ORDER BY priority DESC NULLS FIRST, event_time DESC NULLS LAST) FILTER (WHERE priority > 0) IGNORE NULLS AS latest, ARRAY_AGG(value ORDER BY priority ASC) AS values",
            "CREATE VIEW selected AS SELECT * FROM events WHERE id > 0;").await;
        let orders = plan
            .aggr_expr
            .iter()
            .take(3)
            .map(|expr| {
                let Some(ExprType::AggregateExpr(expr)) = &expr.expr_type else {
                    panic!("expected aggregate expression")
                };
                expr.ordering_req.clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            orders.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 3, 1]
        );
        for order in &orders[..2] {
            let Some(ExprType::Column(column)) = &order[0].expr.as_ref().unwrap().expr_type else {
                panic!("expected raw clock column")
            };
            assert_eq!(column.name, "_timestamp");
            assert!(order[0].asc);
            assert!(!order[0].nulls_first);
        }
        assert!(!orders[1][1].asc);
        assert!(orders[1][1].nulls_first);
        assert!(!orders[1][2].asc);
        assert!(!orders[1][2].nulls_first);
        assert!(plan.filter_expr[1].expr.is_some());
        let Some(ExprType::AggregateExpr(last)) = &plan.aggr_expr[1].expr_type else {
            panic!("expected last selector")
        };
        assert!(last.ignore_nulls);
        assert!(orders[2][0].asc);
    }

    #[tokio::test]
    async fn undeclared_selectors_keep_explicit_order_only() {
        let plan = aggregate(false,
            "FIRST_VALUE(value ORDER BY priority DESC NULLS LAST) AS earliest, LAST_VALUE(value ORDER BY priority ASC NULLS FIRST) AS latest",
            "CREATE VIEW selected AS SELECT * FROM events;").await;
        for (index, expr) in plan.aggr_expr.iter().take(2).enumerate() {
            let Some(ExprType::AggregateExpr(expr)) = &expr.expr_type else {
                panic!("expected aggregate expression")
            };
            assert_eq!(expr.ordering_req.len(), 1);
            assert_eq!(expr.ordering_req[0].asc, index == 1);
            assert_eq!(expr.ordering_req[0].nulls_first, index == 1);
        }
    }
    #[tokio::test]
    async fn declared_selector_keeps_raw_clock_when_view_hides_declared_column() {
        let plan = aggregate(true, "LAST_VALUE(value ORDER BY priority ASC) AS latest",
            "CREATE VIEW renamed AS SELECT id, value, priority, event_time AS renamed_time FROM events; CREATE VIEW selected AS SELECT id, value, priority FROM renamed;").await;
        let Some(ExprType::AggregateExpr(last)) = &plan.aggr_expr[0].expr_type else {
            panic!("expected selector")
        };
        assert_eq!(last.ordering_req.len(), 2);
        let Some(ExprType::Column(clock)) = &last.ordering_req[0].expr.as_ref().unwrap().expr_type
        else {
            panic!("expected raw clock")
        };
        assert_eq!(clock.name, "_timestamp");
    }

    #[tokio::test]
    async fn declared_selector_rejects_ambiguous_sources() {
        let sql = "CREATE TABLE events (id BIGINT, event_time TIMESTAMP NOT NULL,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND)
            WITH (connector='single_file', path='/tmp/selectors.jsonl', format='json', type='source');
            SELECT a.id, LAST_VALUE(b.id) FROM events a JOIN events b ON a.id = b.id GROUP BY a.id";
        let error = parse_and_get_program(sql, ArroyoSchemaProvider::new(), SqlConfig::default())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("ambiguous triggering sources"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn analytic_selector_keeps_declared_sql_order() {
        use arroyo_rpc::grpc::api::WindowFunctionOperator;
        let sql = "CREATE TABLE events (id BIGINT, event_time TIMESTAMP NOT NULL, priority BIGINT,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND)
            WITH (connector='single_file', path='/tmp/analytic-selectors.jsonl', format='json', type='source');
            SELECT id, LAST_VALUE(priority) OVER (PARTITION BY window ORDER BY priority DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS latest FROM (
                SELECT id, MAX(priority) AS priority, TUMBLE(INTERVAL '10' SECOND) AS window
                FROM events GROUP BY id, window)";
        let compiled =
            parse_and_get_program(sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                .await
                .unwrap();
        let mut found = false;
        for (operator, _) in compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
        {
            if operator.operator_name == OperatorName::WindowFunction {
                let config =
                    WindowFunctionOperator::decode(operator.operator_config.as_slice()).unwrap();
                let proto =
                    PhysicalPlanNode::decode(config.window_function_plan.as_slice()).unwrap();
                let display = format!("{proto:?}");
                assert!(display.contains("priority"), "{display}");
                // The declared clock remains an input field, but is never an
                // additional analytic ordering key.
                let provider = ArroyoSchemaProvider::new();
                let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                    .build()
                    .unwrap();
                let codec = crate::physical::ArroyoPhysicalExtensionCodec {
                    context: crate::physical::DecodingContext::Planning,
                };
                use datafusion_proto::physical_plan::AsExecutionPlan;
                let plan = proto
                    .try_into_physical_plan(&provider, &runtime, &codec)
                    .unwrap();
                let display = format!(
                    "{}",
                    datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
                );
                let window = display
                    .lines()
                    .find(|line| line.contains("WindowAggExec"))
                    .unwrap();
                assert!(!window.contains("_timestamp"), "{window}");
                found = true;
            }
        }
        assert!(found);
    }
}

#[cfg(test)]
mod companion_projection_tests {
    use super::*;
    use crate::{SqlConfig, parse_and_get_program};
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::{
        KeyPlanOperator, SessionWindowAggregateOperator, TumblingWindowAggregateOperator,
        UpdatingAggregateOperator,
    };
    use datafusion::physical_expr::{ScalarFunctionExpr, expressions::Column};
    use datafusion::physical_plan::{aggregates::AggregateExec, projection::ProjectionExec};
    use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
    use prost::Message;

    #[tokio::test]
    async fn computed_variable_companions_are_stored_once_across_ordinary_and_window_plans() {
        for window in [
            None,
            Some("TUMBLE(INTERVAL '10' SECOND)"),
            Some("SESSION(INTERVAL '10' SECOND)"),
        ] {
            for volatile in [false, true] {
                let argument = if volatile {
                    "CAST(random() AS TEXT)"
                } else {
                    "repeat(value, 2)"
                };
                let count_argument = if volatile {
                    argument
                } else {
                    "repeat(value, 3)"
                };
                let window_select =
                    window.map_or(String::new(), |window| format!(", {window} AS window"));
                let window_group = if window.is_some() { ", window" } else { "" };
                let sql = format!("CREATE TABLE src (
                    __streamr_companion_arg_v1_0_0 BIGINT, value TEXT, event_time TIMESTAMP NOT NULL,
                    completeness TIMESTAMP NOT NULL,
                    WATERMARK FOR event_time AS completeness - INTERVAL '5' SECOND
                ) WITH (connector='single_file', path='/tmp/companion-projection.jsonl', format='json', type='source');
                SELECT __streamr_companion_arg_v1_0_0,
                    APPROX_DISTINCT({argument}) FILTER (WHERE value <> '') AS approx_values,
                    COUNT(DISTINCT {count_argument}) FILTER (WHERE value IS NOT NULL) AS distinct_values,
                    FIRST_VALUE(value) AS first_value{window_select}
                FROM src GROUP BY __streamr_companion_arg_v1_0_0{window_group}");
                let provider = ArroyoSchemaProvider::new();
                let compiled = parse_and_get_program(&sql, provider.clone(), SqlConfig::default())
                    .await
                    .unwrap_or_else(|e| panic!("{e}: {sql}"));
                let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                    .build()
                    .unwrap();
                let codec = crate::physical::ArroyoPhysicalExtensionCodec {
                    context: crate::physical::DecodingContext::Planning,
                };
                let decode = |bytes: &[u8]| {
                    PhysicalPlanNode::decode(bytes)
                        .unwrap()
                        .try_into_physical_plan(&provider, &runtime, &codec)
                        .unwrap()
                };
                let mut projected_names = Vec::new();
                let mut aggregate_names = Vec::new();
                let mut random_calls = 0;
                for (operator, _) in compiled
                    .program
                    .graph
                    .node_weights()
                    .flat_map(|node| node.operator_chain.iter())
                {
                    let plan = match operator.operator_name {
                        OperatorName::ArrowKey => {
                            let config =
                                KeyPlanOperator::decode(operator.operator_config.as_slice())
                                    .unwrap();
                            let plan = decode(&config.physical_plan);
                            plan.apply(|node| {
                                if let Some(projection) =
                                    node.as_any().downcast_ref::<ProjectionExec>()
                                {
                                    for (expression, name) in projection.expr() {
                                        if name.starts_with("__streamr_companion_arg_v1_")
                                            && !expression.as_any().is::<Column>()
                                        {
                                            projected_names.push(name.clone());
                                            if !volatile {
                                                use arrow_array::{
                                                    ArrayRef, Int64Array, RecordBatch, StringArray,
                                                    TimestampNanosecondArray,
                                                };
                                                let schema = projection.input().schema();
                                                let columns = schema
                                                    .fields()
                                                    .iter()
                                                    .map(|field| match field.data_type() {
                                                        arrow_schema::DataType::Int64 => {
                                                            Arc::new(Int64Array::from(vec![
                                                                1, 2, 3,
                                                            ]))
                                                                as ArrayRef
                                                        }
                                                        arrow_schema::DataType::Utf8 => {
                                                            Arc::new(StringArray::from(vec![
                                                                Some("ab"),
                                                                Some(""),
                                                                None,
                                                            ]))
                                                                as ArrayRef
                                                        }
                                                        arrow_schema::DataType::Timestamp(
                                                            arrow_schema::TimeUnit::Nanosecond,
                                                            _,
                                                        ) => Arc::new(
                                                            TimestampNanosecondArray::from(vec![
                                                                1, 2, 3,
                                                            ]),
                                                        )
                                                            as ArrayRef,
                                                        kind => panic!(
                                                            "unexpected source test field {kind:?}"
                                                        ),
                                                    })
                                                    .collect();
                                                let batch =
                                                    RecordBatch::try_new(schema, columns).unwrap();
                                                let actual = expression
                                                    .evaluate(&batch)
                                                    .unwrap()
                                                    .into_array(3)
                                                    .unwrap();
                                                let expected = if name
                                                    .starts_with("__streamr_companion_arg_v1_0_0")
                                                {
                                                    "abab"
                                                } else {
                                                    "ababab"
                                                };
                                                assert_eq!(
                                                    actual.as_ref(),
                                                    &StringArray::from(vec![
                                                        Some(expected),
                                                        Some(""),
                                                        None
                                                    ])
                                                );
                                            }
                                            expression.apply(|child| {
                                                random_calls += usize::from(
                                                    child
                                                        .as_any()
                                                        .downcast_ref::<ScalarFunctionExpr>()
                                                        .is_some_and(|function| {
                                                            function.name() == "random"
                                                        }),
                                                );
                                                Ok(TreeNodeRecursion::Continue)
                                            })?;
                                        }
                                    }
                                }
                                Ok(TreeNodeRecursion::Continue)
                            })
                            .unwrap();
                            continue;
                        }
                        OperatorName::UpdatingAggregate => decode(
                            &UpdatingAggregateOperator::decode(operator.operator_config.as_slice())
                                .unwrap()
                                .aggregate_exec,
                        ),
                        OperatorName::TumblingWindowAggregate => decode(
                            &TumblingWindowAggregateOperator::decode(
                                operator.operator_config.as_slice(),
                            )
                            .unwrap()
                            .partial_aggregation_plan,
                        ),
                        OperatorName::SessionWindowAggregate => decode(
                            &SessionWindowAggregateOperator::decode(
                                operator.operator_config.as_slice(),
                            )
                            .unwrap()
                            .final_aggregation_plan,
                        ),
                        _ => continue,
                    };
                    plan.apply(|node| {
                        if let Some(aggregate) = node.as_any().downcast_ref::<AggregateExec>() {
                            for (index, expression) in aggregate.aggr_expr().iter().enumerate() {
                                if expression.fun().name() == "approx_distinct" || (expression.fun().name() == "count" && expression.is_distinct()) {
                                    let arguments = expression.expressions();
                                    let column = arguments[0].as_any().downcast_ref::<Column>().expect("native admission must see the stored argument, never re-evaluate the expression");
                                    aggregate_names.push(column.name().to_string());
                                    assert!(aggregate.filter_expr()[index].is_some());
                                }
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    }).unwrap();
                }
                projected_names.sort();
                projected_names.dedup();
                aggregate_names.sort();
                aggregate_names.dedup();
                assert_eq!(projected_names.len(), 2, "{sql}");
                assert_eq!(aggregate_names, projected_names);
                assert!(
                    projected_names
                        .iter()
                        .all(|name| name != "__streamr_companion_arg_v1_0_0"),
                    "source column name must not collide"
                );
                assert_eq!(
                    random_calls,
                    if volatile { 2 } else { 0 },
                    "separate volatile aggregate occurrences must remain separate: {sql}"
                );
            }
        }
    }
}
