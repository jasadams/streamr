use crate::extension::aggregate::AggregateExtension;
use crate::extension::key_calculation::{KeyCalculationExtension, KeysOrExprs};
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::updating_aggregate::{CurrentResultExpiry, UpdatingAggregateExtension};
use crate::plan::{WindowDetectingVisitor, extract_column};
use crate::{
    ArroyoSchemaProvider, DFField, WindowBehavior, fields_with_qualifiers, find_window,
    schema_from_df_fields_with_metadata,
};
use arroyo_datastream::WindowType;
use arroyo_rpc::TIMESTAMP_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{DFSchema, DataFusionError, Result, not_impl_err, plan_err};
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr;
use datafusion::logical_expr::expr::{AggregateFunction, Alias, ScalarFunction, Sort};
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

fn watermark_date(expression: &Expr) -> bool {
    matches!(unalias(expression), Expr::ScalarFunction(function)
        if function.name() == "watermark_date" && function.args.is_empty())
}

fn horizon_lower(expression: &Expr) -> Option<u32> {
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;
    let Expr::BinaryExpr(binary) = unalias(expression) else {
        return None;
    };
    if binary.op != Operator::Minus || !watermark_date(&binary.left) {
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
            if watermark_date(&binary.right) {
                Some((*binary.left.clone(), 1))
            } else if watermark_date(&binary.left) {
                Some((*binary.right.clone(), 1))
            } else {
                None
            }
        }
        Expr::Between(between) if !between.negated && watermark_date(&between.high) => {
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
                        "unsupported clock-dependent FILTER: expected DATE = WATERMARK_DATE() or DATE BETWEEN WATERMARK_DATE() - constant DAY AND WATERMARK_DATE(); retained contribution time is a distinct input"
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
            if watermark_date(expression) {
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

fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(Alias { expr, .. }) => strip_alias(expr),
        other => other,
    }
}

/// Identifies `LAST_VALUE(result ORDER BY window_end ASC)` over finalized
/// window results — the current-result stage of the selected rolling-result
/// composition. Returns the result timestamp column of the single ordered
/// last-value aggregate, or None for any other aggregate shape.
fn last_value_result_timestamp(aggr_expr: &[Expr]) -> Option<&datafusion::common::Column> {
    let [expr] = aggr_expr else {
        return None;
    };
    let Expr::AggregateFunction(AggregateFunction { func, params }) = strip_alias(expr) else {
        return None;
    };
    if !func.name().eq_ignore_ascii_case("last_value")
        || params.args.len() != 1
        || params.filter.is_some()
        || params.distinct
        || params.null_treatment.is_some()
    {
        return None;
    }
    let [
        Sort {
            expr: order,
            asc: true,
            ..
        },
    ] = params.order_by.as_deref()?
    else {
        return None;
    };
    extract_column(order)
}

/// Resolves which upstream finalized HOP/TUMBLE window stamped the projected
/// scalar at `target`, by walking the linear projection/alias/materialization
/// chain down to the window definition. Any other plan shape returns None.
fn finalized_window_at(node: &LogicalPlan, target: usize) -> Option<WindowType> {
    match node {
        LogicalPlan::SubqueryAlias(alias) => finalized_window_at(&alias.input, target),
        LogicalPlan::Projection(projection) => {
            let expr = strip_alias(projection.expr.get(target)?);
            match expr {
                Expr::ScalarFunction(ScalarFunction { func, args })
                    if func.name() == "get_field" && args.len() == 2 =>
                {
                    let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(field)), _) =
                        &args[1]
                    else {
                        return None;
                    };
                    if field != "end" {
                        return None;
                    }
                    let Expr::Column(struct_column) = strip_alias(&args[0]) else {
                        return None;
                    };
                    window_struct_definition(&projection.input, struct_column)
                }
                Expr::Column(column) => {
                    let index = projection.input.schema().index_of_column(column).ok()?;
                    finalized_window_at(&projection.input, index)
                }
                _ => None,
            }
        }
        LogicalPlan::Extension(Extension { node })
            if node.as_any().is::<RemoteTableExtension>() && !node.inputs().is_empty() =>
        {
            finalized_window_at(node.inputs()[0], target)
        }
        _ => None,
    }
}

/// The window struct field `column` is produced by a windowed aggregate and
/// renamed by its projection (the aggregate's window field carries the raw
/// window expression text). Lineage translates the name through projections
/// and materialization boundaries down to that window.
fn window_struct_definition(
    node: &LogicalPlan,
    column: &datafusion::common::Column,
) -> Option<WindowType> {
    match node {
        LogicalPlan::SubqueryAlias(alias) => window_struct_definition(&alias.input, column),
        LogicalPlan::Projection(projection) => {
            let index = projection
                .schema
                .fields()
                .iter()
                .position(|field| field.name() == &column.name)?;
            match strip_alias(&projection.expr[index]) {
                Expr::Column(next) => window_struct_definition(&projection.input, next),
                expr => {
                    let Ok(Some(window)) = find_window(expr) else {
                        return None;
                    };
                    // Only a finalized window result counts: the projection
                    // names a window produced by the windowed aggregate below.
                    windowed_aggregate_window(&projection.input).filter(|found| *found == window)
                }
            }
        }
        LogicalPlan::Extension(Extension { node })
            if node.as_any().is::<RemoteTableExtension>() && !node.inputs().is_empty() =>
        {
            window_struct_definition(node.inputs()[0], column)
        }
        LogicalPlan::Extension(Extension { node }) => {
            let aggregate_extension = node.as_any().downcast_ref::<AggregateExtension>()?;
            match &aggregate_extension.window_behavior {
                WindowBehavior::FromOperator {
                    window,
                    window_field,
                    ..
                } if window_field.name() == &column.name => Some(window.clone()),
                WindowBehavior::FromOperator { .. } | WindowBehavior::InData => None,
            }
        }
        _ => None,
    }
}

fn windowed_aggregate_window(node: &LogicalPlan) -> Option<WindowType> {
    match node {
        LogicalPlan::SubqueryAlias(alias) => windowed_aggregate_window(&alias.input),
        LogicalPlan::Projection(projection) => windowed_aggregate_window(&projection.input),
        LogicalPlan::Extension(Extension { node })
            if node.as_any().is::<RemoteTableExtension>() && !node.inputs().is_empty() =>
        {
            windowed_aggregate_window(node.inputs()[0])
        }
        LogicalPlan::Extension(Extension { node }) => {
            let aggregate_extension = node.as_any().downcast_ref::<AggregateExtension>()?;
            match &aggregate_extension.window_behavior {
                WindowBehavior::FromOperator { window, .. } => Some(window.clone()),
                WindowBehavior::InData => None,
            }
        }
        _ => None,
    }
}

/// The selected rolling-result composition path retains one current result per
/// key over finalized HOP/TUMBLE window rows and stamps its event-time validity
/// at the result timestamp plus one window slide (the first empty closed
/// boundary). Watermarks past the deadline retract the retained result so the
/// composition can express zero; aggregates outside this pattern are
/// unchanged.
pub(super) fn current_result_expiry(
    input: &LogicalPlan,
    aggr_expr: &[Expr],
) -> Result<Option<CurrentResultExpiry>> {
    let Some(order_column) = last_value_result_timestamp(aggr_expr) else {
        return Ok(None);
    };
    let target = input.schema().index_of_column(order_column).map_err(|_| {
        DataFusionError::Plan(
            "LAST_VALUE ORDER BY column is not an input field of its aggregate".to_string(),
        )
    })?;
    let Some(window) = finalized_window_at(input, target) else {
        return Ok(None);
    };
    let delay = match window {
        WindowType::Sliding { slide, .. } => slide,
        WindowType::Tumbling { width } => width,
        WindowType::Instant | WindowType::Session { .. } => return Ok(None),
    };
    Ok(Some(CurrentResultExpiry {
        // Operator input schemas are unqualified Arrow schemas; the planned
        // column qualifier only identifies the field in the logical input.
        result_timestamp: col(&order_column.name),
        delay,
    }))
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
        // Expiry requires whole-program composition and shared-consumer proof.
        // Views are rewritten before their consumers, so attach it only after
        // all sink inputs have been assembled.
        let event_time_expiry = None;
        let calendar_aggregates = calendar_filters(&mut aggr_expr, &input)?;
        let key_count = key_fields.len();
        key_fields.extend(fields_with_qualifiers(input.schema()));

        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);

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
            event_time_expiry,
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
            aggr_expr,
            schema,
            ..
        }) = node
        else {
            return Ok(Transformed::no(node));
        };
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

        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);

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
