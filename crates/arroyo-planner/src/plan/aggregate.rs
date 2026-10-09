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
