//! JSON_TABLE uses the existing finite SELECT unnest execution path.
use super::lowering::{function_call, string_literal};
use super::path::compile_literal_path;
use datafusion::common::{DataFusionError, Result};
use sqlparser::ast::*;
use std::collections::HashMap;

const ROW: &str = "__unnested";
fn error(message: &str) -> DataFusionError {
    DataFusionError::Plan(format!("unsupported JSON_TABLE feature: {message}"))
}
fn path(value: &Value) -> Result<&str> {
    let Value::SingleQuotedString(path) = value else {
        return Err(error("dynamic path"));
    };
    compile_literal_path(path)?;
    Ok(path)
}
fn key(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Replace JSON_TABLE with a lateral derived SELECT. DataFusion binds the
/// document against the actual preceding FROM schema and preserves the public
/// relation alias; no user column expression or qualifier is rewritten.
pub(super) fn lower_relation(relation: &mut TableFactor) -> Result<()> {
    let TableFactor::JsonTable {
        json_expr,
        json_path,
        columns,
        alias,
    } = relation
    else {
        return Ok(());
    };
    let row = Expr::Identifier(Ident::new(ROW));
    let mut replacements = HashMap::new();
    let mut ordered = Vec::new();
    for column in columns.iter() {
        let JsonTableColumn::Named(column) = column else {
            return Err(error("NESTED COLUMNS or FOR ORDINALITY"));
        };
        if column.on_empty.is_some() || column.on_error.is_some() {
            return Err(error("ON EMPTY / ON ERROR overrides"));
        }
        let function = match (&column.r#type, column.exists, column.format_json) {
            (DataType::Varchar(None), false, false) => "json_value",
            (DataType::Boolean, false, false) => "json_value_boolean",
            (DataType::Varchar(None), false, true) => "json_query",
            (DataType::Boolean, true, false) => "json_exists",
            _ => {
                return Err(error(
                    "column type (supported: VARCHAR PATH, BOOLEAN PATH, VARCHAR FORMAT JSON PATH, BOOLEAN EXISTS PATH)",
                ));
            }
        };
        let expression = function_call(
            function,
            vec![row.clone(), string_literal(path(&column.path)?)],
        );
        if key(&column.name) == ROW
            || replacements
                .insert(key(&column.name), expression.clone())
                .is_some()
        {
            return Err(error("duplicate or reserved column name"));
        }
        ordered.push((column.name.clone(), expression));
    }
    if ordered.is_empty() {
        return Err(error("empty COLUMNS"));
    }
    let rows = function_call(
        "unnest",
        vec![function_call(
            "json_table_rows",
            vec![json_expr.clone(), string_literal(path(json_path)?)],
        )],
    );
    // Neutral skeletons supply defaults; user AST nodes never roundtrip through
    // text and retain identifier quoting and source document expressions.
    let mut skeleton = crate::parse_sql("SELECT 1 FROM (SELECT 1) AS __streamr_json_table_items")
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let Statement::Query(mut outer) = skeleton.remove(0) else {
        unreachable!()
    };
    let SetExpr::Select(outer_select) = outer.body.as_mut() else {
        unreachable!()
    };
    outer_select.projection = ordered
        .into_iter()
        .map(|(alias, expr)| SelectItem::ExprWithAlias { expr, alias })
        .collect();
    let TableFactor::Derived {
        subquery: inner, ..
    } = &mut outer_select.from[0].relation
    else {
        unreachable!()
    };
    let SetExpr::Select(inner_select) = inner.body.as_mut() else {
        unreachable!()
    };
    inner_select.projection = vec![SelectItem::ExprWithAlias {
        expr: rows,
        alias: Ident::new(ROW),
    }];
    *relation = TableFactor::Derived {
        lateral: true,
        subquery: outer,
        alias: alias.clone(),
    };
    Ok(())
}

/// Eliminate the correlated FROM rowset before general optimizer rules can
/// decorrelate it as a stream join. The SQL binder has already assigned every
/// outer reference and output qualifier using the actual relation schemas.
pub(crate) fn bind_rowsets(
    plan: datafusion::logical_expr::LogicalPlan,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::{JoinType, LogicalPlan};
    plan.transform_up(|plan| {
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if !is_direct_rowset(&join.right)? {
            return Ok(Transformed::no(plan));
        }
        if join.join_type != JoinType::Inner || !join.on.is_empty() || join.filter.is_some() {
            return Err(error(
                "JSON_TABLE joined with ON or outer JOIN; use the ordinary comma FROM form",
            ));
        }
        // A noncorrelated rowset still expands once per left row through the
        // same attachment. This also avoids an unnecessary finite cross join.
        Ok(Transformed::yes(attach(&join.right, &join.left)?))
    })
    .map(|result| result.data)
}

/// Follow only the emitted linear SELECT chain. A containing SELECT with a
/// filter, join or aggregate is a separate relation, even if it contains its
/// own JSON_TABLE. Never classify it by a recursive marker search.
fn is_direct_rowset(plan: &datafusion::logical_expr::LogicalPlan) -> Result<bool> {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan};
    match plan {
        LogicalPlan::SubqueryAlias(alias)
            if alias.alias.to_string() == "__streamr_json_table_items" =>
        {
            let LogicalPlan::Projection(projection) = alias.input.as_ref() else {
                return Ok(false);
            };
            if !matches!(projection.input.as_ref(), LogicalPlan::EmptyRelation(empty) if empty.produce_one_row)
                || projection.expr.len() != 1
            {
                return Ok(false);
            }
            projection.expr[0].exists(|expr| Ok(matches!(expr, LogicalExpr::ScalarFunction(function) if function.name() == "json_table_rows")))
        }
        LogicalPlan::SubqueryAlias(alias) => is_direct_rowset(&alias.input),
        LogicalPlan::Subquery(subquery) => is_direct_rowset(&subquery.subquery),
        LogicalPlan::Projection(projection) => is_direct_rowset(&projection.input),
        _ => Ok(false),
    }
}

fn attach(
    plan: &datafusion::logical_expr::LogicalPlan,
    left: &std::sync::Arc<datafusion::logical_expr::LogicalPlan>,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan, Projection};
    use std::sync::Arc;
    let left_columns = || {
        left.schema()
            .columns()
            .into_iter()
            .map(LogicalExpr::Column)
            .collect::<Vec<_>>()
    };
    match plan {
        LogicalPlan::EmptyRelation(empty) if empty.produce_one_row => Ok(left.as_ref().clone()),
        LogicalPlan::Subquery(subquery) => attach(&subquery.subquery, left),
        LogicalPlan::Projection(projection) => {
            let input = attach(&projection.input, left)?;
            let mut expressions = left_columns();
            // Right columns are mapped positionally from the binder's old
            // input schema into the attached schema. Name lookup after adding
            // left fields could otherwise select an unrelated same-name field.
            let input_columns = input.schema().columns();
            let mut scope = "__streamr_json_table_scope".to_string();
            while left
                .schema()
                .iter()
                .any(|(q, _)| q.is_some_and(|q| q.to_string() == scope))
            {
                scope.push('_');
            }
            for (index, expression) in projection.expr.iter().enumerate() {
                let expression = expression
                    .clone()
                    .transform_up(|expression| match expression {
                        LogicalExpr::OuterReferenceColumn(_, column) => {
                            left.schema().index_of_column(&column)?;
                            Ok(Transformed::yes(LogicalExpr::Column(column)))
                        }
                        LogicalExpr::Column(column) => {
                            let index = projection.input.schema().index_of_column(&column)?;
                            Ok(Transformed::yes(LogicalExpr::Column(
                                input_columns[left.schema().fields().len() + index].clone(),
                            )))
                        }
                        expression => Ok(Transformed::no(expression)),
                    })?
                    .data;
                let (qualifier, field) = projection.schema.qualified_field(index);
                let qualifier = qualifier
                    .cloned()
                    .unwrap_or_else(|| datafusion::common::TableReference::bare(scope.clone()));
                expressions.push(expression.alias_qualified(Some(qualifier), field.name().clone()));
            }
            Ok(LogicalPlan::Projection(Projection::try_new(
                expressions,
                Arc::new(input),
            )?))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input = attach(&alias.input, left)?;
            let mut expressions = left_columns();
            let input_columns = input.schema().columns();
            for (index, output) in alias.schema.columns().into_iter().enumerate() {
                let column = input_columns[left.schema().fields().len() + index].clone();
                expressions.push(
                    LogicalExpr::Column(column)
                        .alias_qualified(Some(alias.alias.clone()), output.name),
                );
            }
            Ok(LogicalPlan::Projection(Projection::try_new(
                expressions,
                Arc::new(input),
            )?))
        }
        _ => Err(error("unexpected rowset logical plan shape")),
    }
}
