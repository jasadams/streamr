//! SQL lowering and admission for finite expression-local rowsets.
//!
//! These plans run inside an expression evaluation, never as streaming operators.
use std::ops::ControlFlow;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan};
use sqlparser::ast::{FunctionArguments, SetExpr, Statement, VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Preserve ARRAY(query) before DataFusion's generic function argument lowering.
/// A private concrete marker retains the original one-column finite rowset.
/// Native execution preserves its ORDER BY; collection supplies the typed empty list.
pub fn lower_array_queries<T: VisitMut>(ast: &mut T) -> anyhow::Result<()> {
    struct Lower;
    impl VisitorMut for Lower {
        type Break = anyhow::Error;

        fn post_visit_expr(&mut self, expr: &mut sqlparser::ast::Expr) -> ControlFlow<Self::Break> {
            let sqlparser::ast::Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            if !function.name.to_string().eq_ignore_ascii_case("array") {
                return ControlFlow::Continue(());
            }
            let FunctionArguments::Subquery(input) = &function.args else {
                return ControlFlow::Continue(());
            };
            let original = input.clone();
            let mut template = match Parser::parse_sql(
                &GenericDialect {},
                "SELECT __streamr_array_query((SELECT 1))",
            ) {
                Ok(template) => template,
                Err(error) => return ControlFlow::Break(error.into()),
            };
            let Statement::Query(query) = template.remove(0) else {
                unreachable!()
            };
            let SetExpr::Select(select) = *query.body else {
                unreachable!()
            };
            let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(mut marker)) =
                select.projection.into_iter().next().unwrap()
            else {
                unreachable!()
            };
            let FunctionArguments::List(args) = &mut marker.args else {
                unreachable!()
            };
            args.args[0] = sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(sqlparser::ast::Expr::Subquery(original)),
            );
            *expr = sqlparser::ast::Expr::Function(marker);
            ControlFlow::Continue(())
        }
    }
    match ast.visit(&mut Lower) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

/// Admit only native finite relations. CTEs have already been expanded by SQL
/// planning. A scan or extension cannot acquire locality by appearing beneath a
/// finite projection, join, aggregate or LIMIT.
///
/// `outer_scopes` are ordered from innermost to outermost lexical query scope.
pub fn validate_finite_plan(plan: &LogicalPlan, outer_scopes: &[DFSchemaRef]) -> Result<()> {
    match plan {
        LogicalPlan::Values(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_) => {}
        _ => {
            return Err(DataFusionError::Plan(format!(
                "finite local SQL does not allow this relation: {}",
                plan.display()
            )));
        }
    }
    let mut nested_scopes: Vec<_> = plan.inputs().iter().map(|p| p.schema().clone()).collect();
    nested_scopes.extend_from_slice(outer_scopes);
    for expression in plan.expressions() {
        expression.apply(|expr| {
            match expr {
                Expr::OuterReferenceColumn(data_type, column) => {
                    let field = outer_scopes.iter().find_map(|schema| {
                        schema.qualified_field_from_column(column).ok().map(|(_, field)| field)
                    }).ok_or_else(|| DataFusionError::Plan(format!(
                        "unbound lexical outer reference {column} in finite local SQL"
                    )))?;
                    if field.data_type() != data_type {
                        return Err(DataFusionError::Plan(format!(
                            "outer reference {column} has type {data_type:?}, but its lexical binding has type {:?}",
                            field.data_type()
                        )));
                    }
                }
                Expr::ScalarSubquery(subquery) => {
                    if subquery.subquery.schema().fields().len() != 1 {
                        return Err(DataFusionError::Plan("local scalar subquery must return exactly one column".into()));
                    }
                    validate_finite_plan(&subquery.subquery, &nested_scopes)?;
                }
                Expr::Exists(exists) => validate_finite_plan(&exists.subquery.subquery, &nested_scopes)?,
                Expr::InSubquery(in_query) => validate_finite_plan(&in_query.subquery.subquery, &nested_scopes)?,
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    for input in plan.inputs() {
        validate_finite_plan(input, outer_scopes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_query_preserves_original_rowset_and_order() {
        let mut sql = Parser::parse_sql(
            &GenericDialect {},
            "SELECT ARRAY(SELECT v FROM (VALUES (2), (1)) AS t(v) ORDER BY v)",
        )
        .unwrap();
        lower_array_queries(&mut sql).unwrap();
        let rendered = sql[0].to_string();
        assert!(rendered.contains("__streamr_array_query"));
        assert!(rendered.contains("ORDER BY v"));
    }

    #[test]
    fn finite_roots_are_admitted_and_stream_roots_are_rejected() {
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::logical_plan::{EmptyRelation, RecursiveQuery};
        use std::sync::Arc;

        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        validate_finite_plan(&empty, &[]).unwrap();
        let recursive = LogicalPlan::RecursiveQuery(RecursiveQuery {
            name: "recursive".into(),
            static_term: Arc::new(empty.clone()),
            recursive_term: Arc::new(empty),
            is_distinct: true,
        });
        assert!(validate_finite_plan(&recursive, &[]).is_err());
    }

    #[test]
    fn nested_array_queries_are_lowered_inside_out() {
        let mut sql =
            Parser::parse_sql(&GenericDialect {}, "SELECT ARRAY(SELECT ARRAY(SELECT 1))").unwrap();
        lower_array_queries(&mut sql).unwrap();
        assert_eq!(
            sql[0].to_string().matches("__streamr_array_query").count(),
            2
        );
    }
}
