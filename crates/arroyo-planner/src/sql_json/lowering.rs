//! AST lowering for the ANSI SQL/JSON constructors.
//!
//! DataFusion's `SqlToRel` rejects the sqlparser `Expr::Json*` variants at
//! runtime, so every path from parse to plan rewrites them into ordinary
//! `Expr::Function` calls that resolve through the registered UDFs (see
//! [`crate::sql_json::kernels`]). The rewrite is applied:
//!
//! * in `produce_optimized_plan` before `SqlToRel::sql_statement_to_plan`
//!   (covering SELECT/INSERT/CREATE VIEW planning, generating expressions
//!   and the MERGE source query), and
//! * in `continuous_merge::expression` before `sql_to_expr` (covering MERGE
//!   ON predicates, WHEN predicates, UPDATE assignments and INSERT values).
//!
//! Both call sites use [`rewrite_statement`] / [`rewrite_expr`], which walk
//! the whole tree so nesting inside CASE/WHERE/SET clauses is handled at any
//! depth. Literal paths are compiled here, so an invalid path or a duplicate
//! JSON_OBJECT key is a planner diagnostic naming the offending path/key,
//! never a runtime "missing field" or a silent overwrite.
//!
//! JSON_VALUE's RETURNING clause becomes a distinct UDF *name*
//! (`json_value` / `json_value_boolean` / `json_value_double`): DataFusion
//! derives an expression's type from `udf.return_type(&arg_types)`, where
//! argument values are invisible, so the returning type has to be part of
//! the function identity to survive name-based physical-plan serialization.
//!
//! JSON_OBJECT pairs (key order + FORMAT JSON flags) are encoded into a
//! single constant UTF8 metadata argument followed by the value expressions;
//! the metadata literal serializes through datafusion-proto unchanged, so no
//! object key, pair order or FORMAT JSON flag is lost across plan roundtrips.

use core::ops::ControlFlow;

use datafusion::common::{DataFusionError, Result};
use sqlparser::ast::{
    DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, Value, VisitMut, VisitorMut,
};

use super::path::compile_literal_path;

/// Rewrite every SQL/JSON constructor in `statement` into ordinary function
/// calls. Errors are planner diagnostics for invalid literal paths and
/// duplicate JSON_OBJECT keys.
pub(crate) fn rewrite_statement(statement: &mut Statement) -> Result<()> {
    let mut lowering = SqlJsonLowering { error: None };
    let _ = statement.visit(&mut lowering);
    match lowering.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Single-expression form of [`rewrite_statement`] for entry points that plan
/// stored expressions (native MERGE clauses) rather than statements.
pub(crate) fn rewrite_expr(expr: &mut Expr) -> Result<()> {
    let mut lowering = SqlJsonLowering { error: None };
    let _ = expr.visit(&mut lowering);
    match lowering.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct SqlJsonLowering {
    error: Option<DataFusionError>,
}

impl SqlJsonLowering {
    fn fail(&mut self, message: impl Into<String>) -> ControlFlow<()> {
        self.error = Some(DataFusionError::Plan(message.into()));
        ControlFlow::Break(())
    }

    /// Compile a literal path, recording a planner diagnostic on failure.
    fn validate_path(&mut self, path: &str) -> ControlFlow<()> {
        if let Err(error) = compile_literal_path(path) {
            return self.fail(error.to_string());
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for SqlJsonLowering {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        // Children (documents, JSON_OBJECT values) were already rewritten by
        // the time this node is visited, so nesting works at any depth.
        match expr {
            Expr::JsonExists { doc, path } => {
                self.validate_path(path)?;
                let doc = take_expr(doc);
                *expr = function_call("json_exists", vec![doc, string_literal(path)]);
            }
            Expr::JsonValue {
                doc,
                path,
                returning,
            } => {
                self.validate_path(path)?;
                let name = match returning {
                    None | Some(DataType::Varchar(_)) => "json_value",
                    Some(DataType::Boolean) => "json_value_boolean",
                    Some(DataType::DoublePrecision) => "json_value_double",
                    Some(other) => {
                        return self.fail(format!(
                            "unsupported JSON_VALUE RETURNING type in lowering: {other}"
                        ));
                    }
                };
                let doc = take_expr(doc);
                *expr = function_call(name, vec![doc, string_literal(path)]);
            }
            Expr::JsonQuery { doc, path } => {
                self.validate_path(path)?;
                let doc = take_expr(doc);
                *expr = function_call("json_query", vec![doc, string_literal(path)]);
            }
            Expr::JsonObject { pairs } => {
                let mut seen = std::collections::HashSet::with_capacity(pairs.len());
                let mut meta: Vec<(String, bool)> = Vec::with_capacity(pairs.len());
                let mut args = Vec::with_capacity(pairs.len() + 1);
                for pair in pairs.iter_mut() {
                    if !seen.insert(pair.key.clone()) {
                        return self.fail(format!(
                            "duplicate JSON_OBJECT key '{}'; JSON_OBJECT keys must be unique",
                            pair.key
                        ));
                    }
                    meta.push((pair.key.clone(), pair.format_json));
                    args.push(take_expr(&mut pair.value));
                }
                let meta = match serde_json::to_string(&meta) {
                    Ok(meta) => meta,
                    Err(error) => {
                        return self.fail(format!("JSON_OBJECT key encoding failed: {error}"));
                    }
                };
                args.insert(0, string_literal(&meta));
                *expr = function_call("json_object", args);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Take ownership of a nested expression, leaving a neutral placeholder that
/// is immediately discarded with the parent node.
fn take_expr(expr: &mut Expr) -> Expr {
    std::mem::replace(expr, Expr::Value(Value::Null.into()))
}

fn string_literal(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

/// Build `name(args)` as an ordinary sqlparser function call. DataFusion's
/// generic function path resolves the lowercase name through the schema
/// provider's registry, where the SQL/JSON kernels are registered.
fn function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sql;

    fn lowered(sql: &str) -> String {
        let mut statements = parse_sql(sql).unwrap();
        rewrite_statement(&mut statements[0]).unwrap();
        statements[0].to_string()
    }

    #[test]
    fn exists_value_query_become_function_calls() {
        let text = lowered(
            "SELECT JSON_EXISTS(d, '$.a'), JSON_VALUE(d, '$.b'), JSON_QUERY(d, '$.c') FROM t",
        );
        assert!(text.contains("json_exists(d, '$.a')"), "{text}");
        assert!(text.contains("json_value(d, '$.b')"), "{text}");
        assert!(text.contains("json_query(d, '$.c')"), "{text}");
        assert!(!text.contains("JSON_EXISTS"), "{text}");
    }

    #[test]
    fn returning_selects_the_typed_function_name() {
        let text = lowered(
            "SELECT JSON_VALUE(d, '$.a' RETURNING BOOLEAN), JSON_VALUE(d, '$.b' RETURNING DOUBLE PRECISION), JSON_VALUE(d, '$.c' RETURNING VARCHAR) FROM t",
        );
        assert!(text.contains("json_value_boolean(d, '$.a')"), "{text}");
        assert!(text.contains("json_value_double(d, '$.b')"), "{text}");
        assert!(text.contains("json_value(d, '$.c')"), "{text}");
    }

    #[test]
    fn object_pairs_carry_order_and_format_flags_in_metadata() {
        let text = lowered(
            "SELECT JSON_OBJECT('source' VALUE 'google', 'medium' VALUE '', 'accounts' VALUE JSON_QUERY(d, '$.a') FORMAT JSON) FROM t",
        );
        // The metadata literal lists keys in source order with FORMAT flags.
        assert!(
            text.contains(r#"json_object('[["source",false],["medium",false],["accounts",true]]'"#),
            "{text}"
        );
        // The nested JSON_QUERY was lowered first and passed as a value arg.
        assert!(text.contains("json_query(d, '$.a')"), "{text}");
    }

    #[test]
    fn empty_object_lowers_to_metadata_only() {
        let text = lowered("SELECT JSON_OBJECT() FROM t");
        assert!(text.contains(r#"json_object('[]')"#), "{text}");
    }

    #[test]
    fn nesting_inside_case_and_where_is_rewritten() {
        let text = lowered(
            "SELECT CASE WHEN JSON_EXISTS(d, '$.a') THEN JSON_VALUE(d, '$.b') END FROM t WHERE JSON_EXISTS(d, '$.c')",
        );
        assert!(text.contains("json_exists(d, '$.a')"), "{text}");
        assert!(text.contains("json_value(d, '$.b')"), "{text}");
        assert!(text.contains("json_exists(d, '$.c')"), "{text}");
    }

    #[test]
    fn invalid_path_is_a_planner_diagnostic_naming_path_and_segment() {
        let mut statements = parse_sql("SELECT JSON_VALUE(d, '$.a[0]') FROM t").unwrap();
        let error = rewrite_statement(&mut statements[0]).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("$.a[0]"), "{message}");
        assert!(message.contains("array wildcard"), "{message}");
    }

    #[test]
    fn duplicate_object_keys_are_a_planner_diagnostic() {
        let mut statements =
            parse_sql("SELECT JSON_OBJECT('a' VALUE 1, 'a' VALUE 2) FROM t").unwrap();
        let error = rewrite_statement(&mut statements[0]).unwrap_err();
        assert!(
            error.to_string().contains("duplicate JSON_OBJECT key 'a'"),
            "{error}"
        );
    }

    #[test]
    fn distinct_keys_are_accepted() {
        let text = lowered("SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 2) FROM t");
        assert!(text.contains(r#"[["a",false],["b",false]]"#), "{text}");
    }

    #[test]
    fn expr_rewrite_covers_stored_merge_expressions() {
        let mut statements = parse_sql("SELECT JSON_QUERY(d, '$.x')").unwrap();
        let Statement::Query(query) = &mut statements[0] else {
            panic!("expected SELECT");
        };
        let sqlparser::ast::SetExpr::Select(select) = &mut *query.body else {
            panic!("expected SELECT body");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &mut select.projection[0] else {
            panic!("expected unnamed projection");
        };
        rewrite_expr(expr).unwrap();
        assert!(
            expr.to_string().starts_with("json_query(d, '$.x')"),
            "{expr}"
        );
    }

    #[test]
    fn rewrite_is_a_no_op_without_json_functions() {
        let text = lowered("SELECT a + 1 FROM t WHERE b > 2");
        assert!(text.contains("a + 1"), "{text}");
    }

    #[test]
    fn object_keys_with_quotes_roundtrip_through_metadata() {
        // The parser unescapes '' inside single quotes; the metadata JSON
        // carries the raw key and Display re-escapes it for SQL text.
        let text = lowered("SELECT JSON_OBJECT('it''s' VALUE 1) FROM t");
        assert!(text.contains("json_object('"), "{text}");
        assert!(text.contains("it''s"), "{text}");
    }

    #[test]
    fn plan_error_not_used_for_valid_lax_paths() {
        // Lax missing members are runtime-empty, not plan errors.
        let text = lowered("SELECT JSON_VALUE(d, '$.missing.deeper') FROM t");
        assert!(text.contains("json_value(d, '$.missing.deeper')"), "{text}");
    }

    #[test]
    fn explicit_mode_paths_are_rejected() {
        let mut statements = parse_sql("SELECT JSON_VALUE(d, 'LAX $.a') FROM t").unwrap();
        let error = rewrite_statement(&mut statements[0]).unwrap_err();
        assert!(error.to_string().contains("LAX/STRICT"), "{error}");
    }
}
