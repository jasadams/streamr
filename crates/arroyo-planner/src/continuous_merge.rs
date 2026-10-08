//! Native, event-driven retained table planning. No storage or runtime SQL lives here.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, TableReference, plan_err};
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, Extension, LogicalPlan, Operator};
use datafusion::sql::planner::{PlannerContext, SqlToRel};
use sqlparser::ast::{
    AssignmentTarget, MergeAction, MergeClauseKind, MergeInsertKind, ObjectName, SelectItem,
    Statement, TableFactor,
};
use sqlparser::dialect::ArroyoDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::extension::state_table::{StateTableAccess, StateTableClause, StateTableDescriptor};
use crate::tables::{Table, produce_optimized_plan};
use crate::{ArroyoSchemaProvider, parse_sql, rewrite_plan};

#[derive(Debug)]
pub(crate) struct NamedMerge {
    pub name: ObjectName,
    pub statement: Statement,
    pub returning: Vec<SelectItem>,
}

/// The reserved CREATE VIEW ... AS MERGE grammar extends statements rather than
/// masquerading as a SELECT/UDF. The ordinary parser continues to own MERGE ASTs.
pub(crate) fn is_named_merge(parser: &Parser<'_>) -> bool {
    if !matches!(parser.peek_token().token, Token::Word(ref w) if w.keyword == Keyword::CREATE)
        || !matches!(parser.peek_nth_token(1).token, Token::Word(ref w) if w.keyword == Keyword::VIEW)
    {
        return false;
    }
    let mut i = 2;
    loop {
        match parser.peek_nth_token(i).token {
            Token::EOF | Token::SemiColon => return false,
            Token::Word(ref w) if w.keyword == Keyword::AS => {
                return matches!(parser.peek_nth_token(i + 1).token, Token::Word(ref w) if w.keyword == Keyword::MERGE);
            }
            _ => i += 1,
        }
    }
}

pub(crate) fn parse_named_merge(
    parser: &mut Parser<'_>,
) -> std::result::Result<NamedMerge, ParserError> {
    parser.expect_keyword(Keyword::CREATE)?;
    parser.expect_keyword(Keyword::VIEW)?;
    let name = parser.parse_object_name(false)?;
    parser.expect_keyword(Keyword::AS)?;
    parser.expect_keyword(Keyword::MERGE)?;

    // Arroyo's upstream MERGE parser reads WHEN clauses until EOF/semicolon.
    // Isolate the MERGE body at the final top-level RETURNING token so the
    // original SQL tokens and AST parser handle both halves without rewriting
    // SQL text or treating RETURNING as another WHEN clause.
    let mut depth = 0usize;
    let mut index = 0usize;
    let mut returning = None;
    loop {
        let token = parser.peek_nth_token(index);
        match &token.token {
            Token::EOF | Token::SemiColon => break,
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(word) if depth == 0 && word.keyword == Keyword::RETURNING => {
                returning = Some(index);
            }
            _ => {}
        }
        index += 1;
    }
    let boundary = returning.ok_or_else(|| {
        ParserError::ParserError("named MERGE requires RETURNING source AS source, old AS old, new AS new, action AS action".into())
    })?;
    let tokens = (0..boundary)
        .map(|position| parser.peek_nth_token(position))
        .collect();
    let dialect = ArroyoDialect {};
    let mut merge_parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let statement = merge_parser.parse_merge()?;
    if merge_parser.peek_token().token != Token::EOF {
        return Err(ParserError::ParserError(
            "unsupported tokens after named MERGE clauses".into(),
        ));
    }
    for _ in 0..boundary {
        parser.next_token();
    }
    parser.expect_keyword(Keyword::RETURNING)?;
    let returning = parser.parse_comma_separated(|p| p.parse_select_item())?;
    Ok(NamedMerge {
        name,
        statement,
        returning,
    })
}

fn plain_table(factor: &TableFactor) -> Result<(&ObjectName, TableReference)> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        partitions,
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints,
    } = factor
    else {
        return plan_err!(
            "MERGE target must be a declared state table without scans or table functions"
        );
    };
    if !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
        || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
    {
        return plan_err!("state-table target modifiers are unsupported");
    }
    let qualifier = alias
        .as_ref()
        .map(|a| TableReference::bare(a.name.value.clone()))
        .unwrap_or_else(|| {
            let parts = name
                .0
                .iter()
                .map(|part| {
                    part.as_ident()
                        .expect("table name identifier")
                        .value
                        .clone()
                })
                .collect::<Vec<_>>();
            match parts.as_slice() {
                [table] => TableReference::bare(table.clone()),
                [schema, table] => TableReference::partial(schema.clone(), table.clone()),
                [catalog, schema, table] => {
                    TableReference::full(catalog.clone(), schema.clone(), table.clone())
                }
                _ => TableReference::parse_str(&name.to_string()),
            }
        });
    Ok((name, qualifier))
}

fn expression(
    sql: &sqlparser::ast::Expr,
    schema: &DFSchema,
    provider: &ArroyoSchemaProvider,
) -> Result<Expr> {
    let expr =
        SqlToRel::new(provider).sql_to_expr(sql.clone(), schema, &mut PlannerContext::new())?;
    expr.apply(|expr| {
        match expr {
            Expr::AggregateFunction(_) | Expr::WindowFunction(_) | Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => {
                return plan_err!("MERGE clauses support scalar expressions only; subqueries, aggregates and windows are unsupported");
            }
            Expr::ScalarFunction(f) if crate::rewriters::contains_state_function(expr) || f.func.name().starts_with("sql_state_") => {
                return plan_err!("state UDFs cannot be used inside native MERGE");
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(expr)
}

/// Require exactly one equality binding for every PK field, without residual
/// scans or extra target predicates. Null source key values are handled by the
/// operator as no-match/no-action; they must never be filtered from MERGE input.
pub(crate) fn bind_keys(
    on: &Expr,
    target_schema: &DFSchema,
    input_schema: &DFSchema,
    table: &StateTableDescriptor,
) -> Result<Vec<Expr>> {
    fn equalities<'a>(expr: &'a Expr, result: &mut Vec<(&'a Expr, &'a Expr)>) -> Result<()> {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                equalities(left, result)?;
                equalities(right, result)
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => {
                result.push((left, right));
                Ok(())
            }
            _ => plan_err!(
                "state-table ON requires equality against the complete primary key; scans and residual predicates are unsupported"
            ),
        }
    }
    let mut pairs = vec![];
    equalities(on, &mut pairs)?;
    let mut bindings = HashMap::new();
    for (left, right) in pairs {
        let target_index = |expr: &Expr| -> Option<usize> {
            let Expr::Column(c) = expr else {
                return None;
            };
            // A target key must explicitly bind this target qualifier.
            target_schema.index_of_column(c).ok()
        };
        let (index, source) = match (target_index(left), target_index(right)) {
            (Some(index), None) => (index, right),
            (None, Some(index)) => (index, left),
            _ => {
                return plan_err!(
                    "ambiguous state-table key equality; bind one target primary-key column to one source expression"
                );
            }
        };
        if !table.primary_key.contains(&index) {
            return plan_err!("state-table ON may reference only target primary-key columns");
        }
        if source
            .column_refs()
            .iter()
            .any(|c| input_schema.index_of_column(c).is_err())
        {
            return plan_err!("state-table key expressions must reference only the input event");
        }
        source.apply(|node| {
            if let Expr::ScalarFunction(function) = node
                && function.func.signature().volatility
                    == datafusion::logical_expr::Volatility::Volatile
            {
                return plan_err!(
                    "volatile functions cannot define any state-table primary-key expression"
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if source.get_type(input_schema)? != *table.schema.field(index).data_type() {
            return plan_err!(
                "state-table key expression type must equal the declared primary-key type"
            );
        }
        if bindings.insert(index, source.clone()).is_some() {
            return plan_err!(
                "ambiguous state-table ON: primary-key column is bound more than once"
            );
        }
    }
    table
        .primary_key
        .iter()
        .map(|i| {
            bindings.remove(i).ok_or_else(|| {
                datafusion::common::plan_datafusion_err!(
                    "state-table ON must bind the complete primary key"
                )
            })
        })
        .collect()
}

pub(crate) fn plan_named_merge(
    merge: NamedMerge,
    provider: &ArroyoSchemaProvider,
) -> Result<(Table, LogicalPlan)> {
    let expected = ["source", "old", "new", "action"];
    if merge.returning.len() != expected.len() || !merge.returning.iter().zip(expected).all(|(item, name)| {
        matches!(item, SelectItem::ExprWithAlias { expr: sqlparser::ast::Expr::Identifier(ident), alias } if ident.value == name && alias.value == name)
    }) {
        return plan_err!("native MERGE requires RETURNING source AS source, old AS old, new AS new, action AS action");
    }
    let Statement::Merge {
        table: target,
        source,
        on,
        clauses,
        ..
    } = &merge.statement
    else {
        unreachable!()
    };
    let (target_name, target_alias) = plain_table(target)?;
    let table = provider
        .state_tables
        .get(&crate::state_tables::object_name_components(target_name))
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "MERGE target {target_name} is not a declared state table"
            )
        })?;
    let descriptor = StateTableDescriptor::from(table);
    if clauses.is_empty() {
        return plan_err!("native MERGE requires at least one WHEN clause");
    }
    // Plan/evaluate the source exactly once. Consumers receive captured context,
    // never a separately regenerated source query or a relookup of old/new.
    let source_sql = format!("SELECT * FROM {source}");
    let source_statement = parse_sql(&source_sql)?.remove(0);
    let input = rewrite_plan(
        produce_optimized_plan(&source_statement, provider)?,
        provider,
    )?;
    if input
        .schema()
        .has_column_with_unqualified_name(arroyo_rpc::UPDATING_META_FIELD)
    {
        return plan_err!("native MERGE source must be an append event stream, not a changelog");
    }
    let target_schema =
        DFSchema::try_from_qualified_schema(target_alias, descriptor.schema.as_ref())?;
    let mut fields = input
        .schema()
        .iter()
        .map(|(q, f)| (q.cloned(), f.clone()))
        .collect::<Vec<_>>();
    fields.extend(
        target_schema
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::new(f.as_ref().clone().with_nullable(true)))),
    );
    let expression_schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);
    let on = expression(on, &expression_schema, provider)?;
    let keys = bind_keys(&on, &target_schema, input.schema(), &descriptor)?;
    let mut planned = vec![];
    let mut unconditional = HashSet::new();
    for clause in clauses {
        let matched = match clause.clause_kind {
            MergeClauseKind::Matched => true,
            MergeClauseKind::NotMatched => false,
            _ => {
                return plan_err!(
                    "native MERGE supports WHEN MATCHED and WHEN NOT MATCHED only; BY SOURCE/TARGET scans are unsupported"
                );
            }
        };
        if unconditional.contains(&matched) {
            return plan_err!(
                "unreachable MERGE clause after an unconditional clause of the same match kind"
            );
        }
        if clause.predicate.is_none() {
            unconditional.insert(matched);
        }
        let predicate = clause
            .predicate
            .as_ref()
            .map(|sql| expression(sql, &expression_schema, provider))
            .transpose()?;
        if predicate
            .as_ref()
            .is_some_and(|e| e.get_type(&expression_schema).ok() != Some(DataType::Boolean))
        {
            return plan_err!("MERGE WHEN predicate must be boolean");
        }
        let mut values = vec![];
        let action = match &clause.action {
            MergeAction::Update { assignments } if matched => {
                let mut seen = HashSet::new();
                for assignment in assignments {
                    let AssignmentTarget::ColumnName(name) = &assignment.target else {
                        return plan_err!("MERGE tuple assignments are unsupported");
                    };
                    let components = crate::state_tables::object_name_components(name);
                    let name = components.last().unwrap();
                    if components.len() != 1 {
                        return plan_err!(
                            "MERGE UPDATE columns must be unqualified target column names"
                        );
                    }
                    let index = descriptor
                        .schema
                        .fields()
                        .iter()
                        .position(|f| unicase::UniCase::new(f.name()).to_folded_case() == *name)
                        .ok_or_else(|| {
                            datafusion::common::plan_datafusion_err!(
                                "unknown MERGE UPDATE column {name}"
                            )
                        })?;
                    if descriptor.primary_key.contains(&index) {
                        return plan_err!("MERGE cannot update primary-key columns");
                    }
                    if !seen.insert(index) {
                        return plan_err!("MERGE UPDATE assigns a column twice");
                    }
                    let value = expression(&assignment.value, &expression_schema, provider)?
                        .cast_to(
                            descriptor.schema.field(index).data_type(),
                            &expression_schema,
                        )?;
                    values.push((index, value));
                }
                "update"
            }
            MergeAction::Delete if matched => "delete",
            MergeAction::Insert(insert) if !matched => {
                let MergeInsertKind::Values(rows) = &insert.kind else {
                    return plan_err!(
                        "MERGE INSERT ROW is unsupported; use explicit columns and VALUES"
                    );
                };
                if rows.rows.len() != 1
                    || insert.columns.len() != descriptor.schema.fields().len()
                    || rows.rows[0].len() != insert.columns.len()
                {
                    return plan_err!(
                        "MERGE INSERT requires one VALUES row and every target column exactly once"
                    );
                }
                let mut seen = HashSet::new();
                for (column, sql) in insert.columns.iter().zip(&rows.rows[0]) {
                    let index = descriptor
                        .schema
                        .fields()
                        .iter()
                        .position(|f| {
                            unicase::UniCase::new(f.name()) == unicase::UniCase::new(&column.value)
                        })
                        .ok_or_else(|| {
                            datafusion::common::plan_datafusion_err!(
                                "unknown MERGE INSERT column {column}"
                            )
                        })?;
                    if !seen.insert(index) {
                        return plan_err!("MERGE INSERT specifies a column twice");
                    }
                    let value = expression(sql, &expression_schema, provider)?;
                    if let Some(key_position) =
                        descriptor.primary_key.iter().position(|&i| i == index)
                        && value != keys[key_position]
                    {
                        return plan_err!(
                            "MERGE INSERT primary keys must equal the ON source key bindings"
                        );
                    }
                    values.push((
                        index,
                        value.cast_to(
                            descriptor.schema.field(index).data_type(),
                            &expression_schema,
                        )?,
                    ));
                }
                "insert"
            }
            _ => {
                return plan_err!(
                    "MERGE supports matched UPDATE/DELETE and not-matched INSERT only"
                );
            }
        };
        planned.push(StateTableClause {
            matched,
            predicate,
            action: action.into(),
            values,
        });
    }
    let source_fields = input
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != arroyo_rpc::TIMESTAMP_FIELD)
        .cloned()
        .collect::<Vec<_>>();
    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        Field::new("source", DataType::Struct(source_fields.into()), false),
        Field::new(
            "old",
            DataType::Struct(descriptor.schema.fields().clone()),
            true,
        ),
        Field::new(
            "new",
            DataType::Struct(descriptor.schema.fields().clone()),
            true,
        ),
        Field::new("action", DataType::Utf8, false),
        input
            .schema()
            .field_with_unqualified_name(arroyo_rpc::TIMESTAMP_FIELD)?
            .clone(),
    ]))?);
    let name = merge.name.to_string();
    let access = StateTableAccess::new(
        input,
        descriptor,
        keys,
        expression_schema,
        schema,
        Some(name.clone()),
        planned,
        None,
    )?;
    let logical_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(access),
    });
    Ok((
        Table::TableFromQuery {
            name,
            logical_plan: logical_plan.clone(),
        },
        logical_plan,
    ))
}
