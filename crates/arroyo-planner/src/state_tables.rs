//! Typed state-table declarations. Runtime lowering is supplied by later operators.
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use sqlparser::ast::{ColumnOption, CreateTable, Expr, SqlOption, Statement, TableConstraint};
use sqlparser::dialect::ArroyoDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::types::convert_data_type;

/// Catalog metadata, independent of backend choice and runtime row encoding.
#[derive(Clone, Debug)]
pub struct StateTable {
    pub name: String,
    /// Canonical identifier values; quoting and dots never merge components.
    pub name_components: Vec<String>,
    pub table_identity: String,
    pub schema_identity: String,
    pub schema: SchemaRef,
    pub primary_key: Vec<usize>,
    pub partition_key: Vec<usize>,
    pub parallelism: usize,
}

#[derive(Debug)]
pub(crate) enum SqlStatement {
    Ordinary(Statement),
    StateTable(CreateTable),
    NamedMerge(crate::continuous_merge::NamedMerge),
}

/// Extend top-level syntax with tokens; column/type/constraint parsing remains
/// the existing Arroyo parser's responsibility.
pub(crate) fn parse_statements(sql: &str) -> std::result::Result<Vec<SqlStatement>, ParserError> {
    let dialect = ArroyoDialect {};
    // Hive's earlier PARTITIONED BY parser expects typed partition columns and
    // otherwise intercepts Arroyo's ownership spelling. Normalize this token
    // alias only inside state declarations before delegating column parsing.
    let mut tokens = Tokenizer::new(&dialect, sql).tokenize_with_location()?;
    let significant = tokens
        .iter()
        .enumerate()
        .filter_map(|(i, t)| (!matches!(t.token, Token::Whitespace(_))).then_some(i))
        .collect::<Vec<_>>();
    let mut state_declaration = false;
    let mut depth = 0usize;
    for (position, &index) in significant.iter().enumerate() {
        let next = significant.get(position + 1).map(|&i| &tokens[i].token);
        match &tokens[index].token {
            Token::Word(w) if depth == 0 && w.keyword == Keyword::CREATE => {
                state_declaration = matches!(next, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("state"));
            }
            Token::Word(w)
                if state_declaration
                    && depth == 0
                    && w.keyword == Keyword::PARTITIONED
                    && matches!(next, Some(Token::Word(w)) if w.keyword == Keyword::BY) =>
            {
                let Token::Word(word) = &mut tokens[index].token else {
                    unreachable!()
                };
                word.value = "PARTITION".into();
                word.keyword = Keyword::PARTITION;
            }
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::SemiColon if depth == 0 => state_declaration = false,
            _ => {}
        }
    }
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let mut statements = vec![];
    loop {
        while parser.consume_token(&Token::SemiColon) {}
        if parser.peek_token().token == Token::EOF {
            return Ok(statements);
        }
        let is_state = matches!(parser.peek_token().token, Token::Word(ref w) if w.keyword == Keyword::CREATE)
            && matches!(parser.peek_nth_token(1).token, Token::Word(ref w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("state"));
        if crate::continuous_merge::is_named_merge(&parser) {
            statements.push(SqlStatement::NamedMerge(
                crate::continuous_merge::parse_named_merge(&mut parser)?,
            ));
        } else if is_state {
            parser.expect_keyword(Keyword::CREATE)?;
            parser.next_token(); // STATE is an extension keyword, not an identifier.
            parser.expect_keyword(Keyword::TABLE)?;
            let Statement::CreateTable(mut table) =
                parser.parse_create_table(false, false, None, false)?
            else {
                unreachable!("parse_create_table returns a table declaration")
            };
            if parser.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
                if table.arroyo_partitions.is_some() {
                    return Err(ParserError::ParserError(
                        "state table declares partition ownership twice".into(),
                    ));
                }
                let exprs = parser.parse_comma_separated(|p| p.parse_expr())?;
                table.arroyo_partitions = Some(
                    exprs
                        .into_iter()
                        .flat_map(|expr| match expr {
                            Expr::Tuple(exprs) => exprs,
                            expr => vec![expr],
                        })
                        .collect(),
                );
                let options = parser.parse_options(Keyword::WITH)?;
                if !options.is_empty() {
                    table.with_options.extend(options);
                }
            }
            statements.push(SqlStatement::StateTable(table));
        } else {
            statements.push(SqlStatement::Ordinary(parser.parse_statement()?));
        }
        if parser.peek_token().token != Token::EOF && !parser.consume_token(&Token::SemiColon) {
            return Err(ParserError::ParserError(format!(
                "expected end of statement, found {}",
                parser.peek_token().token
            )));
        }
    }
}

pub(crate) fn object_name_components(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|part| {
            let ident = part
                .as_ident()
                .expect("SQL object names contain identifiers");
            unicase::UniCase::new(&ident.value).to_folded_case()
        })
        .collect()
}

pub(crate) fn reference_components(name: &datafusion::common::TableReference) -> Vec<String> {
    name.to_vec()
        .into_iter()
        .map(|part| unicase::UniCase::new(part).to_folded_case())
        .collect()
}

impl StateTable {
    /// Related reads/writes require the same ordered ownership key and fixed
    /// parallelism. This validates declarations, not runtime scheduling.
    pub fn validate_partition_compatibility(&self, other: &Self) -> Result<()> {
        let ownership_fields = |table: &Self| {
            table
                .partition_key
                .iter()
                .map(|&i| {
                    (
                        table.schema.field(i).name().clone(),
                        table.schema.field(i).data_type().clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        if self.parallelism != other.parallelism
            || ownership_fields(self) != ownership_fields(other)
        {
            return plan_err!(
                "state tables '{}' and '{}' have incompatible partition ownership",
                self.name,
                other.name
            );
        }
        Ok(())
    }

    pub(crate) fn from_declaration(table: &CreateTable, parallelism: usize) -> Result<Self> {
        if parallelism != 1 {
            return plan_err!(
                "state tables require fixed parallelism 1; configured parallelism is {parallelism}"
            );
        }
        // Reject every unimplemented modifier instead of silently accepting it.
        let supported = CreateTableBuilder::new(table.name.clone())
            .columns(table.columns.clone())
            .constraints(table.constraints.clone())
            .with_options(table.with_options.clone())
            .hive_formats(Some(Default::default()))
            .arroyo_partitions(table.arroyo_partitions.clone())
            .build();
        if supported != Statement::CreateTable(table.clone()) {
            return plan_err!(
                "unsupported CREATE STATE TABLE modifier; only typed columns, PRIMARY KEY and PARTITION BY are supported"
            );
        }
        if !table.with_options.is_empty() {
            let option = match &table.with_options[0] {
                SqlOption::KeyValue { key, .. } => key.to_string(),
                option => option.to_string(),
            };
            return plan_err!(
                "state tables do not accept WITH option '{option}'; backend and limits are configured outside SQL"
            );
        }
        if table.columns.is_empty() {
            return plan_err!("state table requires typed columns");
        }
        let name_components = object_name_components(&table.name);
        let name = name_components.join(".");
        let mut key_declarations = vec![];
        for constraint in &table.constraints {
            match constraint {
                TableConstraint::PrimaryKey {
                    columns,
                    index_name: None,
                    index_type: None,
                    index_options,
                    characteristics: None,
                    ..
                } if index_options.is_empty() => {
                    key_declarations.push(
                        columns
                            .iter()
                            .map(|c| unicase::UniCase::new(&c.value).to_folded_case())
                            .collect::<Vec<_>>(),
                    );
                }
                other => return plan_err!("unsupported state table constraint '{other}'"),
            }
        }
        let mut fields = vec![];
        let mut names = HashSet::new();
        for column in &table.columns {
            let column_name = unicase::UniCase::new(&column.name.value).to_folded_case();
            if !names.insert(column_name.clone()) {
                return plan_err!("duplicate state table column '{column_name}'");
            }
            for option in &column.options {
                match &option.option {
                    ColumnOption::Null | ColumnOption::NotNull => {}
                    ColumnOption::Unique {
                        is_primary: true,
                        characteristics: None,
                    } => key_declarations.push(vec![column_name.clone()]),
                    other => return plan_err!("unsupported state table column option '{other}'"),
                }
            }
            let (data_type, extension) = convert_data_type(&column.data_type)?;
            fields.push(arroyo_types::ArroyoExtensionType::add_metadata(
                extension,
                Field::new(
                    column_name,
                    data_type,
                    !column
                        .options
                        .iter()
                        .any(|o| matches!(o.option, ColumnOption::NotNull)),
                ),
            ));
        }
        if key_declarations.len() != 1 || key_declarations[0].is_empty() {
            return plan_err!("state table requires exactly one non-empty PRIMARY KEY declaration");
        }
        let resolve = |names: &[String]| -> Result<Vec<usize>> {
            let mut seen = HashSet::new();
            names
                .iter()
                .map(|name| {
                    if !seen.insert(name) {
                        return plan_err!("duplicate state key column '{name}'");
                    }
                    fields
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| plan_datafusion_err!("unknown state key column '{name}'"))
                })
                .collect()
        };
        let primary_key = resolve(&key_declarations[0])?;
        let partition_names = table
            .arroyo_partitions
            .as_ref()
            .ok_or_else(|| {
                plan_datafusion_err!("state table requires explicit PARTITION BY ownership")
            })?
            .iter()
            .map(|expr| {
                let mut column = expr;
                while let Expr::Nested(inner) = column {
                    column = inner.as_ref();
                }
                match column {
                    Expr::Identifier(id) => Ok(unicase::UniCase::new(&id.value).to_folded_case()),
                    _ => plan_err!("state partition keys must be column names"),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let partition_key = resolve(&partition_names)?;
        if partition_key.is_empty() || partition_key.iter().any(|i| !primary_key.contains(i)) {
            return plan_err!("state partition key must be a non-empty subset of PRIMARY KEY");
        }
        for &index in &primary_key {
            let column = &table.columns[index];
            if column
                .options
                .iter()
                .any(|o| matches!(o.option, ColumnOption::Null))
            {
                return plan_err!(
                    "state primary key column '{}' cannot declare NULL",
                    fields[index].name()
                );
            }
            if !matches!(
                fields[index].data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::Date32
                    | DataType::Timestamp(..)
                    | DataType::Decimal128(..)
            ) || !fields[index].metadata().is_empty()
            {
                return plan_err!(
                    "unsupported state primary key type for column '{}': {:?}",
                    fields[index].name(),
                    fields[index].data_type()
                );
            }
        }
        for &index in &primary_key {
            fields[index] = fields[index].clone().with_nullable(false);
        }
        // Hash a deterministic typed description, never a randomized metadata map.
        let encoding = serde_json::to_vec(&(
            fields
                .iter()
                .map(|f| {
                    (
                        f.name(),
                        f.data_type(),
                        f.is_nullable(),
                        f.metadata()
                            .iter()
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    )
                })
                .collect::<Vec<_>>(),
            &primary_key,
            &partition_key,
        ))
        .map_err(|e| plan_datafusion_err!("state schema encoding: {e}"))?;
        let schema_identity = format!(
            "state-schema-v1:{:032x}",
            xxhash_rust::xxh3::xxh3_128(&encoding)
        );
        Ok(Self {
            table_identity: format!(
                "state-table-v1:{}",
                serde_json::to_string(&name_components)
                    .map_err(|e| plan_datafusion_err!("state table identity: {e}"))?
            ),
            name,
            name_components,
            schema_identity,
            schema: Arc::new(Schema::new(fields)),
            primary_key,
            partition_key,
            parallelism,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};

    fn declaration(sql: &str) -> CreateTable {
        match parse_statements(sql).unwrap().remove(0) {
            SqlStatement::StateTable(table) => table,
            _ => panic!("expected state declaration"),
        }
    }

    const VALID: &str = "CREATE STATE TABLE items (tenant TEXT, id BIGINT, value TEXT, PRIMARY KEY (tenant, id)) PARTITION BY tenant";

    #[test]
    fn composite_keys_and_stable_identities() {
        let table = StateTable::from_declaration(&declaration(VALID), 1).unwrap();
        assert_eq!(table.primary_key, vec![0, 1]);
        assert_eq!(table.partition_key, vec![0]);
        assert!(!table.schema.field(0).is_nullable());
        assert!(!table.schema.field(1).is_nullable());
        assert!(table.schema.field(2).is_nullable());
        let explicit = VALID
            .replace("tenant TEXT", "tenant TEXT NOT NULL")
            .replace("id BIGINT", "id BIGINT NOT NULL");
        let same = StateTable::from_declaration(&declaration(&explicit), 1).unwrap();
        assert_eq!(table.schema_identity, same.schema_identity);
        assert_eq!(table.table_identity, same.table_identity);
        let changed = StateTable::from_declaration(
            &declaration(&VALID.replace("value TEXT", "value BIGINT")),
            1,
        )
        .unwrap();
        assert_ne!(table.schema_identity, changed.schema_identity);
        assert_eq!(table.table_identity, changed.table_identity);
    }

    #[test]
    fn declared_partition_compatibility() {
        let table = StateTable::from_declaration(&declaration(VALID), 1).unwrap();
        let compatible =
            StateTable::from_declaration(&declaration(&VALID.replace("items", "other")), 1)
                .unwrap();
        table.validate_partition_compatibility(&compatible).unwrap();
        let incompatible = StateTable::from_declaration(
            &declaration(&VALID.replace("tenant TEXT", "tenant BIGINT")),
            1,
        )
        .unwrap();
        assert!(
            table
                .validate_partition_compatibility(&incompatible)
                .unwrap_err()
                .to_string()
                .contains("incompatible partition ownership")
        );
        for suffix in ["PARTITION BY tenant, id", "PARTITION BY (tenant, id)"] {
            let sql = VALID.replace("PARTITION BY tenant", suffix);
            assert_eq!(
                StateTable::from_declaration(&declaration(&sql), 1)
                    .unwrap()
                    .partition_key,
                vec![0, 1]
            );
        }
    }

    #[test]
    fn singleton_partition_lists_accept_columns_and_reject_computed_expressions() {
        let plain = StateTable::from_declaration(&declaration(VALID), 1).unwrap();
        for suffix in [
            "PARTITION BY (tenant)",
            "PARTITIONED BY (tenant)",
            "PARTITION BY ((tenant))",
        ] {
            let sql = VALID.replace("PARTITION BY tenant", suffix);
            let table = StateTable::from_declaration(&declaration(&sql), 1).unwrap();
            assert_eq!(table.partition_key, vec![0]);
            assert_eq!(table.schema_identity, plain.schema_identity);
        }
        for suffix in ["PARTITION BY (id + 1)", "PARTITIONED BY ((id + 1))"] {
            let sql = VALID.replace("PARTITION BY tenant", suffix);
            assert!(
                StateTable::from_declaration(&declaration(&sql), 1)
                    .unwrap_err()
                    .to_string()
                    .contains("must be column names")
            );
        }
    }

    #[test]
    fn parser_handles_comments_quoted_names_and_existing_partition_syntax() {
        let sql = "-- declaration\nCREATE /* extension */ STATE TABLE \"select\" (\"from\" BIGINT PRIMARY KEY) PARTITIONED BY (\"from\"); SELECT 'CREATE STATE TABLE';";
        let statements = parse_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        let SqlStatement::StateTable(table) = &statements[0] else {
            panic!()
        };
        assert!(StateTable::from_declaration(table, 1).is_ok());
        let ordinary = "CREATE TABLE archive (id BIGINT) PARTITIONED BY (day TEXT)";
        let SqlStatement::Ordinary(extended) = parse_statements(ordinary).unwrap().remove(0) else {
            panic!()
        };
        assert_eq!(extended, crate::parse_sql(ordinary).unwrap().remove(0));
        assert!(parse_statements(&format!("{VALID} SELECT 1")).is_err());
    }

    #[test]
    fn deterministic_declaration_diagnostics() {
        for (sql, expected) in [
            (
                "CREATE STATE TABLE t (id BIGINT) PARTITION BY id",
                "exactly one non-empty PRIMARY KEY",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT NULL PRIMARY KEY) PARTITION BY id",
                "cannot declare NULL",
            ),
            (
                "CREATE STATE TABLE t (id FLOAT PRIMARY KEY) PARTITION BY id",
                "unsupported state primary key type",
            ),
            (
                "CREATE STATE TABLE t (id JSON PRIMARY KEY) PARTITION BY id",
                "unsupported state primary key type",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY, value TEXT) PARTITION BY value",
                "subset of PRIMARY KEY",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY) PARTITION BY missing",
                "unknown state key column",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY) PARTITION BY id + 1",
                "must be column names",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY)",
                "explicit PARTITION BY",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY, ID TEXT) PARTITION BY id",
                "duplicate state table column",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT, PRIMARY KEY (id, id)) PARTITION BY id",
                "duplicate state key column",
            ),
            (
                "CREATE STATE TABLE t (id BIGINT PRIMARY KEY) WITH (backend = 'rocksdb') PARTITION BY id",
                "do not accept WITH option 'backend'",
            ),
            (
                "CREATE STATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY) PARTITION BY id",
                "unsupported CREATE STATE TABLE modifier",
            ),
        ] {
            let error = StateTable::from_declaration(&declaration(sql), 1)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(expected),
                "{sql}: expected {expected}, got {error}"
            );
        }
        assert!(
            StateTable::from_declaration(&declaration(VALID), 4)
                .unwrap_err()
                .to_string()
                .contains("fixed parallelism 1")
        );
    }

    #[test]
    fn catalog_is_distinct_from_intermediate_relations() {
        let mut provider = ArroyoSchemaProvider::default();
        provider
            .register_state_table(&declaration(VALID), 1)
            .unwrap();
        assert!(provider.get_table("items").is_none());
        assert!(provider.get_state_table("ITEMS").is_some());
        assert!(provider.get_state_table("merge_output").is_none());
        assert!(
            provider
                .register_state_table(&declaration(VALID), 1)
                .unwrap_err()
                .to_string()
                .contains("relation 'items' already exists")
        );
    }

    #[test]
    fn quoted_names_share_identity_without_merging_namespace_components() {
        let plain = StateTable::from_declaration(&declaration(VALID), 1).unwrap();
        let quoted =
            StateTable::from_declaration(&declaration(&VALID.replace("items", "\"ITEMS\"")), 1)
                .unwrap();
        assert_eq!(plain.table_identity, quoted.table_identity);
        let qualified =
            StateTable::from_declaration(&declaration(&VALID.replace("items", "public.items")), 1)
                .unwrap();
        let dotted = StateTable::from_declaration(
            &declaration(&VALID.replace("items", "\"public.items\"")),
            1,
        )
        .unwrap();
        assert_ne!(qualified.table_identity, dotted.table_identity);
        let mut provider = ArroyoSchemaProvider::default();
        provider
            .register_state_table(&declaration(&VALID.replace("items", "\"ITEMS\"")), 1)
            .unwrap();
        assert!(provider.get_state_table("items").is_some());
        assert!(provider.get_state_table("\"items\"").is_some());
        provider
            .register_state_table(&declaration(&VALID.replace("items", "\"public.items\"")), 1)
            .unwrap();
        assert!(provider.get_state_table("public.items").is_none());
        assert!(provider.get_state_table("\"public.items\"").is_some());
    }

    #[tokio::test]
    async fn quoted_and_unquoted_collisions_and_read_guards() {
        for declaration_sql in [VALID.to_string(), VALID.replace("items", "\"items\"")] {
            for suffix in ["SELECT * FROM items", "SELECT * FROM \"items\""] {
                let error = parse_and_get_program(
                    &format!("{declaration_sql}; {suffix}"),
                    ArroyoSchemaProvider::new(),
                    SqlConfig {
                        default_parallelism: 1,
                    },
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(error.contains("state-table scans require"), "{error}");
            }
            for suffix in [
                "CREATE TABLE items (id BIGINT)",
                "CREATE TABLE \"items\" (id BIGINT)",
                "CREATE VIEW items AS SELECT counter FROM events",
                "CREATE VIEW \"items\" AS SELECT counter FROM events",
            ] {
                let error = parse_and_get_program(
                    &format!("CREATE TABLE events WITH (connector = 'impulse', event_rate = '1'); {declaration_sql}; {suffix}; SELECT counter FROM events"),
                    ArroyoSchemaProvider::new(),
                    SqlConfig {
                        default_parallelism: 1,
                    },
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(error.contains("already exists"), "{error}");
                let error = parse_and_get_program(
                    &format!("CREATE TABLE events WITH (connector = 'impulse', event_rate = '1'); {suffix}; {declaration_sql}; SELECT counter FROM events"),
                    ArroyoSchemaProvider::new(),
                    SqlConfig {
                        default_parallelism: 1,
                    },
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(error.contains("already exists"), "{error}");
            }
        }
    }

    #[tokio::test]
    async fn dotted_view_names_preserve_namespace_components_in_both_orders() {
        let prefix = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');";
        for (state_name, view_name, collides) in [
            ("public.items", "\"public.items\"", false),
            ("\"public.items\"", "public.items", false),
            ("\"public.items\"", "\"public.items\"", true),
            ("public.items", "\"public\".\"items\"", true),
        ] {
            let state = VALID.replace("items", state_name);
            let view = format!("CREATE VIEW {view_name} AS SELECT counter FROM events");
            for declarations in [format!("{state}; {view}"), format!("{view}; {state}")] {
                let sql = format!("{prefix} {declarations}; SELECT counter FROM events");
                let result = parse_and_get_program(
                    &sql,
                    ArroyoSchemaProvider::new(),
                    SqlConfig {
                        default_parallelism: 1,
                    },
                )
                .await;
                if collides {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("already exists"), "{sql}: {error}");
                } else {
                    result.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn declaration_coexists_with_an_executable_generic_query() {
        let sql = format!(
            "{VALID}; CREATE TABLE events WITH (connector = 'impulse', event_rate = '1'); SELECT counter FROM events"
        );
        parse_and_get_program(
            &sql,
            ArroyoSchemaProvider::new(),
            SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
                .await
                .unwrap_err()
                .to_string()
                .contains("fixed parallelism 1")
        );
    }

    #[tokio::test]
    async fn plan_rejects_unsupported_runtime_and_name_collisions() {
        for (suffix, expected) in [
            ("SELECT * FROM items", "state-table scans require"),
            (
                "INSERT INTO items SELECT 'a', 1, 'b'",
                "INSERT into state table",
            ),
            (
                "UPDATE items SET value = 'b'",
                "MERGE, UPDATE and DELETE are not implemented",
            ),
            (
                "DELETE FROM items",
                "MERGE, UPDATE and DELETE are not implemented",
            ),
            (
                "MERGE INTO items USING items AS src ON items.id = src.id WHEN MATCHED THEN DELETE",
                "MERGE, UPDATE and DELETE are not implemented",
            ),
            (
                "CREATE TABLE items (id BIGINT); SELECT 1",
                "relation 'items' already exists",
            ),
            (
                "CREATE VIEW items AS SELECT 1",
                "relation 'items' already exists",
            ),
        ] {
            let sql = format!("{VALID}; {suffix}");
            let error = parse_and_get_program(
                &sql,
                ArroyoSchemaProvider::new(),
                SqlConfig {
                    default_parallelism: 1,
                },
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(expected),
                "{suffix}: expected {expected}, got {error}"
            );
        }
        let sql = format!("CREATE TABLE items (id BIGINT); {VALID}; SELECT 1");
        assert!(
            parse_and_get_program(
                &sql,
                ArroyoSchemaProvider::new(),
                SqlConfig {
                    default_parallelism: 1
                }
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("relation 'items' already exists")
        );
    }
}
