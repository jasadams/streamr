use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::StateTableOperator;
use prost::Message;
use test_log::test;

const DECLARATIONS: &str = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY counter;";

async fn plan(sql: &str) -> crate::CompiledSql {
    parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("failed to plan: {error}\n{sql}"))
}

async fn reject(sql: &str, expected: &str) {
    let error = parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains(expected),
        "expected {expected:?}, got {error}"
    );
}

fn state_operators(compiled: &crate::CompiledSql) -> Vec<StateTableOperator> {
    compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::StateTable)
        .map(|(operator, _)| {
            StateTableOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .collect()
}

fn merge(action: &str) -> String {
    format!(
        "{DECLARATIONS}
         CREATE VIEW applied AS
         MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter
         {action}
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT source.counter, old.quantity, new.quantity, action FROM applied"
    )
}

#[test(tokio::test)]
async fn named_merge_has_one_captured_effectful_producer() {
    let query = merge(
        "WHEN MATCHED AND source.counter > 0 THEN UPDATE SET quantity = source.counter
         WHEN MATCHED THEN DELETE
         WHEN NOT MATCHED THEN INSERT (counter, quantity) VALUES (source.counter, 1)",
    );
    let compiled = plan(&query).await;
    let operators = state_operators(&compiled);
    assert_eq!(operators.len(), 1);
    let config = &operators[0];
    assert!(config.requires_fused_serial_owner);
    assert_eq!(config.captured_result_name.as_deref(), Some("applied"));
    assert_eq!(config.key_expressions.len(), 1);
    assert_eq!(config.clauses.len(), 3);
    assert_eq!(config.clauses[0].action, "update");
    assert_eq!(config.clauses[1].action, "delete");
    assert_eq!(config.clauses[2].action, "insert");
    let output: arrow_schema::Schema =
        serde_json::from_str(&config.output_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert_eq!(
        output
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["source", "old", "new", "action", "_timestamp"]
    );
    assert!(!output.field(0).is_nullable());
    assert!(output.field(1).is_nullable());
    assert!(output.field(2).is_nullable());
}

#[test(tokio::test)]
async fn merge_rejects_key_mutation_before_execution() {
    let query = merge("WHEN MATCHED THEN UPDATE SET counter = source.counter");
    reject(&query, "cannot update primary-key").await;
}

#[test(tokio::test)]
async fn state_table_cannot_be_scanned_without_event_keyed_join() {
    let query = format!("{DECLARATIONS} SELECT * FROM inventory");
    reject(&query, "state-table scans require").await;
}

#[test(tokio::test)]
async fn captured_merge_is_one_producer_across_two_consumers() {
    let query = format!(
        "{DECLARATIONS}
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter
         WHEN NOT MATCHED THEN INSERT (counter, quantity) VALUES (source.counter, 1)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT source.counter FROM applied;
         SELECT action FROM applied"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].captured_result_name.as_deref(), Some("applied"));
}

#[test(tokio::test)]
async fn unused_merge_remains_a_graph_root() {
    let query = format!(
        "{DECLARATIONS}
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter
         WHEN NOT MATCHED AND FALSE THEN INSERT (counter, quantity) VALUES (source.counter, 1)
         RETURNING source AS source, old AS old, new AS new, action AS action"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].clauses.len(), 1);
    assert!(configs[0].clauses[0].predicate.is_some());
    assert!(configs[0].output_schema.is_some());
}

#[test(tokio::test)]
async fn inner_and_left_join_are_current_row_keyed_lookups() {
    for join in ["INNER", "LEFT"] {
        let query = format!(
            "{DECLARATIONS} SELECT events.counter FROM events {join} JOIN inventory AS target
             ON events.counter = target.counter"
        );
        let configs = state_operators(&plan(&query).await);
        assert_eq!(configs.len(), 1, "{join}");
        assert_eq!(
            configs[0].lookup_join_type.as_deref(),
            Some(if join == "LEFT" { "Left" } else { "Inner" })
        );
        assert_eq!(configs[0].key_expressions.len(), 1);
        assert!(configs[0].clauses.is_empty());
    }
}

#[test(tokio::test)]
async fn composite_key_requires_each_equality_once_and_no_residual() {
    let base = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE pairs (tenant BIGINT, counter BIGINT, quantity BIGINT,
        PRIMARY KEY (tenant, counter)) PARTITION BY tenant;";
    let prefix = format!("{base} SELECT events.counter FROM events LEFT JOIN pairs AS target ON ");
    let accepted =
        format!("{prefix}target.tenant = events.counter AND target.counter = events.counter + 1");
    let configs = state_operators(&plan(&accepted).await);
    assert_eq!(configs[0].key_expressions.len(), 2);
    for (on, message) in [
        ("target.tenant = events.counter", "complete primary key"),
        (
            "target.tenant = events.counter AND target.counter = events.counter AND target.counter = events.counter + 1",
            "more than once",
        ),
        (
            "target.tenant = events.counter AND target.counter = events.counter AND target.quantity > 0",
            "equality against the complete primary key",
        ),
    ] {
        reject(&format!("{prefix}{on}"), message).await;
    }
}

#[test(tokio::test)]
async fn all_primary_key_components_reject_volatile_sources() {
    let query = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE pairs (tenant BIGINT, counter BIGINT,
        PRIMARY KEY (tenant, counter)) PARTITION BY tenant;
        CREATE VIEW applied AS MERGE INTO pairs AS target USING events AS source
        ON target.tenant = source.counter AND target.counter = CAST(RANDOM() * 100 AS BIGINT)
        WHEN MATCHED THEN DELETE
        RETURNING source AS source, old AS old, new AS new, action AS action;
        SELECT action FROM applied";
    reject(query, "volatile functions cannot define").await;
}

#[test(tokio::test)]
async fn nullable_source_key_is_not_filtered_from_merge_input() {
    let query = format!(
        "{DECLARATIONS}
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = CAST(NULL AS BIGINT)
         WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT source.counter, old.quantity, new.quantity, action FROM applied"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs[0].key_expressions.len(), 1);
    assert!(configs[0].output_schema.is_some());
}

#[test(tokio::test)]
async fn empty_string_key_remains_eligible_for_event_lookup() {
    let query = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE labels (label TEXT PRIMARY KEY, quantity BIGINT) PARTITION BY label;
        CREATE VIEW applied AS MERGE INTO labels AS target USING events AS source
        ON target.label = CAST('' AS TEXT)
        WHEN NOT MATCHED THEN INSERT (label, quantity) VALUES (CAST('' AS TEXT), 1)
        RETURNING source AS source, old AS old, new AS new, action AS action;
        SELECT action FROM applied";
    let configs = state_operators(&plan(query).await);
    assert_eq!(configs[0].key_expressions.len(), 1);
}

#[test(tokio::test)]
async fn named_result_resolves_quoted_dotted_names() {
    for (name, reference) in [
        ("sales.applied", "sales.applied"),
        ("\"sales.applied\"", "\"sales.applied\""),
        ("\"SALES.APPLIED\"", "\"sales.applied\""),
    ] {
        let query = format!(
            "{DECLARATIONS}
             CREATE VIEW {name} AS MERGE INTO inventory AS target USING events AS source
             ON target.counter = source.counter WHEN MATCHED THEN DELETE
             RETURNING source AS source, old AS old, new AS new, action AS action;
             SELECT action FROM {reference}"
        );
        assert_eq!(state_operators(&plan(&query).await).len(), 1, "{name}");
    }
}

#[test(tokio::test)]
async fn related_merge_and_lookup_share_one_event_owner() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE limits (counter BIGINT PRIMARY KEY, allowed BOOLEAN)
         PARTITION BY counter;
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT applied.source.counter, limits.allowed FROM applied LEFT JOIN limits
         ON applied.source.counter = limits.counter"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].event_scope_id, configs[1].event_scope_id);
    assert_eq!(configs[0].ownership_bindings, configs[1].ownership_bindings);
}

#[test(tokio::test)]
async fn related_tables_reject_mismatched_ownership_fields() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE limits (tenant BIGINT PRIMARY KEY, allowed BOOLEAN)
         PARTITION BY tenant;
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT applied.source.counter FROM applied LEFT JOIN limits
         ON applied.source.counter = limits.tenant"
    );
    reject(&query, "compatible partition ownership").await;
}
