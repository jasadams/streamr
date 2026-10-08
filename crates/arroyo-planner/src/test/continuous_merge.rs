use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::{FusedStateTableOperator, StateTableOperator};
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
        .filter(|(operator, _)| operator.operator_name == OperatorName::FusedStateTable)
        .flat_map(|(operator, _)| {
            FusedStateTableOperator::decode(operator.operator_config.as_slice())
                .unwrap()
                .steps
                .into_iter()
                .filter(|step| step.kind == "state_access")
                .map(|step| StateTableOperator::decode(step.operator_config.as_slice()).unwrap())
                .collect::<Vec<_>>()
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
    let graph_operators = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| {
            node.operator_chain
                .iter()
                .map(|(operator, _)| operator.operator_name)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        graph_operators
            .iter()
            .filter(|name| **name == OperatorName::FusedStateTable)
            .count(),
        1
    );
    assert!(graph_operators.contains(&OperatorName::StateTableCapture));
    assert!(!graph_operators.contains(&OperatorName::StateTable));
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
async fn captured_merge_can_feed_an_updating_grouped_aggregate() {
    let query = format!(
        "{DECLARATIONS}
         CREATE VIEW applied AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter
         WHEN NOT MATCHED THEN INSERT (counter, quantity) VALUES (source.counter, 1)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         CREATE VIEW flags AS SELECT source.counter AS counter,
           CASE WHEN action = 'insert' THEN CAST(1 AS BIGINT)
                ELSE CAST(0 AS BIGINT) END AS started FROM applied;
         SELECT counter, SUM(started) AS total FROM flags GROUP BY counter"
    );
    let compiled = plan(&query).await;
    let names = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator.operator_name)
        .collect::<Vec<_>>();
    assert_eq!(state_operators(&compiled).len(), 1);
    assert_eq!(
        names
            .iter()
            .filter(|name| **name == OperatorName::FusedStateTable)
            .count(),
        1
    );
    assert!(names.contains(&OperatorName::StateTableCapture));
    assert!(names.contains(&OperatorName::ArrowKey));
    assert!(names.contains(&OperatorName::UpdatingAggregate));
    assert!(!names.contains(&OperatorName::StateTable));
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
        let input: arrow_schema::Schema =
            serde_json::from_str(&configs[0].input_schema.as_ref().unwrap().arrow_schema).unwrap();
        let output: arrow_schema::Schema =
            serde_json::from_str(&configs[0].output_schema.as_ref().unwrap().arrow_schema).unwrap();
        let event_input_index = configs[0].input_schema.as_ref().unwrap().timestamp_index as usize;
        let event_output_index =
            configs[0].output_schema.as_ref().unwrap().timestamp_index as usize;
        assert_eq!(
            output.field(event_output_index),
            input.field(event_input_index)
        );
        let actual_non_timestamp = output
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != event_output_index)
            .map(|(_, field)| field.name().as_str())
            .collect::<Vec<_>>();
        let mut expected_non_timestamp = input
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != event_input_index)
            .map(|(_, field)| field.name().as_str())
            .collect::<Vec<_>>();
        expected_non_timestamp.extend(["counter", "quantity"]);
        assert_eq!(actual_non_timestamp, expected_non_timestamp);
    }
}

#[test(tokio::test)]
async fn target_timestamp_does_not_replace_the_event_timestamp() {
    let query = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE retained (counter BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY counter;
        SELECT events.counter, target._timestamp AS retained_time FROM events LEFT JOIN retained AS target
        ON events.counter = target.counter";
    let configs = state_operators(&plan(query).await);
    assert_eq!(configs.len(), 1);
    let output = configs[0].output_schema.as_ref().unwrap();
    let schema: arrow_schema::Schema = serde_json::from_str(&output.arrow_schema).unwrap();
    let timestamps = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field.name() == "_timestamp").then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(
        timestamps.len(),
        2,
        "lookup retains target and event timestamps"
    );
    assert_eq!(
        output.timestamp_index as usize,
        *timestamps.last().unwrap(),
        "event timestamp must be the appended field for the impulse source"
    );
}

#[test(tokio::test)]
async fn unaliased_retained_timestamp_cannot_shadow_event_time() {
    let query = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE retained (counter BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY counter;
        SELECT target._timestamp FROM events LEFT JOIN retained AS target
        ON events.counter = target.counter";
    reject(query, "alias the retained value").await;
    let computed = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE retained (counter BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY counter;
        SELECT COALESCE(target._timestamp, target._timestamp) AS _timestamp
        FROM events LEFT JOIN retained AS target ON events.counter = target.counter";
    reject(computed, "alias the retained value").await;
}

#[test(tokio::test)]
async fn merge_new_timestamp_cannot_shadow_event_time() {
    let declarations = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE retained (counter BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY counter;
        CREATE VIEW applied AS MERGE INTO retained AS target USING events AS source
        ON target.counter = source.counter
        WHEN NOT MATCHED THEN INSERT (counter, _timestamp)
        VALUES (source.counter, CAST('2020-01-01' AS TIMESTAMP))
        RETURNING source AS source, old AS old, new AS new, action AS action;";
    reject(
        &format!("{declarations} SELECT new._timestamp AS _timestamp FROM applied"),
        "alias the retained value",
    )
    .await;
    let accepted = plan(&format!(
        "{declarations} SELECT new._timestamp AS stored_time FROM applied"
    ))
    .await;
    assert_eq!(state_operators(&accepted).len(), 1);
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
async fn lookup_then_dependent_merges_keep_event_ownership() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE limits (counter BIGINT PRIMARY KEY, allowed BOOLEAN)
         PARTITION BY counter;
         CREATE STATE TABLE totals (counter BIGINT PRIMARY KEY, quantity BIGINT)
         PARTITION BY counter;
         CREATE VIEW looked AS SELECT events.counter AS event_counter, limits.allowed
         FROM events LEFT JOIN limits ON events.counter = limits.counter;
         CREATE VIEW written AS MERGE INTO inventory AS target USING looked AS source
         ON target.counter = source.event_counter
         WHEN NOT MATCHED THEN INSERT (counter, quantity)
         VALUES (source.event_counter, 1)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         CREATE VIEW next_events AS SELECT written.source.event_counter AS event_counter
         FROM written;
         CREATE VIEW followed AS MERGE INTO totals AS target USING next_events AS source
         ON target.counter = source.event_counter
         WHEN NOT MATCHED THEN INSERT (counter, quantity)
         VALUES (source.event_counter, 1)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM followed"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs.len(), 3);
    assert!(
        configs
            .iter()
            .all(|config| config.event_scope_id == configs[0].event_scope_id)
    );
    assert!(
        configs
            .iter()
            .all(|config| config.ownership_bindings == configs[0].ownership_bindings)
    );
}

#[test(tokio::test)]
async fn materialized_uuid_value_is_shared_by_dependent_merges_and_fanout() {
    let query = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, candidate TEXT)
        PARTITION BY counter;
        CREATE STATE TABLE observed (counter BIGINT PRIMARY KEY, candidate TEXT)
        PARTITION BY counter;
        CREATE VIEW candidates AS SELECT events.counter AS counter,
          uuid() AS candidate FROM events;
        CREATE VIEW before_write AS SELECT c.counter, c.candidate,
          inventory.candidate AS prior
          FROM candidates AS c LEFT JOIN inventory
          ON c.counter = inventory.counter;
        CREATE VIEW first_write AS MERGE INTO inventory AS target
          USING before_write AS source ON target.counter = source.counter
          WHEN NOT MATCHED THEN INSERT (counter, candidate)
          VALUES (source.counter, source.candidate)
          RETURNING source AS source, old AS old, new AS new, action AS action;
        CREATE VIEW forwarded AS SELECT r.source.counter AS counter,
          r.source.candidate AS candidate FROM first_write AS r;
        CREATE VIEW second_write AS MERGE INTO observed AS target
          USING forwarded AS source ON target.counter = source.counter
          WHEN NOT MATCHED THEN INSERT (counter, candidate)
          VALUES (source.counter, source.candidate)
          RETURNING source AS source, old AS old, new AS new, action AS action;
        SELECT source.counter, source.candidate, new.candidate FROM first_write;
        SELECT source.counter, source.candidate, new.candidate FROM second_write";
    let compiled = plan(query).await;
    let graph = &compiled.program.graph;
    let owners = graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operation, _)| operation.operator_name == OperatorName::FusedStateTable)
        .map(|(operation, _)| {
            FusedStateTableOperator::decode(operation.operator_config.as_slice()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(owners.len(), 1);
    let owner = &owners[0];
    assert_eq!(
        owner
            .steps
            .iter()
            .filter(|step| step.kind == "state_access")
            .count(),
        3
    );
    assert!(
        owner.capture_schemas.len() >= 2,
        "both outputs must be captured"
    );
    let uuid_producers = graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operation, _)| operation.operator_name == OperatorName::ArrowValue)
        .filter(|(operation, _)| {
            operation
                .operator_config
                .windows(4)
                .any(|bytes| bytes == b"uuid")
        })
        .count()
        + owner
            .steps
            .iter()
            .filter(|step| {
                step.operator_config
                    .windows(4)
                    .any(|bytes| bytes == b"uuid")
            })
            .count();
    assert_eq!(
        uuid_producers, 1,
        "one UUID-producing projection per input event"
    );
    let accesses = state_operators(&compiled);
    assert!(
        accesses
            .iter()
            .all(|access| access.ownership_bindings == accesses[0].ownership_bindings)
    );
}

#[test(tokio::test)]
async fn uuid_cannot_define_state_ownership_or_a_fused_filter() {
    let key = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
      CREATE STATE TABLE generated (id TEXT PRIMARY KEY, candidate TEXT) PARTITION BY id;
      CREATE VIEW applied AS MERGE INTO generated AS target USING events AS source
      ON target.id = uuid()
      WHEN NOT MATCHED THEN INSERT (id, candidate) VALUES (uuid(), 'value')
      RETURNING source AS source, old AS old, new AS new, action AS action;
      SELECT action FROM applied";
    reject(
        key,
        "volatile functions cannot define any state-table primary-key expression",
    )
    .await;
    let filter = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
      CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, candidate TEXT)
      PARTITION BY counter;
      CREATE VIEW filtered AS SELECT counter FROM events WHERE uuid() <> '';
      CREATE VIEW applied AS MERGE INTO inventory AS target USING filtered AS source
      ON target.counter = source.counter
      WHEN NOT MATCHED THEN INSERT (counter, candidate) VALUES (source.counter, 'value')
      RETURNING source AS source, old AS old, new AS new, action AS action;
      SELECT action FROM applied";
    reject(filter, "unqualified purity or allocation bounds").await;
}

#[test(tokio::test)]
async fn lookup_target_alias_cannot_define_later_merge_ownership() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE limits (counter BIGINT PRIMARY KEY, allowed BOOLEAN)
         PARTITION BY counter;
         CREATE VIEW looked AS SELECT limits.counter AS event_counter
         FROM events LEFT JOIN limits ON events.counter = limits.counter;
         CREATE VIEW written AS MERGE INTO inventory AS target USING looked AS source
         ON target.counter = source.event_counter
         WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM written"
    );
    reject(
        &query,
        "state ownership must be bound to captured source fields",
    )
    .await;
}

#[test(tokio::test)]
async fn previous_merge_target_cannot_define_followup_ownership() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE totals (counter BIGINT PRIMARY KEY, quantity BIGINT)
         PARTITION BY counter;
         CREATE VIEW written AS MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         CREATE VIEW next_events AS SELECT written.old.quantity AS event_counter
         FROM written;
         CREATE VIEW followed AS MERGE INTO totals AS target USING next_events AS source
         ON target.counter = source.event_counter WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM followed"
    );
    reject(&query, "state ownership must use the captured MERGE source").await;
}

#[test(tokio::test)]
async fn lookup_timestamp_lineage_distinguishes_event_from_same_named_target_field() {
    let declarations = "CREATE TABLE events (
          event_time TIMESTAMP NOT NULL, counter BIGINT NOT NULL
        ) WITH (connector = 'single_file', path = '/tmp/streamr-planner-events.json',
          format = 'json', type = 'source', event_time_field = 'event_time');
        CREATE STATE TABLE retained (event_time TIMESTAMP PRIMARY KEY, _timestamp TIMESTAMP)
        PARTITION BY event_time;
        CREATE STATE TABLE timed (event_time TIMESTAMP PRIMARY KEY, quantity BIGINT)
        PARTITION BY event_time;";
    let looked = "FROM events LEFT JOIN retained AS target
        ON events.event_time = target.event_time";
    let source_event = format!(
        "{declarations}
         CREATE VIEW looked AS SELECT events.event_time AS event_time,
         target._timestamp AS stored_time {looked};
         CREATE VIEW written AS MERGE INTO timed AS target USING looked AS source
         ON target.event_time = source.event_time WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM written"
    );
    let configs = state_operators(&plan(&source_event).await);
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].ownership_bindings, configs[1].ownership_bindings);

    let target_alias = format!(
        "{declarations}
         CREATE VIEW looked AS SELECT target._timestamp AS event_time {looked};
         CREATE VIEW written AS MERGE INTO timed AS target USING looked AS source
         ON target.event_time = source.event_time WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM written"
    );
    reject(
        &target_alias,
        "state ownership must be bound to captured source fields",
    )
    .await;
}

#[test(tokio::test)]
async fn second_lookup_keeps_appended_event_timestamp_owned_by_source() {
    let query = format!(
        "{DECLARATIONS}
         CREATE STATE TABLE limits (counter BIGINT PRIMARY KEY, allowed BOOLEAN)
         PARTITION BY counter;
         CREATE STATE TABLE flags (counter BIGINT PRIMARY KEY, enabled BOOLEAN)
         PARTITION BY counter;
         CREATE VIEW looked AS SELECT events.counter AS event_counter
         FROM events LEFT JOIN limits ON events.counter = limits.counter
         LEFT JOIN flags ON events.counter = flags.counter;
         CREATE VIEW written AS MERGE INTO inventory AS target USING looked AS source
         ON target.counter = source.event_counter WHEN MATCHED THEN DELETE
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT action FROM written"
    );
    let configs = state_operators(&plan(&query).await);
    assert_eq!(configs.len(), 3);
    assert!(
        configs
            .iter()
            .all(|config| config.ownership_bindings == configs[0].ownership_bindings)
    );
    let lookups = configs
        .iter()
        .filter(|config| config.lookup_join_type.is_some())
        .collect::<Vec<_>>();
    assert_eq!(lookups.len(), 2);
    assert!(lookups.iter().all(|lookup| {
        let output = lookup.output_schema.as_ref().unwrap();
        let schema: arrow_schema::Schema = serde_json::from_str(&output.arrow_schema).unwrap();
        output.timestamp_index as usize == schema.fields().len() - 1
    }));
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
