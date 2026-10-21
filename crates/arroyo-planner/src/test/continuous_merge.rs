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

fn aggregate_merge(settings: &str, aggregate: &str) -> String {
    format!(
        "{settings} {DECLARATIONS}
         CREATE VIEW counts AS SELECT counter, {aggregate} AS quantity FROM events GROUP BY counter;
         CREATE VIEW applied AS MERGE INTO inventory AS target USING counts AS source
         ON target.counter = source.counter
         WHEN MATCHED THEN UPDATE SET quantity = source.quantity
         WHEN NOT MATCHED THEN INSERT (counter,quantity) VALUES (source.counter,source.quantity)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT new.quantity FROM applied;"
    )
}

#[test(tokio::test)]
async fn maintained_merge_uses_separate_native_snapshot_producer() {
    let compiled = plan(&aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)")).await;
    let aggregates = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(
                operator.operator_config.as_slice(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 1);
    assert!(aggregates[0].emit_per_event);
    let output: arrow_schema::Schema =
        serde_json::from_str(&aggregates[0].final_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert!(output.index_of(arroyo_rpc::UPDATING_META_FIELD).is_err());
    assert_eq!(state_operators(&compiled).len(), 1);
}

#[test(tokio::test)]
async fn maintained_merge_does_not_upgrade_an_ordinary_view_consumer() {
    let mut sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)");
    sql.push_str("SELECT counter,quantity FROM counts;");
    let compiled = plan(&sql).await;
    let mut modes = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(
                operator.operator_config.as_slice(),
            )
            .unwrap()
            .emit_per_event
        })
        .collect::<Vec<_>>();
    modes.sort_unstable();
    assert_eq!(modes, [false, true]);
}

#[test(tokio::test)]
async fn maintained_merge_event_clock_uses_the_accepted_trigger() {
    let mut sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)");
    sql.push_str("SELECT EVENT_TIMESTAMP() AS trigger_time FROM applied;");
    plan(&sql).await;
}

#[test(tokio::test)]
async fn maintained_merge_event_clock_rejects_an_overwritten_input_clock() {
    let mut sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)").replace(
        "CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM events GROUP BY counter;",
        "CREATE VIEW replaced AS SELECT counter, CAST('2020-01-01' AS TIMESTAMP) AS _timestamp FROM events;
         CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM replaced GROUP BY counter;",
    );
    sql.push_str("SELECT EVENT_TIMESTAMP() FROM applied;");
    reject(&sql, "triggering timestamp was replaced by a projection").await;
}

#[test(tokio::test)]
async fn maintained_merge_rejects_unresolved_retention_and_other_aggregates() {
    reject(
        &aggregate_merge("", "COUNT(*)"),
        "requires SET updating_ttl = NULL",
    )
    .await;
    reject(
        &aggregate_merge("SET updating_ttl = NULL;", "SUM(counter)"),
        "COUNT/MAX only",
    )
    .await;
    reject(
        &aggregate_merge("SET updating_ttl = NULL;", "COUNT(DISTINCT counter)"),
        "non-distinct COUNT/MAX",
    )
    .await;
}

#[test(tokio::test)]
async fn maintained_merge_rejects_unsupported_event_lineage_and_ordering() {
    let base = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)");
    let view =
        "CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM events GROUP BY counter;";
    let cases = [
        ("join", base.replace(view, "CREATE VIEW counts AS SELECT a.counter, COUNT(*) AS quantity FROM events a JOIN events b ON a.counter=b.counter GROUP BY a.counter;")),
        ("nested", base.replace(view, "CREATE VIEW inner_counts AS SELECT counter, COUNT(*) AS n FROM events GROUP BY counter; CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM inner_counts GROUP BY counter;")),
        ("ordered", aggregate_merge("SET updating_ttl = NULL;", "FIRST_VALUE(counter ORDER BY counter)")),
    ];
    for (case, sql) in cases {
        let result = parse_and_get_program(
            &sql,
            ArroyoSchemaProvider::new(),
            SqlConfig {
                default_parallelism: 1,
            },
        )
        .await;
        let error = result.expect_err(case);
        println!("STR64 unsupported {case}: {error}");
    }
}

#[test(tokio::test)]
async fn maintained_merge_rejects_calendar_results_before_worker_construction() {
    let declarations = "CREATE TABLE events (counter BIGINT NOT NULL, event_time TIMESTAMP NOT NULL,
        WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
        WITH (connector='single_file',path='/tmp/str64-rejected-calendar.jsonl',format='json',type='source');
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY counter;";
    let sql = aggregate_merge(
        "SET updating_ttl = NULL;",
        "COUNT(*) FILTER (WHERE CAST(event_time AS DATE) = EVENT_DATE())",
    )
    .replace(DECLARATIONS, declarations);
    reject(&sql, "no calendar aggregates").await;
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
    let mut compiled = plan(&query).await;
    compiled
        .program
        .optimize(&arroyo_datastream::optimizers::ChainingOptimizer {});
    let configs = state_operators(&compiled);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].captured_result_name.as_deref(), Some("applied"));
    let graph = &compiled.program.graph;
    let roots = graph
        .externals(petgraph::Direction::Incoming)
        .collect::<Vec<_>>();
    assert_eq!(
        roots.len(),
        1,
        "captured consumers must stay attached to their source"
    );
    assert!(graph[roots[0]].operator_chain.is_source());
    let mut reachable = petgraph::visit::Dfs::new(graph, roots[0]);
    let mut count = 0;
    while reachable.next(graph).is_some() {
        count += 1;
    }
    assert_eq!(count, graph.node_count());
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

#[test(tokio::test)]
async fn maintained_merge_correction_preserves_private_operation_marker() {
    let sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)").replace(
        "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');",
        "CREATE TABLE events (counter BIGINT PRIMARY KEY) WITH (connector='kafka',bootstrap_servers='localhost:9092',topic='changes',type='source',format='debezium_json');",
    );
    let compiled = plan(&sql).await;
    for node in compiled.program.graph.node_weights() {
        assert_eq!(node.parallelism, 1);
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name == OperatorName::UpdatingAggregate {
                let config = arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(
                    operator.operator_config.as_slice(),
                )
                .unwrap();
                assert!(config.emit_per_event);
                let input: arroyo_rpc::df::ArroyoSchema =
                    config.input_schema.unwrap().try_into().unwrap();
                assert!(
                    input
                        .schema
                        .index_of(arroyo_rpc::SOURCE_ENVELOPE_FIELD)
                        .is_ok()
                );
                assert!(
                    input
                        .schema
                        .index_of(arroyo_rpc::UPDATING_META_FIELD)
                        .is_ok()
                );
                let output: arroyo_rpc::df::ArroyoSchema =
                    config.final_schema.unwrap().try_into().unwrap();
                assert!(
                    output
                        .schema
                        .index_of(arroyo_rpc::SOURCE_ENVELOPE_FIELD)
                        .is_err()
                );
                assert!(
                    output
                        .schema
                        .index_of(arroyo_rpc::UPDATING_META_FIELD)
                        .is_err()
                );
            }
            if operator.operator_name == OperatorName::ExpressionWatermark {
                let config = arroyo_rpc::grpc::api::ExpressionWatermarkConfig::decode(
                    operator.operator_config.as_slice(),
                )
                .unwrap();
                assert!(config.preserve_source_envelope);
                assert!(config.source_envelope_index.is_some());
            }
        }
    }
}

#[test(tokio::test)]
async fn maintained_merge_correction_keeps_declared_clock_through_alias_projection() {
    let sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)")
        .replace(
            "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');",
            "CREATE TABLE events (counter BIGINT PRIMARY KEY, event_time TIMESTAMP NOT NULL,
             WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
             WITH (connector='single_file',path='/tmp/str64-qualified-correction.jsonl',
             type='source',format='debezium_json');
             CREATE VIEW projected AS SELECT e.counter,e.event_time FROM events AS e
             WHERE e.counter > 0;",
        )
        .replace(
            "AS quantity FROM events GROUP BY",
            "AS quantity FROM projected GROUP BY",
        );
    let sql = format!("{sql} SELECT EVENT_TIMESTAMP() AS original_trigger FROM applied;");
    let compiled = plan(&sql).await;
    assert_eq!(state_operators(&compiled).len(), 1);
}

fn correction_boundary_query(format: &str, peak_type: &str, views: &str) -> String {
    format!("SET updating_ttl = NULL;
        CREATE TABLE changes (row_id BIGINT PRIMARY KEY,k TEXT NOT NULL,
            value BIGINT NOT NULL,active BOOLEAN NOT NULL,event_time TIMESTAMP NOT NULL,
            WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
            WITH (connector='single_file',path='/tmp/str64-boundary.jsonl',format='{format}',type='source');
        CREATE STATE TABLE totals (k TEXT PRIMARY KEY,total BIGINT,peak {peak_type},last_seen TIMESTAMP) PARTITION BY k;
        {views}
        CREATE VIEW applied AS MERGE INTO totals AS previous USING accepted AS incoming
        ON previous.k=incoming.k
        WHEN MATCHED THEN UPDATE SET total=incoming.total,peak=incoming.peak,last_seen=incoming.last_seen
        WHEN NOT MATCHED THEN INSERT(k,total,peak,last_seen) VALUES(incoming.k,incoming.total,incoming.peak,incoming.last_seen)
        RETURNING source AS source,old AS old,new AS new,action AS action;
        SELECT new.total,new.peak FROM applied;")
}
const BOUNDARY_AGGREGATE: &str = "CREATE VIEW accepted AS SELECT k,COUNT(*) AS total,MAX(value) AS peak,MAX(event_time) AS last_seen FROM changes GROUP BY k;";
fn distinct_predicate(sql: String) -> String {
    sql.replace("WHEN MATCHED THEN", "WHEN MATCHED AND (previous.total IS DISTINCT FROM incoming.total OR previous.peak IS DISTINCT FROM incoming.peak OR previous.last_seen IS DISTINCT FROM incoming.last_seen) THEN")
}

#[test(tokio::test)]
async fn str64_correction_boundary_probe_append_distinct_or_control() {
    plan(&distinct_predicate(correction_boundary_query(
        "json",
        "BIGINT",
        BOUNDARY_AGGREGATE,
    )))
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_cdc_distinct_or() {
    plan(&distinct_predicate(correction_boundary_query(
        "debezium_json",
        "BIGINT",
        BOUNDARY_AGGREGATE,
    )))
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_named_volatile_projection() {
    let sql=correction_boundary_query("debezium_json","TEXT", "CREATE VIEW sampled AS SELECT k,uuid() AS candidate,event_time FROM changes;
        CREATE VIEW accepted AS SELECT k,COUNT(*) AS total,MAX(candidate) AS peak,MAX(event_time) AS last_seen FROM sampled GROUP BY k;");
    reject(
        &sql,
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_volatile_where() {
    let views = BOUNDARY_AGGREGATE.replace(
        "FROM changes GROUP BY",
        "FROM changes WHERE random() > 0.5 GROUP BY",
    );
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_volatile_argument() {
    let views = BOUNDARY_AGGREGATE.replace("MAX(value)", "MAX(uuid())");
    reject(
        &correction_boundary_query("debezium_json", "TEXT", &views),
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_volatile_aggregate_filter() {
    let views = BOUNDARY_AGGREGATE.replace("COUNT(*)", "COUNT(*) FILTER (WHERE random() > 0.5)");
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_trigger_where() {
    let views = BOUNDARY_AGGREGATE.replace(
        "FROM changes GROUP BY",
        "FROM changes WHERE EVENT_TIMESTAMP() > TIMESTAMP '1970-01-01 00:00:07' GROUP BY",
    );
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_trigger_alias_where() {
    let views = format!(
        "CREATE VIEW clocked AS SELECT k,value,event_time,EVENT_TIMESTAMP() AS sampled_clock FROM changes; {}",
        BOUNDARY_AGGREGATE.replace(
            "FROM changes GROUP BY",
            "FROM clocked WHERE sampled_clock > TIMESTAMP '1970-01-01 00:00:07' GROUP BY"
        )
    );
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction requires repeatable source expressions",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_overwritten_clock_literal() {
    let views = format!(
        "CREATE VIEW clocked AS SELECT k,value,event_time,TIMESTAMP '1970-01-01 00:00:55' AS _timestamp FROM changes; {}",
        BOUNDARY_AGGREGATE.replace("FROM changes GROUP BY", "FROM clocked GROUP BY")
    );
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction must preserve the source trigger timestamp",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_overwritten_clock_payload() {
    let views = format!(
        "CREATE VIEW clocked AS SELECT k,value,event_time,event_time AS _timestamp FROM changes; {}",
        BOUNDARY_AGGREGATE.replace("FROM changes GROUP BY", "FROM clocked GROUP BY")
    );
    reject(
        &correction_boundary_query("debezium_json", "BIGINT", &views),
        "per-event correction must preserve the source trigger timestamp",
    )
    .await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_append_volatile_control() {
    let views = BOUNDARY_AGGREGATE.replace("MAX(value)", "MAX(uuid())");
    plan(&correction_boundary_query("json", "TEXT", &views)).await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_postaggregate_clock_uuid_control() {
    let sql=correction_boundary_query("debezium_json","BIGINT",BOUNDARY_AGGREGATE).replace("SELECT new.total,new.peak FROM applied;", "SELECT new.total,new.peak,uuid() AS capture_id,EVENT_TIMESTAMP() AS original_trigger FROM applied;");
    plan(&sql).await;
}
#[test(tokio::test)]
async fn str64_correction_boundary_probe_declared_payload_time_control() {
    let views = BOUNDARY_AGGREGATE.replace(
        "FROM changes GROUP BY",
        "FROM changes WHERE event_time > TIMESTAMP '1970-01-01 00:00:07' GROUP BY",
    );
    plan(&correction_boundary_query(
        "debezium_json",
        "BIGINT",
        &views,
    ))
    .await;
}
