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
async fn maintained_merge_uses_native_capabilities_and_rejects_unresolved_retention_or_distinct() {
    reject(
        &aggregate_merge("", "COUNT(*)"),
        "requires SET updating_ttl = NULL",
    )
    .await;
    plan(&aggregate_merge("SET updating_ttl = NULL;", "SUM(counter)")).await;
    reject(
        &aggregate_merge("SET updating_ttl = NULL;", "COUNT(DISTINCT counter)"),
        "verified non-distinct native result capability",
    )
    .await;
}

#[test(tokio::test)]
async fn maintained_merge_nested_result_uses_one_root_and_terminal_accepted_policy() {
    let base = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)");
    let sql = base.replace(
        "CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM events GROUP BY counter;",
        "CREATE VIEW inner_counts AS SELECT counter, COUNT(*) AS n FROM events GROUP BY counter; CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM inner_counts GROUP BY counter;",
    );
    let compiled = plan(&sql).await;
    let aggregates = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(op, _)| op.operator_name == OperatorName::UpdatingAggregate)
        .map(|(op, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(op.operator_config.as_slice())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 2);
    assert!(aggregates.iter().all(|config| config.response_root_operator
        == aggregates[0].response_root_operator
        && config.response_root_operator.is_some()
        && !config.emit_per_event));
    assert_eq!(
        aggregates
            .iter()
            .filter(|config| config.accepted_output_schema.is_some())
            .count(),
        1
    );
}

#[test(tokio::test)]
async fn maintained_merge_nested_canonical_virtual_source_requires_repeatability() {
    let query = "SET updating_ttl = NULL;
        CREATE TABLE events (counter BIGINT NOT NULL,event_time TEXT NOT NULL,
          event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
          event_ms BIGINT GENERATED ALWAYS AS (CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)) STORED,
          WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP)-INTERVAL '5 seconds')
          WITH(connector='single_file',path='/tmp/str64-native-source.jsonl',format='json',type='source');
        CREATE VIEW first_level AS SELECT counter,MAX(event_ms) AS latest FROM events GROUP BY counter;
        CREATE VIEW counts AS SELECT counter,MAX(latest) AS quantity FROM first_level GROUP BY counter;
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY,quantity BIGINT) PARTITION BY counter;
        CREATE VIEW applied AS MERGE INTO inventory AS old USING counts AS incoming ON old.counter=incoming.counter
        WHEN MATCHED THEN UPDATE SET quantity=incoming.quantity
        WHEN NOT MATCHED THEN INSERT(counter,quantity) VALUES(incoming.counter,incoming.quantity)
        RETURNING source AS source,old AS old,new AS new,action AS action;
        SELECT source.quantity,EVENT_TIMESTAMP() AS original_trigger FROM applied;";
    plan(query).await;
    let volatile = query.replace(
        "CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)",
        "CAST(random()*1000 AS BIGINT)",
    );
    reject(&volatile, "repeatable source expressions").await;
}

#[test(tokio::test)]
async fn maintained_merge_rejects_unsupported_event_lineage_and_ordering() {
    let base = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)");
    let view =
        "CREATE VIEW counts AS SELECT counter, COUNT(*) AS quantity FROM events GROUP BY counter;";
    let cases = [
        ("join", base.replace(view, "CREATE VIEW counts AS SELECT a.counter, COUNT(*) AS quantity FROM events a JOIN events b ON a.counter=b.counter GROUP BY a.counter;")),
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
async fn maintained_merge_preserves_native_calendar_descriptors_and_accepted_schema() {
    let declarations = "CREATE TABLE events (counter BIGINT NOT NULL, event_time TIMESTAMP NOT NULL,
        WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
        WITH (connector='single_file',path='/tmp/str64-rejected-calendar.jsonl',format='json',type='source');
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY counter;";
    let sql = aggregate_merge(
        "SET updating_ttl = NULL;",
        "COUNT(*) FILTER (WHERE CAST(event_time AS DATE) = EVENT_DATE())",
    )
    .replace(DECLARATIONS, declarations);
    let compiled = plan(&sql).await;
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
    assert!(aggregates[0].response_root_operator.is_none());
    assert_eq!(aggregates[0].calendar_aggregates.len(), 1);
    assert_eq!(aggregates[0].calendar_aggregates[0].horizon_days, 1);
    let output: arrow_schema::Schema =
        serde_json::from_str(&aggregates[0].final_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert!(output.index_of(arroyo_rpc::UPDATING_META_FIELD).is_err());
    assert_eq!(state_operators(&compiled).len(), 1);
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

// Shared only with final-reader clock regressions. Generic JSON values keep the
// actual selector/clock/view/MERGE composition independent of an application.
pub(super) fn str64_pending_selector_parts(format: &str, selector: &str) -> (String, String) {
    let declarations = format!(
        "SET updating_ttl = NULL;
         CREATE TABLE selector_events (
          row_id BIGINT PRIMARY KEY,k TEXT NOT NULL,payload TEXT,eligible BOOLEAN NOT NULL,
          event_time TIMESTAMP NOT NULL,
          WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY
         ) WITH(connector='single_file',path='/tmp/str64-pending-selector.jsonl',format='{format}',type='source');
         CREATE STATE TABLE selector_values (k TEXT PRIMARY KEY,v TEXT,clock TIMESTAMP) PARTITION BY k;
         CREATE VIEW selector_inputs AS SELECT k,
          JSON_VALUE(payload,'$.v') AS v,eligible FROM selector_events
          WHERE JSON_EXISTS(payload,'$.v');
         CREATE VIEW pending_selector AS SELECT k,{selector} AS v,
          EVENT_TIMESTAMP() AS clock FROM selector_inputs GROUP BY k;"
    );
    let mutation = "CREATE VIEW selector_applied AS
         MERGE INTO selector_values AS previous USING pending_selector AS incoming
         ON previous.k=incoming.k
         WHEN MATCHED THEN UPDATE SET v=incoming.v,clock=incoming.clock
         WHEN NOT MATCHED THEN INSERT(k,v,clock) VALUES(incoming.k,incoming.v,incoming.clock)
         RETURNING source AS source,old AS old,new AS new,action AS action;"
        .to_owned();
    (declarations, mutation)
}

#[test(tokio::test)]
async fn str64_pending_selector_clock_view_binds_only_after_merge_route_validation() {
    use datafusion_proto::physical_plan::AsExecutionPlan;
    let (declarations, mutation) = str64_pending_selector_parts("json", "LAST_VALUE(v)");
    let compiled = plan(&format!(
        "{declarations}{mutation}
         SELECT source.k,source.clock AS source_clock,new.clock AS accepted_clock,EVENT_TIMESTAMP() AS original_clock FROM selector_applied;"
    ))
    .await;
    let aggregates: Vec<_> = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(op, _)| op.operator_name == OperatorName::UpdatingAggregate)
        .map(|(op, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(op.operator_config.as_slice())
                .unwrap()
        })
        .collect();
    assert_eq!(aggregates.len(), 1);
    let aggregate = &aggregates[0];
    assert!(aggregate.emit_per_event);
    let accesses = state_operators(&compiled);
    assert_eq!(accesses.len(), 1);
    let source: arrow_schema::Schema =
        serde_json::from_str(&accesses[0].input_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert_eq!(
        source.field_with_name("clock").unwrap().data_type(),
        &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
    );
    let wire =
        datafusion_proto::protobuf::PhysicalPlanNode::decode(aggregate.aggregate_exec.as_slice())
            .unwrap();
    str64_assert_serialized_clock_bound(&wire);
    let mut value_plans = 0;
    for node in compiled.program.graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            match operator.operator_name {
                OperatorName::ArrowValue => {
                    let config = arroyo_rpc::grpc::api::ValuePlanOperator::decode(
                        operator.operator_config.as_slice(),
                    )
                    .unwrap();
                    let wire = datafusion_proto::protobuf::PhysicalPlanNode::decode(
                        config.physical_plan.as_slice(),
                    )
                    .unwrap();
                    str64_assert_serialized_clock_bound(&wire);
                    value_plans += 1;
                }
                OperatorName::ArrowKey => {
                    let config = arroyo_rpc::grpc::api::KeyPlanOperator::decode(
                        operator.operator_config.as_slice(),
                    )
                    .unwrap();
                    let wire = datafusion_proto::protobuf::PhysicalPlanNode::decode(
                        config.physical_plan.as_slice(),
                    )
                    .unwrap();
                    str64_assert_serialized_clock_bound(&wire);
                }
                _ => {}
            }
        }
    }
    assert!(
        value_plans > 0,
        "serialized worker value plans must be inspected"
    );
    let registry = ArroyoSchemaProvider::new();
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .build()
        .unwrap();
    let codec = crate::physical::ArroyoPhysicalExtensionCodec {
        context: crate::physical::DecodingContext::Planning,
    };
    let physical = wire
        .try_into_physical_plan(&registry, &runtime, &codec)
        .unwrap();
    let physical = physical
        .as_any()
        .downcast_ref::<datafusion::physical_plan::aggregates::AggregateExec>()
        .unwrap();
    let selectors: Vec<_> = physical
        .aggr_expr()
        .iter()
        .filter(|expression| crate::grouped_selector_udaf::classify(expression.fun()).is_some())
        .collect();
    assert_eq!(selectors.len(), 1);
    assert_eq!(
        crate::grouped_selector_udaf::classify(selectors[0].fun()),
        Some((crate::grouped_selector::SelectorKind::Last, true))
    );
    crate::grouped_selector_udaf::selector_spec(selectors[0], &physical.input_schema()).unwrap();
    let primary = &selectors[0].order_bys().unwrap()[0];
    assert!(!primary.options.descending && !primary.options.nulls_first);
    let column = primary
        .expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        .unwrap();
    assert_eq!(column.name(), arroyo_rpc::TIMESTAMP_FIELD);
    assert_eq!(
        column.index(),
        physical
            .input_schema()
            .index_of(arroyo_rpc::TIMESTAMP_FIELD)
            .unwrap()
    );
}

#[test(tokio::test)]
async fn str64_pending_correction_selector_generated_ordinal_and_primary_are_valid() {
    let (declarations, mutation) = str64_pending_selector_parts(
        "debezium_json",
        "LAST_VALUE(v ORDER BY v) FILTER (WHERE eligible)",
    );
    let compiled = plan(&format!(
        "{declarations}{mutation} SELECT source.clock,new.v FROM selector_applied;"
    ))
    .await;
    assert_eq!(state_operators(&compiled).len(), 1);
}

#[test(tokio::test)]
async fn str64_pending_correction_selector_rejects_volatile_user_value_filter_and_secondary() {
    for selector in [
        "LAST_VALUE(uuid())",
        "LAST_VALUE(v) FILTER (WHERE uuid() IS NOT NULL)",
        "LAST_VALUE(v ORDER BY uuid())",
    ] {
        let (declarations, mutation) = str64_pending_selector_parts("debezium_json", selector);
        reject(
            &format!("{declarations}{mutation} SELECT new.v FROM selector_applied;"),
            "per-event correction requires repeatable source expressions",
        )
        .await;
    }
}

#[test(tokio::test)]
async fn str64_pending_correction_selector_rejects_user_trigger_value_filter_and_secondary() {
    for selector in [
        "LAST_VALUE(CAST(EVENT_TIMESTAMP() AS TEXT))",
        "LAST_VALUE(v) FILTER (WHERE EVENT_TIMESTAMP() > TIMESTAMP '1970-01-01 00:00:07')",
        "LAST_VALUE(v ORDER BY EVENT_TIMESTAMP())",
    ] {
        let (declarations, mutation) = str64_pending_selector_parts("debezium_json", selector);
        let sql = format!("{declarations}{mutation} SELECT new.v FROM selector_applied;");
        let error = parse_and_get_program(
            &sql,
            ArroyoSchemaProvider::new(),
            SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("unsupported clock-dependent aggregate")
                || message.contains("per-event correction requires repeatable source expressions"),
            "unexpected rejection for {selector}: {message}"
        );
    }
}

fn str64_assert_serialized_clock_bound(wire: &impl std::fmt::Debug) {
    // These fixtures explicitly alias output clock fields. In decoded physical
    // protobufs a marker UDF is identified by its exact function name, rather
    // than matching display/SQL substrings or stripping user expressions.
    let wire = format!("{wire:?}");
    for marker in ["event_timestamp", "event_date"] {
        assert!(
            !wire.contains(&format!("name: \"{marker}\"")),
            "unresolved contextual UDF in worker wire: {wire}"
        );
    }
}

#[test(tokio::test)]
async fn maintained_merge_count_ordering_reaches_the_route_guard() {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{Expr, LogicalPlan};
    let view = "CREATE VIEW counts AS SELECT counter,
        COUNT(counter ORDER BY counter) AS quantity,
        EVENT_TIMESTAMP() AS trigger_clock FROM events GROUP BY counter;";
    // The pending contextual projection retains the analyzed aggregate order
    // until the MERGE route validator; inspect that exact native view template.
    let mut provider = ArroyoSchemaProvider::new();
    let source = crate::parse_sql("CREATE TABLE events WITH (connector='impulse',event_rate='1');")
        .unwrap()
        .remove(0);
    let table = crate::tables::Table::try_from_statement(&source, &provider)
        .unwrap()
        .unwrap();
    provider.insert_table(table, vec!["events".to_owned()]);
    let statement = crate::parse_sql(view).unwrap().remove(0);
    let table = crate::tables::Table::try_from_statement(&statement, &provider)
        .unwrap()
        .unwrap();
    let crate::tables::Table::TableFromQuery { logical_plan, .. } = table else {
        panic!("expected native view template");
    };
    let mut ordered_counts = 0;
    logical_plan.apply(|node| {
        if let LogicalPlan::Extension(extension)=node
            && let Some(aggregate)=extension.node.as_any().downcast_ref::<crate::extension::updating_aggregate::UpdatingAggregateExtension>()
            && let LogicalPlan::Aggregate(plan)=&aggregate.aggregate {
            for expression in &plan.aggr_expr {
                let mut expression=expression;
                while let Expr::Alias(alias)=expression {expression=&alias.expr;}
                if let Expr::AggregateFunction(function)=expression && function.func.name()=="count" {
                    assert!(!function.params.distinct);
                    let orders=function.params.order_by.as_ref().expect("COUNT ORDER BY must remain present before route validation");
                    assert_eq!(orders.len(),1);
                    assert!(matches!(&orders[0].expr,Expr::Column(column) if column.name=="counter"));
                    ordered_counts+=1;
                }
            }
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    }).unwrap();
    assert_eq!(ordered_counts, 1);
    let sql=aggregate_merge("SET updating_ttl = NULL;","COUNT(counter)").replace(
        "CREATE VIEW counts AS SELECT counter, COUNT(counter) AS quantity FROM events GROUP BY counter;",view);
    plan(&sql.replace("COUNT(counter ORDER BY counter)", "COUNT(counter)")).await;
    reject(
        &sql,
        "per-event aggregate MERGE requires a verified non-distinct native result capability",
    )
    .await;
}

#[test(tokio::test)]
async fn maintained_merge_accepts_native_calendar_sum_with_nullable_snapshot() {
    let declarations = "CREATE TABLE events (counter BIGINT NOT NULL, event_time TIMESTAMP NOT NULL,
        WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
        WITH (connector='single_file',path='/tmp/str64-calendar-sum.jsonl',format='json',type='source');
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY counter;";
    let sql = aggregate_merge(
        "SET updating_ttl = NULL;",
        "SUM(counter) FILTER (WHERE CAST(event_time AS DATE)=EVENT_DATE())",
    )
    .replace(DECLARATIONS, declarations);
    let compiled = plan(&sql).await;
    let aggregates = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(op, _)| op.operator_name == OperatorName::UpdatingAggregate)
        .map(|(op, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(op.operator_config.as_slice())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 1);
    assert!(aggregates[0].emit_per_event);
    assert!(aggregates[0].response_root_operator.is_none());
    assert_eq!(aggregates[0].calendar_aggregates.len(), 1);
    assert_eq!(aggregates[0].calendar_aggregates[0].horizon_days, 1);
    let schema: arrow_schema::Schema =
        serde_json::from_str(&aggregates[0].final_schema.as_ref().unwrap().arrow_schema).unwrap();
    // Physical aggregate fields keep expression names; the calendar descriptor
    // addresses that field after the configured grouping-key prefix.
    let grouping_fields = aggregates[0]
        .input_schema
        .as_ref()
        .unwrap()
        .key_indices
        .len();
    let calendar_index = aggregates[0].calendar_aggregates[0].aggregate_index as usize;
    let result = schema.field(grouping_fields + calendar_index);
    assert_eq!(result.data_type(), &arrow_schema::DataType::Int64);
    assert!(result.is_nullable());
    assert!(schema.index_of(arroyo_rpc::UPDATING_META_FIELD).is_err());
    // The public alias is applied by the scalar projection before MERGE. Prove
    // that handoff independently rather than assuming the physical field name.
    let accesses = state_operators(&compiled);
    assert_eq!(accesses.len(), 1);
    let public: arrow_schema::Schema =
        serde_json::from_str(&accesses[0].input_schema.as_ref().unwrap().arrow_schema).unwrap();
    let quantity = public.field_with_name("quantity").unwrap();
    assert_eq!(quantity.data_type(), &arrow_schema::DataType::Int64);
    assert!(quantity.is_nullable());
}

#[test(tokio::test)]
async fn maintained_merge_direct_array_retains_native_collection_result_and_public_source() {
    let declarations = "CREATE TABLE events (counter BIGINT NOT NULL, item TEXT,
        event_time TIMESTAMP NOT NULL, WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
        WITH (connector='single_file',path='/tmp/native-array-handoff.jsonl',format='json',type='source');
        CREATE STATE TABLE inventory (counter BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY counter;";
    let sql = aggregate_merge("SET updating_ttl = NULL;", "COUNT(*)")
        .replace(DECLARATIONS, declarations)
        .replace(
            "COUNT(*) AS quantity",
            "COUNT(*) AS quantity, ARRAY_AGG(item ORDER BY item) AS items",
        )
        .replace(
            "SELECT new.quantity FROM applied",
            "SELECT source.items, old.quantity, new.quantity, action FROM applied",
        );
    let compiled = plan(&sql).await;
    let operations = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .collect::<Vec<_>>();
    let aggregates = operations
        .iter()
        .filter(|(op, _)| op.operator_name == OperatorName::UpdatingAggregate)
        .map(|(op, _)| {
            arroyo_rpc::grpc::api::UpdatingAggregateOperator::decode(op.operator_config.as_slice())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 1);
    assert!(aggregates[0].emit_per_event);
    assert!(aggregates[0].response_root_operator.is_none());
    let physical: arrow_schema::Schema =
        serde_json::from_str(&aggregates[0].final_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert!(physical.fields().iter().any(|field| matches!(field.data_type(), arrow_schema::DataType::List(child) if child.data_type() == &arrow_schema::DataType::Utf8)));
    let accesses = state_operators(&compiled);
    assert_eq!(accesses.len(), 1);
    let source: arrow_schema::Schema =
        serde_json::from_str(&accesses[0].input_schema.as_ref().unwrap().arrow_schema).unwrap();
    assert!(matches!(
        source.field_with_name("items").unwrap().data_type(),
        arrow_schema::DataType::List(_)
    ));
}

fn finalized_session_merge() -> String {
    "CREATE TABLE events(t TEXT NOT NULL,k TEXT NOT NULL,event_time TIMESTAMP NOT NULL,
       WATERMARK FOR event_time AS event_time-INTERVAL '5 seconds')
       WITH(connector='single_file',path='/tmp/str64-session-plan.jsonl',format='json',type='source');
     CREATE VIEW computed AS SELECT t,k,SESSION(INTERVAL '30 minutes') AS window,COUNT(*) AS n
       FROM events GROUP BY t,k,window;
     CREATE STATE TABLE snapshots(t TEXT,k TEXT,n BIGINT,PRIMARY KEY(t,k)) PARTITION BY t;
     CREATE VIEW applied AS MERGE INTO snapshots AS previous USING computed AS incoming
       ON previous.t=incoming.t AND previous.k=incoming.k
       WHEN MATCHED THEN UPDATE SET n=incoming.n
       WHEN NOT MATCHED THEN INSERT(t,k,n) VALUES(incoming.t,incoming.k,incoming.n)
       RETURNING source AS source,old AS old,new AS new,action AS action;
     SELECT source.t,source.k,old.n AS previous_n,new.n AS n,action,
       applied._timestamp AS finalized_at FROM applied;".into()
}

#[test(tokio::test)]
async fn session_cast_cse_reader_matches_registered_input() {
    use crate::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};

    // Preserve the complete captured failure, including downstream native MERGE.
    let sql = include_str!("queries/str155-session-cast.sql");
    let compiled = plan(sql).await;
    let registry = ArroyoSchemaProvider::new();
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .build()
        .unwrap();
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let decode = |bytes: &[u8]| {
        PhysicalPlanNode::decode(bytes)
            .unwrap()
            .try_into_physical_plan(&registry, &runtime, &codec)
            .unwrap()
    };
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_expr::expressions::CastExpr;
    use datafusion::physical_plan::projection::ProjectionExec;
    use petgraph::visit::EdgeRef;

    let graph = &compiled.program.graph;
    let mut sessions = 0;
    for index in graph.node_indices() {
        let chain = graph[index].operator_chain.iter().collect::<Vec<_>>();
        for (position, (operator, _)) in chain.iter().enumerate() {
            if operator.operator_name != OperatorName::SessionWindowAggregate {
                continue;
            }
            sessions += 1;
            let config = arroyo_rpc::grpc::api::SessionWindowAggregateOperator::decode(
                operator.operator_config.as_slice(),
            )
            .unwrap();
            let input: arroyo_rpc::df::ArroyoSchema =
                config.input_schema.unwrap().try_into().unwrap();
            // Bind the proof to this SESSION's actual producer, including
            // chained edges. Other routing adapters can carry the CSE column.
            let (key_operator, edge_schema) = if position > 0 {
                let (previous, output) = chain[position - 1];
                (previous.clone(), output.unwrap().clone())
            } else {
                let incoming = graph
                    .edges_directed(index, petgraph::Direction::Incoming)
                    .collect::<Vec<_>>();
                assert_eq!(incoming.len(), 1);
                let edge = &incoming[0];
                (
                    graph[edge.source()]
                        .operator_chain
                        .iter()
                        .last()
                        .unwrap()
                        .0
                        .clone(),
                    edge.weight().schema.clone(),
                )
            };
            assert_eq!(edge_schema.as_ref(), &input);
            assert_eq!(key_operator.operator_name, OperatorName::ArrowKey);
            let key = arroyo_rpc::grpc::api::KeyPlanOperator::decode(
                key_operator.operator_config.as_slice(),
            )
            .unwrap();
            let key_physical = decode(&key.physical_plan);
            assert_eq!(key_physical.schema(), input.schema);
            let mut casts = 0;
            key_physical
                .apply(|node| {
                    if let Some(projection) = node.as_any().downcast_ref::<ProjectionExec>() {
                        casts += projection
                            .expr()
                            .iter()
                            .filter(|(expression, name)| {
                                name == "__common_expr_1" && expression.as_any().is::<CastExpr>()
                            })
                            .count();
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            assert_eq!(
                casts, 1,
                "SESSION's shared CAST must remain in its actual producer"
            );
            let physical = decode(&config.final_aggregation_plan);
            crate::physical::validate_session_result_plan(physical.as_ref(), &input.schema)
                .unwrap();
            assert!(
                !input
                    .schema
                    .field_with_name("__common_expr_1")
                    .unwrap()
                    .is_nullable()
            );
            assert!(!input.schema.field_with_name("ts").unwrap().is_nullable());
            let aggregate = physical.as_any().downcast_ref::<AggregateExec>().unwrap();
            let selectors: Vec<_> = aggregate
                .aggr_expr()
                .iter()
                .filter(|expression| {
                    crate::grouped_selector_udaf::classify(expression.fun()).is_some()
                })
                .collect();
            assert_eq!(selectors.len(), 2);
            for selector in selectors {
                crate::grouped_selector_udaf::selector_spec(selector, &aggregate.input_schema())
                    .unwrap();
                let primary = &selector.order_bys().unwrap()[0];
                let column = primary
                    .expr
                    .as_any()
                    .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                    .unwrap();
                assert_eq!(column.name(), arroyo_rpc::TIMESTAMP_FIELD);
                assert_eq!(column.index(), input.timestamp_index);
                assert!(!primary.options.descending && !primary.options.nulls_first);
            }
            let reader = aggregate.input_schema();
            assert_eq!(reader.fields().len(), input.schema.fields().len() + 1);
            assert_eq!(
                &reader.fields()[..input.schema.fields().len()],
                input.schema.fields().as_ref()
            );
            assert_eq!(
                reader.fields().last().unwrap().name(),
                crate::grouped_selector::ORDINAL_FIELD
            );
        }
    }
    assert_eq!(sessions, 1);
}
// Record the ordinary native producer before proving its MERGE consumer. This
// keeps its actual admission/key/physical lowering visible when a proof fails.
async fn record_ordinary_session_plan(sql: &str) {
    use petgraph::visit::EdgeRef;
    let declarations = sql.split("CREATE STATE TABLE snapshots").next().unwrap();
    let ordinary = plan(&format!("{declarations} SELECT * FROM computed;")).await;
    let nodes = ordinary.program.graph.node_indices().map(|index| {
        let node = &ordinary.program.graph[index];
        let operators = node.operator_chain.iter().map(|(operator, output)| {
            let config = match operator.operator_name {
                OperatorName::ExpressionWatermark => format!("{:#?}",
                    arroyo_rpc::grpc::api::ExpressionWatermarkConfig::decode(operator.operator_config.as_slice()).unwrap()),
                OperatorName::SessionWindowAggregate => format!("{:#?}",
                    arroyo_rpc::grpc::api::SessionWindowAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()),
                OperatorName::ArrowValue => format!("{:#?}",
                    arroyo_rpc::grpc::api::ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap()),
                OperatorName::ArrowKey => format!("{:#?}",
                    arroyo_rpc::grpc::api::KeyPlanOperator::decode(operator.operator_config.as_slice()).unwrap()),
                _ => String::new(),
            };
            let physical = match operator.operator_name {
                OperatorName::ArrowValue => Some(arroyo_rpc::grpc::api::ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap().physical_plan),
                OperatorName::ArrowKey => Some(arroyo_rpc::grpc::api::KeyPlanOperator::decode(operator.operator_config.as_slice()).unwrap().physical_plan),
                OperatorName::SessionWindowAggregate => Some(arroyo_rpc::grpc::api::SessionWindowAggregateOperator::decode(operator.operator_config.as_slice()).unwrap().final_aggregation_plan),
                _ => None,
            }.map(|bytes| format!("{:#?}", datafusion_proto::protobuf::PhysicalPlanNode::decode(bytes.as_slice()).unwrap()));
            serde_json::json!({"id":operator.operator_id,"kind":format!("{:?}",operator.operator_name),
                "config":config,"serialized_config":operator.operator_config,"physical_plan":physical,
                "output":format!("{output:#?}")})
        }).collect::<Vec<_>>();
        serde_json::json!({"index":index.index(),"node_id":node.node_id,
            "parallelism":node.parallelism,"operators":operators})
    }).collect::<Vec<_>>();
    let edges = ordinary
        .program
        .graph
        .edge_references()
        .map(|edge| {
            serde_json::json!({
                "source":edge.source().index(),"target":edge.target().index(),
                "edge":format!("{:#?}", edge.weight())
            })
        })
        .collect::<Vec<_>>();
    let evidence = serde_json::json!({"sql":format!("{declarations} SELECT * FROM computed;"),"nodes":nodes,"edges":edges});
    eprintln!(
        "SESSION_ORDINARY_PLAN {}",
        serde_json::to_string(&evidence).unwrap()
    );
    if let Ok(path) = std::env::var("STREAMR_TEST_SESSION_PLAN_EVIDENCE") {
        let suffix = if sql.contains("GENERATED ALWAYS") {
            "generated"
        } else {
            "declared"
        };
        std::fs::write(
            format!("{path}-{suffix}.json"),
            serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
    }
}

#[test(tokio::test)]
async fn finalized_session_merge_keeps_native_append_producer_and_shared_result_cut() {
    let sql = finalized_session_merge();
    record_ordinary_session_plan(&sql).await;
    let compiled = plan(&sql).await;
    let operators = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .collect::<Vec<_>>();
    for (kind, count) in [
        (OperatorName::ConnectorSource, 1),
        (OperatorName::SessionWindowAggregate, 1),
        (OperatorName::FusedStateTable, 1),
        (OperatorName::UpdatingAggregate, 0),
    ] {
        assert_eq!(
            operators
                .iter()
                .filter(|operator| operator.operator_name == kind)
                .count(),
            count
        );
    }
    let native = operators
        .iter()
        .find(|op| op.operator_name == OperatorName::SessionWindowAggregate)
        .unwrap();
    let native = arroyo_rpc::grpc::api::SessionWindowAggregateOperator::decode(
        native.operator_config.as_slice(),
    )
    .unwrap();
    let output: arroyo_rpc::df::ArroyoSchema = native.output_schema.unwrap().try_into().unwrap();
    assert_eq!(output.schema.field(0).name(), "t");
    assert_eq!(output.schema.field(1).name(), "k");
    assert_eq!(output.timestamp_index, output.schema.fields().len() - 1);
    assert_eq!(
        output.schema.field(output.timestamp_index).data_type(),
        &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
    );
    assert!(
        output
            .schema
            .index_of(arroyo_rpc::UPDATING_META_FIELD)
            .is_err()
    );
    assert_eq!(state_operators(&compiled).len(), 1);
}
#[test(tokio::test)]
async fn finalized_session_merge_rejects_replaced_result_clock_and_prior_state_access() {
    let sql = finalized_session_merge();
    let replaced=sql.replace("USING computed AS incoming", "USING replaced AS incoming")
        .replace("CREATE STATE TABLE snapshots", "CREATE VIEW replaced AS SELECT t,k,n,TIMESTAMP '2000-01-01 00:00:00' AS _timestamp FROM computed; CREATE STATE TABLE snapshots");
    reject(&replaced, "preserve the native producer clock").await;
    let prior=sql.replace("CREATE VIEW computed AS", "CREATE STATE TABLE retained(t TEXT,k TEXT,n BIGINT,PRIMARY KEY(t,k)) PARTITION BY t; CREATE VIEW looked AS SELECT events.* FROM events LEFT JOIN retained ON events.t=retained.t AND events.k=retained.k; CREATE VIEW computed AS")
        .replace("FROM events GROUP BY", "FROM looked GROUP BY");
    reject(&prior, "finalized").await;
}

#[test(tokio::test)]
async fn finalized_session_merge_preserves_exact_canonical_generated_source_clock() {
    let sql = finalized_session_merge().replace(
        "event_time TIMESTAMP NOT NULL,\n       WATERMARK FOR event_time AS event_time-INTERVAL '5 seconds'",
        "event_time TEXT NOT NULL,event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,\n       WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP)-INTERVAL '5 seconds'",
    );
    assert!(sql.contains("GENERATED ALWAYS"));
    record_ordinary_session_plan(&sql).await;
    let compiled = plan(&sql).await;
    assert_eq!(
        compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::SessionWindowAggregate)
            .count(),
        1
    );
    assert_eq!(state_operators(&compiled).len(), 1);
}

fn finalized_four_branch_merge() -> String {
    "SET updating_ttl = NULL;
    CREATE TABLE measurements (counter BIGINT NOT NULL, session_key TEXT NOT NULL,
      value BIGINT NOT NULL, event_time TIMESTAMP NOT NULL,
      WATERMARK FOR event_time AS event_time - INTERVAL '5 seconds')
      WITH(connector='single_file',path='/tmp/composed-finalized-input.jsonl',format='json',type='source');
    CREATE VIEW a AS SELECT counter,COUNT(*) AS n FROM measurements GROUP BY counter;
    CREATE VIEW b AS SELECT counter,value,COUNT(*) AS n FROM measurements GROUP BY counter,value;
    CREATE VIEW c AS SELECT counter,COUNT(*) AS n FROM measurements WHERE value>0 GROUP BY counter;
    CREATE VIEW completed AS SELECT session_key,SESSION(INTERVAL '30 minutes') AS window,
      FIRST_VALUE(counter) AS counter,MAX(value) AS amount
      FROM measurements GROUP BY session_key,window;
    CREATE VIEW completed_values AS SELECT counter,amount FROM completed;
    CREATE VIEW d AS SELECT counter,CAST(SUM(amount) AS DOUBLE)/CAST(COUNT(*) AS DOUBLE) AS n
      FROM completed_values GROUP BY counter;
    CREATE VIEW tagged AS
      SELECT counter,CAST(n AS DOUBLE) AS n FROM a UNION ALL
      SELECT counter,CAST(n AS DOUBLE) AS n FROM b UNION ALL
      SELECT counter,CAST(n AS DOUBLE) AS n FROM c UNION ALL
      SELECT counter,n FROM d;
    CREATE VIEW result AS SELECT counter,MAX(n) AS quantity FROM tagged GROUP BY counter;
    CREATE STATE TABLE inventory(counter BIGINT PRIMARY KEY,quantity DOUBLE) PARTITION BY counter;
    CREATE VIEW applied AS MERGE INTO inventory AS target USING result AS source
      ON target.counter=source.counter
      WHEN MATCHED THEN UPDATE SET quantity=source.quantity
      WHEN NOT MATCHED THEN INSERT(counter,quantity) VALUES(source.counter,source.quantity)
      RETURNING source AS source,old AS old,new AS new,action AS action;
    SELECT new.quantity,action FROM applied;".to_owned()
}

#[test(tokio::test)]
async fn maintained_merge_composes_four_branches_with_native_finalized_session() {
    use arroyo_rpc::grpc::api::{SessionWindowAggregateOperator, UpdatingAggregateOperator};
    let compiled = plan(&finalized_four_branch_merge()).await;
    let operators = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .collect::<Vec<_>>();
    let aggregates = operators
        .iter()
        .filter(|operator| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|operator| {
            UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .collect::<Vec<_>>();
    let sessions = operators
        .iter()
        .filter(|operator| operator.operator_name == OperatorName::SessionWindowAggregate)
        .map(|operator| {
            SessionWindowAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregates.len(), 5);
    assert_eq!(sessions.len(), 1);
    // This topology mirrors native raw branches plus finalized-SESSION average.
    // Actual runtime response binding rejects even singleton Shuffle edges.
    use petgraph::Direction::Incoming;
    let session_node = compiled
        .program
        .graph
        .node_indices()
        .find(|index| {
            compiled.program.graph[*index]
                .operator_chain
                .iter()
                .any(|(operator, _)| operator.operator_name == OperatorName::SessionWindowAggregate)
        })
        .unwrap();
    let inputs = compiled
        .program
        .graph
        .edges_directed(session_node, Incoming)
        .collect::<Vec<_>>();
    assert_eq!(inputs.len(), 1);
    assert_eq!(
        inputs[0].weight().edge_type,
        arroyo_datastream::logical::LogicalEdgeType::Forward
    );
    let root = sessions[0]
        .response_root_operator
        .expect("SESSION shares original admission root");
    assert!(aggregates.iter().all(
        |aggregate| aggregate.response_root_operator == Some(root) && !aggregate.emit_per_event
    ));
    assert_eq!(
        aggregates
            .iter()
            .filter(|aggregate| aggregate.response_finalized_append_input)
            .count(),
        1
    );
    assert_eq!(
        aggregates
            .iter()
            .filter(|aggregate| aggregate.accepted_output_schema.is_some())
            .count(),
        1
    );
    let finalized = aggregates
        .iter()
        .find(|aggregate| aggregate.response_finalized_append_input)
        .unwrap();
    assert_eq!(finalized.response_key_fields, vec![0]);
    let input =
        arroyo_rpc::df::ArroyoSchema::try_from(finalized.input_schema.clone().unwrap()).unwrap();
    assert!(
        input
            .schema
            .index_of(arroyo_rpc::UPDATING_META_FIELD)
            .is_err(),
        "SESSION retains append closure layout"
    );
}

#[test(tokio::test)]
async fn maintained_merge_finalized_branch_rejects_a_different_original_source() {
    let sql = finalized_four_branch_merge().replace("CREATE VIEW completed AS", "CREATE TABLE other_measurements(counter BIGINT NOT NULL,session_key TEXT NOT NULL,value BIGINT NOT NULL,event_time TIMESTAMP NOT NULL,WATERMARK FOR event_time AS event_time - INTERVAL '5 seconds') WITH(connector='single_file',path='/tmp/other-composed-input.jsonl',format='json',type='source'); CREATE VIEW completed AS")
        .replace("FROM measurements GROUP BY session_key,window", "FROM other_measurements GROUP BY session_key,window");
    reject(&sql, "same physical source definition").await;
}

#[test(tokio::test)]
async fn maintained_merge_rejects_two_finalized_response_producers() {
    let sql = finalized_four_branch_merge().replace(
        "SELECT counter,CAST(n AS DOUBLE) AS n FROM c UNION ALL",
        "SELECT counter,n FROM d UNION ALL",
    );
    reject(&sql, "one finalized native producer").await;
}

#[test(tokio::test)]
async fn maintained_merge_session_route_keeps_ordinary_window_shuffle() {
    let query = finalized_four_branch_merge();
    let ordinary = query.split("CREATE VIEW d AS").next().unwrap().to_owned()
        + "SELECT counter,amount FROM completed_values;";
    let compiled = plan(&ordinary).await;
    use petgraph::Direction::Incoming;
    let node = compiled
        .program
        .graph
        .node_indices()
        .find(|index| {
            compiled.program.graph[*index]
                .operator_chain
                .iter()
                .any(|(operator, _)| operator.operator_name == OperatorName::SessionWindowAggregate)
        })
        .unwrap();
    let inputs = compiled
        .program
        .graph
        .edges_directed(node, Incoming)
        .collect::<Vec<_>>();
    assert_eq!(inputs.len(), 1);
    assert_eq!(
        inputs[0].weight().edge_type,
        arroyo_datastream::logical::LogicalEdgeType::Shuffle
    );
}
