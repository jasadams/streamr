//! Generic native plan probes for STR-28. These exercise physical-plan lowering,
//! not aggregate values, operator recovery, resource bounds or RocksDB admission.

use super::get_test_schema_provider;
use crate::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
use crate::{SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::{
    SessionWindowAggregateOperator, SlidingWindowAggregateOperator,
    TumblingWindowAggregateOperator, UpdatingAggregateOperator, ValuePlanOperator,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use petgraph::Direction;
use prost::Message;
use test_log::test;

async fn native_operator_config(sql: &str, expected: OperatorName) -> Vec<u8> {
    let compiled = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("native probe failed to plan: {error}\n{sql}"));
    let operators: Vec<_> = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .collect();
    let matching: Vec<_> = operators
        .iter()
        .filter(|operator| operator.operator_name == expected)
        .collect();
    assert_eq!(matching.len(), 1, "expected one {expected} operator");
    matching[0].operator_config.clone()
}

fn assert_physical_plan(bytes: &[u8]) {
    let plan = PhysicalPlanNode::decode(bytes).expect("serialized physical plan must decode");
    assert!(plan.physical_plan_type.is_some());
}

async fn rejects_with(sql: &str, diagnostic: &str) {
    let error = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(diagnostic),
        "expected {diagnostic:?}, received {error}"
    );
}

fn assert_union_consolidation_is_one_identity_reader(
    graph: &arroyo_datastream::logical::LogicalGraph,
    expected_nodes: usize,
) {
    let provider = get_test_schema_provider();
    let runtime = RuntimeEnvBuilder::new().build().unwrap();
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let mut nodes = 0;
    for node in graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name != OperatorName::ArrowValue {
                continue;
            }
            let config = ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap();
            if config.name != "value_calculation(union_output)" {
                continue;
            }
            nodes += 1;
            let proto = PhysicalPlanNode::decode(config.physical_plan.as_slice()).unwrap();
            let physical = proto
                .try_into_physical_plan(&provider, &runtime, &codec)
                .expect("UNION consolidation must decode as a physical plan");
            assert!(
                physical.as_any().is::<ArroyoMemExec>(),
                "UNION consolidation must read each incoming batch once"
            );
            assert!(physical.children().is_empty());
        }
    }
    assert_eq!(
        nodes, expected_nodes,
        "expected one boundary per logical UNION"
    );
}

#[test(tokio::test)]
async fn union_materialization_widens_nullability_from_either_branch() {
    for nullable_first in [false, true] {
        let present = "SELECT CAST(7 AS BIGINT) AS stable, CAST(1 AS BIGINT) AS variant \
                       FROM nexmark WHERE bid IS NOT NULL";
        let absent = "SELECT CAST(7 AS BIGINT) AS stable, CAST(NULL AS BIGINT) AS variant \
                      FROM nexmark WHERE bid IS NOT NULL";
        let sql = if nullable_first {
            format!("{absent} UNION ALL {present}")
        } else {
            format!("{present} UNION ALL {absent}")
        };
        let compiled =
            parse_and_get_program(&sql, get_test_schema_provider(), SqlConfig::default())
                .await
                .unwrap_or_else(|error| panic!("UNION failed to plan: {error}\n{sql}"));
        let graph = &compiled.program.graph;
        assert_union_consolidation_is_one_identity_reader(graph, 1);
        let mut branches = 0;
        for node_index in graph.node_indices() {
            let node = &graph[node_index];
            for (operator, internal_schema) in node.operator_chain.iter() {
                if operator.operator_name != OperatorName::ArrowValue {
                    continue;
                }
                let config =
                    ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap();
                if config.name != "value_calculation(union_input)" {
                    continue;
                }
                branches += 1;
                let output = internal_schema
                    .map(|schema| schema.as_ref())
                    .or_else(|| {
                        graph
                            .edges_directed(node_index, Direction::Outgoing)
                            .next()
                            .map(|edge| edge.weight().schema.as_ref())
                    })
                    .expect("materialized UNION branch must have an output schema");
                let schema = output.schema.as_ref();
                assert!(!schema.field_with_name("stable").unwrap().is_nullable());
                assert!(schema.field_with_name("variant").unwrap().is_nullable());
                assert!(schema.field_with_name("_timestamp").is_ok());
            }
        }
        assert_eq!(branches, 2, "each UNION input must use the widened schema");
    }
}

#[test(tokio::test)]
async fn updating_union_group_aggregate_plans_with_one_input() {
    let setup = "CREATE TABLE timestamp_input (
                   row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, position BIGINT NOT NULL
                 ) WITH (connector='single_file', path='/tmp/union-timestamp-input.jsonl',
                         format='debezium_json', type='source');";
    let branch = "SELECT k, position FROM timestamp_input";
    let queries = [
        format!(
            "SELECT k, COUNT(*) AS active, MAX(position) AS max_position \
             FROM ({branch} UNION ALL {branch}) AS combined GROUP BY k"
        ),
        format!(
            "WITH pair AS ({branch} UNION ALL {branch}) \
             SELECT k, COUNT(*) AS active, MAX(position) AS max_position \
             FROM (SELECT * FROM pair UNION ALL SELECT * FROM pair) AS combined GROUP BY k"
        ),
    ];
    // DataFusion inlines each reference to `pair`: two inner UNIONs feed the
    // outer UNION, so all three need a separate one-batch consolidation.
    for (query, expected_boundaries) in queries.into_iter().zip([1, 3]) {
        let sql = format!("{setup} {query}");
        let compiled =
            parse_and_get_program(&sql, get_test_schema_provider(), SqlConfig::default())
                .await
                .unwrap_or_else(|error| {
                    panic!("updating UNION aggregate failed to plan: {error}\n{sql}")
                });
        assert_union_consolidation_is_one_identity_reader(
            &compiled.program.graph,
            expected_boundaries,
        );
        let aggregates = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .count();
        assert_eq!(aggregates, 1, "UNION must feed one updating aggregate");
    }
}

#[test(tokio::test)]
async fn updating_union_scopes_equal_primary_keys_at_each_materialized_edge() {
    let sql = "CREATE TABLE left_changes (row_id BIGINT PRIMARY KEY, k TEXT) WITH (
                   connector = 'single_file', path = '/tmp/left-changes.jsonl',
                   format = 'debezium_json', type = 'source');
               CREATE TABLE right_changes (row_id BIGINT PRIMARY KEY, k TEXT) WITH (
                   connector = 'single_file', path = '/tmp/right-changes.jsonl',
                   format = 'debezium_json', type = 'source');
               CREATE VIEW combined AS
                   SELECT row_id, k FROM left_changes
                   UNION ALL
                   SELECT row_id, k FROM right_changes;
               SELECT k, COUNT(*) AS n FROM combined GROUP BY k";
    let branch_plans = || async {
        let compiled = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
            .await
            .unwrap_or_else(|error| panic!("updating UNION failed to plan: {error}"));
        compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::ArrowValue)
            .filter_map(|(operator, _)| {
                let config = ValuePlanOperator::decode(operator.operator_config.as_slice()).ok()?;
                (config.name == "value_calculation(union_input)").then_some(config.physical_plan)
            })
            .collect::<Vec<_>>()
    };
    let first = branch_plans().await;
    let second = branch_plans().await;
    assert_eq!(
        first.len(),
        2,
        "each updating UNION edge needs its own projection"
    );
    assert_ne!(
        first[0], first[1],
        "branch scopes must differ for equal row IDs"
    );
    assert_eq!(
        first, second,
        "branch scopes must be stable across replanning"
    );
}

#[test(tokio::test)]
async fn native_lifetime_scalar_aggregate_plan() {
    let bytes = native_operator_config(
        "SELECT bid.auction AS key, COUNT(*) AS n, SUM(bid.price) AS total, \
         MIN(bid.price) AS lo, MAX(bid.price) AS hi, \
         COUNT(*) FILTER (WHERE bid.price > 100) AS selected \
         FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction",
        OperatorName::UpdatingAggregate,
    )
    .await;
    let config = UpdatingAggregateOperator::decode(bytes.as_slice()).unwrap();
    assert_physical_plan(&config.aggregate_exec);
    assert!(config.input_schema.is_some());
    assert!(config.final_schema.is_some());
    // No ordered FIRST_VALUE/LAST_VALUE value claim follows from this plan probe.
}

#[test(tokio::test)]
async fn native_tumble_aggregate_plan() {
    let bytes = native_operator_config(
        "SELECT bid.auction AS key, TUMBLE(INTERVAL '1 minute') AS window, COUNT(*) AS n \
         FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window",
        OperatorName::TumblingWindowAggregate,
    )
    .await;
    let config = TumblingWindowAggregateOperator::decode(bytes.as_slice()).unwrap();
    assert_eq!(config.width_micros, 60_000_000);
    assert_physical_plan(&config.partial_aggregation_plan);
    assert_physical_plan(&config.final_aggregation_plan);
}

#[test(tokio::test)]
async fn native_hop_aggregate_plan() {
    let bytes = native_operator_config(
        "SELECT bid.auction AS key, HOP(INTERVAL '1 minute', INTERVAL '5 minutes') AS window, \
         COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window",
        OperatorName::SlidingWindowAggregate,
    )
    .await;
    let config = SlidingWindowAggregateOperator::decode(bytes.as_slice()).unwrap();
    assert_eq!(config.slide_micros, 60_000_000);
    assert_eq!(config.width_micros, 300_000_000);
    assert_physical_plan(&config.partial_aggregation_plan);
    assert_physical_plan(&config.final_aggregation_plan);
}

#[test(tokio::test)]
async fn finalized_hop_projection_can_feed_an_ordinary_aggregate() {
    let sql = "WITH closed AS (
        SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
               COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
      )
      SELECT k, COUNT(*) AS closed_windows, SUM(n) AS pane_memberships,
             MAX(n) AS peak, MAX(window_end) AS latest_end
      FROM (SELECT k, window.end AS window_end, n FROM closed) AS finished GROUP BY k";
    let compiled = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("finalized HOP aggregate failed to plan: {error}\n{sql}"));
    let operators = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator.operator_name)
        .collect::<Vec<_>>();
    assert_eq!(
        operators
            .iter()
            .filter(|name| **name == OperatorName::SlidingWindowAggregate)
            .count(),
        1
    );
    assert_eq!(
        operators
            .iter()
            .filter(|name| **name == OperatorName::UpdatingAggregate)
            .count(),
        1
    );
}

#[test(tokio::test)]
async fn carrying_the_window_struct_keeps_window_scope() {
    let sql = "WITH closed AS (
        SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
               COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
      )
      SELECT k, COUNT(carried_window.end) AS closed_windows
      FROM (SELECT k, window AS carried_window, n FROM closed) AS carried GROUP BY k";
    rejects_with(sql, "must have window in aggregate").await;
}

#[test(tokio::test)]
async fn carrying_an_aliased_window_still_allows_nested_window_aggregation() {
    let sql = "WITH closed AS (
        SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
               COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
      )
      SELECT k, carried_window, SUM(n) AS total
      FROM (SELECT k, window AS carried_window, n FROM closed) AS carried
      GROUP BY k, carried_window";
    let compiled = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("aliased nested window failed to plan: {error}\n{sql}"));
    let operators = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .collect::<Vec<_>>();
    assert_eq!(
        operators
            .iter()
            .filter(|operator| operator.operator_name == OperatorName::SlidingWindowAggregate)
            .count(),
        1,
    );
    let nested = operators
        .iter()
        .filter(|operator| operator.operator_name == OperatorName::TumblingWindowAggregate)
        .collect::<Vec<_>>();
    assert_eq!(nested.len(), 1);
    let config =
        TumblingWindowAggregateOperator::decode(nested[0].operator_config.as_slice()).unwrap();
    assert_eq!(config.name, "InstantWindow");
    assert_eq!(config.width_micros, 0);
    let input: arrow_schema::Schema =
        serde_json::from_str(&config.input_schema.unwrap().arrow_schema).unwrap();
    assert!(matches!(
        input.field_with_name("carried_window").unwrap().data_type(),
        arrow_schema::DataType::Struct(_)
    ));
    assert!(
        operators
            .iter()
            .all(|operator| operator.operator_name != OperatorName::UpdatingAggregate)
    );
}

#[test(tokio::test)]
async fn native_session_aggregate_plan() {
    let bytes = native_operator_config(
        "SELECT bid.auction AS key, SESSION(INTERVAL '30 minutes') AS window, COUNT(*) AS n \
         FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window",
        OperatorName::SessionWindowAggregate,
    )
    .await;
    let config = SessionWindowAggregateOperator::decode(bytes.as_slice()).unwrap();
    assert_eq!(config.gap_micros, 1_800_000_000);
    assert_physical_plan(&config.final_aggregation_plan);
    // SESSION currently retains raw input; a plan does not prove partial state,
    // inclusive closure, late-input compatibility or a maximum-duration policy.
}

#[test(tokio::test)]
async fn native_updating_input_joins_reject() {
    for (left, right, diagnostic) in [
        ("counts", "raw", "can't handle updating left side of join"),
        ("raw", "counts", "can't handle updating right side of join"),
    ] {
        let sql = format!(
            "WITH counts AS (SELECT bid.auction AS key, COUNT(*) AS n FROM nexmark \
             WHERE bid IS NOT NULL GROUP BY bid.auction), \
             raw AS (SELECT bid.auction AS key FROM nexmark WHERE bid IS NOT NULL) \
             SELECT l.key, r.key FROM {left} AS l JOIN {right} AS r ON l.key = r.key"
        );
        rejects_with(&sql, diagnostic).await;
    }
}

#[test(tokio::test)]
async fn native_incompatible_window_join_rejects() {
    rejects_with(
        "WITH left_counts AS (SELECT bid.auction AS key, TUMBLE(INTERVAL '1 minute') AS window, \
         COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window), \
         right_counts AS (SELECT bid.auction AS key, TUMBLE(INTERVAL '2 minutes') AS window, \
         COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window) \
         SELECT l.key, r.n FROM left_counts AS l JOIN right_counts AS r \
         ON l.key = r.key AND l.window = r.window",
        "can't handle mixed windowing between left and right",
    )
    .await;
}

#[test(tokio::test)]
async fn native_session_ranking_rejects() {
    rejects_with(
        "SELECT key, n, ROW_NUMBER() OVER (PARTITION BY window ORDER BY n DESC) AS rank \
         FROM (SELECT bid.auction AS key, SESSION(INTERVAL '30 minutes') AS window, \
         COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window)",
        "Window functions do not support session windows",
    )
    .await;
}
