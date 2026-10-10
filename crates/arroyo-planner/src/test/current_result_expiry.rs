//! Selected rolling-result composition probes: watermark-driven current-result
//! expiry lowering. These assert the planned operator contract, not runtime
//! values, recovery or resource bounds.

use super::get_test_schema_provider;
use crate::ArroyoSchemaProvider;
use crate::{SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::{EventTimeExpiry, UpdatingAggregateOperator};
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use prost::Message;
use test_log::test;

const COMPOSITION_SQL: &str = "
WITH lifetime AS (
    SELECT bid.auction AS k, COUNT(*) AS lifetime_count
    FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction
),
rolling AS (
    SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
           COUNT(*) AS recent_count
    FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
),
closed AS (SELECT k, window.end AS window_end, recent_count FROM rolling),
latest_rolling AS (
    SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count
    FROM closed GROUP BY k
),
normalized AS (
    SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime
    UNION ALL
    SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling
)
SELECT k, MAX(lifetime_count) AS lifetime_count,
       COALESCE(MAX(recent_count), 0) AS recent_count
FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL";

async fn updating_aggregate_configs(sql: &str) -> Vec<UpdatingAggregateOperator> {
    let compiled = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("composition probe failed to plan: {error}\n{sql}"));
    compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .collect()
}

fn expiry_of(config: &UpdatingAggregateOperator) -> &EventTimeExpiry {
    config
        .event_time_expiry
        .as_ref()
        .unwrap_or_else(|| panic!("{} is missing event-time expiry", config.name))
}

#[test(tokio::test)]
async fn current_result_composition_plans_zero_and_ownership_sql_with_expiry() {
    let configs = updating_aggregate_configs(COMPOSITION_SQL).await;
    assert_eq!(configs.len(), 3, "lifetime, current result and outer");
    let expiring: Vec<_> = configs
        .iter()
        .filter(|config| config.event_time_expiry.is_some())
        .collect();
    assert_eq!(
        expiring.len(),
        1,
        "only the current-result aggregate expires"
    );
    let expiry = expiry_of(expiring[0]);
    assert_eq!(
        expiry.delay_nanos, 2_000_000_000,
        "validity runs one HOP slide past the result timestamp"
    );
    let node = PhysicalExprNode::decode(expiry.result_timestamp_expr.as_slice()).unwrap();
    let ExprType::Column(column) = node.expr_type.unwrap() else {
        panic!("result timestamp must lower to a column expression");
    };
    assert_eq!(column.name, "window_end");
}

#[test(tokio::test)]
async fn tumble_current_result_expiry_delays_by_the_width() {
    let sql = COMPOSITION_SQL.replace(
        "HOP(INTERVAL '2 seconds', INTERVAL '4 seconds')",
        "TUMBLE(INTERVAL '4 seconds')",
    );
    let configs = updating_aggregate_configs(&sql).await;
    let expiring: Vec<_> = configs
        .iter()
        .filter(|config| config.event_time_expiry.is_some())
        .collect();
    assert_eq!(expiring.len(), 1);
    assert_eq!(expiry_of(expiring[0]).delay_nanos, 4_000_000_000);
}

#[test(tokio::test)]
async fn ordered_last_value_without_finalized_window_lineage_has_no_expiry() {
    let sql = "SELECT bid.auction AS k,
       LAST_VALUE(bid.price ORDER BY bid.datetime) AS last_price
       FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction";
    for config in updating_aggregate_configs(sql).await {
        assert!(
            config.event_time_expiry.is_none(),
            "plain current values keep their existing retention"
        );
    }
}

#[test(tokio::test)]
async fn ordinary_finalized_window_reaggregation_has_no_expiry() {
    let sql = "WITH closed AS (
        SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
               COUNT(*) AS n FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
      )
      SELECT k, COUNT(*) AS closed_windows, SUM(n) AS pane_memberships,
             MAX(n) AS peak, MAX(window_end) AS latest_end
      FROM (SELECT k, window.end AS window_end, n FROM closed) AS finished GROUP BY k";
    for config in updating_aggregate_configs(sql).await {
        assert!(
            config.event_time_expiry.is_none(),
            "ordinary window reaggregation is not a current-result composition"
        );
    }
}

#[test(tokio::test)]
async fn retaining_key_deletion_composition_over_a_retracting_source_plans() {
    // The ownership rule (HAVING on the lifetime contribution) needs a
    // lifetime relation whose keys can actually be removed. Update-mode
    // sources reject event-time fields, so the retaining key relation is a
    // separate changelog source while the rolling windows keep event time on
    // the append source.
    let sql = "
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '/tmp/str29-deletion-events.jsonl',
      format = 'json', type = 'source');
CREATE TABLE key_relation (k TEXT NOT NULL PRIMARY KEY, v BIGINT)
WITH (connector = 'single_file', path = '/tmp/str29-deletion-keys.jsonl',
      format = 'debezium_json', type = 'source');
CREATE TABLE out_sink (k TEXT, lifetime_count BIGINT, recent_count BIGINT)
WITH (connector = 'single_file', path = '/tmp/str29-deletion-output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW lifetime AS SELECT k, COUNT(*) AS lifetime_count
  FROM key_relation GROUP BY k;
CREATE VIEW rolling AS SELECT k,
  HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
  COUNT(*) AS recent_count FROM events GROUP BY k, window;
CREATE VIEW closed AS SELECT k, window.end AS window_end, recent_count FROM rolling;
CREATE VIEW latest_rolling AS SELECT k,
  LAST_VALUE(recent_count ORDER BY window_end) AS recent_count
  FROM closed GROUP BY k;
CREATE VIEW normalized AS
  SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime
  UNION ALL
  SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling;
INSERT INTO out_sink
SELECT k, MAX(lifetime_count) AS lifetime_count,
       COALESCE(MAX(recent_count), 0) AS recent_count
FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL";
    let compiled = parse_and_get_program(sql, ArroyoSchemaProvider::new(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("deletion composition failed to plan: {error}\n{sql}"));
    let configs: Vec<_> = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| (operator.operator_name, operator.operator_config.clone()))
        .collect();
    assert!(
        configs.iter().any(|(name, bytes)| {
            *name == OperatorName::UpdatingAggregate
                && UpdatingAggregateOperator::decode(bytes.as_slice())
                    .unwrap()
                    .event_time_expiry
                    .is_some()
        }),
        "the current-result stage keeps watermark expiry beside a retracting key relation"
    );
}

async fn assert_no_expiry(sql: &str) {
    for config in updating_aggregate_configs(sql).await {
        assert!(
            config.event_time_expiry.is_none(),
            "ordinary SQL must retain its established state: {}\n{sql}",
            config.name
        );
    }
}

const FINALIZED_WINDOWS: &str = "WITH rolling AS (
    SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
           COUNT(*) AS n
    FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window
), closed AS (SELECT k, window.end AS ended, n FROM rolling)";

#[test(tokio::test)]
async fn standalone_last_value_of_finalized_windows_keeps_history() {
    assert_no_expiry(&format!(
        "{FINALIZED_WINDOWS} SELECT k, LAST_VALUE(n ORDER BY ended) AS n FROM closed GROUP BY k"
    ))
    .await;
}

#[test(tokio::test)]
async fn last_value_beside_historical_sum_keeps_all_accumulators() {
    assert_no_expiry(&COMPOSITION_SQL.replace(
        "LAST_VALUE(recent_count ORDER BY window_end) AS recent_count",
        "LAST_VALUE(recent_count ORDER BY window_end) AS recent_count, SUM(recent_count) AS historical_sum",
    )).await;
    assert_no_expiry(&format!(
        "{FINALIZED_WINDOWS} SELECT k, LAST_VALUE(n ORDER BY ended), SUM(n) FROM closed GROUP BY k"
    ))
    .await;
}

#[test(tokio::test)]
async fn multiple_or_filtered_last_values_have_no_expiry() {
    for replacement in [
        "LAST_VALUE(recent_count ORDER BY window_end) AS recent_count, LAST_VALUE(recent_count ORDER BY window_end DESC) AS other",
        "LAST_VALUE(recent_count ORDER BY window_end) AS recent_count, LAST_VALUE(recent_count ORDER BY window_end) FILTER (WHERE recent_count > 1) AS other",
        "LAST_VALUE(recent_count ORDER BY window_end) FILTER (WHERE recent_count > 1) AS recent_count",
        "LAST_VALUE(recent_count ORDER BY window_end DESC) AS recent_count",
        "LAST_VALUE(recent_count ORDER BY window_end, recent_count) AS recent_count",
    ] {
        assert_no_expiry(&COMPOSITION_SQL.replace(
            "LAST_VALUE(recent_count ORDER BY window_end) AS recent_count",
            replacement,
        ))
        .await;
    }
}

#[test(tokio::test)]
async fn zero_and_retaining_ownership_are_required() {
    for sql in [
        COMPOSITION_SQL.replace(" HAVING MAX(lifetime_count) IS NOT NULL", ""),
        COMPOSITION_SQL.replace(
            "HAVING MAX(lifetime_count) IS NOT NULL",
            "HAVING MAX(recent_count) IS NOT NULL",
        ),
        COMPOSITION_SQL.replace(
            "COALESCE(MAX(recent_count), 0)",
            "COALESCE(MAX(recent_count), 7)",
        ),
        COMPOSITION_SQL.replace("COALESCE(MAX(recent_count), 0)", "MAX(recent_count)"),
        COMPOSITION_SQL.replace(
            "CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling",
            "recent_count AS lifetime_count, recent_count FROM latest_rolling",
        ),
        COMPOSITION_SQL.replace(
            "lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime",
            "lifetime_count, lifetime_count AS recent_count FROM lifetime",
        ),
    ] {
        assert_no_expiry(&sql).await;
    }
}

#[test(tokio::test)]
async fn differing_current_and_retaining_partitions_have_no_expiry() {
    let sql = COMPOSITION_SQL.replace(
        "SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count\n    FROM closed GROUP BY k",
        "SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count\n    FROM closed GROUP BY k, recent_count",
    );
    assert_no_expiry(&sql).await;
}

#[test(tokio::test)]
async fn composition_recognition_follows_fields_without_semantic_names() {
    let sql = COMPOSITION_SQL
        .replace("lifetime_count", "owned")
        .replace("recent_count", "live")
        .replace("window_end", "completed_at")
        .replace("latest_rolling", "selected")
        .replace("normalized", "contributions");
    assert_eq!(
        updating_aggregate_configs(&sql)
            .await
            .iter()
            .filter(|config| config.event_time_expiry.is_some())
            .count(),
        1
    );
}

#[test(tokio::test)]
async fn shared_materialized_current_result_gives_composition_private_expiry() {
    let sql = materialized_composition();
    let ordinary = "INSERT INTO ordinary SELECT k, recent_count AS n FROM latest_rolling;\n";
    let composed = "SELECT k, MAX(lifetime_count) AS lifetime_count, COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL;\n";
    for query in [
        format!("{sql}{ordinary}{composed}"),
        format!("{sql}{composed}{ordinary}"),
    ] {
        let configs = updating_aggregate_configs(&query).await;
        assert_eq!(
            configs.len(),
            4,
            "shared lifetime/outer and two current-result owners"
        );
        assert_eq!(
            configs
                .iter()
                .filter(|config| config.event_time_expiry.is_some())
                .count(),
            1,
            "only the private composed owner expires; ordinary LAST_VALUE keeps history"
        );
    }
}

#[test(tokio::test)]
async fn latest_value_and_window_partition_must_prove_a_current_count() {
    for sql in [
        COMPOSITION_SQL.replace(
            "COUNT(*) AS recent_count\n    FROM nexmark",
            "SUM(bid.price) AS recent_count\n    FROM nexmark",
        ),
        COMPOSITION_SQL.replace(
            "GROUP BY bid.auction, window",
            "GROUP BY bid.auction, bid.price, window",
        ),
    ] {
        assert_no_expiry(&sql).await;
    }
}

fn materialized_composition() -> String {
    let mut sql = String::from(
        "CREATE TABLE ordinary (k BIGINT, n BIGINT) WITH (connector = 'single_file', path = '/tmp/str29-ordinary.json', format = 'debezium_json', type = 'sink');\n",
    );
    // CREATE VIEW is rewritten before either sink. Both sinks reference the
    // same named RemoteTable operator; sink order must not select its semantics.
    sql.push_str("CREATE VIEW lifetime AS SELECT bid.auction AS k, COUNT(*) AS lifetime_count FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction;\n");
    sql.push_str("CREATE VIEW rolling AS SELECT bid.auction AS k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window, COUNT(*) AS recent_count FROM nexmark WHERE bid IS NOT NULL GROUP BY bid.auction, window;\n");
    sql.push_str(
        "CREATE VIEW closed AS SELECT k, window.end AS window_end, recent_count FROM rolling;\n",
    );
    sql.push_str("CREATE VIEW latest_rolling AS SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count FROM closed GROUP BY k;\n");
    sql.push_str("CREATE VIEW normalized AS SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime UNION ALL SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling;\n");
    sql
}

#[test(tokio::test)]
async fn independent_equivalent_materialized_current_results_keep_distinct_contracts() {
    let sql = materialized_composition();
    let query = format!("{sql}
        CREATE VIEW independent AS SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count FROM closed GROUP BY k;
        INSERT INTO ordinary SELECT k, recent_count AS n FROM independent;
        SELECT k, MAX(lifetime_count) AS lifetime_count, COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL");
    let configs = updating_aggregate_configs(&query).await;
    assert_eq!(configs.len(), 4);
    assert_eq!(
        configs
            .iter()
            .filter(|config| config.event_time_expiry.is_some())
            .count(),
        1
    );
}

#[test(tokio::test)]
async fn selected_fanout_and_private_name_collision_are_deterministic() {
    let sql = materialized_composition();
    let query = format!("{sql}
        CREATE VIEW __arroyo_current_result_0 AS SELECT k, recent_count AS n FROM latest_rolling;
        INSERT INTO ordinary SELECT k, n FROM __arroyo_current_result_0;
        CREATE TABLE composed (k BIGINT, lifetime_count BIGINT, recent_count BIGINT) WITH (connector = 'single_file', path = '/tmp/str29-composed.json', format = 'debezium_json', type = 'sink');
        INSERT INTO composed SELECT k, MAX(lifetime_count) AS lifetime_count, COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL;
        SELECT k, MAX(lifetime_count) AS lifetime_count, COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k HAVING MAX(lifetime_count) IS NOT NULL");
    let compiled = parse_and_get_program(&query, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap();
    assert_eq!(
        compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == OperatorName::SlidingWindowAggregate)
            .count(),
        1,
        "private current-result ownership must keep the finalized window producer shared"
    );
    let configs = updating_aggregate_configs(&query).await;
    assert_eq!(
        configs
            .iter()
            .filter(|config| config.event_time_expiry.is_some())
            .count(),
        2,
        "each proven composition owns its expiry alongside ordinary history"
    );
    assert_eq!(
        configs.len(),
        6,
        "shared lifetime, ordinary current, and two private current/outer pairs"
    );
    assert_eq!(
        configs,
        updating_aggregate_configs(&query).await,
        "private names and serialized plans are stable on recompilation"
    );
}
