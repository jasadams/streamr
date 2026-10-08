//! Retention configuration probes; worker value and recovery assertions are separate.

use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
use prost::Message;
use test_log::test;

async fn aggregate_config(settings: &str) -> UpdatingAggregateOperator {
    let sql = format!(
        "{settings}
         CREATE TABLE src WITH (connector = 'impulse', event_rate = '1');
         SELECT counter, COUNT(*) AS n FROM src GROUP BY counter"
    );
    let compiled = parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default())
        .await
        .unwrap_or_else(|error| panic!("retention probe failed to plan: {error}\n{sql}"));
    let operators: Vec<_> = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .collect();
    assert_eq!(operators.len(), 1);
    UpdatingAggregateOperator::decode(operators[0].0.operator_config.as_slice()).unwrap()
}

#[test(tokio::test)]
async fn updating_retention_default_is_24_hours() {
    let config = aggregate_config("").await;
    assert_eq!(config.ttl_micros, 86_400_000_000);
    assert_eq!(config.retain_indefinitely, None);
}

#[test(tokio::test)]
async fn updating_retention_finite_override() {
    let config = aggregate_config("SET updating_ttl = INTERVAL '30 minutes';").await;
    assert_eq!(config.ttl_micros, 1_800_000_000);
    assert_eq!(config.retain_indefinitely, None);
}

#[test(tokio::test)]
async fn updating_retention_zero_interval_preserves_legacy_marker() {
    let config = aggregate_config("SET updating_ttl = INTERVAL '0 seconds';").await;
    assert_eq!(config.ttl_micros, 0);
    assert_eq!(config.retain_indefinitely, None);
}

#[test(tokio::test)]
async fn updating_retention_explicit_null() {
    let config = aggregate_config("SET updating_ttl = NULL;").await;
    assert_eq!(config.ttl_micros, 0);
    assert_eq!(config.retain_indefinitely, Some(true));
}

#[test(tokio::test)]
async fn updating_retention_finite_setting_replaces_disabled_setting() {
    let config =
        aggregate_config("SET updating_ttl = NULL; SET updating_ttl = INTERVAL '1 hour';").await;
    assert_eq!(config.ttl_micros, 3_600_000_000);
    assert_eq!(config.retain_indefinitely, None);
}

#[test]
fn updating_retention_invalid_value_keeps_previous_setting() {
    let mut provider = ArroyoSchemaProvider::new();
    let invalid = crate::parse_sql("SET updating_ttl = 'indefinite'").unwrap();
    assert!(crate::try_handle_set_variable(&invalid[0], &mut provider).is_err());
    assert_eq!(
        provider.planning_options.ttl,
        Some(std::time::Duration::from_secs(86_400))
    );
}

#[test]
fn indefinite_aggregate_retention_keeps_join_retention_finite() {
    let mut provider = ArroyoSchemaProvider::new();
    for statement in
        crate::parse_sql("SET updating_ttl = INTERVAL '30 minutes'; SET updating_ttl = NULL;")
            .unwrap()
    {
        crate::try_handle_set_variable(&statement, &mut provider).unwrap();
    }
    assert_eq!(provider.planning_options.ttl, None);
    assert_eq!(
        provider.planning_options.join_ttl,
        std::time::Duration::from_secs(30 * 60)
    );
}
