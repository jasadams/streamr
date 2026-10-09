//! Event clock context and physical serialization regressions (STR-61).
use crate::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::ValuePlanOperator;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use test_log::test;

const SOURCE: &str = "CREATE TABLE events (
    id BIGINT PRIMARY KEY, event_time TIMESTAMP NOT NULL,
    completeness_time TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS completeness_time - INTERVAL '5' SECOND
) WITH (connector='single_file', path='/tmp/event-clock.jsonl', format='json', type='source');";

async fn compile(sql: &str) -> crate::CompiledSql {
    parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("{error}\n{sql}"))
}

async fn rejects(sql: &str, diagnostic: &str) {
    let error = parse_and_get_program(sql, ArroyoSchemaProvider::new(), SqlConfig::default())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(diagnostic),
        "expected {diagnostic}, got {error}"
    );
}

#[test(tokio::test)]
async fn event_clock_projection_view_filter_and_physical_round_trip() {
    assert!(
        ArroyoSchemaProvider::new()
            .functions
            .contains_key("current_timestamp")
    );
    let sql = format!(
        "{SOURCE}
        CREATE VIEW renamed AS SELECT id, event_time AS renamed_time FROM events;
        CREATE VIEW hidden AS SELECT id FROM renamed;
        SELECT id, WATERMARK_TIMESTAMP() AS reference, WATERMARK_DATE() AS reference_date,
            CURRENT_TIMESTAMP AS processing_timestamp, CURRENT_DATE AS processing_date
        FROM hidden WHERE WATERMARK_DATE() >= DATE '1969-12-31'"
    );
    let compiled = compile(&sql).await;
    let provider = ArroyoSchemaProvider::new();
    let runtime = RuntimeEnvBuilder::new().build().unwrap();
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let mut decoded = 0;
    for node in compiled.program.graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name != OperatorName::ArrowValue {
                continue;
            }
            let config = ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap();
            let proto = PhysicalPlanNode::decode(config.physical_plan.as_slice()).unwrap();
            let physical = proto
                .try_into_physical_plan(&provider, &runtime, &codec)
                .expect("context binding must survive worker physical reconstruction");
            let display = format!(
                "{}",
                datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
            );
            assert!(
                !display.contains("watermark_timestamp("),
                "unbound clock: {display}"
            );
            assert!(
                !display.contains("watermark_date("),
                "unbound clock: {display}"
            );
            decoded += 1;
        }
    }
    assert!(decoded > 0);
}

#[test(tokio::test)]
async fn event_clock_cdc_allows_independent_for_and_as_columns() {
    let source = SOURCE.replace("format='json'", "format='debezium_json'");
    compile(&format!(
        "{source} SELECT id, WATERMARK_TIMESTAMP(), WATERMARK_DATE() FROM events"
    ))
    .await;
    let source = source.replace(
        "WATERMARK FOR event_time",
        "WATERMARK FOR completeness_time",
    );
    compile(&format!(
        "{source} SELECT id, WATERMARK_TIMESTAMP(), WATERMARK_DATE() FROM events"
    ))
    .await;
}

#[test(tokio::test)]
async fn event_clock_lookup_keeps_trigger_designation() {
    compile(&format!(
        "{SOURCE}
        CREATE STATE TABLE retained (id BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY id;
        SELECT events.id, target._timestamp AS retained_time,
            WATERMARK_TIMESTAMP() AS event_reference, WATERMARK_DATE() AS date_reference
        FROM events LEFT JOIN retained AS target ON events.id=target.id
        WHERE events.id > 0 AND WATERMARK_DATE() >= DATE '1969-12-31'"
    ))
    .await;
}

#[test(tokio::test)]
async fn event_clock_missing_and_ambiguous_designation_are_errors() {
    rejects(
        "SELECT WATERMARK_DATE()",
        "requires a source with WATERMARK FOR",
    )
    .await;
    let no_designation = "CREATE TABLE events WITH (connector='impulse', event_rate='1');
        SELECT WATERMARK_TIMESTAMP() FROM events";
    rejects(no_designation, "requires a source with WATERMARK FOR").await;
    let second = SOURCE.replace("events", "other_events");
    rejects(&format!("{SOURCE}{second}
        SELECT WATERMARK_TIMESTAMP() FROM events INNER JOIN other_events ON events.id=other_events.id"),
        "ambiguous WATERMARK FOR sources").await;
}

#[test(tokio::test)]
async fn event_clock_aggregate_dependency_is_not_lowered_to_contribution_time() {
    rejects(&format!("{SOURCE} SELECT COUNT(*) FILTER (WHERE WATERMARK_DATE() = DATE '2026-10-10') FROM events"),
        "retained contribution time is a distinct input").await;
    rejects(
        &format!(
            "{SOURCE} CREATE VIEW counts AS SELECT COUNT(*) AS n FROM events;
        SELECT n, WATERMARK_DATE() FROM counts"
        ),
        "aggregate output time is not an event clock",
    )
    .await;
}

#[test(tokio::test)]
async fn event_clock_rejects_a_view_that_overwrites_trigger_time() {
    rejects(
        &format!(
            "{SOURCE} CREATE VIEW replaced AS
        SELECT id, completeness_time AS _timestamp FROM events;
        SELECT WATERMARK_TIMESTAMP() FROM replaced"
        ),
        "triggering timestamp was replaced by a projection",
    )
    .await;
}

#[test(tokio::test)]
async fn event_clock_view_alias_retains_aggregate_dependency() {
    rejects(
        &format!(
            "{SOURCE}
        CREATE VIEW clock_rows AS SELECT id, WATERMARK_DATE() AS d FROM events;
        CREATE VIEW renamed_clock AS SELECT id, d AS reference_date FROM clock_rows;
        SELECT COUNT(*) FILTER (WHERE reference_date = DATE '2026-10-10') FROM renamed_clock"
        ),
        "retained contribution time is a distinct input",
    )
    .await;
    compile(&format!(
        "{SOURCE}
        CREATE VIEW contribution_rows AS SELECT id, CAST(event_time AS DATE) AS d FROM events;
        SELECT COUNT(*) FILTER (WHERE d = DATE '2026-10-10') FROM contribution_rows"
    ))
    .await;
}
