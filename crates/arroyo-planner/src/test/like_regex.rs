//! Standard LIKE_REGEX source-to-planner-to-physical-codec evidence.
use crate::ArroyoSchemaProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
const AGENTS: &str = "SELECT incoming.agent, NOT (COALESCE(incoming.agent,'') LIKE_REGEX '^(node|nextjs-auth0|Go-http-client|Go-Auth0-SDK)') AS player_agent FROM (VALUES ('node/20'), ('Go-http-client/1.1'), ('Mozilla/5.0'), (''), (CAST(NULL AS VARCHAR))) AS incoming(agent)";
const CHANNELS: &str = r"SELECT p.host, CASE WHEN p.host LIKE_REGEX '(^|\.)(chatgpt\.com|claude\.ai|gemini\.google\.com|perplexity\.ai|copilot\.microsoft\.com)$' THEN 'AI Assistant' WHEN p.host LIKE_REGEX '(^|\.)(google\.com|bing\.com)$' THEN 'Organic search' ELSE 'Referral' END AS channel FROM (VALUES ('gemini.google.com'), ('www.google.com'), ('evilgoogle.com'), ('googleXcom'), ('chatgpt.com.example')) AS p(host)";
const PAID: &str = "SELECT p.medium, LOWER(TRIM(p.medium)) LIKE_REGEX '^cp[acemivx]$|_5653372' AS paid_pattern FROM (VALUES (' CPC '), ('track_5653372_more'), ('email'), (CAST(NULL AS VARCHAR))) AS p(medium)";
const NULLS: &str = "SELECT p.value LIKE_REGEX 'x' AS matched, NOT (p.value LIKE_REGEX 'x') AS negated FROM (VALUES (CAST(NULL AS VARCHAR))) AS p(value)";
async fn physical(sql: &str) -> Arc<dyn ExecutionPlan> {
    let statement = crate::parse_sql(sql).unwrap().remove(0);
    let provider = ArroyoSchemaProvider::new();
    let logical = crate::tables::produce_optimized_plan(&statement, &provider).unwrap();
    // Finite VALUES inputs need only the established SELECT unnest rewrite.
    let logical = logical
        .rewrite_with_subqueries(&mut crate::rewriters::UnnestRewriter {})
        .unwrap()
        .data;
    // Match the production finite executor's physical planner configuration
    // (parse_and_get_arrow_program). A default SessionContext introduces
    // multi-partition operators whose state persists across input batches.
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    config.options_mut().optimizer.repartition_aggregations = false;
    config.options_mut().optimizer.repartition_windows = false;
    config.options_mut().optimizer.repartition_sorts = false;
    config.options_mut().optimizer.repartition_joins = false;
    config.options_mut().execution.target_partitions = 1;
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(vec![])
        .build();
    DefaultPhysicalPlanner::default()
        .create_physical_plan(&logical, &state)
        .await
        .unwrap()
}

async fn actual(sql: &str) -> Vec<serde_json::Value> {
    let batches =
        datafusion::physical_plan::collect(physical(sql).await, SessionContext::new().task_ctx())
            .await
            .unwrap();
    let mut writer: arrow::json::Writer<_, arrow::json::writer::LineDelimited> =
        arrow::json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    let mut rows: Vec<serde_json::Value> = String::from_utf8(writer.into_inner())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    rows.sort_by_key(serde_json::Value::to_string);
    rows
}
fn expected(mut rows: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    rows.sort_by_key(serde_json::Value::to_string);
    rows
}

#[tokio::test]
async fn like_regex_complete_ticket_oracles() {
    use serde_json::json;
    for (sql, oracle) in [
        (
            AGENTS,
            vec![
                json!({"agent":"node/20","player_agent":false}),
                json!({"agent":"Go-http-client/1.1","player_agent":false}),
                json!({"agent":"Mozilla/5.0","player_agent":true}),
                json!({"agent":"","player_agent":true}),
                json!({"agent":null,"player_agent":true}),
            ],
        ),
        (
            CHANNELS,
            vec![
                json!({"host":"gemini.google.com","channel":"AI Assistant"}),
                json!({"host":"www.google.com","channel":"Organic search"}),
                json!({"host":"evilgoogle.com","channel":"Referral"}),
                json!({"host":"googleXcom","channel":"Referral"}),
                json!({"host":"chatgpt.com.example","channel":"Referral"}),
            ],
        ),
        (
            PAID,
            vec![
                json!({"medium":" CPC ","paid_pattern":true}),
                json!({"medium":"track_5653372_more","paid_pattern":true}),
                json!({"medium":"email","paid_pattern":false}),
                json!({"medium":null,"paid_pattern":null}),
            ],
        ),
        (NULLS, vec![json!({"matched":null,"negated":null})]),
    ] {
        assert_eq!(actual(sql).await, expected(oracle), "{sql}");
    }
    assert_eq!(
        actual("SELECT NULL LIKE_REGEX 'x' AS matched, NOT (NULL LIKE_REGEX 'x') AS negated").await,
        vec![json!({"matched":null,"negated":null})]
    );
    assert_eq!(actual("SELECT 'NODE' LIKE_REGEX 'node' AS sensitive, 'xnode' NOT LIKE_REGEX '^node' AS negated, 'xnode' LIKE_REGEX 'node' AND FALSE OR TRUE AS precedence").await, vec![json!({"sensitive":false,"negated":true,"precedence":true})]);
}

#[test]
fn like_regex_planning_diagnostics() {
    for (sql, message) in [
        ("SELECT 'x' LIKE_REGEX '['", "invalid pattern"),
        ("SELECT 'x' LIKE_REGEX '(?i)x'", "unsupported advanced"),
        (r"SELECT 'x' LIKE_REGEX '\d'", "unsupported regex escape"),
        (
            "SELECT 'x' LIKE_REGEX 'x' FLAG 'i'",
            "FLAG clauses are unsupported",
        ),
        ("SELECT 'x' LIKE_REGEX NULL", "non-null UTF8 literal"),
        (
            "SELECT v LIKE_REGEX v FROM (VALUES ('x')) AS s(v)",
            "non-null UTF8 literal",
        ),
    ] {
        let statement = crate::parse_sql(sql).unwrap().remove(0);
        let error = crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new())
            .unwrap_err();
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }
    let sql = format!(
        "SELECT 'x' LIKE_REGEX '{}'",
        "x".repeat(crate::sql_json::path::MAX_PATH_LENGTH + 1)
    );
    let statement = crate::parse_sql(&sql).unwrap().remove(0);
    assert!(
        crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new())
            .unwrap_err()
            .to_string()
            .contains("pattern exceeds")
    );
    let mut provider = ArroyoSchemaProvider::new();
    assert!(
        provider
            .add_rust_udf("#[udf] fn like_regex(x: i64) -> i64 { x }", "")
            .unwrap_err()
            .to_string()
            .contains("reserved")
    );
}

#[tokio::test]
#[ignore = "exports source-bound worker physical fixtures"]
async fn like_regex_complete_worker_fixtures() {
    use crate::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use prost::Message;
    fn adapt(
        plan: Arc<dyn ExecutionPlan>,
        values: &mut Vec<arrow_array::RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        if let Some(source) = plan
            .as_any()
            .downcast_ref::<datafusion::datasource::source::DataSourceExec>()
        {
            let memory = source
                .data_source()
                .as_any()
                .downcast_ref::<MemorySourceConfig>()
                .unwrap();
            assert!(values.is_empty());
            assert!(memory.projection().is_none());
            values.extend(memory.partitions().iter().flatten().cloned());
            return Arc::new(ArroyoMemExec::new("ticket_values".into(), plan.schema()));
        }
        let children = plan
            .children()
            .iter()
            .map(|child| adapt((*child).clone(), values))
            .collect();
        plan.with_new_children(children).unwrap()
    }

    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_LIKE_REGEX_FIXTURE_DIR").unwrap());
    std::fs::create_dir_all(&directory).unwrap();
    for (name, sql) in [
        ("agents", AGENTS),
        ("channels", CHANNELS),
        ("paid", PAID),
        ("nulls", NULLS),
    ] {
        let plan = physical(sql).await;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        std::fs::write(directory.join(format!("{name}.sql")), sql).unwrap();
        let mut values = Vec::new();
        let adapted = adapt(plan, &mut values);
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(adapted, &codec)
                .unwrap();
        std::fs::write(directory.join(format!("{name}.pb")), proto.encode_to_vec()).unwrap();
        std::fs::write(
            directory.join(format!("{name}_input.pb")),
            proto.encode_to_vec(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("{name}_input.plan.txt")),
            format!("{proto:?}"),
        )
        .unwrap();
        let file = std::fs::File::create(directory.join(format!("{name}_input.arrow"))).unwrap();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(file, values[0].schema().as_ref()).unwrap();
        for batch in values {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
}
