//! Source SQL -> logical binding -> finite unnest -> worker codec fixtures.
use crate::ArroyoSchemaProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

const TAGS: &str = r#"SELECT patch.tag, patch.enabled FROM JSON_TABLE('{"traits":{"tags":{"vip":true,"old":false,"gone":null}}}', '$.traits.tags.keyvalue()' COLUMNS (tag VARCHAR PATH '$.key', enabled BOOLEAN PATH '$.value')) AS patch"#;
const ACCOUNTS: &str = r#"SELECT patch.account, patch.details, patch.removed FROM JSON_TABLE('{"discord":{"external_id":"d2"},"steam":null}', '$.keyvalue()' COLUMNS (account VARCHAR PATH '$.key', details VARCHAR FORMAT JSON PATH '$.value', removed BOOLEAN EXISTS PATH '$.value ? (@ == null)')) AS patch"#;
const CORRELATED: &str = r#"SELECT incoming.canonical_id, patch.tag, patch.enabled FROM (VALUES ('p1', '{"traits":{"tags":{"vip":true}}}'), ('p2', '{"traits":{"tags":{"beta":false}}}')) AS incoming(canonical_id, payload), JSON_TABLE(incoming.payload, '$.traits.tags.keyvalue()' COLUMNS (tag VARCHAR PATH '$.key', enabled BOOLEAN PATH '$.value')) AS patch"#;

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
async fn exact_ticket_queries_plan_to_finite_rows() {
    use serde_json::json;
    assert_eq!(
        actual(TAGS).await,
        expected(vec![
            json!({"tag":"vip","enabled":true}),
            json!({"tag":"old","enabled":false}),
            json!({"tag":"gone","enabled":null})
        ])
    );
    assert_eq!(
        actual(ACCOUNTS).await,
        expected(vec![
            json!({"account":"discord","details":"{\"external_id\":\"d2\"}","removed":false}),
            json!({"account":"steam","details":"null","removed":true})
        ])
    );
    assert_eq!(
        actual(CORRELATED).await,
        expected(vec![
            json!({"canonical_id":"p1","tag":"vip","enabled":true}),
            json!({"canonical_id":"p2","tag":"beta","enabled":false})
        ])
    );
}

#[tokio::test]
async fn normal_alias_binding_and_unqualified_columns() {
    physical(&TAGS.replace("patch.tag, patch.enabled", "tag, enabled")).await;
    physical(&CORRELATED.replace("patch.tag, patch.enabled", "tag, enabled")).await;
    physical(
        &CORRELATED
            .replace(
                " AS incoming(canonical_id, payload),",
                " AS incoming(canonical_id, payload), (VALUES ('x')) AS extra(other),",
            )
            .replace(
                "SELECT incoming.canonical_id",
                "SELECT extra.other, incoming.canonical_id",
            ),
    )
    .await;
    physical(
        &TAGS
            .replace("AS patch", "AS patch(name, flag)")
            .replace("patch.tag, patch.enabled", "patch.name, patch.flag"),
    )
    .await;
}

#[test]
fn unsupported_columns_are_precise_errors() {
    for (columns, message) in [
        ("tag FOR ORDINALITY", "FOR ORDINALITY"),
        (
            "NESTED PATH '$.x' COLUMNS (x VARCHAR PATH '$')",
            "NESTED COLUMNS",
        ),
        ("tag INTEGER PATH '$.key'", "column type"),
        (
            "tag VARCHAR PATH '$.key' NULL ON EMPTY",
            "ON EMPTY / ON ERROR",
        ),
    ] {
        let sql = format!("SELECT * FROM JSON_TABLE('{{}}', '$' COLUMNS ({columns})) AS patch");
        let mut statement = crate::parse_sql(&sql).unwrap().remove(0);
        let error = crate::sql_json::lowering::rewrite_statement(&mut statement).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[tokio::test]
#[ignore = "exports source-bound worker physical fixtures"]
async fn json_table_complete_worker_fixtures() {
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
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    std::fs::create_dir_all(&directory).unwrap();
    let tags_variable = TAGS.replace("FROM JSON_TABLE(", "FROM (VALUES ('{\"traits\":{\"tags\":{\"vip\":true,\"old\":false,\"gone\":null}}}')) AS incoming(payload), JSON_TABLE(").replace("JSON_TABLE('{\"traits\":{\"tags\":{\"vip\":true,\"old\":false,\"gone\":null}}}',", "JSON_TABLE(incoming.payload,");
    let accounts_variable = ACCOUNTS.replace("FROM JSON_TABLE(", "FROM (VALUES ('{\"discord\":{\"external_id\":\"d2\"},\"steam\":null}')) AS incoming(payload), JSON_TABLE(").replace("JSON_TABLE('{\"discord\":{\"external_id\":\"d2\"},\"steam\":null}',", "JSON_TABLE(incoming.payload,");
    let collision = CORRELATED
        .replace("incoming.canonical_id", "incoming.__unnested")
        .replace(
            "incoming(canonical_id, payload)",
            "incoming(__unnested, payload)",
        );
    let presence = r#"SELECT incoming.case_name, JSON_EXISTS(payload, '$.traits.tags') AS present, JSON_EXISTS(payload, '$.traits.tags ? (@ == null)') AS is_null FROM (VALUES ('missing', '{}'), ('null', '{"traits":{"tags":null}}'), ('empty', '{"traits":{"tags":{}}}'), ('malformed', '{'), ('sql_null', CAST(NULL AS VARCHAR)), ('scalar', '{"traits":{"tags":true}}'), ('false', '{"traits":{"tags":false}}'), ('parent_null', '{"traits":null}')) AS incoming(case_name, payload)"#;
    for (name, sql) in [
        ("tags_standalone", TAGS),
        ("accounts_standalone", ACCOUNTS),
        ("tags", tags_variable.as_str()),
        ("accounts", accounts_variable.as_str()),
        ("correlated", CORRELATED),
        ("collision", collision.as_str()),
        ("presence", presence),
    ] {
        let plan = physical(sql).await;
        let mut values = Vec::new();
        let adapted = adapt(plan, &mut values);
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(adapted, &codec)
                .unwrap();
        std::fs::write(directory.join(format!("{name}.pb")), proto.encode_to_vec()).unwrap();
        std::fs::write(
            directory.join(format!("{name}.plan.txt")),
            format!("{proto:?}"),
        )
        .unwrap();
        std::fs::write(directory.join(format!("{name}.sql")), sql).unwrap();
        if !values.is_empty() {
            let file = std::fs::File::create(directory.join(format!("{name}.arrow"))).unwrap();
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(file, values[0].schema().as_ref())
                    .unwrap();
            for batch in values {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
    }
}

#[tokio::test]
async fn correlated_alias_overrides_and_private_name_collision_keep_values() {
    use serde_json::json;
    let renamed = CORRELATED
        .replace("AS patch", "AS patch(\"Name\", \"Flag\")")
        .replace("patch.tag, patch.enabled", "patch.\"Name\", patch.\"Flag\"");
    assert_eq!(
        actual(&renamed).await,
        expected(vec![
            json!({"canonical_id":"p1","Name":"vip","Flag":true}),
            json!({"canonical_id":"p2","Name":"beta","Flag":false})
        ])
    );
    let collision = CORRELATED
        .replace("incoming.canonical_id", "incoming.__unnested")
        .replace(
            "incoming(canonical_id, payload)",
            "incoming(__unnested, payload)",
        );
    assert_eq!(
        actual(&collision).await,
        expected(vec![
            json!({"__unnested":"p1","tag":"vip","enabled":true}),
            json!({"__unnested":"p2","tag":"beta","enabled":false})
        ])
    );
    let column_collision = CORRELATED
        .replace("incoming.canonical_id", "incoming.tag")
        .replace("incoming(canonical_id, payload)", "incoming(tag, payload)")
        .replace(
            "SELECT incoming.tag, patch.tag, patch.enabled",
            "SELECT incoming.tag AS outer_tag, patch.tag, patch.enabled",
        )
        .replace("AS patch", "AS patch(name, flag)")
        .replace("patch.tag, patch.enabled", "patch.name, patch.flag");
    assert_eq!(
        actual(&column_collision).await,
        expected(vec![
            json!({"outer_tag":"p1","name":"vip","flag":true}),
            json!({"outer_tag":"p2","name":"beta","flag":false})
        ])
    );
}

#[tokio::test]
async fn containing_derived_queries_keep_their_own_scope() {
    use serde_json::json;
    let sql = format!(
        "SELECT outside.id, nested.tag, nested.enabled FROM (VALUES ('outer')) AS outside(id), (SELECT * FROM ({TAGS}) AS own WHERE own.enabled = TRUE) AS nested"
    );
    assert_eq!(
        actual(&sql).await,
        vec![json!({"id":"outer","tag":"vip","enabled":true})]
    );
    let sql = format!(
        "SELECT outside.id, nested.tag FROM (VALUES ('outer')) AS outside(id), (SELECT own.tag, COUNT(*) AS n FROM ({TAGS}) AS own GROUP BY own.tag) AS nested"
    );
    assert_eq!(
        actual(&sql).await,
        expected(vec![
            json!({"id":"outer","tag":"vip"}),
            json!({"id":"outer","tag":"old"}),
            json!({"id":"outer","tag":"gone"})
        ])
    );
    let sql = "SELECT tag FROM (VALUES ('outer')) AS incoming(tag), JSON_TABLE('{\"a\":true}', '$.keyvalue()' COLUMNS(tag VARCHAR PATH '$.key')) AS patch";
    let statement = crate::parse_sql(sql).unwrap().remove(0);
    assert!(
        crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new()).is_err()
    );
}

#[tokio::test]
async fn correlated_stream_source_plans_through_arroyo_graph() {
    let sql = r#"CREATE TABLE payload_source (canonical_id TEXT NOT NULL, payload TEXT NOT NULL) WITH (connector = 'single_file', path = '/tmp/str57-input.json', format = 'json', type = 'source');
CREATE TABLE result (canonical_id TEXT, tag TEXT, enabled BOOLEAN) WITH (connector = 'single_file', path = '/tmp/str57-result.json', format = 'json', type = 'sink');
INSERT INTO result SELECT incoming.canonical_id, patch.tag, patch.enabled FROM payload_source AS incoming, JSON_TABLE(incoming.payload, '$.traits.tags.keyvalue()' COLUMNS (tag VARCHAR PATH '$.key', enabled BOOLEAN PATH '$.value')) AS patch;"#;
    crate::parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        crate::SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_select_unnest_preserves_same_named_source_columns() {
    use serde_json::json;
    let sql = r#"SELECT incoming.__unnested, incoming.__unnested_, unnest(json_table_rows(incoming.payload, '$.keyvalue()')) AS entry FROM (VALUES ('carry', 'carry_next', '{"first":true,"second":false}')) AS incoming(__unnested, __unnested_, payload)"#;
    assert_eq!(
        actual(sql).await,
        expected(vec![
            json!({"__unnested":"carry","__unnested_":"carry_next","entry":"{\"key\":\"first\",\"value\":true,\"id\":0}"}),
            json!({"__unnested":"carry","__unnested_":"carry_next","entry":"{\"key\":\"second\",\"value\":false,\"id\":0}"}),
        ])
    );
}

#[tokio::test]
async fn json_table_exists_physical_schema_is_nonnullable_after_unnest() {
    let standalone = physical(ACCOUNTS).await;
    assert!(
        !standalone
            .schema()
            .field_with_name("removed")
            .unwrap()
            .is_nullable()
    );
    let variable = r#"SELECT patch.account, patch.details, patch.removed FROM (VALUES ('{"discord":{"external_id":"d2"},"steam":null}')) AS incoming(payload), JSON_TABLE(incoming.payload, '$.keyvalue()' COLUMNS (account VARCHAR PATH '$.key', details VARCHAR FORMAT JSON PATH '$.value', removed BOOLEAN EXISTS PATH '$.value ? (@ == null)')) AS patch"#;
    let plan = physical(variable).await;
    assert!(
        !plan
            .schema()
            .field_with_name("removed")
            .unwrap()
            .is_nullable()
    );
}
