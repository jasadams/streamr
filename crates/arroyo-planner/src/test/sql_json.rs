//! Planner integration tests for the ANSI SQL/JSON constructors (STR-56
//! Phase B): full-ticket SELECT planning, nesting in CASE/WHERE/MERGE SET,
//! plan-time diagnostics, physical-codec roundtrips and UDF shadowing.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, Volatility, create_udf};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use prost::Message;
use test_log::test;

use crate::physical::new_registry;
use crate::sql_json::kernels::{
    json_exists, json_object, json_query, json_value, json_value_boolean, json_value_double,
};
use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};

const TICKET_SOURCE: &str = "CREATE TABLE payload_source (
    canonical_id TEXT NOT NULL,
    payload TEXT NOT NULL
) WITH (connector = 'single_file', path = '/tmp/str56-source.json', format = 'json', type = 'source');";

const TICKET_SINK: &str = "CREATE TABLE result (
    canonical_id TEXT,
    name_present BOOLEAN,
    name TEXT,
    score DOUBLE PRECISION,
    wishlisted BOOLEAN,
    email TEXT,
    accounts TEXT,
    coherent_payload TEXT
) WITH (connector = 'single_file', path = '/tmp/str56-sink.json', format = 'json', type = 'sink');";

const TEXT_SINK: &str = "CREATE TABLE result (v TEXT) WITH (connector = 'single_file', path = '/tmp/str56-sink.json', format = 'json', type = 'sink');";

async fn plan(sql: &str) {
    parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("failed to plan: {error}\n{sql}"));
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

/// The complete ordinary projection from the ticket, against a table source.
#[test(tokio::test)]
async fn ticket_projection_parses_and_plans() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TICKET_SINK}
         INSERT INTO result
         SELECT e.canonical_id,
           JSON_EXISTS(e.payload, '$.traits.name') AS name_present,
           JSON_VALUE(e.payload, '$.traits.name') AS name,
           JSON_VALUE(e.payload, '$.traits.quality_score'
                      RETURNING DOUBLE PRECISION) AS score,
           JSON_VALUE(e.payload, '$.traits.steam_wishlisted'
                      RETURNING BOOLEAN) AS wishlisted,
           JSON_VALUE(e.payload,
             '$.identifiers[*] ? (@.identity_type == \"email\").value') AS email,
           JSON_QUERY(e.payload, '$.traits.linked_accounts') AS accounts,
           JSON_OBJECT('source' VALUE 'google', 'medium' VALUE '',
             'accounts' VALUE JSON_QUERY(e.payload, '$.traits.linked_accounts')
               FORMAT JSON) AS coherent_payload
         FROM payload_source e;"
    );
    plan(&sql).await;
}

#[test(tokio::test)]
async fn json_functions_nest_in_case_where_and_projection() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TEXT_SINK}
         INSERT INTO result
         SELECT CASE
             WHEN JSON_EXISTS(payload, '$.traits.name') THEN JSON_VALUE(payload, '$.traits.name')
             ELSE COALESCE(JSON_VALUE(payload, '$.fallback'), 'none')
           END AS v
         FROM payload_source
         WHERE JSON_EXISTS(payload, '$.traits') AND payload IS NOT NULL;"
    );
    plan(&sql).await;
}

#[test(tokio::test)]
async fn json_functions_work_in_merge_set_and_predicate() {
    let sql = "CREATE TABLE events WITH (connector = 'impulse', event_rate = '1');
         CREATE STATE TABLE inventory (
            counter BIGINT PRIMARY KEY,
            quantity BIGINT,
            note TEXT
         ) PARTITION BY counter;
         CREATE VIEW applied AS
         MERGE INTO inventory AS target USING events AS source
         ON target.counter = source.counter
         WHEN MATCHED AND JSON_EXISTS(CAST(source.counter AS TEXT), '$') THEN
            UPDATE SET note = JSON_OBJECT('n' VALUE JSON_QUERY(CAST(source.counter AS TEXT), '$'))
         WHEN NOT MATCHED THEN
            INSERT (counter, quantity, note) VALUES (source.counter, 1, NULL)
         RETURNING source AS source, old AS old, new AS new, action AS action;
         SELECT source.counter, new.note FROM applied;";
    plan(sql).await;
}

#[test(tokio::test)]
async fn invalid_literal_path_is_a_plan_error_naming_the_path() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TEXT_SINK}
         INSERT INTO result SELECT JSON_VALUE(payload, '$.a[0]') FROM payload_source;"
    );
    reject(&sql, "$.a[0]").await;
}

#[test(tokio::test)]
async fn explicit_path_mode_is_rejected() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TEXT_SINK}
         INSERT INTO result SELECT JSON_VALUE(payload, 'STRICT $.a') FROM payload_source;"
    );
    reject(&sql, "LAX/STRICT").await;
}

#[test(tokio::test)]
async fn duplicate_object_keys_are_a_plan_error() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TEXT_SINK}
         INSERT INTO result SELECT JSON_OBJECT('a' VALUE 1, 'a' VALUE 2) FROM payload_source;"
    );
    reject(&sql, "duplicate JSON_OBJECT key 'a'").await;
}

/// Bare `VALUE NULL` and `CAST(NULL AS BIGINT)` are the canonical NULL ON
/// NULL spellings; DataFusion plans bare NULL as ScalarValue::Null under
/// VariadicAny, so both must plan and execute as JSON null.
#[test(tokio::test)]
async fn object_bare_and_typed_null_values_plan_and_execute() {
    let sql = format!(
        "{TICKET_SOURCE}
         {TEXT_SINK}
         INSERT INTO result SELECT JSON_OBJECT(
             'a' VALUE NULL,
             'b' VALUE CAST(NULL AS BIGINT),
             'c' VALUE JSON_QUERY(payload, '$.absent')
         ) FROM payload_source;"
    );
    plan(&sql).await;
}

#[test(tokio::test)]
async fn user_udf_cannot_shadow_sql_json_builtins() {
    let mut schema_provider = ArroyoSchemaProvider::new();
    for name in [
        "json_value",
        "json_value_boolean",
        "json_value_double",
        "json_query",
        "json_exists",
        "json_object",
    ] {
        let body = format!("#[udf] fn {name}(x: i64) -> i64 {{ x }}");
        let error = schema_provider.add_rust_udf(&body, "").unwrap_err();
        assert!(
            error.to_string().contains("reserved"),
            "expected reservation error for {name}, got: {error}"
        );
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Physical codec roundtrip: plan → serialize → decode with new_registry()
// → identical behavior, including JSON_OBJECT pair order and FORMAT JSON.
// ---------------------------------------------------------------------------

fn session_ctx() -> SessionContext {
    SessionContext::new()
}

fn one_row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        true,
    )]))
}

fn one_row(payload: Option<&str>) -> arrow_array::RecordBatch {
    arrow_array::RecordBatch::try_new(
        one_row_schema(),
        vec![Arc::new(StringArray::from(vec![payload]))],
    )
    .unwrap()
}

fn lit(value: &str) -> Expr {
    datafusion::prelude::lit(value)
}

fn column(name: &str) -> Expr {
    datafusion::prelude::col(name)
}

fn scalar_call(udf: Arc<datafusion::logical_expr::ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
}

/// Roundtrip a physical scalar expression through datafusion-proto's
/// name-based UDF serialization, decoding with the worker registry
/// ([`new_registry`]), and return two executable projections (original and
/// decoded) over the same input batch plus the context.
fn roundtrip_plan(
    expr: &Expr,
    schema: Arc<Schema>,
    batch: arrow_array::RecordBatch,
) -> (
    SessionContext,
    Arc<dyn ExecutionPlan>,
    Arc<dyn ExecutionPlan>,
) {
    use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
    use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;

    let ctx = session_ctx();
    let df_schema = datafusion::common::DFSchema::try_from(schema.clone()).unwrap();
    let physical = ctx
        .state()
        .create_physical_expr(expr.clone(), &df_schema)
        .unwrap();

    // Serialize just the scalar expression: physical UDFs are encoded by
    // name + args, and decode resolves the name through the registry.
    let proto = serialize_physical_expr(&physical, &DefaultPhysicalExtensionCodec {}).unwrap();
    let encoded = proto.encode_to_vec();
    let decoded_proto =
        datafusion_proto::protobuf::PhysicalExprNode::decode(encoded.as_slice()).unwrap();
    let registry = new_registry();
    let decoded_expr = parse_physical_expr(
        &decoded_proto,
        &registry,
        schema.as_ref(),
        &DefaultPhysicalExtensionCodec {},
    )
    .unwrap();

    let projection_for = |expr: Arc<dyn PhysicalExpr>| {
        let source: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], schema.clone(), None).unwrap();
        Arc::new(ProjectionExec::try_new(vec![(expr, "out".to_string())], source).unwrap())
            as Arc<dyn ExecutionPlan>
    };

    (ctx, projection_for(physical), projection_for(decoded_expr))
}

async fn first_column(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn Array> {
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.len(), 1);
    batches[0].column(0).clone()
}

fn assert_text(column: &Arc<dyn Array>, expected: Option<&str>, label: &str) {
    let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
    match expected {
        Some(expected) => {
            assert!(!strings.is_null(0), "{label}: unexpectedly null");
            assert_eq!(strings.value(0), expected, "{label}");
        }
        None => assert!(strings.is_null(0), "{label}: unexpectedly set"),
    }
}

#[test(tokio::test)]
async fn json_object_roundtrip_preserves_key_order_and_format_json() {
    // A separate column supplies the FORMAT JSON value so the call is not
    // const-folded away; the metadata literal carries pair order and flags.
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
        Field::new("embedded", DataType::Utf8, true),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("ignored")])),
            Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])),
        ],
    )
    .unwrap();

    // The lowered form: json_object(meta, v1, v2, ...) with key order
    // z, a, embedded (deliberately not sorted) and FORMAT JSON on 'embedded'.
    let meta = r#"[["z",false],["a",false],["embedded",true]]"#;
    let expr = scalar_call(
        json_object(),
        vec![
            lit(meta),
            lit("first"),
            datafusion::prelude::lit(2_i64),
            column("embedded"),
        ],
    );

    let (ctx, original, decoded) = roundtrip_plan(&expr, schema, batch);
    let expected = r#"{"z":"first","a":2,"embedded":{"a":1}}"#;
    for (label, plan) in [("original", original), ("decoded", decoded)] {
        let out = first_column(&ctx, plan).await;
        assert_text(&out, Some(expected), label);
    }
}

#[test(tokio::test)]
async fn extraction_functions_roundtrip_through_worker_registry() {
    let schema = one_row_schema();
    let batch = one_row(Some(
        r#"{"traits":{"name":null,"score":0,"flag":false},"xs":[1]}"#,
    ));

    let cases: Vec<(Arc<datafusion::logical_expr::ScalarUDF>, &str, Option<&str>)> = vec![
        (json_exists(), "$.traits.name", Some("true")),
        (json_exists(), "$.missing", Some("false")),
        (json_value(), "$.traits.name", None),
        (json_value(), "$.traits.score", Some("0")),
        (json_value_boolean(), "$.traits.flag", Some("false")),
        (json_value_double(), "$.traits.score", Some("0")),
        (
            json_query(),
            "$.traits",
            // serde_json's default map serializes keys in sorted order.
            Some(r#"{"flag":false,"name":null,"score":0}"#),
        ),
        (json_query(), "$.traits.name", Some("null")),
        // A single-element array wildcard is one item; JSON_VALUE of the
        // number 1 as VARCHAR is "1".
        (json_value(), "$.xs[*]", Some("1")),
    ];

    for (udf, path, expected) in cases {
        let expr = scalar_call(udf.clone(), vec![column("payload"), lit(path)]);
        let (ctx, original, decoded) = roundtrip_plan(&expr, schema.clone(), batch.clone());
        for (label, plan) in [("original", original), ("decoded", decoded)] {
            let out = first_column(&ctx, plan).await;
            match expected {
                Some(expected) => {
                    if let Some(strings) = out.as_any().downcast_ref::<StringArray>() {
                        assert!(!strings.is_null(0), "{label}: {}({path}) null", udf.name());
                        assert_eq!(
                            strings.value(0),
                            expected,
                            "{label}: {}({path})",
                            udf.name()
                        );
                    } else if let Some(booleans) = out.as_any().downcast_ref::<BooleanArray>() {
                        assert!(!booleans.is_null(0), "{label}: {}({path}) null", udf.name());
                        assert_eq!(
                            booleans.value(0).to_string(),
                            expected,
                            "{label}: {}({path})",
                            udf.name()
                        );
                    } else if let Some(floats) =
                        out.as_any().downcast_ref::<arrow_array::Float64Array>()
                    {
                        assert!(!floats.is_null(0), "{label}: {}({path}) null", udf.name());
                        assert_eq!(
                            floats.value(0).to_string(),
                            expected,
                            "{label}: {}({path})",
                            udf.name()
                        );
                    } else {
                        panic!("{label}: unexpected output type for {}({path})", udf.name());
                    }
                }
                None => {
                    assert!(
                        out.is_null(0),
                        "{label}: {}({path}) expected null",
                        udf.name()
                    );
                }
            }
        }
    }
}

#[test(tokio::test)]
async fn decoded_spoof_udf_is_not_deserialized_as_builtin() {
    // A same-named user UDF must not resolve in place of the builtin: the
    // planner rejects such registrations (test above), and the worker registry
    // only contains the trusted implementations. Prove the registry lookup by
    // name yields the builtin implementation, and that registering a spoof on
    // a scratch registry does not affect the trusted one.
    let registry = new_registry();
    let udf = registry.udf("json_value").unwrap();
    assert_eq!(udf.name(), "json_value");
    assert!(
        udf.inner()
            .as_any()
            .is::<crate::sql_json::kernels::JsonValueFunc>()
    );

    let spoof = create_udf(
        "json_value",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|_| panic!("must not run")),
    );
    // The scratch registry accepts the spoof, but the trusted registry used
    // for deserialization is unaffected.
    let mut scratch = new_registry();
    scratch.register_udf(Arc::new(spoof)).unwrap();
    assert!(
        !scratch
            .udf("json_value")
            .unwrap()
            .inner()
            .as_any()
            .is::<crate::sql_json::kernels::JsonValueFunc>()
    );
    assert!(
        registry
            .udf("json_value")
            .unwrap()
            .inner()
            .as_any()
            .is::<crate::sql_json::kernels::JsonValueFunc>()
    );
}

/// Emit a complete SQL-planned projection for the actual worker executor test.
/// Only its native bounded VALUES source is adapted to the existing worker
/// batch input placeholder; no projection expression or VALUES row is rebuilt.
#[test(tokio::test)]
#[ignore = "opt-in complete SELECT physical fixture for worker execution"]
async fn sql_json_complete_select_worker_fixture() {
    use crate::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
    use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
    use datafusion_proto::physical_plan::AsExecutionPlan;

    fn adapt_values(
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
                .expect("complete SELECT must have only its native VALUES source");
            assert!(
                values.is_empty(),
                "fixture must have exactly one VALUES source"
            );
            assert!(
                memory.projection().is_none(),
                "native VALUES source must retain full input schema"
            );
            values.extend(memory.partitions().iter().flatten().cloned());
            return Arc::new(ArroyoMemExec::new("ticket_values".into(), plan.schema()));
        }
        let children = plan
            .children()
            .iter()
            .map(|child| adapt_values((*child).clone(), values))
            .collect();
        plan.with_new_children(children).unwrap()
    }

    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_SQL_JSON_FIXTURE_DIR").unwrap());
    std::fs::create_dir_all(&directory).unwrap();
    let sql = include_str!("../../../../scripts/fixtures/sql-json/ticket-projection.sql");
    let statement = crate::parse_sql(sql).unwrap().remove(0);
    let logical =
        crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new()).unwrap();
    let context = SessionContext::new();
    let physical = DefaultPhysicalPlanner::default()
        .create_physical_plan(&logical, &context.state())
        .await
        .unwrap();
    let mut values = Vec::new();
    let adapted = adapt_values(physical, &mut values);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].num_rows(), 1);
    assert_eq!(values[0].num_columns(), 2);
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let proto =
        datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(adapted, &codec)
            .unwrap();
    let serialized_plan = format!("{proto:?}");
    for function in [
        "json_exists",
        "json_value",
        "json_value_double",
        "json_value_boolean",
        "json_query",
        "json_object",
    ] {
        assert!(
            serialized_plan.contains(&format!("name: \"{function}\"")),
            "complete query must retain {function} through physical serialization"
        );
    }
    std::fs::write(directory.join("select-plan.txt"), serialized_plan).unwrap();
    std::fs::write(directory.join("select.pb"), proto.encode_to_vec()).unwrap();
    std::fs::write(directory.join("select.sql"), sql).unwrap();
    let file = std::fs::File::create(directory.join("values.arrow")).unwrap();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(file, values[0].schema().as_ref()).unwrap();
    writer.write(&values[0]).unwrap();
    writer.finish().unwrap();
    println!("SQL_JSON_COMPLETE_SELECT_FIXTURE {}", directory.display());
}
