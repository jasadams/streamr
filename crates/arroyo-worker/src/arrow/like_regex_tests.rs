//! Current-source SQL -> physical decode -> worker execution regressions.
use super::super::StatelessPhysicalExecutor;
use super::*;
use arrow_array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_rpc::{df::ArroyoSchema, errors::DataflowResult};
use arroyo_state::live::{
    LiveStateBackend, lifecycle::RocksStateConfig, memory::MemoryLiveState,
    resources::ResourceConfig, rocks::RocksLiveState,
};
use arroyo_types::{TaskInfo, Watermark};
use datafusion::physical_plan::common::collect;
use serde_json::{Value, json};

fn rows(batches: &[RecordBatch]) -> Vec<Value> {
    let mut writer: arrow::json::Writer<_, arrow::json::writer::LineDelimited> =
        arrow::json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    String::from_utf8(writer.into_inner())
        .unwrap()
        .lines()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect()
}

#[tokio::test]
#[ignore = "requires current-source LIKE_REGEX planner fixture export"]
async fn like_regex_decoded_complete_sql_and_source_batches_1_8() {
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_LIKE_REGEX_FIXTURE_DIR").unwrap());
    for (fixture, expected) in [
        (
            "agents",
            vec![
                json!({"agent":"node/20","player_agent":false}),
                json!({"agent":"Go-http-client/1.1","player_agent":false}),
                json!({"agent":"Mozilla/5.0","player_agent":true}),
                json!({"agent":"","player_agent":true}),
                json!({"agent":null,"player_agent":true}),
            ],
        ),
        (
            "channels",
            vec![
                json!({"host":"gemini.google.com","channel":"AI Assistant"}),
                json!({"host":"www.google.com","channel":"Organic search"}),
                json!({"host":"evilgoogle.com","channel":"Referral"}),
                json!({"host":"googleXcom","channel":"Referral"}),
                json!({"host":"chatgpt.com.example","channel":"Referral"}),
            ],
        ),
        (
            "paid",
            vec![
                json!({"medium":" CPC ","paid_pattern":true}),
                json!({"medium":"track_5653372_more","paid_pattern":true}),
                json!({"medium":"email","paid_pattern":false}),
                json!({"medium":null,"paid_pattern":null}),
            ],
        ),
        ("nulls", vec![json!({"matched":null,"negated":null})]),
    ] {
        let inputs = arrow::ipc::reader::StreamReader::try_new(
            std::fs::File::open(directory.join(format!("{fixture}_input.arrow"))).unwrap(),
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let input = arrow::compute::concat_batches(&inputs[0].schema(), &inputs).unwrap();
        assert_eq!(input.num_rows(), expected.len());
        let proto = std::fs::read(directory.join(format!("{fixture}.pb"))).unwrap();
        let mut worker =
            StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry())
                .unwrap();
        let output = collect(worker.process_batch(input.clone()).await)
            .await
            .unwrap();
        assert_eq!(rows(&output), expected, "complete SQL {fixture}");
        let proto = std::fs::read(directory.join(format!("{fixture}_input.pb"))).unwrap();
        // Repeat to ensure the batch-eight path actually receives eight source rows.
        let input = arrow::compute::concat_batches(
            &input.schema(),
            &[
                input.clone(),
                input.clone(),
                input.clone(),
                input.clone(),
                input.clone(),
                input.clone(),
                input.clone(),
                input,
            ],
        )
        .unwrap();
        for batch_size in [1, 8] {
            let mut worker =
                StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry())
                    .unwrap();
            let mut actual = Vec::new();
            for offset in (0..input.num_rows()).step_by(batch_size) {
                actual.extend(rows(
                    &collect(
                        worker
                            .process_batch(
                                input.slice(offset, batch_size.min(input.num_rows() - offset)),
                            )
                            .await,
                    )
                    .await
                    .unwrap(),
                ));
            }
            assert_eq!(
                actual,
                expected
                    .iter()
                    .cycle()
                    .take(expected.len() * 8)
                    .cloned()
                    .collect::<Vec<_>>(),
                "{fixture} source batch {batch_size}"
            );
        }
    }
}

const GUARD_SQL: &str = "CREATE TABLE regex_events (id VARCHAR NOT NULL, text VARCHAR, quantity BIGINT NOT NULL) WITH (connector='single_file',path='/tmp/regex-events.jsonl',format='json',type='source'); CREATE STATE TABLE regex_rows (id VARCHAR PRIMARY KEY, quantity BIGINT NOT NULL) PARTITION BY id; CREATE VIEW applied AS MERGE INTO regex_rows AS previous USING regex_events AS incoming ON previous.id=incoming.id WHEN MATCHED AND incoming.text LIKE_REGEX '^allow' THEN UPDATE SET quantity=incoming.quantity WHEN NOT MATCHED AND incoming.text LIKE_REGEX '^allow' THEN INSERT (id,quantity) VALUES (incoming.id,incoming.quantity) RETURNING source AS source,old AS old,new AS new,action AS action; SELECT * FROM applied;";

#[derive(Default)]
struct Captures(Vec<RecordBatch>);
#[async_trait::async_trait]
impl Collector for Captures {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        self.0.push(batch);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

struct Harness {
    owner: FusedStateTable,
    context: OperatorContext,
    backend: Arc<dyn LiveStateBackend>,
    rocks: Option<Arc<RocksLiveState>>,
    root: std::path::PathBuf,
    input: arrow_schema::SchemaRef,
}
fn unique_root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "streamr-regex-guard-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

async fn harness(rocks: bool) -> Harness {
    let compiled = arroyo_planner::parse_and_get_program(
        GUARD_SQL,
        arroyo_planner::ArroyoSchemaProvider::new(),
        arroyo_planner::SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap();
    let operator = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .find(|operator| {
            operator.operator_name == arroyo_datastream::logical::OperatorName::FusedStateTable
        })
        .expect("planned MERGE owner");
    let config = FusedStateTableOperator::decode(operator.operator_config.as_slice()).unwrap();
    let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
    let limits = guard_limits();
    let resources = WorkerStateResources::new(ResourceConfig {
        block_cache_bytes: 1 << 20,
        memtable_bytes: 1 << 20,
        queued_write_bytes: 2 << 20,
        // Match simultaneous production reservations: working row + pending
        // captures + typed-table overlay (scope_reservation) + decoded read
        // + key_workspace. These are private fixture limits, not worker defaults.
        decoded_value_bytes: limits.max_working_event_bytes
            + limits.max_pending_output_rows * limits.max_captured_event_bytes
            + 3 * limits.scope_bytes
            + 6 * limits.decoded_bytes
            + limits.scope_operations * (128 + std::mem::size_of::<arroyo_state::live::StateKey>())
            + 5 * limits.key_bytes
            + config.tables[0].primary_key.len() * 1024
            + 1024,
        scan_page_bytes: 8 << 20,
        max_blocking_operations: 2,
        max_snapshots: 2,
        max_open_databases: 1,
        disk_reserve_bytes: 0,
    })
    .unwrap();
    let program =
        FusedEventProgram::decode(&config, &limits, &arroyo_planner::physical::new_registry())
            .unwrap();
    // Use the production constructor's complete-event chunk bound. Source
    // batch size remains independent of the owner's bounded commit chunks.
    let mutations_per_event = program.steps.iter()
        .filter(|step| matches!(&step.operation, EventOperation::StateTable(access) if access.is_mutation()))
        .count().max(1);
    let worst_write_bytes = limits
        .row_bytes
        .checked_add(limits.key_bytes)
        .and_then(|bytes| bytes.checked_add(64))
        .and_then(|bytes| bytes.checked_mul(mutations_per_event))
        .unwrap();
    let chunk_rows = limits
        .scope_operations
        .checked_div(mutations_per_event)
        .unwrap_or(0)
        .min(limits.scope_bytes / worst_write_bytes)
        .min(limits.max_pending_output_rows)
        .min(limits.max_pending_output_bytes / (limits.max_captured_event_bytes + 64));
    assert_eq!(chunk_rows, 3, "fixture must use the production chunk bound");
    let mut owner = FusedStateTable {
        config,
        limits: limits.clone(),
        program,
        tables: HashMap::new(),
        resources: Some(resources.clone()),
        chunk_rows,
    };
    let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
    let mut context = OperatorContext::new(
        Arc::new(TaskInfo {
            job_id: "regex-guard".into(),
            operator_idx: 0,
            operator_name: "FusedStateTable".into(),
            operator_id: "owner".into(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        }),
        None,
        control_tx,
        1,
        vec![Arc::new(input.clone())],
        None,
        owner.tables(),
    )
    .await;
    let root = unique_root(if rocks { "rocks" } else { "memory" });
    let rocks_backend = if rocks {
        Some(Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "regex-guard".into(),
                    operator_id: "owner".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        ))
    } else {
        None
    };
    let backend: Arc<dyn LiveStateBackend> = match &rocks_backend {
        Some(backend) => backend.clone(),
        None => Arc::new(
            MemoryLiveState::bounded(resources.clone(), limits.max_resident_bytes).unwrap(),
        ),
    };
    let definitions = owner
        .config
        .tables
        .iter()
        .map(|table| {
            (
                transport_table_name(table.table_identity.as_bytes()).unwrap(),
                (descriptor(table).unwrap(), super::limits(&limits)),
            )
        })
        .collect();
    let handles = context
        .table_manager
        .register_typed_tables(definitions, backend.clone(), resources)
        .await
        .unwrap();
    owner.tables = handles
        .into_iter()
        .map(|(transport, handle)| {
            let table = owner
                .config
                .tables
                .iter()
                .find(|table| {
                    transport_table_name(table.table_identity.as_bytes()).unwrap() == transport
                })
                .unwrap();
            (table.table_identity.clone(), handle)
        })
        .collect();
    Harness {
        owner,
        context,
        backend,
        rocks: rocks_backend,
        root,
        input: input.schema,
    }
}

impl Harness {
    fn input(&self, events: &[(Option<&str>, i64)]) -> RecordBatch {
        let columns = self
            .input
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "id" => Arc::new(StringArray::from(vec!["row"; events.len()])) as ArrayRef,
                "text" => Arc::new(StringArray::from(
                    events.iter().map(|(text, _)| *text).collect::<Vec<_>>(),
                )) as ArrayRef,
                "quantity" => Arc::new(Int64Array::from(
                    events
                        .iter()
                        .map(|(_, quantity)| *quantity)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                "_timestamp" => {
                    Arc::new(TimestampNanosecondArray::from(vec![0; events.len()])) as ArrayRef
                }
                name => panic!("unexpected input {name}"),
            })
            .collect();
        RecordBatch::try_new(self.input.clone(), columns).unwrap()
    }
    async fn quantity(&self) -> Option<i64> {
        let table = self.owner.tables.values().next().unwrap();
        let key_schema = Arc::new(Schema::new(vec![
            table.descriptor().schema.field(0).clone(),
        ]));
        let key = RecordBatch::try_new(key_schema, vec![Arc::new(StringArray::from(vec!["row"]))])
            .unwrap();
        let stored = table.get(&key).await.unwrap()?;
        Some(
            stored
                .batch()
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
        )
    }
    async fn close(self) {
        let Self {
            owner,
            context,
            backend,
            rocks,
            root,
            ..
        } = self;
        drop(owner);
        drop(context);
        drop(backend);
        if let Some(rocks) = rocks {
            Arc::try_unwrap(rocks)
                .unwrap_or_else(|_| panic!("backend retained after shutdown"))
                .close_and_remove()
                .await
                .unwrap();
            if root.exists() {
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn like_regex_native_merge_guard_memory_rocks_batches_1_8_atomic_error() {
    for rocks in [false, true] {
        for batch_size in [1, 8] {
            let mut harness = harness(rocks).await;
            let mut captures = Captures::default();
            let rejected = vec![(Some("deny insertion"), 1); batch_size];
            let input = harness.input(&rejected);
            harness
                .owner
                .process_batch(input, &mut harness.context, &mut captures)
                .await
                .unwrap();
            assert_eq!(
                harness.quantity().await,
                None,
                "nonmatching guard inserted row"
            );
            captures.0.clear();
            let events = [
                (Some("allow seed"), 10),
                (Some("deny"), 20),
                (None, 30),
                (Some("allow update"), 40),
                (Some("xallow"), 50),
                (Some("ALLOW"), 60),
                (Some(""), 70),
                (None, 80),
            ];
            for chunk in events.chunks(batch_size) {
                let input = harness.input(chunk);
                harness
                    .owner
                    .process_batch(input, &mut harness.context, &mut captures)
                    .await
                    .unwrap();
            }
            assert_eq!(
                harness.quantity().await,
                Some(40),
                "guard backend rocks={rocks} batch={batch_size}"
            );
            let mut actions = Vec::new();
            for envelope in &captures.0 {
                for row in 0..envelope.num_rows() {
                    let capture = super::super::state_table::extract_capture(
                        &envelope.slice(row, 1),
                        0,
                        harness.owner.program.capture_schemas[0].clone(),
                    )
                    .unwrap();
                    let action = capture
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    actions.push(action.value(0).to_owned());
                }
            }
            assert_eq!(
                actions,
                [
                    "insert", "none", "none", "update", "none", "none", "none", "none"
                ]
            );
            // Both rows share one production owner scope: an earlier pending update
            // must roll back when regex evaluation on the next event fails.
            let oversized = "a".repeat(16 * 1024 * 1024 + 1);
            let input = harness.input(&[(Some("allow pending"), 90), (Some(&oversized), 100)]);
            let mut failed = Captures::default();
            let error = harness
                .owner
                .process_batch(input, &mut harness.context, &mut failed)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("LIKE_REGEX"), "{error}");
            assert!(failed.0.is_empty(), "failed owner scope emitted output");
            assert_eq!(
                harness.quantity().await,
                Some(40),
                "failed guard committed pending mutation"
            );
            let input = harness.input(&[(Some("allow after error"), 110)]);
            harness
                .owner
                .process_batch(input, &mut harness.context, &mut captures)
                .await
                .unwrap();
            assert_eq!(
                harness.quantity().await,
                Some(110),
                "owner failed to recover after evaluation error"
            );
            harness.close().await;
        }
    }
}

fn guard_limits() -> TypedSqlStateConfig {
    TypedSqlStateConfig {
        key_bytes: 1024,
        row_bytes: 64 * 1024,
        decoded_bytes: 64 * 1024,
        scope_bytes: 256 * 1024,
        scope_operations: 16,
        page_bytes: 256 * 1024,
        page_entries: 2,
        max_working_event_bytes: 128 * 1024 * 1024,
        max_captured_event_bytes: 256 * 1024,
        max_pending_output_rows: 8,
        max_pending_output_bytes: 1024 * 1024,
        max_resident_bytes: 1024 * 1024,
    }
}

async fn planned_owner(sql: &str) -> FusedStateTableOperator {
    let compiled = arroyo_planner::parse_and_get_program(
        sql,
        arroyo_planner::ArroyoSchemaProvider::new(),
        arroyo_planner::SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap();
    let operator = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(operator, _)| operator)
        .find(|operator| {
            operator.operator_name == arroyo_datastream::logical::OperatorName::FusedStateTable
        })
        .expect("SQL must produce a native fused owner");
    FusedStateTableOperator::decode(operator.operator_config.as_slice()).unwrap()
}

fn corrupt_regex_expression(bytes: &mut Vec<u8>, kind: &str) -> usize {
    use datafusion_proto::protobuf::physical_expr_node::ExprType;
    use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalScalarUdfNode};
    fn literal(value: datafusion::common::ScalarValue) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_type: Some(ExprType::Literal((&value).try_into().unwrap())),
        }
    }
    fn mutate(function: &mut PhysicalScalarUdfNode, kind: &str) {
        use datafusion::common::ScalarValue;
        match kind {
            "missing_argument" => {
                function.args.pop();
            }
            "extra_argument" => function
                .args
                .push(literal(ScalarValue::Utf8(Some("extra".into())))),
            "input_type" => function.args[0] = literal(ScalarValue::Int64(Some(1))),
            "null_pattern" => function.args[1] = literal(ScalarValue::Utf8(None)),
            "nonliteral_pattern" => function.args[1] = function.args[0].clone(),
            "different_literal" => {
                function.args[1] = literal(ScalarValue::Utf8(Some("different".into())))
            }
            "different_compiled_source" => {
                function.fun_definition = Some(serde_json::to_vec("different").unwrap())
            }
            "invalid_payload" => function.fun_definition = Some(b"{invalid".to_vec()),
            "oversized_payload" => function.fun_definition = Some(vec![b' '; 4096 * 6 + 3]),
            "return_type" => {
                function.return_type = Some((&arrow_schema::DataType::Utf8).try_into().unwrap())
            }
            "nullable" => function.nullable = !function.nullable,
            _ => panic!("unknown corruption {kind}"),
        }
    }
    fn visit(expression: &mut PhysicalExprNode, kind: &str) -> usize {
        match expression.expr_type.as_mut() {
            Some(ExprType::ScalarUdf(function)) => {
                if function.name == "like_regex" {
                    mutate(function, kind);
                    1
                } else {
                    function.args.iter_mut().map(|arg| visit(arg, kind)).sum()
                }
            }
            Some(ExprType::BinaryExpr(binary)) => {
                binary.l.as_mut().map_or(0, |arg| visit(arg, kind))
                    + binary.r.as_mut().map_or(0, |arg| visit(arg, kind))
            }
            Some(ExprType::NotExpr(not)) => not.expr.as_mut().map_or(0, |arg| visit(arg, kind)),
            _ => 0,
        }
    }
    let mut expression = PhysicalExprNode::decode(bytes.as_slice()).unwrap();
    let count = visit(&mut expression, kind);
    *bytes = expression.encode_to_vec();
    count
}

fn corrupt_regex_plan(
    plan: &mut datafusion_proto::protobuf::PhysicalPlanNode,
    kind: &str,
) -> usize {
    use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
    fn expression(expr: &mut datafusion_proto::protobuf::PhysicalExprNode, kind: &str) -> usize {
        let mut bytes = expr.encode_to_vec();
        let found = corrupt_regex_expression(&mut bytes, kind);
        *expr = datafusion_proto::protobuf::PhysicalExprNode::decode(bytes.as_slice()).unwrap();
        found
    }
    match plan.physical_plan_type.as_mut() {
        Some(PhysicalPlanType::Projection(projection)) => {
            projection
                .expr
                .iter_mut()
                .map(|expr| expression(expr, kind))
                .sum::<usize>()
                + projection
                    .input
                    .as_mut()
                    .map_or(0, |input| corrupt_regex_plan(input, kind))
        }
        Some(PhysicalPlanType::Filter(filter)) => {
            filter
                .expr
                .as_mut()
                .map_or(0, |expr| expression(expr, kind))
                + filter
                    .input
                    .as_mut()
                    .map_or(0, |input| corrupt_regex_plan(input, kind))
        }
        Some(PhysicalPlanType::Extension(extension)) => extension
            .inputs
            .iter_mut()
            .map(|input| corrupt_regex_plan(input, kind))
            .sum(),
        _ => 0,
    }
}

#[tokio::test]
async fn like_regex_native_owner_rejects_malformed_guard_and_projection_metadata() {
    let nested = GUARD_SQL.replace(
        "incoming.text LIKE_REGEX '^allow' THEN",
        "(incoming.text LIKE_REGEX '^allow') AND incoming.quantity > 0 THEN",
    );
    let projected = GUARD_SQL.replace("CREATE STATE TABLE", "CREATE VIEW checked AS SELECT id,text,quantity,text LIKE_REGEX '^allow' AS allowed FROM regex_events; CREATE STATE TABLE")
        .replace("USING regex_events AS incoming", "USING checked AS incoming")
        .replace("incoming.text LIKE_REGEX '^allow'", "incoming.allowed");
    for (label, sql) in [
        ("direct_guard", GUARD_SQL),
        ("nested_guard", nested.as_str()),
        ("fused_projection", projected.as_str()),
    ] {
        let original = planned_owner(sql).await;
        let registry = arroyo_planner::physical::new_registry();
        // Valid source SQL must decode before any mutation variants are tried.
        assert!(
            FusedEventProgram::decode(&original, &guard_limits(), &registry).is_ok(),
            "valid {label}"
        );
        for kind in [
            "missing_argument",
            "extra_argument",
            "input_type",
            "null_pattern",
            "nonliteral_pattern",
            "different_literal",
            "different_compiled_source",
            "invalid_payload",
            "oversized_payload",
            "return_type",
            "nullable",
        ] {
            let mut corrupted = original.clone();
            let mut found = 0;
            for step in &mut corrupted.steps {
                match step.kind.as_str() {
                    "state_access" if label != "fused_projection" => {
                        let mut access = arroyo_rpc::grpc::api::StateTableOperator::decode(
                            step.operator_config.as_slice(),
                        )
                        .unwrap();
                        for clause in &mut access.clauses {
                            if let Some(predicate) = &mut clause.predicate {
                                found += corrupt_regex_expression(predicate, kind);
                            }
                        }
                        step.operator_config = access.encode_to_vec();
                    }
                    "projection" if label == "fused_projection" => {
                        let mut projection = arroyo_rpc::grpc::api::ProjectionOperator::decode(
                            step.operator_config.as_slice(),
                        )
                        .unwrap();
                        for expression in &mut projection.exprs {
                            found += corrupt_regex_expression(expression, kind);
                        }
                        step.operator_config = projection.encode_to_vec();
                    }
                    "value" if label == "fused_projection" => {
                        let mut value = arroyo_rpc::grpc::api::ValuePlanOperator::decode(
                            step.operator_config.as_slice(),
                        )
                        .unwrap();
                        let mut plan = datafusion_proto::protobuf::PhysicalPlanNode::decode(
                            value.physical_plan.as_slice(),
                        )
                        .unwrap();
                        found += corrupt_regex_plan(&mut plan, kind);
                        value.physical_plan = plan.encode_to_vec();
                        step.operator_config = value.encode_to_vec();
                    }
                    _ => {}
                }
            }
            assert!(found > 0, "source SQL must retain regex in {label}");
            let result = FusedEventProgram::decode(&corrupted, &guard_limits(), &registry);
            assert!(
                result.is_err(),
                "native owner decode accepted {label}/{kind}"
            );
            let error = result.err().unwrap();
            assert!(
                format!("{error:#}").contains("LIKE_REGEX"),
                "{label}/{kind}: {error:#}"
            );
        }
    }
}
