//! SQL-planned fused-owner recovery using the existing typed checkpoint codec.
//! This exercises process_batch and a fresh decoded worker/backend. It does not
//! simulate controller selection, Kafka replay, or distributed barrier delivery.
use super::super::state_table::extract_capture;
use super::*;
use arrow_array::{Array, ArrayRef, StringArray, StructArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use arroyo_rpc::{df::ArroyoSchema, grpc::rpc::TypedStateTableSubtaskCheckpointMetadata};
use arroyo_state::live::{
    LiveStateBackend, checkpoint, lifecycle::RocksStateConfig, memory::MemoryLiveState,
    resources::ResourceConfig, rocks::RocksLiveState,
};
use arroyo_state::{StorageProviderFor, get_storage_provider};
use arroyo_types::{TaskInfo, Watermark};

const SQL: &str = "CREATE TABLE exit_events (canonical_id VARCHAR NOT NULL, exit_time TIMESTAMP) WITH (connector='single_file',path='/tmp/local-recovery-exits.jsonl',format='json',type='source'); CREATE STATE TABLE exit_profiles (canonical_id VARCHAR PRIMARY KEY, latest_exit TIMESTAMP) PARTITION BY canonical_id; CREATE VIEW applied AS MERGE INTO exit_profiles AS previous USING exit_events AS incoming ON previous.canonical_id=incoming.canonical_id WHEN MATCHED THEN UPDATE SET latest_exit=(SELECT MAX(candidate.t) FROM (VALUES (previous.latest_exit),(incoming.exit_time)) AS candidate(t)) WHEN NOT MATCHED THEN INSERT (canonical_id,latest_exit) VALUES (incoming.canonical_id,incoming.exit_time) RETURNING source AS source,old AS old,new AS new,action AS action; SELECT * FROM applied;";
const OCTOBER_FIRST: i64 = 1_790_812_800_000_000_000;
const DAY: i64 = 86_400_000_000_000;

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
    input: SchemaRef,
}

fn unique_root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "streamr-local-recovery-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

async fn harness(rocks: bool) -> Harness {
    harness_sql(rocks, SQL).await
}

async fn harness_sql(rocks: bool, sql: &str) -> Harness {
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
        .expect("planned MERGE owner");
    let config = FusedStateTableOperator::decode(operator.operator_config.as_slice()).unwrap();
    let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
    let limits = TypedSqlStateConfig {
        key_bytes: 1024,
        row_bytes: 64 * 1024,
        decoded_bytes: 64 * 1024,
        scope_bytes: 256 * 1024,
        scope_operations: 16,
        page_bytes: 256 * 1024,
        page_entries: 2,
        max_working_event_bytes: 4 * 1024 * 1024,
        max_captured_event_bytes: 256 * 1024,
        max_pending_output_rows: 8,
        max_pending_output_bytes: 1024 * 1024,
        max_resident_bytes: 1024 * 1024,
    };
    // The unconfigured checkpoint API requests a 1 MiB page in ensure_empty.
    // Backend snapshot admission reserves four copies plus namespace overhead
    // (4,195,496 bytes for this fixture); match that existing checkpoint
    // contract with private test headroom rather than changing engine limits.
    let resources = WorkerStateResources::new(ResourceConfig {
        block_cache_bytes: 1 << 20,
        memtable_bytes: 1 << 20,
        queued_write_bytes: 2 << 20,
        decoded_value_bytes: 16 << 20,
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
    let mut owner = FusedStateTable {
        config,
        limits: limits.clone(),
        program,
        tables: HashMap::new(),
        resources: Some(resources.clone()),
        chunk_rows: 8,
    };
    let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
    let mut context = OperatorContext::new(
        Arc::new(TaskInfo {
            job_id: "local-recovery".into(),
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
                    job_id: "local-recovery".into(),
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
                .unwrap_or_else(|_| panic!("worker/backend retained after shutdown"))
                .close_and_remove()
                .await
                .unwrap();
            if root.exists() {
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
    fn checkpoint_config(&self) -> TypedStateTableConfig {
        assert_eq!(self.owner.tables.len(), 1);
        let wire = self.owner.tables().into_values().next().unwrap();
        TypedStateTableConfig::decode(wire.config.as_slice()).unwrap()
    }
    async fn run(&mut self, values: &[(&str, Option<i64>)], batch_size: usize) -> Vec<RecordBatch> {
        let mut collector = Captures::default();
        for chunk in values.chunks(batch_size) {
            let columns = self
                .input
                .fields()
                .iter()
                .map(|field| match field.name().as_str() {
                    "canonical_id" => Arc::new(StringArray::from(
                        chunk.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    "exit_time" => arrow::compute::cast(
                        &TimestampNanosecondArray::from(
                            chunk
                                .iter()
                                .map(|(_, day)| day.map(|day| OCTOBER_FIRST + (day - 1) * DAY))
                                .collect::<Vec<_>>(),
                        ),
                        field.data_type(),
                    )
                    .unwrap(),
                    "_timestamp" => {
                        Arc::new(TimestampNanosecondArray::from(vec![0; chunk.len()])) as ArrayRef
                    }
                    name => panic!("unexpected source column {name}"),
                })
                .collect();
            self.owner
                .process_batch(
                    RecordBatch::try_new(self.input.clone(), columns).unwrap(),
                    &mut self.context,
                    &mut collector,
                )
                .await
                .unwrap();
        }
        collector
            .0
            .iter()
            .flat_map(|envelope| {
                (0..envelope.num_rows()).map(|row| {
                    extract_capture(
                        &envelope.slice(row, 1),
                        0,
                        self.owner.program.capture_schemas[0].clone(),
                    )
                    .unwrap()
                })
            })
            .collect()
    }
    async fn stored(&self, key: &str) -> RecordBatch {
        let key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "canonical_id",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![key]))],
        )
        .unwrap();
        self.owner
            .tables
            .values()
            .next()
            .unwrap()
            .get(&key)
            .await
            .unwrap()
            .unwrap()
            .batch()
            .clone()
    }
}

fn typed_time(row: &StructArray) -> Option<i64> {
    if row.is_null(0) {
        return None;
    }
    let array = arrow::compute::cast(
        row.column(1),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
    )
    .unwrap();
    let array = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    (!array.is_null(0)).then(|| array.value(0))
}

fn assert_capture(
    capture: &RecordBatch,
    key: &str,
    incoming: Option<i64>,
    old: Option<Option<i64>>,
    new: Option<i64>,
) {
    assert_eq!(capture.num_rows(), 1);
    for column in [0, 1, 2] {
        assert!(capture.column(column).as_any().is::<StructArray>());
    }
    let source = capture
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let prior = capture
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let current = capture
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        source
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        key
    );
    assert_eq!(
        typed_time(source),
        incoming.map(|day| OCTOBER_FIRST + (day - 1) * DAY)
    );
    assert_eq!(prior.is_null(0), old.is_none());
    if let Some(old) = old {
        assert_eq!(
            prior
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            key
        );
        assert_eq!(
            typed_time(prior),
            old.map(|day| OCTOBER_FIRST + (day - 1) * DAY)
        );
    }
    assert!(!current.is_null(0));
    assert_eq!(
        current
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        key
    );
    assert_eq!(
        typed_time(current),
        new.map(|day| OCTOBER_FIRST + (day - 1) * DAY)
    );
    assert_eq!(
        capture
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        if old.is_none() { "insert" } else { "update" }
    );
}

#[tokio::test]
async fn local_scalar_merge_checkpoint_fresh_worker_memory_rocks_batches_1_8() {
    // Selected checkpoint contains p1=Oct 3, p2=Oct 2 and p3=NULL.
    // Later local state is deliberately newer, proving restore selects the
    // exported checkpoint instead of reopening the old backend's WAL.
    let prefix = [
        ("p1", Some(3)),
        ("p1", Some(1)),
        ("p2", Some(2)),
        ("p3", None),
    ];
    let suffix = [
        ("p1", Some(1)),
        ("p2", Some(4)),
        ("p1", Some(5)),
        ("p2", Some(1)),
        ("p3", None),
        ("p3", Some(2)),
        ("p4", None),
        ("p4", Some(3)),
    ];
    for rocks in [false, true] {
        for batch_size in [1, 8] {
            let checkpoint_root = unique_root("checkpoint");
            std::fs::create_dir_all(&checkpoint_root).unwrap();
            let storage = get_storage_provider(&StorageProviderFor::Controller {
                storage_url: Some(format!("file://{}", checkpoint_root.display())),
            })
            .await
            .unwrap();
            let mut uninterrupted = harness(rocks).await;
            let prefix_captures = uninterrupted.run(&prefix, batch_size).await;
            assert_eq!(prefix_captures.len(), prefix.len());
            assert_capture(&prefix_captures[0], "p1", Some(3), None, Some(3));
            assert_capture(&prefix_captures[1], "p1", Some(1), Some(Some(3)), Some(3));
            assert_capture(&prefix_captures[2], "p2", Some(2), None, Some(2));
            assert_capture(&prefix_captures[3], "p3", None, None, None);
            let config = uninterrupted.checkpoint_config();
            let namespace = uninterrupted
                .owner
                .tables
                .values()
                .next()
                .unwrap()
                .namespace()
                .clone();
            let snapshot = uninterrupted.backend.snapshot().await.unwrap();
            let selected = checkpoint::export_typed(
                &snapshot,
                &namespace,
                &config,
                &storage,
                &format!(
                    "local-recovery/checkpoints/checkpoint-0000007/operator-owner/table-{}-000",
                    config.transport_name
                ),
                7,
                0,
                0,
                16,
            )
            .await
            .unwrap();
            drop(snapshot);
            assert!(!selected.empty);
            assert!(!selected.files.is_empty());
            // Use the same protobuf bytes a worker would receive for the selected
            // table checkpoint, rather than retaining exporter-local metadata.
            let selected = TypedStateTableSubtaskCheckpointMetadata::decode(
                selected.encode_to_vec().as_slice(),
            )
            .unwrap();
            let oracle = uninterrupted.run(&suffix, batch_size).await;
            assert_eq!(oracle.len(), suffix.len());
            let final_rows = [
                uninterrupted.stored("p1").await,
                uninterrupted.stored("p2").await,
                uninterrupted.stored("p3").await,
                uninterrupted.stored("p4").await,
            ];
            uninterrupted.close().await;
            let mut restored = harness(rocks).await;
            assert_eq!(restored.checkpoint_config(), config);
            let restored_namespace = restored
                .owner
                .tables
                .values()
                .next()
                .unwrap()
                .namespace()
                .clone();
            assert_eq!(restored_namespace, namespace);
            checkpoint::restore_typed(
                restored.backend.as_ref(),
                &restored_namespace,
                &config,
                &selected,
                &storage,
            )
            .await
            .unwrap();
            let continuation = restored.run(&suffix, batch_size).await;
            assert_eq!(continuation.len(), suffix.len());
            assert_eq!(
                continuation, oracle,
                "fresh owner must reproduce complete typed source/old/new/action captures"
            );
            let expected = [
                Some(Some(3)),
                Some(Some(2)),
                Some(Some(3)),
                Some(Some(4)),
                Some(None),
                Some(None),
                None,
                Some(None),
            ];
            let new_values = [
                Some(3),
                Some(4),
                Some(5),
                Some(4),
                None,
                Some(2),
                None,
                Some(3),
            ];
            for (index, capture) in continuation.iter().enumerate() {
                assert_capture(
                    capture,
                    suffix[index].0,
                    suffix[index].1,
                    expected[index],
                    new_values[index],
                );
            }
            for (index, key) in ["p1", "p2", "p3", "p4"].iter().enumerate() {
                assert_eq!(restored.stored(key).await, final_rows[index]);
            }
            restored.close().await;
            drop(storage);
            std::fs::remove_dir_all(checkpoint_root).unwrap();
        }
    }
}

fn inline_merge_sql(projection: &str, named: bool) -> String {
    let view = if named {
        format!("CREATE VIEW selected AS SELECT {projection} FROM items WHERE owner='C';")
    } else {
        String::new()
    };
    let source = if named {
        "selected".to_string()
    } else {
        format!("(SELECT {projection} FROM items WHERE owner='C')")
    };
    format!("CREATE TABLE items(owner TEXT NOT NULL,k BIGINT NOT NULL,payload TEXT NOT NULL) WITH (connector='single_file',path='/tmp/inline-merge-items.jsonl',format='json',type='source');
    CREATE STATE TABLE c_rows(k BIGINT,payload TEXT,PRIMARY KEY(k)) PARTITION BY k;
    {view}
    CREATE VIEW applied AS MERGE INTO c_rows AS target USING {source} AS source ON target.k=source.k
    WHEN MATCHED THEN UPDATE SET payload=source.payload
    WHEN NOT MATCHED THEN INSERT(k,payload) VALUES(source.k,source.payload)
    RETURNING source AS source,old AS old,new AS new,action AS action;
    SELECT * FROM applied;")
}

impl Harness {
    async fn items(&mut self, rows: &[(&str, i64, &str)]) -> Vec<RecordBatch> {
        let columns = self
            .input
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "owner" => Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                "k" => Arc::new(arrow_array::Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                "payload" => Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
                "_timestamp" => {
                    Arc::new(TimestampNanosecondArray::from(vec![17; rows.len()])) as ArrayRef
                }
                name => panic!("unexpected raw column {name}"),
            })
            .collect();
        // All raw source rows, including A/B and dropped columns, enter the
        // SQL-planned worker. No test-side relational transformation occurs.
        assert_eq!(self.input.fields().len(), 4);
        let mut captures = Captures::default();
        self.owner
            .process_batch(
                RecordBatch::try_new(self.input.clone(), columns).unwrap(),
                &mut self.context,
                &mut captures,
            )
            .await
            .unwrap();
        captures
            .0
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    extract_capture(
                        &batch.slice(row, 1),
                        0,
                        self.owner.program.capture_schemas[0].clone(),
                    )
                    .unwrap()
                })
            })
            // Null capture envelopes represent filtered events, not rows.
            .filter(|capture| capture.num_rows() != 0)
            .collect()
    }

    async fn assert_items_stored(&self) {
        for (key, payload) in [(1, Some("replacement")), (2, Some("second")), (3, None)] {
            let key = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
                vec![Arc::new(arrow_array::Int64Array::from(vec![key]))],
            )
            .unwrap();
            let value = self
                .owner
                .tables
                .values()
                .next()
                .unwrap()
                .get(&key)
                .await
                .unwrap();
            match payload {
                Some(payload) => {
                    let value = value.unwrap();
                    assert_eq!(value.batch().num_rows(), 1);
                    assert_eq!(value.batch().column(0), key.column(0));
                    assert_eq!(
                        value
                            .batch()
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        payload
                    );
                }
                None => assert!(value.is_none(), "excluded A key3 must not write state"),
            }
        }
    }
}

fn assert_item(
    capture: &RecordBatch,
    source_fields: &[&str],
    key: i64,
    payload: &str,
    old: Option<&str>,
) {
    assert_eq!(capture.num_rows(), 1);
    for (index, expected_payload) in [(0, Some(payload)), (1, old), (2, Some(payload))] {
        let row = capture
            .column(index)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(row.is_null(0), expected_payload.is_none());
        if let Some(expected_payload) = expected_payload {
            assert_eq!(
                row.column_by_name("k")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                key
            );
            assert_eq!(
                row.column_by_name("payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected_payload
            );
        }
        if index == 0 {
            assert_eq!(
                row.fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                source_fields
            );
            if let Some(owner) = row.column_by_name("owner") {
                assert_eq!(
                    owner
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "C"
                );
            }
        }
    }
    assert_eq!(
        capture
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        if old.is_some() { "update" } else { "insert" }
    );
    assert_eq!(
        capture
            .column(4)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        17
    );
}

#[tokio::test]
async fn inline_merge_filter_projection_checkpoint_fresh_worker_memory_rocks() {
    for rocks in [false, true] {
        for (projection, fields) in [
            ("k,payload", vec!["k", "payload"]),
            ("payload,k", vec!["payload", "k"]),
            ("owner,k,payload", vec!["owner", "k", "payload"]),
        ] {
            for named in [false, true] {
                let sql = inline_merge_sql(projection, named);
                let mut worker = harness_sql(rocks, &sql).await;
                let first = worker
                    .items(&[
                        ("A", 1, "excluded-a"),
                        ("C", 1, "first"),
                        ("B", 2, "excluded-b"),
                    ])
                    .await;
                assert_eq!(first.len(), 1, "{sql}");
                assert_item(&first[0], &fields, 1, "first", None);
                let config = worker.checkpoint_config();
                let namespace = worker
                    .owner
                    .tables
                    .values()
                    .next()
                    .unwrap()
                    .namespace()
                    .clone();
                let root = unique_root("inline-checkpoint");
                std::fs::create_dir_all(&root).unwrap();
                let storage = get_storage_provider(&StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{}", root.display())),
                })
                .await
                .unwrap();
                let snapshot = worker.backend.snapshot().await.unwrap();
                let selected = checkpoint::export_typed(
                    &snapshot,
                    &namespace,
                    &config,
                    &storage,
                    &format!(
                        "local-recovery/checkpoints/checkpoint-0000007/operator-owner/table-{}-000",
                        config.transport_name
                    ),
                    7,
                    0,
                    0,
                    16,
                )
                .await
                .unwrap();
                drop(snapshot);
                let selected = TypedStateTableSubtaskCheckpointMetadata::decode(
                    selected.encode_to_vec().as_slice(),
                )
                .unwrap();
                let suffix = [
                    ("B", 1, "excluded-replacement"),
                    ("C", 1, "replacement"),
                    ("C", 2, "second"),
                    ("A", 3, "excluded-new"),
                ];
                let expected = worker.items(&suffix).await;
                assert_eq!(expected.len(), 2);
                assert_item(&expected[0], &fields, 1, "replacement", Some("first"));
                assert_item(&expected[1], &fields, 2, "second", None);
                worker.assert_items_stored().await;
                worker.close().await;
                let mut fresh = harness_sql(rocks, &sql).await;
                let fresh_namespace = fresh
                    .owner
                    .tables
                    .values()
                    .next()
                    .unwrap()
                    .namespace()
                    .clone();
                assert_eq!(fresh_namespace, namespace);
                checkpoint::restore_typed(
                    fresh.backend.as_ref(),
                    &fresh_namespace,
                    &config,
                    &selected,
                    &storage,
                )
                .await
                .unwrap();
                assert_eq!(fresh.items(&suffix).await, expected);
                fresh.assert_items_stored().await;
                fresh.close().await;
                drop(storage);
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}
