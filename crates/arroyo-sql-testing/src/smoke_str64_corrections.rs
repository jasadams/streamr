//! Actual CDC source operations through native aggregation and one captured MERGE.
use super::*;

#[test_log(tokio::test)]
#[ignore = "explicit memory/RocksDB source batch1/8 correction matrix"]
async fn maintained_merge_same_key_correction_prefix() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    let job = format!(
        "str64-correction-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let directory = env::var_os("STREAMR_TEST_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(&job);
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let input = directory.join("changes.jsonl");
    let output = directory.join("captured.jsonl");
    let mirror = directory.join("mirror.jsonl");
    tokio::fs::write(&input, include_str!("test/str64-correction-prefix.jsonl"))
        .await
        .unwrap();
    let sql = include_str!("test/str64-correction-prefix.sql")
        .replace("__INPUT__", input.to_str().unwrap())
        .replace("__OUTPUT__", output.to_str().unwrap())
        .replace("__MIRROR__", mirror.to_str().unwrap());
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    println!(
        "STR64_CORRECTION_SQL fixture={} raw_operations=3 expected_captures=3 source_batch_max={}",
        directory.display(),
        config::config().pipeline.source_batch_size
    );
    let udfs = get_udfs();
    let logical = get_graph(sql, &udfs)
        .await
        .expect("paired CDC replacement must plan as one maintained event");
    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, None, tx).await;
    let running = Engine::for_local(program, "pipe-test".into(), job)
        .await
        .unwrap()
        .start()
        .await;
    assert_eq!(running.source_controls().len(), 1);
    run_until_finished(&running, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    let rows = read_rows(&output).await;
    assert_eq!(
        rows,
        read_rows(&mirror).await,
        "consumers must share one captured MERGE"
    );
    assert_eq!(
        rows.len(),
        3,
        "one capture per CDC operation; replacement is not retract then insert prefixes"
    );
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row["k"], "a");
        assert_eq!(row["total"], [1, 2, 2][i]);
        assert_eq!(row["peak"], [10, 20, 10][i]);
        assert_time(&row["last_seen"], [10, 20, 10][i]);
        assert_time(&row["trigger_time"], [10, 20, 5][i]);
        assert_eq!(
            row.get("old_total").unwrap_or(&Value::Null),
            &serde_json::json!([None, Some(1), Some(2)][i])
        );
        assert_eq!(
            row.get("old_peak").unwrap_or(&Value::Null),
            &serde_json::json!([None, Some(10), Some(20)][i])
        );
        assert_eq!(row["action"], if i == 0 { "insert" } else { "update" });
    }
}

async fn read_rows(path: &Path) -> Vec<Value> {
    tokio::fs::read_to_string(path)
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn assert_time(value: &Value, seconds: u32) {
    let text = value.as_str().expect("timestamp must serialize as text");
    // Fixture clocks are elapsed epoch seconds within the first UTC day.
    // Normalize hours/minutes rather than treating elapsed seconds as a field.
    assert!(
        seconds < 24 * 60 * 60,
        "fixture clock must fit its declared UTC day"
    );
    let expected = format!(
        "1970-01-01T{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    );
    assert!(
        text.starts_with(&expected),
        "expected epoch second {seconds} ({expected}), got {text}"
    );
}

#[derive(Clone, Copy)]
struct CorrectionCase {
    name: &'static str,
    input: &'static str,
    expected: &'static str,
    source_filter: bool,
    aggregate_filter: bool,
}
const FULL_BATCH: CorrectionCase = CorrectionCase {
    name: "full-batch",
    input: include_str!("test/str64-correction-full-batch.jsonl"),
    expected: include_str!("test/str64-correction-full-batch-expected.json"),
    source_filter: false,
    aggregate_filter: false,
};
const MOVES: CorrectionCase = CorrectionCase {
    name: "moves-absence",
    input: include_str!("test/str64-correction-moves-absence.jsonl"),
    expected: include_str!("test/str64-correction-moves-absence-expected.json"),
    source_filter: false,
    aggregate_filter: false,
};
const SOURCE_FILTER: CorrectionCase = CorrectionCase {
    name: "source-filter",
    input: include_str!("test/str64-correction-source-filter.jsonl"),
    expected: include_str!("test/str64-correction-source-filter-expected.json"),
    source_filter: true,
    aggregate_filter: false,
};
const AGGREGATE_FILTER: CorrectionCase = CorrectionCase {
    name: "aggregate-filter",
    input: include_str!("test/str64-correction-aggregate-filter.jsonl"),
    expected: include_str!("test/str64-correction-aggregate-filter-expected.json"),
    source_filter: false,
    aggregate_filter: true,
};
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_full_batch_and_marker_reuse() {
    correction_setup();
    run_correction_case(FULL_BATCH, false, None).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_pk_group_moves_and_absence_recreate() {
    correction_setup();
    run_correction_case(MOVES, false, None).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_source_filter_image_outcomes() {
    correction_setup();
    run_correction_case(SOURCE_FILTER, false, None).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_aggregate_filter_and_no_action() {
    correction_setup();
    run_correction_case(AGGREGATE_FILTER, false, None).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_completed_checkpoint_replay() {
    correction_setup();
    run_correction_case(FULL_BATCH, true, None).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_aggregate_postcommit_failure_fresh_replay() {
    correction_setup();
    run_correction_case(FULL_BATCH, true, Some(false)).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit correction backend/batch matrix"]
async fn maintained_merge_correction_target_postcommit_failure_fresh_replay() {
    correction_setup();
    run_correction_case(FULL_BATCH, true, Some(true)).await;
}
fn correction_setup() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
}
async fn run_correction_case(case: CorrectionCase, restore: bool, fault_owner: Option<bool>) {
    let job = format!(
        "str64-correction-{}-{}-{}",
        case.name,
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let directory = env::var_os("STREAMR_TEST_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(&job);
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let input = directory.join("changes.jsonl");
    let output = directory.join("captured.jsonl");
    let mirror = directory.join("mirror.jsonl");
    let mut text = case.input.to_string();
    if fault_owner.is_some() {
        // Same proven lifecycle as append fault tests: ten accepted operations,
        // CP1 prefix2, eight remaining operations, filtered tail keeps CP2 alive.
        text.push_str("{\"before\":null,\"after\":{\"row_id\":99,\"k\":\"excluded\",\"value\":19,\"active\":true,\"event_time\":\"1970-01-01T00:00:19Z\"},\"op\":\"c\"}\n");
    }
    tokio::fs::write(&input, &text).await.unwrap();
    let source_predicate = if case.source_filter {
        "k <> 'excluded' AND active"
    } else {
        "k <> 'excluded'"
    };
    let peak = if case.aggregate_filter {
        "MAX(value) FILTER (WHERE active) AS peak"
    } else {
        "MAX(value) AS peak"
    };
    let sql=include_str!("test/str64-correction-prefix.sql")
        .replace("__INPUT__",input.to_str().unwrap()).replace("__OUTPUT__",output.to_str().unwrap()).replace("__MIRROR__",mirror.to_str().unwrap())
        .replace("MAX(value) AS peak",peak)
        .replace("FROM changes GROUP BY k",&format!("FROM changes WHERE {source_predicate} GROUP BY k"))
        .replace("WHEN MATCHED THEN", "WHEN MATCHED AND (previous.total IS DISTINCT FROM incoming.total OR previous.peak IS DISTINCT FROM incoming.peak OR previous.last_seen IS DISTINCT FROM incoming.last_seen) THEN")
        .replace("applied.old.total AS old_total", "applied.old.last_seen AS old_seen,applied.old.total AS old_total");
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    tokio::fs::write(directory.join("expected.json"), case.expected)
        .await
        .unwrap();
    println!(
        "STR64_CORRECTION_CASE name={} fixture={} raw_operations={} restore={restore} fault_owner={fault_owner:?}",
        case.name,
        directory.display(),
        text.lines().count()
    );
    let udfs = get_udfs();
    let logical = Arc::new(
        get_graph(sql, &udfs)
            .await
            .expect("native correction SQL must plan"),
    );
    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, None, tx).await;
    let raw = observe_raw_correction_batches(&program);
    let unrolled = super::smoke_str64::observe_source_batches(&program);
    let running = Engine::for_local(program, "pipe-test".into(), job.clone())
        .await
        .unwrap()
        .start()
        .await;
    assert_eq!(running.source_controls().len(), 1);
    if restore {
        assert_eq!(case.name, "full-batch");
        advance(&running, 1).await;
        let mut stopped = HashSet::new();
        checkpoint_with_stop(
            &mut SmokeTestContext {
                job_id: Arc::new(job.clone()),
                engine: &running,
                control_rx: &mut rx,
                program: logical.clone(),
            },
            1,
            true,
            &mut stopped,
        )
        .await;
        stop_correction_attempt(&running, &mut rx, stopped).await;
        wait_for_capture_state_cleanup().await;
        assert_eq!(read_rows(&output).await.len(), 2);
        assert_eq!(read_rows(&mirror).await.len(), 2);
        if let Some(owner) = fault_owner {
            super::smoke_str64::fail_after_live_commit(
                &job, &logical, &udfs, &output, &mirror, owner,
            )
            .await;
        }
        let (tx, mut rx) = channel(128);
        let program = local_program(&job, &logical.graph, &udfs, Some(1), tx).await;
        let raw = observe_raw_correction_batches(&program);
        let unrolled = super::smoke_str64::observe_source_batches(&program);
        let restored = Engine::for_local(program, "pipe-test".into(), job.clone())
            .await
            .unwrap()
            .start()
            .await;
        run_until_finished(&restored, &mut rx).await;
        assert_correction_batches(
            &directory,
            "restored",
            &raw,
            &unrolled,
            &text.lines().skip(2).map(str::to_string).collect::<Vec<_>>(),
        );
    } else {
        run_until_finished(&running, &mut rx).await;
        assert_correction_batches(
            &directory,
            "initial",
            &raw,
            &unrolled,
            &text.lines().map(str::to_string).collect::<Vec<_>>(),
        );
    }
    wait_for_capture_state_cleanup().await;
    let rows = read_rows(&output).await;
    assert_eq!(
        rows,
        read_rows(&mirror).await,
        "one capture producer must feed both consumers"
    );
    let groups: Vec<Vec<Value>> = serde_json::from_str(case.expected).unwrap();
    assert_eq!(
        rows.len(),
        groups.iter().map(Vec::len).sum::<usize>(),
        "absence has no fabricated row or implicit target delete"
    );
    let mut offset = 0;
    for (operation, expected) in groups.iter().enumerate() {
        let mut actual = rows[offset..offset + expected.len()].to_vec();
        let mut expected = expected.clone();
        // A group move emits two final keys in one operation. Their order is
        // unspecified; operation order and every key snapshot remain asserted.
        actual.sort_by_key(|row| row["k"].as_str().unwrap().to_owned());
        expected.sort_by_key(|row| row["k"].as_str().unwrap().to_owned());
        for (actual, expected) in actual.iter().zip(&expected) {
            for field in ["k", "total", "peak", "old_total", "old_peak", "action"] {
                assert_eq!(
                    actual.get(field).unwrap_or(&Value::Null),
                    &expected[field],
                    "{} operation{} field{field}",
                    case.name,
                    operation
                );
            }
            for field in ["last_seen", "old_seen", "trigger_time"] {
                if expected[field].is_null() {
                    assert!(actual.get(field).unwrap_or(&Value::Null).is_null());
                } else {
                    assert_time(&actual[field], expected[field].as_u64().unwrap() as u32);
                }
            }
        }
        offset += expected.len();
    }
}
async fn stop_correction_attempt(
    running: &RunningEngine,
    rx: &mut Receiver<ControlResp>,
    mut stopped: HashSet<(u32, u32)>,
) {
    let tasks: usize = running.operator_controls().values().map(Vec::len).sum();
    running.abort_workers();
    tokio::time::timeout(test_runtime_timeout(), async {
        while stopped.len() < tasks {
            match rx.recv().await {
                Some(ControlResp::TaskFinished {
                    task_id,
                    subtask_idx,
                    ..
                })
                | Some(ControlResp::TaskFailed {
                    task_id,
                    subtask_idx,
                    ..
                }) => {
                    stopped.insert((task_id, subtask_idx));
                }
                Some(_) => {}
                None => break,
            }
        }
        assert_eq!(stopped.len(), tasks);
    })
    .await
    .expect("correction checkpoint attempt did not terminate");
}

fn assert_correction_batches(
    directory: &Path,
    phase: &str,
    raw: &Arc<std::sync::Mutex<RawSourceTrace>>,
    unrolled: &Arc<std::sync::Mutex<Vec<usize>>>,
    operations: &[String],
) {
    let batch = config::config().pipeline.source_batch_size;
    let expected_raw: Vec<_> = operations.chunks(batch).map(|chunk| chunk.len()).collect();
    let expected_unrolled: Vec<usize> = operations
        .chunks(batch)
        .map(|chunk| {
            chunk
                .iter()
                .map(|line| {
                    let op: Value = serde_json::from_str(line).unwrap();
                    usize::from(!op["before"].is_null()) + usize::from(!op["after"].is_null())
                })
                .sum()
        })
        .collect();
    let trace = raw.lock().unwrap();
    let raw = trace.batches.clone();
    let markers = trace.markers.clone();
    drop(trace);
    let expected_markers: Vec<Vec<u64>> = operations
        .chunks(batch)
        .map(|chunk| {
            chunk
                .iter()
                .enumerate()
                .flat_map(|(ordinal, line)| {
                    let operation: Value = serde_json::from_str(line).unwrap();
                    let images = usize::from(!operation["before"].is_null())
                        + usize::from(!operation["after"].is_null());
                    std::iter::repeat_n(ordinal as u64, images)
                })
                .collect()
        })
        .collect();
    assert_eq!(
        markers, expected_markers,
        "whole source-operation marker runs and batch-local ordinal reuse"
    );
    let unrolled = unrolled.lock().unwrap().clone();
    assert_eq!(raw, expected_raw, "actual pre-unroll raw operation batches");
    assert_eq!(
        unrolled, expected_unrolled,
        "complete CDC image pairs must stay in one physical batch"
    );
    std::fs::write(directory.join(format!("{phase}-source-batches.json")),serde_json::to_vec_pretty(&serde_json::json!({"raw_operation_batches":raw,"unrolled_image_batches":unrolled,"raw_operations":operations.len(),"source_operation_markers":markers})).unwrap()).unwrap();
}
fn observe_raw_correction_batches(program: &Program) -> Arc<std::sync::Mutex<RawSourceTrace>> {
    use arroyo_operator::operator::OperatorNode;
    use arroyo_worker::engine::SubtaskOrQueueNode;
    let rows = Arc::new(std::sync::Mutex::new(RawSourceTrace::default()));
    let mut installed = 0;
    for node in program.graph.write().unwrap().node_weights_mut() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Chained(chain) = &mut node.node
        {
            // This is the existing connector schema/unroll physical executor.
            // Its input is the raw Debezium RecordBatch, before image expansion.
            if chain.operator.name() == "value_calculation(changes)" {
                let inner = std::mem::replace(&mut chain.operator, Box::new(ObserverPlaceholder));
                chain.operator = Box::new(RawBatchObserver {
                    inner,
                    rows: rows.clone(),
                });
                installed += 1;
            }
        }
    }
    assert_eq!(
        installed, 1,
        "one source schema/unroll operator must be observed"
    );
    rows
}
use arrow::array::RecordBatch;
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::ArrowOperator,
};
use arroyo_rpc::errors::DataflowResult;
use arroyo_types::{CheckpointBarrier, SignalMessage};
struct ObserverPlaceholder;
#[async_trait::async_trait]
impl ArrowOperator for ObserverPlaceholder {
    fn name(&self) -> String {
        unreachable!()
    }
    async fn process_batch(
        &mut self,
        _: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        unreachable!()
    }
}
struct RawBatchObserver {
    inner: Box<dyn ArrowOperator>,
    rows: Arc<std::sync::Mutex<RawSourceTrace>>,
}
#[async_trait::async_trait]
impl ArrowOperator for RawBatchObserver {
    fn name(&self) -> String {
        self.inner.name()
    }
    fn tables(&self) -> std::collections::HashMap<String, arroyo_rpc::grpc::rpc::TableConfig> {
        self.inner.tables()
    }
    fn tick_interval(&self) -> Option<Duration> {
        self.inner.tick_interval()
    }
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.inner.on_start(ctx).await
    }
    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.rows.lock().unwrap().batches.push(batch.num_rows());
        self.inner
            .process_batch(
                batch,
                ctx,
                &mut MarkerObserver {
                    inner: collector,
                    trace: self.rows.clone(),
                },
            )
            .await
    }
    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.handle_checkpoint(b, ctx, collector).await
    }
    async fn handle_tick(
        &mut self,
        tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.handle_tick(tick, ctx, collector).await
    }
    async fn on_close(
        &mut self,
        message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.on_close(message, ctx, collector).await
    }
}

#[derive(Default)]
struct RawSourceTrace {
    batches: Vec<usize>,
    markers: Vec<Vec<u64>>,
}
struct MarkerObserver<'a> {
    inner: &'a mut dyn Collector,
    trace: Arc<std::sync::Mutex<RawSourceTrace>>,
}
#[async_trait::async_trait]
impl Collector for MarkerObserver<'_> {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        use arrow::array::{Array, UInt64Array};
        let marker = batch
            .column_by_name(arroyo_rpc::SOURCE_ENVELOPE_FIELD)
            .expect("CDC source unroll must expose operation provenance")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(marker.null_count(), 0);
        self.trace
            .lock()
            .unwrap()
            .markers
            .push(marker.values().to_vec());
        self.inner.collect(batch).await
    }
    async fn broadcast_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
    ) -> DataflowResult<()> {
        self.inner.broadcast_watermark(watermark).await
    }
}

#[path = "smoke_str64_correction_scans.rs"]
mod scan_qualification;
