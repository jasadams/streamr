//! Real planner -> serialized local worker regression for the initial STR-64 route.
use super::*;

#[test_log(tokio::test)]
#[ignore = "run separately for memory/rocksdb and source batch sizes 1/8"]
async fn maintained_aggregate_merge_event_prefixes() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    let batch = config::config().pipeline.source_batch_size;
    assert!(
        matches!(batch, 1 | 8),
        "select batch size 1 or 8 explicitly"
    );
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    for max_only in [false, true] {
        for restore in [false, true] {
            run_prefix_case(max_only, restore, false, false, false, false).await;
        }
    }
}

#[test_log(tokio::test)]
#[ignore = "run separately for memory/rocksdb and source batch sizes 1/8"]
async fn maintained_aggregate_blocked_delivery_failure_replay() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    run_prefix_case(false, true, true, false, false, false).await;
}

#[test_log(tokio::test)]
#[ignore = "run separately for memory/rocksdb and source batch sizes 1/8"]
async fn maintained_aggregate_source_where_and_count_filter() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    run_prefix_case(false, false, false, true, false, false).await;
}

#[test_log(tokio::test)]
#[ignore = "run separately for memory/rocksdb and source batch sizes 1/8"]
async fn maintained_merge_postcommit_capture_failure_replay() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    run_prefix_case(false, true, true, false, true, false).await;
}

#[test_log(tokio::test)]
#[ignore = "ordinary append two-consumer baseline; run explicitly in backend matrix"]
async fn ordinary_append_merge_two_consumers() {
    configure_test_worker();
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    run_prefix_case(true, false, false, false, false, true).await;
}

async fn run_prefix_case(
    max_only: bool,
    restore: bool,
    fail_delivery: bool,
    filter_source: bool,
    fail_owner: bool,
    append_baseline: bool,
) {
    let job = format!(
        "str64-{}-{}-{max_only}",
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
    let input = directory.join("events.jsonl");
    let output = directory.join("captured.jsonl");
    let mirror = directory.join("mirror.jsonl");
    let events = [
        ("a", "x", 10),
        ("a", "x", 20),
        ("a", "x", 5),
        ("b", "x", 7),
        ("a", "y", 8),
        ("a", "x", 30),
        (
            if filter_source { "excluded" } else { "b" },
            "y",
            if filter_source { 19 } else { 9 },
        ),
        ("c", "x", 11),
        ("b", "x", 6),
        ("c", "x", 12),
    ];
    let mut text = events.iter().map(|(tenant,id,time)| serde_json::json!({"tenant":tenant,"id":id,"event_time":format!("1970-01-01T00:00:{time:02}Z")}).to_string()).collect::<Vec<_>>().join("\n") + "\n";
    if fail_delivery {
        // CP2 releases the final accepted row's control wait. A filtered tail
        // keeps the source context and its async checkpoint writer alive.
        text.push_str(&serde_json::json!({"tenant":"excluded","id":"sentinel","event_time":"1970-01-01T00:00:19Z"}).to_string());
        text.push('\n');
    }
    println!(
        "STR64_INPUT raw_rows={} expected_captures={}",
        events.len() + usize::from(fail_delivery),
        if filter_source { 6 } else { 10 }
    );
    tokio::fs::write(&input, text).await.unwrap();
    let count_projection = if max_only {
        ""
    } else {
        ", COUNT(*) AS total, COUNT(*) FILTER (WHERE event_time >= TIMESTAMP '1970-01-01 00:00:10') AS eligible"
    };
    let count_column = if max_only {
        ""
    } else {
        ", total BIGINT, eligible BIGINT"
    };
    let count_update = if max_only {
        ""
    } else {
        ", total = incoming.total, eligible = incoming.eligible"
    };
    let count_insert = if max_only { "" } else { ", total, eligible" };
    let count_value = if max_only {
        ""
    } else {
        ", incoming.total, incoming.eligible"
    };
    let count_capture = if max_only {
        ""
    } else {
        ", applied.old.total AS old_count, applied.new.total AS new_count, applied.new.eligible AS eligible_count"
    };
    let predicate = if max_only {
        " AND previous.last_seen IS DISTINCT FROM incoming.last_seen"
    } else {
        ""
    };
    let having = if filter_source {
        " HAVING MAX(event_time) >= TIMESTAMP '1970-01-01 00:00:10'"
    } else {
        ""
    };
    let maintained = if append_baseline {
        "SELECT tenant,id,event_time AS last_seen FROM events".to_string()
    } else {
        format!(
            "SELECT tenant,id,MAX(event_time) AS last_seen{count_projection} FROM events WHERE tenant <> 'excluded' GROUP BY tenant,id{having}"
        )
    };
    let sql = format!(
        r#"
SET updating_ttl = NULL;
CREATE TABLE events (tenant TEXT NOT NULL, id TEXT NOT NULL, event_time TIMESTAMP NOT NULL,
 WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
 WITH (connector='single_file', path='{input}', format='json', type='source');
CREATE STATE TABLE profiles (tenant TEXT, id TEXT, last_seen TIMESTAMP{count_column}, PRIMARY KEY (tenant,id)) PARTITION BY tenant;
CREATE VIEW maintained AS {maintained};
CREATE VIEW captured AS MERGE INTO profiles AS previous USING maintained AS incoming
 ON previous.tenant=incoming.tenant AND previous.id=incoming.id
 WHEN MATCHED{predicate} THEN UPDATE SET last_seen=incoming.last_seen{count_update}
 WHEN NOT MATCHED THEN INSERT (tenant,id,last_seen{count_insert}) VALUES (incoming.tenant,incoming.id,incoming.last_seen{count_value})
 RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE TABLE results WITH (connector='single_file',path='{output}',format='json',type='sink');
CREATE TABLE mirror WITH (connector='single_file',path='{mirror}',format='json',type='sink');
INSERT INTO results SELECT applied.source.tenant AS tenant, applied.source.id AS id,
 applied.source.last_seen AS source_seen, applied.old.last_seen AS old_seen,
 applied.new.last_seen AS new_seen, applied.action AS action,
 EVENT_TIMESTAMP() AS trigger_time{count_capture} FROM captured AS applied;
INSERT INTO mirror SELECT applied.source.tenant AS tenant, applied.source.id AS id,
 applied.source.last_seen AS source_seen, applied.old.last_seen AS old_seen,
 applied.new.last_seen AS new_seen, applied.action AS action,
 EVENT_TIMESTAMP() AS trigger_time{count_capture} FROM captured AS applied;
"#,
        input = input.display(),
        output = output.display(),
        mirror = mirror.display()
    );
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    println!(
        "STR64_SQL fixture={} max_only={max_only}",
        directory.display()
    );
    let udfs = get_udfs();
    let logical = Arc::new(
        get_graph(sql, &udfs)
            .await
            .expect("STR64 runnable SQL must plan"),
    );
    let roots: Vec<_> = logical
        .graph
        .externals(petgraph::Direction::Incoming)
        .map(|index| logical.graph[index].node_id)
        .collect();
    let nodes: Vec<_> = logical.graph.node_weights().map(|node| serde_json::json!({
        "node_id":node.node_id,"operators":node.operator_chain.iter().map(|(op,_)| format!("{:?}:{}",op.operator_name,op.operator_id)).collect::<Vec<_>>()
    })).collect();
    let edges: Vec<_> = logical
        .graph
        .edge_indices()
        .map(|index| {
            let (from, to) = logical.graph.edge_endpoints(index).unwrap();
            serde_json::json!({"from":logical.graph[from].node_id,"to":logical.graph[to].node_id})
        })
        .collect();
    tokio::fs::write(
        directory.join("planned-graph.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"roots":roots,"nodes":nodes,"edges":edges}))
            .unwrap(),
    )
    .await
    .unwrap();
    println!("STR64_GRAPH append_baseline={append_baseline} root_ids={roots:?}");
    assert_eq!(
        roots.len(),
        1,
        "planned graph must have exactly one event root"
    );
    let root = logical
        .graph
        .externals(petgraph::Direction::Incoming)
        .next()
        .unwrap();
    assert!(
        logical.graph[root]
            .operator_chain
            .iter()
            .any(|(op, _)| op.operator_name == OperatorName::ConnectorSource),
        "event root must be the real connector source"
    );
    let mut reached = HashSet::new();
    let mut pending = vec![root];
    while let Some(index) = pending.pop() {
        if reached.insert(index) {
            pending.extend(
                logical
                    .graph
                    .neighbors_directed(index, petgraph::Direction::Outgoing),
            );
        }
    }
    assert_eq!(
        reached.len(),
        logical.graph.node_count(),
        "all planned consumers must be reachable from the event root"
    );
    let operations: Vec<_> = logical
        .graph
        .node_weights()
        .flat_map(|n| n.operator_chain.iter())
        .map(|(op, _)| op.operator_name)
        .collect();
    assert_eq!(
        operations
            .iter()
            .filter(|kind| **kind == OperatorName::FusedStateTable)
            .count(),
        1,
        "captured MERGE must execute once for both consumers"
    );
    assert_eq!(
        operations
            .iter()
            .filter(|kind| **kind == OperatorName::UpdatingAggregate)
            .count(),
        usize::from(!append_baseline),
        "ordinary append has no aggregate; maintained route has exactly one"
    );

    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, None, tx).await;
    let batches = observe_source_batches(&program);
    let running = Engine::for_local(program, "pipe-test".into(), job.clone())
        .await
        .unwrap()
        .start()
        .await;
    if restore {
        // Source reads one row before waiting; advance exactly one additional row.
        // The checkpoint flushes a partial batch at size 8.
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
        .expect("checkpoint attempt did not terminate");
        wait_for_capture_state_cleanup().await;
        assert_eq!(read_rows(&output).await.len(), 2);
        assert_eq!(read_rows(&mirror).await.len(), 2);
        if fail_delivery {
            fail_after_live_commit(&job, &logical, &udfs, &output, &mirror, fail_owner).await;
        }
        let (tx, mut rx) = channel(128);
        let program = local_program(&job, &logical.graph, &udfs, Some(1), tx).await;
        let restored_batches = observe_source_batches(&program);
        let restored = Engine::for_local(program, "pipe-test".into(), job.clone())
            .await
            .unwrap()
            .start()
            .await;
        run_until_finished(&restored, &mut rx).await;
        let observed = restored_batches.lock().unwrap().clone();
        let expected = if config::config().pipeline.source_batch_size == 8 {
            if fail_delivery { vec![8, 1] } else { vec![8] }
        } else {
            vec![1; 8 + usize::from(fail_delivery)]
        };
        assert_eq!(observed, expected, "actual restored source batches");
        tokio::fs::write(
            directory.join("restored-source-batches.json"),
            serde_json::to_vec(&observed).unwrap(),
        )
        .await
        .unwrap();
    } else {
        run_until_finished(&running, &mut rx).await;
    }
    wait_for_capture_state_cleanup().await;
    if !restore {
        let observed = batches.lock().unwrap().clone();
        let expected = if config::config().pipeline.source_batch_size == 8 {
            vec![8, 2]
        } else {
            vec![1; 10]
        };
        assert_eq!(
            observed, expected,
            "actual batches entering watermark owner"
        );
        tokio::fs::write(
            directory.join("source-batches.json"),
            serde_json::to_vec(&observed).unwrap(),
        )
        .await
        .unwrap();
    }
    let rows = read_rows(&output).await;
    let mirrored = read_rows(&mirror).await;
    assert_eq!(
        rows, mirrored,
        "two consumers must share one captured mutation"
    );
    assert_eq!(
        rows.len(),
        if filter_source { 6 } else { events.len() },
        "unchanged aggregate still completes an event"
    );
    let counts = [1, 2, 3, 1, 1, 4, 1, 1, 2, 2];
    let old_counts = [
        None,
        Some(1),
        Some(2),
        None,
        None,
        Some(3),
        None,
        None,
        Some(1),
        Some(1),
    ];
    let maxima = if append_baseline {
        [10, 20, 5, 7, 8, 30, 9, 11, 6, 12]
    } else {
        [10, 20, 20, 7, 8, 30, 9, 11, 7, 12]
    };
    let old_maxima = if append_baseline {
        [
            None,
            Some(10),
            Some(20),
            None,
            None,
            Some(5),
            None,
            None,
            Some(7),
            Some(11),
        ]
    } else {
        [
            None,
            Some(10),
            Some(20),
            None,
            None,
            Some(20),
            None,
            None,
            Some(7),
            Some(11),
        ]
    };
    let eligible = [1, 2, 2, 0, 0, 3, 0, 1, 0, 2];
    for (output_index, row) in rows.iter().enumerate() {
        let i = if filter_source {
            [0, 1, 2, 5, 7, 9][output_index]
        } else {
            output_index
        };
        assert_eq!(row["tenant"], events[i].0);
        assert_eq!(row["id"], events[i].1);
        assert_time(&row["trigger_time"], Some(events[i].2));
        assert_time(&row["source_seen"], Some(maxima[i]));
        assert_time(&row["new_seen"], Some(maxima[i]));
        assert_time(row.get("old_seen").unwrap_or(&Value::Null), old_maxima[i]);
        assert_eq!(
            row["action"],
            if i == 0 || i == 3 || i == 4 || i == 6 || i == 7 {
                "insert"
            } else if !append_baseline && max_only && (i == 2 || i == 8) {
                "none"
            } else {
                "update"
            }
        );
        if !max_only {
            assert_eq!(row["new_count"], counts[i]);
            assert_eq!(row["eligible_count"], eligible[i]);
            assert_eq!(
                row.get("old_count").unwrap_or(&Value::Null),
                &serde_json::json!(old_counts[i])
            );
        }
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

fn assert_time(value: &Value, expected: Option<i32>) {
    match expected {
        None => assert!(value.is_null()),
        Some(seconds) => {
            // JSON timestamps may omit the UTC suffix and vary fractional precision.
            let text = value
                .as_str()
                .expect("timestamp must be serialized as text");
            assert!(
                text.starts_with(&format!("1970-01-01T00:00:{seconds:02}")),
                "expected second {seconds}, got {text}"
            );
        }
    }
}

// Observe actual connector batches without changing operator execution or clocks.
pub(super) fn observe_source_batches(program: &Program) -> Arc<std::sync::Mutex<Vec<usize>>> {
    observe_operator(program, "expression_watermark_generator", None)
}

fn observe_operator(
    program: &Program,
    name: &str,
    fault: Option<Arc<DeliveryFault>>,
) -> Arc<std::sync::Mutex<Vec<usize>>> {
    use arroyo_operator::operator::OperatorNode;
    use arroyo_worker::engine::SubtaskOrQueueNode;
    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut count = 0;
    for node in program.graph.write().unwrap().node_weights_mut() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Chained(chain) = &mut node.node
        {
            let mut next = Some(chain);
            while let Some(chain) = next {
                if chain.operator.name().starts_with(name) {
                    let inner = std::mem::replace(&mut chain.operator, Box::new(Unreachable));
                    chain.operator = Box::new(BatchObserver {
                        inner,
                        rows: rows.clone(),
                        fault: fault.clone(),
                    });
                    count += 1;
                }
                next = chain.next.as_deref_mut();
            }
        }
    }
    assert_eq!(count, 1);
    rows
}
use arrow::array::RecordBatch;
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::ArrowOperator,
};
use arroyo_rpc::errors::DataflowResult;
use arroyo_types::{CheckpointBarrier, SignalMessage};
struct Unreachable;
#[async_trait::async_trait]
impl ArrowOperator for Unreachable {
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
struct BatchObserver {
    inner: Box<dyn ArrowOperator>,
    rows: Arc<std::sync::Mutex<Vec<usize>>>,
    fault: Option<Arc<DeliveryFault>>,
}
#[async_trait::async_trait]
impl ArrowOperator for BatchObserver {
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
        self.rows.lock().unwrap().push(batch.num_rows());
        if let Some(fault) = &self.fault {
            self.inner
                .process_batch(
                    batch,
                    ctx,
                    &mut FailingCollector {
                        inner: collector,
                        fault: fault.clone(),
                    },
                )
                .await
        } else {
            self.inner.process_batch(batch, ctx, collector).await
        }
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
struct DeliveryFault {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct FailingCollector<'a> {
    inner: &'a mut dyn Collector,
    fault: Arc<DeliveryFault>,
}
#[async_trait::async_trait]
impl Collector for FailingCollector<'_> {
    async fn collect(&mut self, _batch: RecordBatch) -> DataflowResult<()> {
        self.fault.reached.notify_one();
        self.fault.release.notified().await;
        Err(arroyo_rpc::errors::DataflowError::InternalOperatorError {
            error: "STR64 injected blocked delivery failure",
            message: "after real aggregate collect, before downstream delivery".into(),
        })
    }
    async fn broadcast_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
    ) -> DataflowResult<()> {
        self.inner.broadcast_watermark(watermark).await
    }
}

pub(super) async fn fail_after_live_commit(
    job: &str,
    logical: &Arc<LogicalProgram>,
    udfs: &[LocalUdf],
    output: &Path,
    mirror: &Path,
    fail_owner: bool,
) {
    let (tx, mut rx) = channel(128);
    let program = local_program(job, &logical.graph, udfs, Some(1), tx).await;
    let fault = Arc::new(DeliveryFault::default());
    let blocked_name = if fail_owner {
        "fused_state_table_"
    } else {
        "UpdatingAggregatingFunc"
    };
    let blocked_kind = if fail_owner {
        OperatorName::FusedStateTable
    } else {
        OperatorName::UpdatingAggregate
    };
    let _observations = observe_operator(&program, blocked_name, Some(fault.clone()));
    let failed = Engine::for_local(program, "pipe-test".into(), job.into())
        .await
        .unwrap()
        .start()
        .await;
    // Seven NoOps admit the eight remaining accepted rows, producing a full
    // batch8. CP2 then advances into the filtered sentinel's control wait,
    // keeping the source and its async checkpoint writer alive through ACK.
    advance(&failed, 7).await;
    tokio::time::timeout(test_runtime_timeout(), fault.reached.notified())
        .await
        .expect("owner never reached blocked real output");
    assert_eq!(
        read_rows(output).await.len(),
        2,
        "blocked output reached primary sink"
    );
    assert_eq!(
        read_rows(mirror).await.len(),
        2,
        "blocked output reached mirror sink"
    );
    let source_task_id = logical
        .graph
        .node_weights()
        .find(|node| {
            node.operator_chain
                .iter()
                .any(|(op, _)| op.operator_name == OperatorName::ConnectorSource)
        })
        .unwrap()
        .node_id;
    let source_id = logical
        .graph
        .node_weights()
        .flat_map(|n| n.operator_chain.iter())
        .find(|(op, _)| op.operator_name == OperatorName::ConnectorSource)
        .unwrap()
        .0
        .operator_id
        .clone();
    let blocked_id = logical
        .graph
        .node_weights()
        .flat_map(|n| n.operator_chain.iter())
        .find(|(op, _)| op.operator_name == blocked_kind)
        .unwrap()
        .0
        .operator_id
        .clone();
    for source in failed.source_controls() {
        source
            .send(ControlMessage::Checkpoint(CheckpointBarrier {
                epoch: 2,
                min_epoch: 0,
                timestamp: SystemTime::now(),
                then_stop: false,
            }))
            .await
            .unwrap();
    }
    // The source snapshot completes while real owner output remains blocked.
    // Its acknowledgement does not prove barrier enqueue at that owner.
    tokio::time::timeout(test_runtime_timeout(), async {
        loop {
            match rx.recv().await {
                Some(ControlResp::CheckpointCompleted(c)) if c.checkpoint_epoch == 2 => {
                    assert_ne!(
                        c.operator_id, blocked_id,
                        "blocked owner checkpointed ahead of delivery"
                    );
                    if c.operator_id == source_id {
                        break;
                    }
                }
                Some(ControlResp::TaskFailed { error, .. }) => {
                    panic!("failure before fault release {error:?}")
                }
                Some(ControlResp::TaskFinished { task_id, .. }) if task_id == source_task_id => {
                    panic!("source finished before checkpoint snapshot acknowledgement");
                }
                Some(_) => {}
                None => panic!("source stopped before checkpoint acknowledgement"),
            }
        }
    })
    .await
    .expect("source did not acknowledge its checkpoint snapshot");
    fault.release.notify_one();
    let mut stopped = HashSet::new();
    tokio::time::timeout(test_runtime_timeout(), async {
        loop {
            match rx.recv().await {
                Some(ControlResp::TaskFailed {
                    task_id,
                    subtask_idx,
                    error,
                    ..
                }) => {
                    assert!(
                        format!("{error:?}").contains("STR64 injected blocked delivery failure"),
                        "unexpected worker error {error:?}"
                    );
                    stopped.insert((task_id, subtask_idx));
                    break;
                }
                Some(ControlResp::TaskFinished {
                    task_id,
                    subtask_idx,
                    ..
                }) => {
                    stopped.insert((task_id, subtask_idx));
                }
                Some(ControlResp::CheckpointCompleted(c)) if c.checkpoint_epoch == 2 => {
                    assert_ne!(
                        c.operator_id, blocked_id,
                        "failed owner completed queued checkpoint"
                    );
                }
                Some(_) => {}
                None => panic!("attempt ended without injected worker failure"),
            }
        }
    })
    .await
    .expect("injected failure did not terminate worker");
    let tasks: usize = failed.operator_controls().values().map(Vec::len).sum();
    failed.abort_workers();
    tokio::time::timeout(test_runtime_timeout(), async {
        while stopped.len() < tasks {
            match rx.recv().await {
                Some(ControlResp::TaskFailed {
                    task_id,
                    subtask_idx,
                    ..
                })
                | Some(ControlResp::TaskFinished {
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
        assert_eq!(stopped.len(), tasks, "failed attempt workers not drained");
    })
    .await
    .expect("failed attempt cancellation timed out");
    wait_for_capture_state_cleanup().await;
    assert_eq!(read_rows(output).await.len(), 2);
    assert_eq!(read_rows(mirror).await.len(), 2);
    println!(
        "STR64_FAULT observed_real_output_block=true observed_task_failure=true whole_attempt_drained=true checkpoint2_source_acknowledged=true blocked_owner_checkpoint2_completed=false boundary={blocked_name}"
    );
}
