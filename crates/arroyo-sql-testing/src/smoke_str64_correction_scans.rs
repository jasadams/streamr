//! Finite actual Engine SQL qualification from an externally supplied packet.
//! Invoke in a fresh process per packet/backend/batch/page/hook setting.
use super::*;

fn sample_proc(directory: &Path, phase: &str, started: std::time::Instant) {
    let snapshot = serde_json::json!({
        "phase":phase,"monotonic_elapsed_ns":started.elapsed().as_nanos(),
        "pid":std::process::id(),
        "stat":std::fs::read_to_string("/proc/self/stat").unwrap(),
        "status":std::fs::read_to_string("/proc/self/status").unwrap(),
        "io":std::fs::read_to_string("/proc/self/io").unwrap(),
        "note":"Whole test process, including Engine and harness; phase deltas are not isolated operator CPU."
    });
    std::fs::write(
        directory.join(format!("proc-{phase}.json")),
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();
    println!("STREAMR_SCAN_PHASE {snapshot}");
}

fn same_timestamp(actual: &Value, expected: &Value) -> bool {
    let Some(actual) = actual.as_str() else {
        return false;
    };
    let Some(expected) = expected.as_str() else {
        return false;
    };
    let actual = actual.strip_suffix('Z').unwrap_or(actual);
    let expected = expected.strip_suffix('Z').unwrap_or(expected);
    if actual == expected {
        return true;
    }
    let Some(suffix) = actual.strip_prefix(expected) else {
        return false;
    };
    suffix.is_empty()
        || suffix.strip_prefix('.').is_some_and(|fraction| {
            !fraction.is_empty() && fraction.bytes().all(|byte| byte == b'0')
        })
}
fn assert_packet_rows(actual: &[Value], expected: &[Value], timestamps: &[Value]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "exact accepted captures; absence has no row"
    );
    for (row, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let expected = expected.as_object().unwrap();
        for (field, wanted) in expected {
            if matches!(field.as_str(), "operation" | "phase") {
                continue;
            }
            let found = actual.get(field).unwrap_or(&Value::Null);
            if timestamps.iter().any(|name| name.as_str() == Some(field)) && !wanted.is_null() {
                assert!(
                    same_timestamp(found, wanted),
                    "capture{row} {field}: expected{wanted}, got{found}"
                );
            } else {
                assert_eq!(found, wanted, "capture{row} {field}");
            }
        }
        assert!(
            actual
                .as_object()
                .unwrap()
                .keys()
                .all(|field| expected.contains_key(field)
                    && !matches!(field.as_str(), "operation" | "phase")),
            "unexpected SQL output columns"
        );
    }
}

#[test_log(tokio::test)]
#[ignore = "finite external native CDC scan qualification; fresh process per matrix cell"]
async fn maintained_correction_scan_qualification() {
    correction_setup();
    let packet = PathBuf::from(
        env::var_os("STREAMR_TEST_SCAN_PACKET")
            .expect("explicit reviewed packet directory required"),
    );
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(packet.join("manifest.json")).unwrap())
            .unwrap();
    let input_text = std::fs::read_to_string(packet.join("events.jsonl")).unwrap();
    let expected: Vec<Value> = std::fs::read_to_string(packet.join("expected.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let seed = manifest["checkpoint_after_seed_raw_operations"]
        .as_u64()
        .unwrap() as usize;
    let total = manifest["total_raw_operations"].as_u64().unwrap() as usize;
    assert_eq!(input_text.lines().count(), total);
    assert_eq!(
        expected.len(),
        manifest["expected_accepted_rows_per_consumer"]
            .as_u64()
            .unwrap() as usize
    );
    assert!(seed > 0 && seed < total);
    // These are fixture-declared timestamp columns, not engine semantics.
    let timestamps = manifest
        .get("timestamp_columns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            vec![
                serde_json::json!("last_seen"),
                serde_json::json!("trigger_time"),
            ]
        });
    let initial_page = config::config()
        .worker
        .aggregate_state
        .unwrap()
        .page_entries;
    let page = env::var("STREAMR_TEST_SCAN_PAGE_ENTRIES")
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(initial_page);
    assert!(
        page == 1 || page == initial_page,
        "only reviewed page1/current-page control"
    );
    config::update(|c| c.worker.aggregate_state.as_mut().unwrap().page_entries = page);
    let hook = env::var("STREAMR_LIVE_SCAN_DIAGNOSTICS").unwrap_or_else(|_| "0".into());
    assert!(matches!(hook.as_str(), "0" | "1"));
    let job = format!(
        "str64-scan-{}-{}",
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
    tokio::fs::write(&input, &input_text).await.unwrap();
    tokio::fs::write(
        directory.join("expected.jsonl"),
        std::fs::read(packet.join("expected.jsonl")).unwrap(),
    )
    .await
    .unwrap();
    let sql = std::fs::read_to_string(packet.join("query.sql"))
        .unwrap()
        .replace("__INPUT__", input.to_str().unwrap())
        .replace("__OUTPUT__", output.to_str().unwrap())
        .replace("__MIRROR__", mirror.to_str().unwrap());
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    std::fs::write(directory.join("run-manifest.json"),serde_json::to_vec_pretty(&serde_json::json!({"packet":packet,"manifest":manifest,"hook":hook,"source_batch":config::config().pipeline.source_batch_size,"aggregate_limits":config::config().worker.aggregate_state,"typed_limits":config::config().worker.typed_sql_state,"resources":config::config().worker.live_state_resources,"backend":format!("{:?}",config::config().worker.sql_state_backend),"phase_note":"OFF cost pass and ON diagnostic pass are separate test processes; startup/checkpoint/verification have explicit milestones."})).unwrap()).unwrap();
    let started = std::time::Instant::now();
    sample_proc(&directory, "plan_begin", started);
    let udfs = get_udfs();
    let logical = Arc::new(
        get_graph(sql, &udfs)
            .await
            .expect("native qualification SQL must plan"),
    );
    let planned_operators: Vec<_> = logical.graph.node_weights().flat_map(|node| {
        node.operator_chain.iter().enumerate().map(move |(chain_index, (operator, _))| {
            let aggregate = if operator.operator_name == OperatorName::UpdatingAggregate {
                let config = <arroyo_rpc::grpc::api::UpdatingAggregateOperator as prost::Message>::decode(
                    operator.operator_config.as_slice()).unwrap();
                // Preserve ordered protobuf bytes for independent attribution
                // without adding a decoder dependency to this harness crate.
                Some(config.aggregate_exec)
            } else { None };
            serde_json::json!({"node_id":node.node_id,"chain_index":chain_index,
                "operator_id":operator.operator_id,"operator_kind":format!("{:?}",operator.operator_name),
                "operator_config_bytes":operator.operator_config,
                "aggregate_physical_plan_protobuf_bytes":aggregate})
        })
    }).collect();
    std::fs::write(
        directory.join("planned-operator-configs.json"),
        serde_json::to_vec_pretty(&planned_operators).unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, None, tx).await;
    let raw = observe_raw_correction_batches(&program);
    let unrolled = super::super::smoke_str64::observe_source_batches(&program);
    sample_proc(&directory, "engine_start_begin", started);
    let running = Engine::for_local(program, "pipe-test".into(), job.clone())
        .await
        .unwrap()
        .start()
        .await;
    assert_eq!(running.source_controls().len(), 1);
    sample_proc(&directory, "engine_started", started);
    advance(&running, i32::try_from(seed - 1).unwrap()).await;
    sample_proc(&directory, "seed_controls_enqueued", started);
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
    sample_proc(&directory, "seed_checkpoint_stopped", started);
    let seed_expected: Vec<_> = expected
        .iter()
        .filter(|row| row["operation"].as_u64().unwrap() <= seed as u64)
        .cloned()
        .collect();
    let prefix = read_rows(&output).await;
    assert_eq!(prefix, read_rows(&mirror).await);
    assert_packet_rows(&prefix, &seed_expected, &timestamps);
    std::fs::write(
        directory.join("seed-captured.jsonl"),
        std::fs::read(&output).unwrap(),
    )
    .unwrap();
    // The stopping barrier flushes any partial final seed batch (seed4 in
    // the duplicate packet); all preceding batches still use the configured cap.
    let lines: Vec<_> = input_text.lines().map(str::to_owned).collect();
    assert_correction_batches(&directory, "seed", &raw, &unrolled, &lines[..seed]);
    sample_proc(&directory, "seed_verification_complete", started);
    if let Ok(boundary) = env::var("STREAMR_TEST_SCAN_FAULT_BOUNDARY") {
        let kind = match boundary.as_str() {
            "aggregate" => OperatorName::UpdatingAggregate,
            "target" => OperatorName::FusedStateTable,
            _ => panic!("reviewed scan fault boundary must be aggregate or target"),
        };
        let owners: Vec<_> = logical
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(operator, _)| operator.operator_name == kind)
            .map(|(operator, _)| operator.operator_id.clone())
            .collect();
        assert_eq!(
            owners.len(),
            1,
            "one actual graph fault owner of requested kind"
        );
        let admitted = manifest["fault_admit_raw_operations"].as_u64().unwrap() as usize;
        assert!(
            admitted < total - seed,
            "source must have further input for snapshot ACK"
        );
        let prefix_captures = manifest["fault_prefix_captures"].as_u64().unwrap() as usize;
        assert_eq!(prefix_captures, seed_expected.len());
        sample_proc(&directory, "fault_attempt_begin", started);
        fail_scan_after_live_commit(
            &job,
            &logical,
            &udfs,
            &output,
            &mirror,
            ScanFaultParameters {
                completed_epoch: 1,
                prefix_captures,
                admitted_raw_operations: admitted,
                blocked_id: &owners[0],
                directory: &directory,
            },
        )
        .await;
        sample_proc(&directory, "fault_attempt_drained", started);
    }
    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, Some(1), tx).await;
    let raw = observe_raw_correction_batches(&program);
    let unrolled = super::super::smoke_str64::observe_source_batches(&program);
    sample_proc(&directory, "restore_start_begin", started);
    let restored = Engine::for_local(program, "pipe-test".into(), job)
        .await
        .unwrap()
        .start()
        .await;
    sample_proc(&directory, "restored_engine_started", started);
    run_until_finished(&restored, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    sample_proc(&directory, "tail_completed", started);
    let rows = read_rows(&output).await;
    assert_eq!(
        rows,
        read_rows(&mirror).await,
        "one captured effectful producer, two consumers"
    );
    assert_packet_rows(&rows, &expected, &timestamps);
    assert_correction_batches(&directory, "restored_tail", &raw, &unrolled, &lines[seed..]);
    sample_proc(&directory, "verification_complete", started);
    println!(
        "STREAMR_SCAN_COMPLETED {}",
        serde_json::json!({"fixture":directory,"raw_operations":total,"seed_raw_operations":seed,"tail_raw_operations":total-seed,"captures_per_consumer":rows.len(),"hook":hook,"normal_cost_pass":hook=="0","worker_oracle":"PASS","inventory_note":"Native aggregate G/M/R reconstruction uses committed-overlay diagnostic records, excluding target/capture namespaces; this is not backend readback."})
    );
}

fn observe_scan_fault_by_id(
    program: &Program,
    operator_id: &str,
    fault: Option<Arc<ScanDeliveryFault>>,
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
                if chain.context.task_info.operator_id == operator_id {
                    let inner = std::mem::replace(&mut chain.operator, Box::new(ScanUnreachable));
                    chain.operator = Box::new(ScanBatchObserver {
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

struct ScanUnreachable;
#[async_trait::async_trait]
impl ArrowOperator for ScanUnreachable {
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
struct ScanBatchObserver {
    inner: Box<dyn ArrowOperator>,
    rows: Arc<std::sync::Mutex<Vec<usize>>>,
    fault: Option<Arc<ScanDeliveryFault>>,
}
#[async_trait::async_trait]
impl ArrowOperator for ScanBatchObserver {
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
                    &mut ScanFailingCollector {
                        inner: collector,
                        fault: fault.clone(),
                    },
                )
                .await
        } else {
            self.inner.process_batch(batch, ctx, collector).await
        }
    }
    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<arroyo_types::Watermark>> {
        self.inner.handle_watermark(watermark, ctx, collector).await
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
struct ScanDeliveryFault {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct ScanFailingCollector<'a> {
    inner: &'a mut dyn Collector,
    fault: Arc<ScanDeliveryFault>,
}
#[async_trait::async_trait]
impl Collector for ScanFailingCollector<'_> {
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

struct ScanFaultParameters<'a> {
    completed_epoch: u64,
    prefix_captures: usize,
    admitted_raw_operations: usize,
    blocked_id: &'a str,
    directory: &'a Path,
}

async fn fail_scan_after_live_commit(
    job: &str,
    logical: &Arc<LogicalProgram>,
    udfs: &[LocalUdf],
    output: &Path,
    mirror: &Path,
    parameters: ScanFaultParameters<'_>,
) {
    let ScanFaultParameters {
        completed_epoch,
        prefix_captures,
        admitted_raw_operations,
        blocked_id,
        directory,
    } = parameters;
    let next_epoch = completed_epoch
        .checked_add(1)
        .expect("checkpoint epoch exhausted");
    let barrier_epoch =
        u32::try_from(next_epoch).expect("checkpoint epoch must fit native barrier");
    let (tx, mut rx) = channel(128);
    let program = local_program(job, &logical.graph, udfs, Some(completed_epoch), tx).await;
    let fault = Arc::new(ScanDeliveryFault::default());
    assert!(admitted_raw_operations > 0);
    let matching: Vec<_> = logical
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_id == blocked_id)
        .collect();
    assert_eq!(matching.len(), 1, "exact actual graph fault owner");
    assert!(matches!(
        matching[0].0.operator_name,
        OperatorName::UpdatingAggregate | OperatorName::FusedStateTable
    ));
    let _observations = observe_scan_fault_by_id(&program, blocked_id, Some(fault.clone()));
    let raw = observe_raw_correction_batches(&program);
    let failed = Engine::for_local(program, "pipe-test".into(), job.into())
        .await
        .unwrap()
        .start()
        .await;
    // Source initially waits before the next raw operation. Admit exactly
    // one reviewed full batch; later input remains available for CP2 ACK.
    advance(&failed, i32::try_from(admitted_raw_operations - 1).unwrap()).await;
    tokio::time::timeout(test_runtime_timeout(), fault.reached.notified())
        .await
        .expect("owner never reached blocked real output");
    assert_eq!(
        read_rows(output).await.len(),
        prefix_captures,
        "blocked output reached primary sink"
    );
    assert_eq!(
        read_rows(mirror).await.len(),
        prefix_captures,
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
    for source in failed.source_controls() {
        source
            .send(ControlMessage::Checkpoint(CheckpointBarrier {
                completed_base_epoch: None,
                epoch: barrier_epoch,
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
                Some(ControlResp::CheckpointCompleted(c)) if c.checkpoint_epoch == next_epoch => {
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
                Some(ControlResp::CheckpointCompleted(c)) if c.checkpoint_epoch == next_epoch => {
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
                Some(ControlResp::CheckpointCompleted(c)) if c.checkpoint_epoch == next_epoch => {
                    assert_ne!(
                        c.operator_id, blocked_id,
                        "failed owner completed checkpoint during attempt drain"
                    );
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
    assert_eq!(read_rows(output).await.len(), prefix_captures);
    assert_eq!(read_rows(mirror).await.len(), prefix_captures);
    let raw = raw.lock().unwrap();
    assert_eq!(
        raw.batches.first(),
        Some(&admitted_raw_operations),
        "actual first full fault batch"
    );
    std::fs::write(directory.join("fault-source-batches.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"raw_batches":raw.batches,"markers":raw.markers,
        "note":"Observed failed-attempt prefix only; CP source ACK does not imply remaining rows delivered."})).unwrap()).unwrap();
    println!(
        "STR64_FAULT observed_real_output_block=true observed_task_failure=true whole_attempt_drained=true checkpoint2_source_acknowledged=true blocked_owner_checkpoint2_completed=false boundary={blocked_id}"
    );
}

#[test]
fn scan_packet_timestamp_normalization_accepts_only_one_optional_z() {
    let plain = serde_json::json!("1970-01-01T00:01:40");
    let utc = serde_json::json!("1970-01-01T00:01:40Z");
    assert!(same_timestamp(&utc, &plain));
    assert!(same_timestamp(&plain, &utc));
    assert!(same_timestamp(
        &serde_json::json!("1970-01-01T00:01:40.000Z"),
        &utc
    ));
    assert!(!same_timestamp(
        &serde_json::json!("1970-01-01T00:01:40.001Z"),
        &utc
    ));
    assert!(!same_timestamp(
        &serde_json::json!("1970-01-01T00:01:40ZZ"),
        &utc
    ));
    assert!(!same_timestamp(
        &serde_json::json!("1970-01-01T00:01:40.000ZZ"),
        &utc
    ));
}
