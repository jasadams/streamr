//! Caller-supplied native SQL packets. Application SQL and literal oracles stay
//! outside the engine repository; this harness executes them without rewriting.
use super::*;

#[test_log(tokio::test)]
#[ignore = "explicit native selector packet; requires STREAMR_SELECTOR_PACKET"]
async fn native_selector_packet_exact_outputs() {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    // Make the configured maximum reproducible; EOF/barriers flush partials.
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    let packet_path = PathBuf::from(env::var("STREAMR_SELECTOR_PACKET").unwrap());
    assert!(packet_path.is_absolute());
    let packet_bytes = tokio::fs::read(&packet_path).await.unwrap();
    let packet: Value = serde_json::from_slice(&packet_bytes).unwrap();
    let query_path = packet_path_field(&packet, "query_path");
    let input_path = packet_path_field(&packet, "input_path");
    let expected_path = packet_path_field(&packet, "expected_path");
    let outputs: Vec<PathBuf> = packet["output_paths"]
        .as_array()
        .expect("packet output_paths must be an array")
        .iter()
        .map(|path| {
            let path = PathBuf::from(path.as_str().unwrap());
            assert!(path.is_absolute());
            path
        })
        .collect();
    assert_eq!(
        outputs.len(),
        2,
        "packet requires two independent consumers"
    );
    assert_ne!(outputs[0], outputs[1]);
    let timestamp_paths: Vec<String> = packet["timestamp_paths"]
        .as_array()
        .expect("packet timestamp_paths must explicitly declare normalized fields")
        .iter()
        .map(|path| {
            let path = path.as_str().unwrap().to_owned();
            assert!(path.starts_with('/'), "timestamp paths are JSON pointers");
            path
        })
        .collect();
    let query_bytes = tokio::fs::read(&query_path).await.unwrap();
    let query = String::from_utf8(query_bytes.clone()).unwrap();
    let input_bytes = tokio::fs::read(&input_path).await.unwrap();
    let expected_bytes = tokio::fs::read(&expected_path).await.unwrap();
    let expected = packet_rows(&expected_bytes);
    assert!(
        !expected.is_empty(),
        "packet must declare a positive literal oracle"
    );
    for output in &outputs {
        assert_ne!(output, &query_path);
        assert_ne!(output, &input_path);
        assert_ne!(output, &expected_path);
        assert_ne!(output, &packet_path);
    }
    let job = format!(
        "native-selector-packet-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let evidence = PathBuf::from(env::var("STREAMR_TEST_EVIDENCE_DIR").unwrap()).join(&job);
    assert!(evidence.is_absolute());
    tokio::fs::create_dir_all(&evidence).await.unwrap();
    for (name, bytes) in [
        ("packet.json", packet_bytes),
        ("query.sql", query_bytes),
        ("input.jsonl", input_bytes.clone()),
        ("expected.jsonl", expected_bytes),
    ] {
        tokio::fs::write(evidence.join(name), bytes).await.unwrap();
    }
    println!(
        "NATIVE_SELECTOR_PACKET evidence={} input_rows={} expected_rows={} source_batch_max={}",
        evidence.display(),
        std::str::from_utf8(&input_bytes).unwrap().lines().count(),
        expected.len(),
        config::config().pipeline.source_batch_size
    );
    let udfs = get_udfs();
    let logical = Arc::new(match get_graph(query, &udfs).await {
        Ok(logical) => logical,
        Err(error) => {
            tokio::fs::write(
                evidence.join("planning-error.txt"),
                format!("{error:?}\n{error}\n"),
            )
            .await
            .unwrap();
            panic!("unchanged native SQL packet must plan: {error:?}");
        }
    });
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    let mut operator_counts = std::collections::BTreeMap::<String, usize>::new();
    let nodes: Vec<_> = logical.graph.node_weights().map(|node| {
        assert_eq!(node.parallelism, 1, "packet route must use singleton ownership");
        let operators: Vec<_> = node.operator_chain.iter().map(|(op,_)| {
            let kind = format!("{:?}",op.operator_name);
            *operator_counts.entry(kind.clone()).or_default() += 1;
            if matches!(op.operator_name,OperatorName::ConnectorSource|OperatorName::ConnectorSink) {
                let config: arroyo_rpc::grpc::api::ConnectorOp = prost::Message::decode(op.operator_config.as_slice()).unwrap();
                assert_eq!(config.connector,"single_file","packet uses finite file source and sinks");
                let config:arroyo_rpc::OperatorConfig = serde_json::from_str(&config.config).unwrap();
                let path = PathBuf::from(config.table["path"].as_str().unwrap());
                if op.operator_name == OperatorName::ConnectorSource { sources.push(path); } else { sinks.push(path); }
            }
            serde_json::json!({"operator_id":op.operator_id,"kind":kind})
        }).collect();
        serde_json::json!({"node_id":node.node_id,"parallelism":node.parallelism,"operators":operators})
    }).collect();
    let roots: Vec<_> = logical
        .graph
        .externals(petgraph::Direction::Incoming)
        .collect();
    let edges: Vec<_> = logical
        .graph
        .edge_indices()
        .map(|index| {
            let (from, to) = logical.graph.edge_endpoints(index).unwrap();
            serde_json::json!({"from":logical.graph[from].node_id,"to":logical.graph[to].node_id})
        })
        .collect();
    let graph_receipt = serde_json::json!({
        "roots":roots.iter().map(|index|logical.graph[*index].node_id).collect::<Vec<_>>(),
        "nodes":nodes,"edges":edges,"operator_counts":operator_counts,
        "source_paths":sources,"sink_paths":sinks,
        "backend":format!("{:?}",config::config().worker.sql_state_backend),
        "source_batch_max":config::config().pipeline.source_batch_size,
        "oracle_rows":expected.len(),
        "typed_state_limits":config::config().worker.typed_sql_state.as_ref().map(|limits| serde_json::json!({
            "decoded_bytes":limits.decoded_bytes,"row_bytes":limits.row_bytes,
            "page_entries":limits.page_entries,"page_bytes":limits.page_bytes,
            "scope_bytes":limits.scope_bytes,"scope_operations":limits.scope_operations,
            "max_working_event_bytes":limits.max_working_event_bytes,
            "max_captured_event_bytes":limits.max_captured_event_bytes
        }))
    });
    tokio::fs::write(
        evidence.join("planned-graph.json"),
        serde_json::to_vec_pretty(&graph_receipt).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        sources,
        vec![input_path],
        "SQL source must match caller's preserved input"
    );
    let mut expected_outputs = outputs.clone();
    expected_outputs.sort();
    sinks.sort();
    assert_eq!(
        sinks, expected_outputs,
        "SQL must feed both declared consumer files"
    );
    assert_eq!(roots.len(), 1, "packet graph must have one event root");
    assert!(
        logical.graph[roots[0]]
            .operator_chain
            .iter()
            .any(|(op, _)| op.operator_name == OperatorName::ConnectorSource)
    );
    let mut reached = HashSet::new();
    let mut pending = roots;
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
        "all consumers must be reachable"
    );
    if let Some(counts) = packet.get("operator_counts") {
        for (kind, count) in counts.as_object().unwrap() {
            assert_eq!(
                operator_counts.get(kind).copied().unwrap_or(0),
                count.as_u64().unwrap() as usize,
                "packet operator count {kind}"
            );
        }
    }
    for output in &outputs {
        match tokio::fs::symlink_metadata(output).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!(
                "cannot inspect declared output {}: {error}",
                output.display()
            ),
            Ok(_) => panic!(
                "declared output must be absent; caller must provide a fresh owned path: {}",
                output.display()
            ),
        }
    }
    let checkpoint = packet.get("checkpoint").map(|checkpoint| {
        let input_rows = checkpoint["input_rows"].as_u64().unwrap();
        let output_rows = usize::try_from(checkpoint["output_rows"].as_u64().unwrap()).unwrap();
        assert!(
            input_rows > 0
                && input_rows
                    < input_bytes
                        .split(|b| *b == b'\n')
                        .filter(|row| !row.is_empty())
                        .count() as u64
        );
        assert!(output_rows <= expected.len());
        (i32::try_from(input_rows).unwrap(), output_rows)
    });
    for attempt in 0..=usize::from(checkpoint.is_some()) {
        let (tx, mut rx) = channel(128);
        let program =
            local_program(&job, &logical.graph, &udfs, (attempt == 1).then_some(1), tx).await;
        let batches = super::smoke_str64::observe_source_batches(&program);
        let running = Engine::for_local(program, "pipe-test".into(), job.clone())
            .await
            .unwrap()
            .start()
            .await;
        let abort = AbortPacketAttempt(&running);
        if let Some((input_rows, output_rows)) = checkpoint.filter(|_| attempt == 0) {
            assert_eq!(running.source_controls().len(), 1);
            // Controlled single-file sources read the first row immediately.
            // The stopping barrier flushes the remaining partial source batch.
            advance(&running, input_rows - 1).await;
            let mut finished = HashSet::new();
            let bytes = checkpoint_with_stop(
                &mut SmokeTestContext {
                    job_id: Arc::new(job.clone()),
                    engine: &running,
                    control_rx: &mut rx,
                    program: logical.clone(),
                },
                1,
                true,
                &mut finished,
            )
            .await;
            assert!(bytes > 0, "packet checkpoint must contain retained state");
            let tasks: usize = running.operator_controls().values().map(Vec::len).sum();
            tokio::time::timeout(test_runtime_timeout(), async {
                while finished.len() < tasks {
                    match rx.recv().await {
                        Some(ControlResp::TaskFinished {
                            task_id,
                            subtask_idx,
                        }) => {
                            finished.insert((task_id, subtask_idx));
                        }
                        Some(ControlResp::TaskFailed { error, .. }) => {
                            panic!("packet checkpoint attempt failed: {error:?}")
                        }
                        Some(_) => {}
                        None => panic!("packet attempt closed before every task stopped"),
                    }
                }
            })
            .await
            .expect("packet checkpoint-stop tasks did not finish");
            drop(abort);
            drop(running);
            wait_for_capture_state_cleanup().await;
            let expected_prefix =
                normalize_packet_rows(expected[..output_rows].to_vec(), &timestamp_paths);
            for (index, output) in outputs.iter().enumerate() {
                let captured = match tokio::fs::read(output).await {
                    Ok(bytes) => bytes,
                    Err(error)
                        if output_rows == 0 && error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        Vec::new()
                    }
                    Err(error) => panic!(
                        "cannot read checkpoint consumer {}: {error}",
                        output.display()
                    ),
                };
                tokio::fs::write(
                    evidence.join(format!("checkpoint-consumer-{index}.jsonl")),
                    &captured,
                )
                .await
                .unwrap();
                assert_eq!(
                    normalize_packet_rows(packet_rows(&captured), &timestamp_paths),
                    expected_prefix,
                    "completed checkpoint must contain exactly the declared literal prefix"
                );
            }
            let observed_batches = batches.lock().unwrap().clone();
            assert_eq!(
                observed_batches.iter().sum::<usize>(),
                usize::try_from(input_rows).unwrap(),
                "checkpoint must stop at the declared raw prefix"
            );
            tokio::fs::write(evidence.join("checkpoint-receipt.json"), serde_json::to_vec_pretty(&serde_json::json!({
                "epoch":1,"checkpoint_bytes":bytes,"all_tasks_stopped":finished.len(),
                "input_rows":input_rows,"output_rows":output_rows,"source_batches":observed_batches,
                "restore":"fresh local program and owners in the same process",
            })).unwrap()).await.unwrap();
            if let Some(fault) = packet.get("fail_session_delivery") {
                super::smoke_str64::fail_finalized_session_delivery(
                    &job,
                    &logical,
                    &udfs,
                    &outputs,
                    output_rows,
                    i32::try_from(fault["source_noops"].as_u64().unwrap()).unwrap(),
                )
                .await;
                tokio::fs::write(
                    evidence.join("delivery-failure-receipt.json"),
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "producer":"SessionWindowAggregate", "completed_checkpoint":1,
                        "prefix_rows":output_rows,"blocked_finalized_output":true,
                        "checkpoint2_source_acknowledged":true,
                        "blocked_owner_checkpoint2_completed":false,
                        "task_failed":true,"whole_attempt_drained":true,
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            }
        } else {
            run_until_finished(&running, &mut rx).await;
            drop(abort);
            drop(running);
            wait_for_capture_state_cleanup().await;
            let observed_batches = batches.lock().unwrap().clone();
            tokio::fs::write(
                evidence.join(format!("attempt-{attempt}-batches.json")),
                serde_json::to_vec_pretty(&observed_batches).unwrap(),
            )
            .await
            .unwrap();
        }
    }
    let expected = normalize_packet_rows(expected, &timestamp_paths);
    for (index, output) in outputs.iter().enumerate() {
        let bytes = tokio::fs::read(output).await.unwrap();
        tokio::fs::write(evidence.join(format!("consumer-{index}.jsonl")), &bytes)
            .await
            .unwrap();
        let actual = normalize_packet_rows(packet_rows(&bytes), &timestamp_paths);
        assert_eq!(
            actual, expected,
            "consumer {index} must match every independent literal row in order"
        );
    }
    println!(
        "NATIVE_SELECTOR_PACKET_RESULT consumers=2 rows={} exact_literal_oracle=true",
        expected.len()
    );
}
struct AbortPacketAttempt<'a>(&'a RunningEngine);
impl Drop for AbortPacketAttempt<'_> {
    fn drop(&mut self) {
        self.0.abort_workers();
    }
}

fn packet_path_field(packet: &Value, name: &str) -> PathBuf {
    let path = PathBuf::from(packet[name].as_str().unwrap());
    assert!(path.is_absolute(), "packet {name} must be absolute");
    path
}
fn packet_rows(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| {
            let value: Value = serde_json::from_str(line).unwrap();
            assert!(value.is_object());
            value
        })
        .collect()
}
fn normalize_packet_rows(mut rows: Vec<Value>, timestamp_paths: &[String]) -> Vec<Value> {
    for row in &mut rows {
        for path in timestamp_paths {
            let value = row
                .pointer_mut(path)
                .expect("declared timestamp field must exist, including explicit SQL NULL");
            if value.is_null() {
                continue;
            }
            let text = value
                .as_str()
                .expect("declared timestamp must be text or SQL NULL");
            let text = text.strip_suffix('Z').unwrap_or(text);
            let whole = match text.split_once('.') {
                Some((whole, fraction)) => {
                    assert!(
                        !fraction.is_empty() && fraction.bytes().all(|byte| byte == b'0'),
                        "only zero serializer fractions may be normalized"
                    );
                    whole
                }
                None => text,
            };
            assert_eq!(
                whole.len(),
                19,
                "packet clocks must use exact whole-second canonical timestamps"
            );
            for (index, byte) in whole.bytes().enumerate() {
                assert!(
                    match index {
                        4 | 7 => byte == b'-',
                        10 => byte == b'T',
                        13 | 16 => byte == b':',
                        _ => byte.is_ascii_digit(),
                    },
                    "noncanonical packet timestamp"
                );
            }
            *value = Value::String(whole.to_owned());
        }
    }
    rows
}
