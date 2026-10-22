//! Native calendar maintenance feeds a separate MERGE owner before completed CPs.
use super::*;

#[test_log(tokio::test)]
#[ignore = "run explicitly with native aggregates/typed state, backend memory or rocksdb, source max1 or8"]
async fn calendar_merge_quiet_rollover_completed_checkpoint_fresh_restore() {
    run_calendar_case(false).await;
}

#[test_log(tokio::test)]
#[ignore = "run explicitly with native aggregates/typed state, five live-state databases and adequate scan pages"]
async fn calendar_merge_composed_three_branch_quiet_rollover_completed_checkpoint_fresh_restore() {
    run_calendar_case(true).await;
}

async fn run_calendar_case(composed: bool) {
    configure_test_worker();
    if composed {
        assert!(
            config::config()
                .worker
                .live_state_resources
                .as_ref()
                .unwrap()
                .max_open_databases
                >= 5,
            "composed calendar fixture requires explicit STREAMR_TEST_MAX_OPEN_DATABASES=5 and STREAMR_TEST_SCAN_PAGE_BYTES=16777216"
        );
    }
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    let batch_size = config::config().pipeline.source_batch_size;
    assert!(matches!(batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    let job = format!(
        "str64-calendar-{}-{}",
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
    tokio::fs::write(&input, include_str!("str64-calendar-events.jsonl"))
        .await
        .unwrap();
    let query = if composed {
        include_str!("str64-calendar-composed-query.sql")
    } else {
        include_str!("str64-calendar-query.sql")
    };
    let sql = query
        .replace("$input", input.to_str().unwrap())
        .replace("$output", output.to_str().unwrap())
        .replace("$mirror", mirror.to_str().unwrap());
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    tokio::fs::write(
        directory.join("expected.json"),
        include_str!("str64-calendar-expected.json"),
    )
    .await
    .unwrap();
    tokio::fs::write(directory.join("runtime-config.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "backend":format!("{:?}",config::config().worker.sql_state_backend), "source_max_rows":batch_size,
        "source_linger_seconds":3600,"checkpoint_mode":if leader_mode() { "leader" } else { "controller" },
        "processed_raw_rows":5,"unread_sentinel_rows":1,"composed_three_branches":composed,
    })).unwrap()).await.unwrap();
    let udfs = get_udfs();
    let logical = Arc::new(
        get_graph(sql, &udfs)
            .await
            .expect("calendar MERGE SQL must plan"),
    );
    let operators: Vec<_> = logical
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .map(|(op, _)| op.operator_name)
        .collect();
    for (kind, count) in [
        (OperatorName::ConnectorSource, 1),
        (
            OperatorName::UpdatingAggregate,
            if composed { 4 } else { 1 },
        ),
        (OperatorName::FusedStateTable, 1),
        (OperatorName::ConnectorSink, 2),
    ] {
        assert_eq!(
            operators.iter().filter(|op| **op == kind).count(),
            count,
            "actual shared producer/consumer graph: {kind:?}"
        );
    }
    assert!(
        logical
            .graph
            .node_weights()
            .all(|node| node.parallelism == 1)
    );
    let graph = logical.graph.node_weights().map(|node| serde_json::json!({"node_id":node.node_id,"operators":node.operator_chain.iter().map(|(op,_)| serde_json::json!({"kind":format!("{:?}",op.operator_name),"id":op.operator_id,"config":op.operator_config})).collect::<Vec<_>>()})).collect::<Vec<_>>();
    tokio::fs::write(
        directory.join("planned-graph.json"),
        serde_json::to_vec_pretty(&graph).unwrap(),
    )
    .await
    .unwrap();
    let mut receipts = Vec::new();
    // Each stopped attempt consumes exactly its admitted prefix. The unread
    // sixth row rules out terminal EOF as the source of quiet maintenance.
    for epoch in 1..=3u32 {
        let (tx, mut rx) = channel(128);
        let program = local_program(
            &job,
            &logical.graph,
            &udfs,
            (epoch > 1).then_some(u64::from(epoch - 1)),
            tx,
        )
        .await;
        let observed_batches = super::smoke_str64::observe_source_batches(&program);
        let running = Engine::for_local(program, "pipe-test".into(), job.clone())
            .await
            .unwrap()
            .start()
            .await;
        let abort = AbortCalendarAttempt(&running);
        if epoch == 1 {
            advance(&running, 2).await;
        }
        let mut finished = HashSet::new();
        let checkpoint_bytes = checkpoint_with_stop(
            &mut SmokeTestContext {
                job_id: Arc::new(job.clone()),
                engine: &running,
                control_rx: &mut rx,
                program: logical.clone(),
            },
            epoch,
            true,
            &mut finished,
        )
        .await;
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
                        panic!("calendar checkpoint attempt failed: {error:?}")
                    }
                    Some(_) => {}
                    None => panic!("calendar attempt closed before all tasks stopped"),
                }
            }
        })
        .await
        .expect("calendar checkpoint-stop tasks did not finish");
        assert_eq!(finished.len(), tasks);
        drop(abort);
        drop(running);
        wait_for_capture_state_cleanup().await;
        let batches = observed_batches.lock().unwrap().clone();
        let expected_batches = if epoch == 1 && batch_size == 1 {
            vec![1, 1, 1]
        } else {
            vec![if epoch == 1 { 3 } else { 1 }]
        };
        assert_eq!(
            batches, expected_batches,
            "actual input batches for CP{epoch}"
        );
        let expected_count = [3, 7, 9][(epoch - 1) as usize];
        let first = calendar_rows(&output).await;
        let second = calendar_rows(&mirror).await;
        assert_calendar_rows(&first, epoch, expected_count, composed);
        assert_calendar_rows(&second, epoch, expected_count, composed);
        assert_eq!(
            calendar_normalized_rows(&first),
            calendar_normalized_rows(&second),
            "both consumers preserve the actual ordered output of the single MERGE"
        );
        tokio::fs::write(
            directory.join(format!("cp{epoch}-captured.json")),
            serde_json::to_vec_pretty(&first).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(
            directory.join(format!("cp{epoch}-mirror.json")),
            serde_json::to_vec_pretty(&second).unwrap(),
        )
        .await
        .unwrap();
        receipts.push(serde_json::json!({"epoch":epoch,"restored_epoch":(epoch > 1).then_some(epoch-1),"checkpoint_bytes":checkpoint_bytes,"source_batches":batches,"captured_rows":first.len(),"all_tasks_stopped":finished.len()}));
        tokio::fs::write(
            directory.join("checkpoint-receipts.json"),
            serde_json::to_vec_pretty(&receipts).unwrap(),
        )
        .await
        .unwrap();
    }
    println!("STR64_CALENDAR_EVIDENCE {}", directory.display());
}

struct AbortCalendarAttempt<'a>(&'a RunningEngine);
impl Drop for AbortCalendarAttempt<'_> {
    fn drop(&mut self) {
        self.0.abort_workers();
    }
}

async fn calendar_rows(path: &Path) -> Vec<Value> {
    tokio::fs::read_to_string(path)
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn calendar_normalized_rows(rows: &[Value]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut row = row.clone();
            assert_eq!(row["tenant"], "t");
            assert!(row.get(arroyo_rpc::UPDATING_META_FIELD).is_none());
            // The JSON sink may omit SQL NULL fields; materialize only the
            // declared nullable capture field for exact semantic comparison.
            row.as_object_mut()
                .unwrap()
                .entry("old_total")
                .or_insert(Value::Null);
            let clock = row["trigger_time"]
                .as_str()
                .expect("native clock is JSON timestamp");
            // Normalize only the engine's UTC serialization suffix/zero fraction.
            // Date and full seconds remain exact independently specified values.
            assert!(clock.len() >= 19);
            let suffix = clock[19..].strip_suffix('Z').unwrap_or(&clock[19..]);
            assert!(
                suffix.is_empty()
                    || suffix.strip_prefix('.').is_some_and(
                        |digits| !digits.is_empty() && digits.chars().all(|c| c == '0')
                    ),
                "unexpected clock {clock}"
            );
            row["trigger_time"] = Value::String(clock[..19].to_owned());
            row
        })
        .collect()
}

fn calendar_per_key(rows: &[Value]) -> BTreeMap<String, Vec<Value>> {
    let mut result = BTreeMap::<String, Vec<Value>>::new();
    for row in calendar_normalized_rows(rows) {
        result
            .entry(row["k"].as_str().unwrap().to_owned())
            .or_default()
            .push(row);
    }
    result
}

fn assert_calendar_rows(rows: &[Value], epoch: u32, expected_count: usize, composed: bool) {
    assert_eq!(
        rows.len(),
        expected_count,
        "CP{epoch} exact quiet output cardinality"
    );
    let all: BTreeMap<String, Vec<Value>> =
        serde_json::from_str(include_str!("str64-calendar-expected.json")).unwrap();
    let expected = all
        .into_iter()
        .filter_map(|(key, mut values)| {
            if (epoch == 1 && key.as_str() > "c") || (epoch == 2 && key == "e") {
                return None;
            }
            if epoch == 1 || (epoch == 2 && key == "d") {
                values.truncate(1);
            }
            Some((key, values))
        })
        .collect::<BTreeMap<_, _>>();
    let mut actual = calendar_per_key(rows);
    if composed {
        // Independent literal optional-branch values remain unchanged during
        // core quiet rollover and after both later raw events filter out.
        for (key, values) in &mut actual {
            let (a, b) = match key.as_str() {
                "a" | "c" => (Value::from(1), Value::Null),
                "b" => (Value::from(1), Value::from(1)),
                "d" | "e" => (Value::Null, Value::Null),
                other => panic!("unexpected composed calendar key {other}"),
            };
            for row in values {
                assert_eq!(row.get("optional_a").unwrap_or(&Value::Null), &a);
                assert_eq!(row.get("optional_b").unwrap_or(&Value::Null), &b);
                row.as_object_mut().unwrap().remove("optional_a");
                row.as_object_mut().unwrap().remove("optional_b");
            }
        }
    }
    assert_eq!(
        actual, expected,
        "CP{epoch} literal per-key captures; no global quiet-key ordering assumption"
    );
}
