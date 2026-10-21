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
    if actual == expected {
        return true;
    }
    let Some(suffix) = actual.strip_prefix(expected) else {
        return false;
    };
    let suffix = suffix.strip_suffix('Z').unwrap_or(suffix);
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
