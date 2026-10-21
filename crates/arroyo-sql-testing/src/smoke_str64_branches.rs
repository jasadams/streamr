//! Actual native branch reconvergence. The single-aggregate control cannot
//! establish completion of a separate filtered maintained dependency.
use super::*;

#[test_log(tokio::test)]
#[ignore = "explicit memory/RocksDB and batch1/8 branch coherence matrix"]
async fn maintained_merge_count_and_filtered_max_branches() {
    branch_case("branches", include_str!("test/str64-branches-probe.sql")).await;
}

#[test_log(tokio::test)]
#[ignore = "explicit memory/RocksDB and batch1/8 branch coherence matrix"]
async fn maintained_merge_single_aggregate_filter_control() {
    branch_case(
        "single-aggregate-control",
        include_str!("test/str64-branches-control.sql"),
    )
    .await;
}

async fn branch_case(name: &str, template: &str) {
    configure_test_worker();
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(
        config::config().worker.join_state.is_some(),
        "set STREAMR_TEST_NATIVE_UPDATING_JOINS=1 for native branch JOIN execution"
    );
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|c| c.pipeline.source_batch_linger = Duration::from_secs(3600).into());
    let job = format!(
        "str64-{name}-{}-{}",
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
    tokio::fs::write(&input, include_str!("test/str64-branches.jsonl"))
        .await
        .unwrap();
    tokio::fs::write(
        directory.join("expected.jsonl"),
        include_str!("test/str64-branches-expected.jsonl"),
    )
    .await
    .unwrap();
    let sql = template
        .replace("__INPUT__", input.to_str().unwrap())
        .replace("__OUTPUT__", output.to_str().unwrap())
        .replace("__MIRROR__", mirror.to_str().unwrap());
    tokio::fs::write(directory.join("query.sql"), &sql)
        .await
        .unwrap();
    println!(
        "STR64_BRANCH_SQL name={name} fixture={} raw_operations=3 expected_captures=3 source_batch_max={}",
        directory.display(),
        config::config().pipeline.source_batch_size
    );
    let udfs = get_udfs();
    // This positive regression intentionally preserves today's exact planner
    // rejection until the native branch completion capability is repaired.
    let logical = get_graph(sql, &udfs)
        .await
        .expect("native maintained dependency reconvergence must plan");
    let (tx, mut rx) = channel(128);
    let program = local_program(&job, &logical.graph, &udfs, None, tx).await;
    let running = Engine::for_local(program, "pipe-test".into(), job)
        .await
        .unwrap()
        .start()
        .await;
    assert_eq!(
        running.source_controls().len(),
        1,
        "both branches must share one source"
    );
    run_until_finished(&running, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    let rows = read_rows(&output).await;
    assert_eq!(
        rows,
        read_rows(&mirror).await,
        "two consumers must share one coherent captured mutation"
    );
    let expected: Vec<Value> = include_str!("test/str64-branches-expected.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        3,
        "exactly one result per source prefix, including unchanged filtered MAX branch"
    );
    for (index, (actual, expected)) in rows.iter().zip(&expected).enumerate() {
        for field in [
            "k",
            "source_total",
            "source_peak",
            "old_total",
            "old_peak",
            "total",
            "peak",
            "action",
        ] {
            assert_eq!(
                actual.get(field).unwrap_or(&Value::Null),
                &expected[field],
                "{name} prefix{index} field{field}"
            );
        }
        let expected_time = expected["trigger_time"]
            .as_str()
            .unwrap()
            .trim_end_matches('Z');
        let actual_time = actual["trigger_time"]
            .as_str()
            .expect("original trigger must serialize as timestamp");
        let normalized = actual_time.strip_suffix('Z').unwrap_or(actual_time);
        let exact = match normalized.strip_prefix(expected_time) {
            Some("") => true,
            Some(suffix) => suffix.strip_prefix('.').is_some_and(|fraction| {
                !fraction.is_empty() && fraction.bytes().all(|byte| byte == b'0')
            }),
            None => false,
        };
        assert!(
            exact,
            "{name} prefix{index}: expected exact originalclock{expected_time}, got{actual_time}"
        );
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
