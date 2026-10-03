//! Dedicated-process SQL checkpoint fault qualification. Run each ignored test
//! with STREAMR_TEST_BACKEND=rocksdb, selecting controller or leader through
//! STREAMR_TEST_CHECKPOINT_MODE. The single-file sink truncates to the selected
//! checkpoint offset before source replay; this is its delivery contract only.
use super::*;
use arroyo_types::CheckpointFilePathLayout;

struct FaultFixture {
    root: PathBuf,
    job_id: Arc<String>,
    program: LogicalProgram,
    output: PathBuf,
    golden: PathBuf,
    disk_operator: String,
    disk_node: u32,
    expected_records: usize,
}

async fn fixture(label: &str) -> FaultFixture {
    assert_eq!(env::var("STREAMR_TEST_BACKEND").as_deref(), Ok("rocksdb"));
    assert_ne!(env::var("STREAMR_TEST_CHECKPOINT_STOP").as_deref(), Ok("1"));
    configure_test_worker();
    let job_id = Arc::new(format!(
        "{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let root = env::temp_dir().join(job_id.as_str());
    tokio::fs::create_dir_all(root.join("checkpoints"))
        .await
        .unwrap();
    config::update(|c| c.checkpoint_url = format!("file://{}", root.join("checkpoints").display()));
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = root.join("output.json");
    let query =
        read_to_string(crate_root.join("src/test/queries/stateful_processor_operations.sql"))
            .await
            .unwrap()
            .replace("$input_dir", crate_root.join("inputs").to_str().unwrap())
            .replace("$output_path", output.to_str().unwrap());
    let program = get_graph(query, &get_udfs()).await.unwrap();
    let (disk_node, disk_operator) = program
        .graph
        .node_weights()
        .find_map(|node| {
            node.operator_chain
                .iter()
                .find(|(op, _)| op.operator_name == OperatorName::StatefulProcessor)
                .map(|(op, _)| (node.node_id, op.operator_id.clone()))
        })
        .expect("fixture must execute a real SQL state operator");
    let expected_records =
        read_to_string(crate_root.join("golden_outputs/stateful_processor_operations.json"))
            .await
            .unwrap()
            .lines()
            .count();
    FaultFixture {
        root,
        job_id,
        program,
        output,
        golden: crate_root.join("golden_outputs/stateful_processor_operations.json"),
        disk_operator,
        disk_node,
        expected_records,
    }
}

fn paths(fixture: &FaultFixture) -> arroyo_state_protocol::ProtocolPaths {
    arroyo_state_protocol::ProtocolPaths::new(
        arroyo_types::PipelineId::new("pipe-test"),
        arroyo_types::JobId(fixture.job_id.clone()),
    )
}

fn layout() -> CheckpointFilePathLayout {
    if leader_mode() {
        CheckpointFilePathLayout::Protocol {
            pipeline_id: arroyo_types::PipelineId::new("pipe-test"),
            generation: 0,
        }
    } else {
        CheckpointFilePathLayout::Legacy
    }
}

async fn abort_and_wait(
    engine: &RunningEngine,
    graph: &LogicalGraph,
    rx: &mut Receiver<ControlResp>,
) {
    let expected: HashSet<_> = graph.node_weights().map(|node| (node.node_id, 0)).collect();
    engine.abort_workers();
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stopped = HashSet::new();
        while !expected.is_subset(&stopped) {
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
                None => panic!("workers stopped before reporting every aborted stage"),
            }
        }
    })
    .await
    .expect("aborted worker stages did not terminate");
}

async fn restore_and_check(fixture: &FaultFixture, epoch: u64) {
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        Some(epoch),
        tx,
    )
    .await;
    finish_from_checkpoint(&fixture.job_id, program, &mut rx).await;
    check_output_files(
        "fault recovery",
        fixture.output.to_str().unwrap().into(),
        fixture.golden.to_str().unwrap().into(),
        None,
    )
    .await;
}

#[test_log(tokio::test)]
#[ignore = "dedicated process: STREAMR_TEST_BACKEND=rocksdb; select controller or leader"]
async fn milestone2_upload_failure_recreates_worker_from_last_published_checkpoint() {
    let fixture = fixture("upload-failure").await;
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        None,
        tx,
    )
    .await;
    let running = Engine::for_local(program, "pipe-test".into(), (*fixture.job_id).clone())
        .await
        .unwrap()
        .start()
        .await;
    advance(&running, 40).await;
    checkpoint(
        &mut SmokeTestContext {
            job_id: fixture.job_id.clone(),
            engine: &running,
            control_rx: &mut rx,
            program: Arc::new(fixture.program.clone()),
        },
        1,
    )
    .await;
    let committed_output_bytes = tokio::fs::metadata(&fixture.output).await.unwrap().len();
    advance(&running, 40).await;
    let blocked_path = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        "__sp_shared",
        0,
        2,
        false,
    );
    let blocker = fixture.root.join("checkpoints").join(&blocked_path);
    tokio::fs::create_dir_all(blocker.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&blocker, b"injected upload failure: not a directory")
        .await
        .unwrap();
    let barrier = CheckpointBarrier {
        epoch: 2,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    };
    for source in running.source_controls() {
        source
            .send(ControlMessage::Checkpoint(barrier))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match rx
                .recv()
                .await
                .expect("workers stopped before injected fault")
            {
                ControlResp::CheckpointCompleted(c)
                    if c.operator_id == fixture.disk_operator && c.checkpoint_epoch == 2 =>
                {
                    panic!("disk operator reported completion despite failed upload")
                }
                ControlResp::TaskFailed { task_id, error, .. } => {
                    assert_eq!(
                        task_id, fixture.disk_node,
                        "unexpected worker failed: {error:?}"
                    );
                    assert_eq!(
                        error.operator_id.as_deref(),
                        Some(fixture.disk_operator.as_str()),
                        "failure did not come from the disk table exporter"
                    );
                    println!(
                        "EXPECTED_UPLOAD_FAILURE mode={} error={error:?}",
                        if leader_mode() {
                            "leader"
                        } else {
                            "controller"
                        }
                    );
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("file upload failure did not reach worker TaskFailed");
    tokio::time::timeout(Duration::from_secs(30), async {
        while tokio::fs::metadata(&fixture.output).await.unwrap().len() <= committed_output_bytes {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("newer uncommitted SQL rows never reached the sink");
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    if leader_mode() {
        assert!(
            !storage
                .exists(
                    paths(&fixture)
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(2))
                        .to_string()
                )
                .await
                .unwrap()
        );
    } else {
        assert!(
            !storage
                .exists(format!(
                    "{}/checkpoints/checkpoint-0000002/metadata",
                    fixture.job_id
                ))
                .await
                .unwrap()
        );
    }
    abort_and_wait(&running, &fixture.program.graph, &mut rx).await;
    drop(running);
    tokio::fs::remove_file(blocker).await.unwrap();
    restore_and_check(&fixture, 1).await;
    println!(
        "UPLOAD_FAILURE_RECOVERY selected_epoch=1 output_records={} mode={}",
        fixture.expected_records,
        if leader_mode() {
            "leader"
        } else {
            "controller"
        }
    );
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}

#[test_log(tokio::test)]
#[ignore = "dedicated process: STREAMR_TEST_BACKEND=rocksdb; select controller or leader"]
async fn milestone2_retained_checkpoint_survives_runtime_cleanup_and_worker_recreation() {
    let fixture = fixture("retained-recovery").await;
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        None,
        tx,
    )
    .await;
    let running = Engine::for_local(program, "pipe-test".into(), (*fixture.job_id).clone())
        .await
        .unwrap()
        .start()
        .await;
    for epoch in [1, 2] {
        advance(&running, 40).await;
        checkpoint(
            &mut SmokeTestContext {
                job_id: fixture.job_id.clone(),
                engine: &running,
                control_rx: &mut rx,
                program: Arc::new(fixture.program.clone()),
            },
            epoch,
        )
        .await;
    }
    let first_disk_file_dir = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        "__sp_shared",
        0,
        1,
        false,
    );
    let retained_disk_file_dir = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        "__sp_shared",
        0,
        2,
        false,
    );
    let first_dir = fixture.root.join("checkpoints").join(first_disk_file_dir);
    let retained_dir = fixture
        .root
        .join("checkpoints")
        .join(retained_disk_file_dir);
    assert!(
        tokio::fs::read_dir(&first_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        tokio::fs::read_dir(&retained_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    if leader_mode() {
        let protocol_paths = paths(&fixture);
        let head = protocol_paths
            .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(2));
        for _ in 0..2 {
            arroyo_state_protocol::gc::cleanup_leader_checkpoints(
                storage.as_ref(),
                &protocol_paths,
                head.clone(),
                Epoch(2),
            )
            .await
            .unwrap();
        }
        assert!(
            !storage
                .exists(
                    protocol_paths
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(1))
                        .to_string()
                )
                .await
                .unwrap()
        );
    } else {
        for _ in 0..2 {
            for operator in tasks_per_operator(&fixture.program.graph).keys() {
                ParquetBackend::cleanup_operator(
                    &StorageProviderFor::Worker,
                    (*fixture.job_id).clone(),
                    operator.clone(),
                    1,
                    2,
                )
                .await
                .unwrap();
            }
        }
    }
    assert!(match tokio::fs::read_dir(&first_dir).await {
        Ok(mut files) => files.next_entry().await.unwrap().is_none(),
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    });
    assert!(
        tokio::fs::read_dir(&retained_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    abort_and_wait(&running, &fixture.program.graph, &mut rx).await;
    drop(running);
    restore_and_check(&fixture, 2).await;
    println!(
        "RETAINED_CLEANUP_RECOVERY selected_epoch=2 removed_epoch=1 retries=2 output_records={} mode={}",
        fixture.expected_records,
        if leader_mode() {
            "leader"
        } else {
            "controller"
        }
    );
    tokio::fs::remove_dir_all(fixture.root).await.unwrap();
}
