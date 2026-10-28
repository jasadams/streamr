//! Real native SQL checkpoint fixtures. Inputs and literal oracles are caller owned.
//! Run each invocation alone: worker configuration and checkpoint storage are global.
use super::*;
use std::io::{Read, Seek};
use std::time::Instant;

/// Test input pacing installed through Program's existing physical source slot.
/// SQL operators, queues, source offsets and publication use the real engine.
/// Production single_file advances after any control, including a checkpoint;
/// that behavior cannot represent repeated checkpoints over a truly idle input.
struct FixtureControlledSource {
    input_path: String,
    rows: usize,
}

#[async_trait::async_trait]
impl arroyo_operator::operator::SourceOperator for FixtureControlledSource {
    fn name(&self) -> String {
        "NativeCheckpointFixtureSource".into()
    }

    fn tables(&self) -> HashMap<String, arroyo_rpc::grpc::rpc::TableConfig> {
        arroyo_state::global_table_config("f", "file_source")
    }

    async fn run(
        &mut self,
        ctx: &mut arroyo_operator::context::SourceContext,
        collector: &mut arroyo_operator::context::SourceCollector,
    ) -> arroyo_rpc::errors::DataflowResult<arroyo_operator::SourceFinishType> {
        use arroyo_operator::SourceFinishType;
        self.rows = ctx
            .table_manager
            .get_global_keyed_state::<String, usize>("f")
            .await?
            .get(&self.input_path)
            .copied()
            .unwrap_or_default();
        collector.initialize_deserializer(
            arroyo_rpc::formats::Format::Json(Default::default()),
            None,
            None,
            &[],
        );
        let mut lines = BufReader::new(
            File::open(&self.input_path)
                .await
                .map_err(anyhow::Error::from)?,
        )
        .lines();
        let mut skipped = 0usize;
        let mut first = true;
        while let Some(line) = lines.next_line().await.map_err(anyhow::Error::from)? {
            if skipped < self.rows {
                skipped += 1;
                continue;
            }
            if !first {
                loop {
                    match ctx.control_rx.recv().await {
                        Some(ControlMessage::NoOp) => break,
                        Some(ControlMessage::Checkpoint(barrier)) => {
                            collector.flush_buffer().await?;
                            ctx.table_manager
                                .get_global_keyed_state::<String, usize>("f")
                                .await?
                                .insert(self.input_path.clone(), self.rows)
                                .await;
                            if self.start_checkpoint(barrier, ctx, collector).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                        Some(ControlMessage::Stop {
                            mode: StopMode::Graceful,
                        }) => {
                            return Ok(SourceFinishType::Graceful);
                        }
                        Some(ControlMessage::Stop { .. }) | None => {
                            return Ok(SourceFinishType::Immediate);
                        }
                        Some(_) => {}
                    }
                }
            }
            first = false;
            collector
                .deserialize_slice(line.as_bytes(), SystemTime::now(), None)
                .await?;
            if collector.should_flush() {
                collector.flush_buffer().await?;
            }
            self.rows += 1;
            skipped += 1;
        }
        collector.flush_buffer().await?;
        Ok(SourceFinishType::Final)
    }
}

fn install_fixture_source(program: &Program) {
    use arroyo_operator::operator::OperatorNode;
    use arroyo_worker::engine::SubtaskOrQueueNode;
    let input = fixture_path("STREAMR_NATIVE_INPUT_PATH");
    let mut replaced = 0;
    for node in program.graph.write().unwrap().node_weights_mut() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Source(source) = &mut node.node
        {
            assert_eq!(source.operator.name(), "SingleFileSource");
            source.operator = Box::new(FixtureControlledSource {
                input_path: input.to_str().unwrap().into(),
                rows: 0,
            });
            replaced += 1;
        }
    }
    assert_eq!(
        replaced, 1,
        "fixture requires exactly one ordinary file input"
    );
    println!("NATIVE_FIXTURE_INPUT explicit_noop_pacing=true checkpoint_advances_input=false");
}

#[derive(Default)]
struct OutputProgress {
    offset: u64,
    rows: usize,
    partial: Vec<u8>,
}

impl OutputProgress {
    fn poll(&mut self, path: &Path) {
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => panic!("cannot observe native fixture output: {error}"),
        };
        assert!(file.metadata().unwrap().len() >= self.offset, "sink shrank");
        file.seek(std::io::SeekFrom::Start(self.offset)).unwrap();
        let mut bytes = [0u8; 65536];
        loop {
            let count = file.read(&mut bytes).unwrap();
            if count == 0 {
                break;
            }
            self.offset += count as u64;
            for byte in &bytes[..count] {
                if *byte == b'\n' {
                    let value: Value = serde_json::from_slice(&self.partial)
                        .expect("native fixture emitted invalid JSON");
                    assert!(value.is_object());
                    self.rows += 1;
                    self.partial.clear();
                } else {
                    self.partial.push(*byte);
                    assert!(self.partial.len() <= 65536, "fixture output row too large");
                }
            }
        }
    }
}

fn fixture_path(name: &str) -> PathBuf {
    let path = PathBuf::from(env::var(name).unwrap_or_else(|_| panic!("missing {name}")));
    assert!(path.is_absolute(), "{name} must be absolute");
    path
}

fn fixture_rows(name: &str) -> Vec<usize> {
    env::var(name)
        .unwrap()
        .split(',')
        .map(|item| item.parse::<usize>().unwrap())
        .collect()
}

async fn observe_until(
    output: &Path,
    progress: &mut OutputProgress,
    expected: usize,
    rx: &mut Receiver<ControlResp>,
    deadline: Instant,
) -> bool {
    loop {
        progress.poll(output);
        assert!(
            progress.rows <= expected,
            "unexpected native fixture output rows"
        );
        if progress.rows == expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            response = rx.recv() => check_idle_response(response),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}

async fn stop_fixture(running: RunningEngine, rx: &mut Receiver<ControlResp>) {
    let expected: usize = running.operator_controls().values().map(Vec::len).sum();
    running.abort_workers();
    tokio::time::timeout(test_runtime_timeout(), async {
        let mut stopped = HashSet::new();
        while stopped.len() < expected {
            match rx.recv().await {
                Some(ControlResp::TaskFinished {
                    task_id,
                    subtask_idx,
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
        assert_eq!(stopped.len(), expected, "fixture workers did not terminate");
    })
    .await
    .expect("native fixture shutdown timed out");
    drop(running);
    wait_for_capture_state_cleanup().await;
}

async fn graph(query: &Path) -> Arc<LogicalProgram> {
    let logical = Arc::new(
        get_graph(read_to_string(query).await.unwrap(), &get_udfs())
            .await
            .unwrap(),
    );
    assert!(
        logical
            .graph
            .node_weights()
            .all(|node| node.parallelism == 1)
    );
    logical
}

/// Captures repeated/changed/deleted/empty epochs, or restores one selected epoch
/// in this new OS process and continues the same native SQL plan to EOF.
#[test_log(tokio::test)]
#[ignore = "caller supplied native incremental checkpoint lifecycle fixture"]
async fn native_incremental_checkpoint_capture() {
    tokio::time::timeout(test_runtime_timeout(), lifecycle())
        .await
        .expect("native checkpoint fixture exceeded its finite deadline");
}

async fn lifecycle() {
    configure_test_worker();
    let query = fixture_path("STREAMR_CAPTURE_QUERY");
    let output = fixture_path("STREAMR_CAPTURE_OUTPUT");
    let root = query.parent().unwrap();
    let logical = graph(&query).await;
    let job_id = env::var("STREAMR_NATIVE_JOB").unwrap();
    let restore = env::var("STREAMR_NATIVE_RESTORE_EPOCH")
        .ok()
        .map(|value| value.parse::<u32>().unwrap());
    let (tx, mut rx) = channel(128);
    let program = local_program_selected(
        &job_id,
        &logical.graph,
        &get_udfs(),
        restore.map(u64::from),
        tx,
        restore.is_some(),
    )
    .await;
    let source_reader = CaptureSourceReader::new(
        &program,
        fixture_path("STREAMR_NATIVE_INPUT_PATH").to_str().unwrap(),
    );
    install_fixture_source(&program);
    let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    if let Some(epoch) = restore {
        drop(source_reader); // Do not retain a control sender while awaiting EOF.
        run_until_finished(&running, &mut rx).await;
        wait_for_capture_state_cleanup().await;
        println!("NATIVE_FIXTURE_RESTORED epoch={epoch} job={job_id}");
        return;
    }
    assert_eq!(running.source_controls().len(), 1);
    let prefixes = fixture_rows("STREAMR_NATIVE_EPOCH_PREFIXES");
    assert_eq!(prefixes.len(), 6);
    assert!(prefixes[0] > 0 && prefixes.windows(2).all(|pair| pair[0] <= pair[1]));
    let mut released = 1usize; // single_file's established automatic first row
    let mut progress = OutputProgress::default();
    let mut completed = None;
    for (index, target) in prefixes.into_iter().enumerate() {
        advance(&running, i32::try_from(target - released).unwrap()).await;
        released = target;
        assert!(
            observe_until(
                &output,
                &mut progress,
                target,
                &mut rx,
                Instant::now() + test_runtime_timeout()
            )
            .await,
            "native fixture input did not complete"
        );
        let epoch = u32::try_from(index + 1).unwrap();
        checkpoint_with_completed_base(
            &mut SmokeTestContext {
                job_id: Arc::new(job_id.clone()),
                engine: &running,
                control_rx: &mut rx,
                program: logical.clone(),
            },
            epoch,
            false,
            &mut HashSet::new(),
            completed,
        )
        .await;
        completed = Some(epoch); // helper returned only after authoritative publication
        source_reader
            .assert_saved_offset(
                &job_id,
                epoch,
                i32::try_from(target).unwrap(),
                "native_lifecycle",
            )
            .await;
        tokio::fs::copy(
            &output,
            root.join(format!("output.checkpoint-{epoch}.jsonl")),
        )
        .await
        .unwrap();
        println!("NATIVE_FIXTURE_PUBLISHED epoch={epoch} rows={target} job={job_id}");
    }
    stop_fixture(running, &mut rx).await;
}

/// Measurement-only real engine driver, shared unchanged by baseline/candidate.
/// The caller owns controls, fixture/oracle freezing, process samples and ABBA.
#[test_log(tokio::test)]
#[ignore = "finite native checkpoint performance arm"]
async fn native_checkpoint_performance_arm() {
    tokio::time::timeout(Duration::from_secs(174), performance_arm())
        .await
        .expect("native checkpoint arm exceeded its outer deadline");
}

async fn performance_arm() {
    configure_test_worker();
    let resources = arroyo_state::live::worker::configured_worker_resources()
        .unwrap()
        .unwrap();
    let config = resources.config();
    assert_eq!(
        config.block_cache_bytes
            + config.memtable_bytes
            + config.queued_write_bytes
            + config.decoded_value_bytes
            + config.scan_page_bytes,
        32 * 1024 * 1024,
        "measurement backend pools must remain 32MiB"
    );
    let query = fixture_path("STREAMR_CAPTURE_QUERY");
    let output = fixture_path("STREAMR_CAPTURE_OUTPUT");
    let root = query.parent().unwrap();
    let targets = fixture_rows("STREAMR_NATIVE_PHASE_TARGETS");
    assert_eq!(targets.len(), 3);
    assert_eq!(targets[0], 49152);
    assert!(targets.windows(2).all(|pair| pair[0] < pair[1]));
    let cadence_ms: u64 = env::var("STREAMR_NATIVE_CHECKPOINT_MS")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1000..=5000).contains(&cadence_ms));
    let logical = graph(&query).await;
    let job_id = env::var("STREAMR_NATIVE_JOB").unwrap();
    let (tx, mut rx) = channel(128);
    let program =
        local_program_selected(&job_id, &logical.graph, &get_udfs(), None, tx, false).await;
    let source_reader = CaptureSourceReader::new(
        &program,
        fixture_path("STREAMR_NATIVE_INPUT_PATH").to_str().unwrap(),
    );
    install_fixture_source(&program);
    let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    let started = Instant::now();
    println!(
        "NATIVE_PERF_START unix_micros={}",
        to_micros(SystemTime::now())
    );
    let deadline = started + Duration::from_secs(144);
    let released = Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let feed = Arc::new(tokio::sync::Mutex::new(()));
    let sources = running.source_controls();
    assert_eq!(sources.len(), 1);
    let source = sources[0].clone();
    let mut progress = OutputProgress::default();
    let mut checkpoints = Vec::new();
    let mut phase_first_complete_seconds = [None; 3];
    {
        // These futures are polled concurrently in this scope. DATA admission
        // continues throughout asynchronous export; only barrier enqueue holds
        // the same short feed gate as each finite producer control batch.
        let producer = async {
            loop {
                let elapsed = started.elapsed().as_secs_f64();
                let phase = if elapsed < 88.0 {
                    0
                } else if elapsed < 116.0 {
                    1
                } else {
                    2
                };
                let target = targets[phase];
                {
                    let _gate = feed.lock().await;
                    let count = target
                        .saturating_sub(released.load(std::sync::atomic::Ordering::Acquire))
                        .min(32);
                    for _ in 0..count {
                        source
                            .send(ControlMessage::NoOp)
                            .await
                            .expect("native fixture source ended early");
                        released.fetch_add(1, std::sync::atomic::Ordering::Release);
                    }
                }
                if released.load(std::sync::atomic::Ordering::Acquire) >= target {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        };
        let checkpoint_driver = async {
            let mut epoch = 0u32;
            let mut completed = None;
            let mut next_checkpoint = started + Duration::from_millis(cadence_ms);
            loop {
                tokio::select! {
                    response = rx.recv() => check_idle_response(response),
                    () = tokio::time::sleep(next_checkpoint.saturating_duration_since(Instant::now())) => {}
                }
                if Instant::now() < next_checkpoint {
                    continue;
                }
                epoch += 1;
                let before = started.elapsed().as_secs_f64();
                let (frontier, bytes) = checkpoint_with_completed_base_and_feed(
                    &mut SmokeTestContext {
                        job_id: Arc::new(job_id.clone()),
                        engine: &running,
                        control_rx: &mut rx,
                        program: logical.clone(),
                    },
                    epoch,
                    false,
                    &mut HashSet::new(),
                    completed,
                    &feed,
                    &released,
                )
                .await;
                let published_seconds = started.elapsed().as_secs_f64();
                completed = Some(epoch);
                source_reader
                    .assert_saved_offset(
                        &job_id,
                        epoch,
                        i32::try_from(frontier).unwrap(),
                        "native_performance",
                    )
                    .await;
                checkpoints.push(serde_json::json!({"epoch":epoch,"started_seconds":before,
                    "published_seconds":published_seconds,"reported_bytes":bytes,
                    "released_rows":frontier}));
                next_checkpoint = Instant::now() + Duration::from_millis(cadence_ms);
            }
        };
        let monitor = async {
            loop {
                progress.poll(&output);
                let elapsed = started.elapsed().as_secs_f64();
                assert!(
                    progress.rows <= targets[2],
                    "unexpected native performance output"
                );
                for (index, target) in targets.iter().enumerate() {
                    if progress.rows >= *target && phase_first_complete_seconds[index].is_none() {
                        phase_first_complete_seconds[index] = Some(elapsed);
                    }
                }
                if elapsed >= 60.0
                    && !phase_first_complete_seconds[0].is_some_and(|seconds| seconds <= 60.0)
                {
                    return;
                }
                if Instant::now() >= deadline {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            () = monitor => {},
            _ = producer => unreachable!("producer must remain live until cancelled"),
            _ = checkpoint_driver => unreachable!("checkpoint driver must remain live until cancelled"),
        }
    }
    progress.poll(&output);
    let processing_seconds = started.elapsed().as_secs_f64();
    let phase_complete = std::array::from_fn::<_, 3, _>(|index| {
        phase_first_complete_seconds[index]
            .is_some_and(|seconds| seconds <= [60.0, 116.0, 144.0][index])
    });
    let seed_complete = phase_complete[0];
    // Freeze the completed prefix before aborting: teardown may append more
    // records, which must never inflate the bounded processing denominator.
    let prefix_path = root.join("output.processing.jsonl");
    tokio::fs::copy(&output, &prefix_path).await.unwrap();
    let prefix = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&prefix_path)
        .await
        .unwrap();
    prefix
        .set_len(progress.offset - progress.partial.len() as u64)
        .await
        .unwrap();
    let receipt = serde_json::json!({"seed_complete":seed_complete,
        "phase_complete":phase_complete,"processing_seconds":processing_seconds,
        "completed_rows":progress.rows,"released_rows":released.load(std::sync::atomic::Ordering::Acquire),
        "phase_first_complete_seconds":phase_first_complete_seconds,
        "completed_output_bytes":progress.offset - progress.partial.len() as u64,
        "checkpoints":checkpoints,"job_id":job_id,
        "phase_boundaries_seconds":[0,60,88,116,144]});
    tokio::fs::write(
        root.join("arm.json"),
        serde_json::to_vec_pretty(&receipt).unwrap(),
    )
    .await
    .unwrap();
    println!(
        "NATIVE_PERF_STOP processing_seconds={processing_seconds:.6} seed_complete={seed_complete}"
    );
    stop_fixture(running, &mut rx).await;
}
