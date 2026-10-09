//! Opt-in multi-epoch conformance; business oracles live outside the engine.
use super::*;

async fn stop_attempt(
    running: RunningEngine,
    rx: &mut Receiver<ControlResp>,
    mut stopped: HashSet<(u32, u32)>,
) {
    let count: usize = running.operator_controls().values().map(Vec::len).sum();
    running.abort_workers();
    tokio::time::timeout(test_runtime_timeout(), async {
        while stopped.len() < count {
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
        assert_eq!(stopped.len(), count, "lifecycle workers did not terminate");
    })
    .await
    .expect("lifecycle cancellation timed out");
}

#[test_log(tokio::test)]
#[ignore = "opt-in native state-table three-epoch lifecycle matrix"]
async fn native_state_table_multi_epoch_capture() {
    tokio::time::timeout(test_runtime_timeout() * 12, lifecycle())
        .await
        .expect("multi-epoch lifecycle timed out");
}

async fn lifecycle() {
    configure_test_worker();
    let backend = env::var("STREAMR_TEST_BACKEND").unwrap();
    assert!(matches!(backend.as_str(), "memory" | "rocksdb"));
    assert_eq!(
        matches!(
            config::config().worker.sql_state_backend,
            arroyo_rpc::config::SqlStateBackend::Rocksdb
        ),
        backend == "rocksdb"
    );
    assert!(matches!(
        env::var("STREAMR_TEST_CHECKPOINT_MODE").unwrap().as_str(),
        "controller" | "leader"
    ));
    let query_path = PathBuf::from(env::var("STREAMR_CAPTURE_QUERY").unwrap());
    let directory = query_path.parent().unwrap();
    let query = read_to_string(&query_path).await.unwrap();
    let udfs = get_udfs();
    let logical = Arc::new(get_graph(query, &udfs).await.unwrap());
    assert!(logical.graph.node_weights().all(|n| n.parallelism == 1));
    let prefixes: Vec<i32> = env::var("STREAMR_CAPTURE_EPOCH_PREFIXES")
        .unwrap()
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect();
    assert_eq!(prefixes.len(), 3);
    assert!(prefixes[0] > 0 && prefixes.windows(2).all(|p| p[0] < p[1]));
    let restore_job = env::var("STREAMR_LIFECYCLE_RESTORE_JOB").ok();
    let job_id = restore_job.clone().unwrap_or_else(|| {
        format!(
            "state-table-lifecycle-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    });
    let sinks: Vec<PathBuf> = logical
        .graph
        .node_weights()
        .flat_map(|n| n.operator_chain.iter())
        .filter(|(op, _)| op.operator_name == OperatorName::ConnectorSink)
        .map(|(op, _)| {
            let op: arroyo_rpc::grpc::api::ConnectorOp =
                prost::Message::decode(op.operator_config.as_slice()).unwrap();
            assert_eq!(op.connector, "single_file");
            let config: arroyo_rpc::OperatorConfig = serde_json::from_str(&op.config).unwrap();
            PathBuf::from(config.table["path"].as_str().unwrap())
        })
        .collect();
    assert_eq!(
        sinks.len(),
        3,
        "requires output, independent mirror, and intermediate fanout"
    );
    let mut manifests = Vec::new();
    if restore_job.is_none() {
        for path in &sinks {
            if path.exists() {
                tokio::fs::remove_file(path).await.unwrap();
            }
        }
        for epoch in 1..=3u32 {
            let (tx, mut rx) = channel(128);
            let program = local_program_selected(
                &job_id,
                &logical.graph,
                &udfs,
                (epoch > 1).then_some(u64::from(epoch - 1)),
                tx,
                false,
            )
            .await;
            let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
                .await
                .unwrap()
                .start()
                .await;
            assert_eq!(running.source_controls().len(), 1);
            let previous = if epoch == 1 {
                0
            } else {
                prefixes[epoch as usize - 2]
            };
            advance(&running, prefixes[epoch as usize - 1] - previous - 1).await;
            let mut stopped = HashSet::new();
            checkpoint_with_stop(
                &mut SmokeTestContext {
                    job_id: Arc::new(job_id.clone()),
                    engine: &running,
                    control_rx: &mut rx,
                    program: logical.clone(),
                },
                epoch,
                true,
                &mut stopped,
            )
            .await;
            stop_attempt(running, &mut rx, stopped).await;
            for sink in &sinks {
                tokio::fs::copy(
                    sink,
                    sink.with_extension(format!("checkpoint-{epoch}.jsonl")),
                )
                .await
                .unwrap();
            }
            let reference = if leader_mode() {
                let paths = arroyo_state_protocol::ProtocolPaths::new(
                    arroyo_types::PipelineId::new("pipe-test"),
                    arroyo_types::JobId::new(job_id.clone()),
                );
                let reference = paths.checkpoint_manifest(
                    arroyo_state_protocol::types::Generation(0),
                    Epoch(u64::from(epoch)),
                );
                let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
                    .await
                    .unwrap();
                let metadata: arroyo_rpc::grpc::rpc::CheckpointManifest =
                    arroyo_state_protocol::store::read_protobuf(storage.as_ref(), &reference)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(metadata.epoch, u64::from(epoch));
                assert_eq!(metadata.job_id, job_id);
                assert_typed_epoch(&metadata.operators, epoch);
                reference.to_string()
            } else {
                let metadata = StateBackend::load_checkpoint_metadata(
                    &StorageProviderFor::Worker,
                    &job_id,
                    epoch,
                )
                .await
                .unwrap();
                assert_eq!(metadata.epoch, epoch);
                assert_eq!(metadata.job_id, job_id);
                let mut operators = Vec::new();
                for operator in &metadata.operator_ids {
                    operators.push(
                        StateBackend::load_operator_metadata(
                            &StorageProviderFor::Worker,
                            &job_id,
                            operator,
                            epoch,
                        )
                        .await
                        .unwrap()
                        .unwrap(),
                    );
                }
                assert_typed_epoch(&operators, epoch);
                format!("{job_id}/checkpoints/checkpoint-{epoch:07}/metadata")
            };
            let artifacts: Vec<_> = sinks.iter().map(|sink| {
                let stem = sink.file_stem().unwrap().to_str().unwrap();
                serde_json::json!({
                    "sink": sink,
                    "checkpoint_path": sink.with_extension(format!("checkpoint-{epoch}.jsonl")),
                    "native_path": directory.join(format!("{stem}.epoch-{epoch}.native.jsonl")),
                    "switched_path": directory.join(format!("{stem}.epoch-{epoch}.switched.jsonl"))
                })
            }).collect();
            manifests.push(serde_json::json!({"committed_epoch": epoch, "input_prefix": prefixes[epoch as usize - 1], "checkpoint_ref": reference, "job_id": job_id, "artifacts": artifacts}));
        }
        // Retry bounded cleanup while retaining every selected commit. No minimum
        // greater than one is admissible while epochs one and two remain selected.
        for _ in 0..2 {
            if leader_mode() {
                let paths = arroyo_state_protocol::ProtocolPaths::new(
                    arroyo_types::PipelineId::new("pipe-test"),
                    arroyo_types::JobId::new(job_id.clone()),
                );
                let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
                    .await
                    .unwrap();
                arroyo_state_protocol::gc::cleanup_leader_checkpoints(
                    storage.as_ref(),
                    &paths,
                    paths
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(3)),
                    Epoch(1),
                )
                .await
                .unwrap();
            } else {
                for node in logical.graph.node_weights() {
                    for (op, _) in node.operator_chain.iter() {
                        ParquetBackend::cleanup_operator(
                            &StorageProviderFor::Worker,
                            job_id.clone(),
                            op.operator_id.clone(),
                            1,
                            1,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
        }
    }
    for epoch in 1..=3u32 {
        for sink in &sinks {
            tokio::fs::copy(
                sink.with_extension(format!("checkpoint-{epoch}.jsonl")),
                sink,
            )
            .await
            .unwrap();
        }
        let (tx, mut rx) = channel(128);
        let program = local_program_selected(
            &job_id,
            &logical.graph,
            &udfs,
            Some(u64::from(epoch)),
            tx,
            true,
        )
        .await;
        let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
            .await
            .unwrap()
            .start()
            .await;
        run_until_finished(&running, &mut rx).await;
        for sink in &sinks {
            tokio::fs::copy(sink, sink.with_extension(format!("epoch-{epoch}.jsonl")))
                .await
                .unwrap();
        }
    }
    if restore_job.is_none() {
        // No fourth checkpoint was published; restoration must not manufacture one.
        if leader_mode() {
            let paths = arroyo_state_protocol::ProtocolPaths::new(
                arroyo_types::PipelineId::new("pipe-test"),
                arroyo_types::JobId::new(job_id.clone()),
            );
            let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
                .await
                .unwrap();
            let missing: Option<arroyo_rpc::grpc::rpc::CheckpointManifest> =
                arroyo_state_protocol::store::read_protobuf(
                    storage.as_ref(),
                    &paths
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(4)),
                )
                .await
                .unwrap();
            assert!(
                missing.is_none(),
                "uncommitted epoch must remain unavailable"
            );
            assert_selection_rejected(
                &job_id,
                logical.clone(),
                4,
                "selected checkpoint was not published",
            )
            .await;
            let head =
                paths.checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(3));
            let mut abandoned: arroyo_rpc::grpc::rpc::CheckpointManifest =
                arroyo_state_protocol::store::read_protobuf(storage.as_ref(), &head)
                    .await
                    .unwrap()
                    .unwrap();
            abandoned.epoch = 4;
            abandoned.needs_commit = true;
            arroyo_state_protocol::store::put_protobuf(
                storage.as_ref(),
                &paths.checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(4)),
                &abandoned,
            )
            .await
            .unwrap();
            assert_selection_rejected(
                &job_id,
                logical.clone(),
                4,
                "selected checkpoint requires commit",
            )
            .await;
            abandoned.epoch = 5;
            abandoned.needs_commit = false;
            arroyo_state_protocol::store::put_protobuf(
                storage.as_ref(),
                &paths.checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(5)),
                &abandoned,
            )
            .await
            .unwrap();
            assert_selection_rejected(
                &job_id,
                logical.clone(),
                5,
                "selected epoch is not in committed history",
            )
            .await;
            let generation: arroyo_state_protocol::types::GenerationManifest =
                arroyo_state_protocol::store::read_json(
                    storage.as_ref(),
                    &paths.generation_manifest(arroyo_state_protocol::types::Generation(0)),
                )
                .await
                .unwrap()
                .unwrap();
            let resolved = arroyo_state_protocol::workflow::resolve_generation_manifest(
                storage.as_ref(),
                &generation,
                arroyo_state_protocol::types::Generation(0),
            )
            .await
            .unwrap();
            assert!(
                matches!(resolved, arroyo_state_protocol::workflow::GenerationResolution::Ready { checkpoint_ref } if checkpoint_ref == head),
                "abandoned unpublished epoch must not replace the committed head"
            );
        } else {
            assert_selection_rejected(&job_id, logical.clone(), 4, "could not load").await;
            // Stage one operator candidate without the job commit metadata,
            // matching an interrupted controller publication.
            let committed =
                StateBackend::load_checkpoint_metadata(&StorageProviderFor::Worker, &job_id, 3)
                    .await
                    .unwrap();
            let mut candidate = StateBackend::load_operator_metadata(
                &StorageProviderFor::Worker,
                &job_id,
                &committed.operator_ids[0],
                3,
            )
            .await
            .unwrap()
            .unwrap();
            candidate.operator_metadata.as_mut().unwrap().epoch = 4;
            StateBackend::write_operator_checkpoint_metadata(
                &StorageProviderFor::Worker,
                candidate,
            )
            .await
            .unwrap();
            assert_selection_rejected(&job_id, logical.clone(), 4, "could not load").await;
            assert!(
                StateBackend::load_checkpoint_metadata(&StorageProviderFor::Worker, &job_id, 4)
                    .await
                    .is_err(),
                "uncommitted epoch must remain unavailable"
            );
        }
    }
    if restore_job.is_none() {
        tokio::fs::write(
            directory.join("committed-manifests.json"),
            serde_json::to_vec_pretty(&manifests).unwrap(),
        )
        .await
        .unwrap();
    }
}

fn assert_typed_epoch(operators: &[arroyo_rpc::grpc::rpc::OperatorCheckpointMetadata], epoch: u32) {
    use arroyo_rpc::grpc::rpc::{
        TableEnum, TypedStateTableConfig, TypedStateTableTaskCheckpointMetadata,
    };
    use prost::Message;
    let mut tables = 0;
    for operator in operators {
        for (name, config) in &operator.table_configs {
            if config.table_type() != TableEnum::TypedStateTable {
                continue;
            }
            let config = TypedStateTableConfig::decode(config.config.as_slice()).unwrap();
            let metadata = TypedStateTableTaskCheckpointMetadata::decode(
                operator.table_checkpoint_metadata[name].data.as_slice(),
            )
            .unwrap();
            arroyo_state_protocol::typed_checkpoint::validate_table(&config, &metadata).unwrap();
            let subtask = &metadata.subtasks[&0];
            assert_eq!(subtask.epoch, epoch);
            assert_eq!(
                subtask.empty,
                epoch == 3,
                "explicit empty marker required for both tables"
            );
            assert_eq!(subtask.files.is_empty(), epoch == 3);
            let mut incompatible = config.clone();
            incompatible.schema_identity.push(0);
            let error =
                arroyo_state_protocol::typed_checkpoint::validate_table(&incompatible, &metadata)
                    .unwrap_err();
            assert!(error.contains("descriptor"));
            let mut foreign = metadata.clone();
            foreign.subtasks.get_mut(&0).unwrap().namespace.push(0);
            let error = arroyo_state_protocol::typed_checkpoint::validate_table(&config, &foreign)
                .unwrap_err();
            assert!(error.contains("ownership"));
            let mut unsupported = metadata.clone();
            unsupported.format_version = u32::MAX;
            let error =
                arroyo_state_protocol::typed_checkpoint::validate_table(&config, &unsupported)
                    .unwrap_err();
            assert!(error.contains("singleton checkpoint"));
            tables += 1;
        }
    }
    assert_eq!(
        tables, 2,
        "both typed tables must be explicitly checkpointed"
    );
}

// Exercise selection only: rejected metadata must never start workers.
async fn assert_selection_rejected(
    job_id: &str,
    logical: Arc<LogicalProgram>,
    epoch: u64,
    expected: &str,
) {
    let job_id = job_id.to_owned();
    let rejected = tokio::spawn(async move {
        let (tx, _rx) = channel(128);
        let udfs = get_udfs();
        local_program_selected(&job_id, &logical.graph, &udfs, Some(epoch), tx, true).await
    })
    .await;
    let failure = match rejected {
        Err(error) if error.is_panic() => error.into_panic(),
        _ => panic!("unpublished checkpoint selection did not reject epoch {epoch}"),
    };
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(expected),
        "unexpected checkpoint rejection: {message}"
    );
}
