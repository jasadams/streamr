use anyhow::Result;
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, LogicalProgram, OperatorName,
    ProgramConfig,
};
use arroyo_planner::{ArroyoSchemaProvider, SqlConfig, parse_and_get_arrow_program};
use arroyo_state::parquet::ParquetBackend;
use petgraph::algo::has_path_connecting;
use petgraph::visit::EdgeRef;
use rstest::rstest;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, time::SystemTime};
use tokio::sync::mpsc::{Receiver, channel};

use crate::udfs::get_udfs;
use arroyo_rpc::config;
use arroyo_rpc::grpc::rpc::{
    StopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq, WorkerContext,
};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
use arroyo_state_protocol::types::Epoch;
use arroyo_types::{CheckpointBarrier, to_micros};
use arroyo_udf_host::LocalUdf;
use arroyo_worker::engine::Engine;
use arroyo_worker::engine::{Program, RunningEngine};
use arroyo_worker::job_controller::checkpoint_state::CheckpointState;
use petgraph::{Direction, Graph};
use serde_json::Value;
use test_log::test as test_log;
use tokio::fs::read_to_string;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::info;

#[test_log(rstest)]
fn for_each_file(#[files("src/test/queries/*.sql")] path: PathBuf) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run_smoketest(&path).await;
        });
}

async fn run_smoketest(path: &Path) {
    configure_test_worker();

    // read text at path
    let test_name = path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .split('.')
        .next()
        .unwrap();
    let query = read_to_string(path).await.unwrap();
    let checkpoint_interval = query
        .lines()
        .find_map(|line| line.strip_prefix("--checkpoint-interval="))
        .map(|value| value.trim().parse::<i32>().unwrap())
        .unwrap_or(20);
    assert!(checkpoint_interval >= 0);
    let fail = query.starts_with("--fail");
    let error_message = query.starts_with("--fail=").then(|| {
        query
            .lines()
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1
            .trim()
    });

    let pk = query.starts_with("--pk=").then(|| {
        query
            .lines()
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1
            .trim()
            .split(',')
            .collect::<Vec<_>>()
    });

    match (
        correctness_run_codegen(test_name, query.clone(), pk.as_deref(), checkpoint_interval).await,
        fail,
    ) {
        (Ok(_), false) => {
            // ok
        }
        (Ok(_), true) => {
            panic!("Expected pipeline to fail, but it passed");
        }
        (Err(err), true) => {
            if let Some(error_message) = error_message {
                assert!(
                    err.to_string().contains(error_message),
                    "expected error message '{error_message}' not found; instead got '{err}'"
                );
            }
        }
        (Err(err), false) => {
            panic!("Expected pipeline to pass, but it failed: {err:?}");
        }
    }
}

struct SmokeTestContext<'a> {
    job_id: Arc<String>,
    engine: &'a RunningEngine,
    control_rx: &'a mut Receiver<ControlResp>,
    program: Arc<LogicalProgram>,
}

async fn checkpoint(ctx: &mut SmokeTestContext<'_>, epoch: u32) -> u64 {
    let then_stop =
        epoch == 3 && std::env::var("STREAMR_TEST_CHECKPOINT_STOP").as_deref() == Ok("1");
    checkpoint_with_stop(ctx, epoch, then_stop, &mut HashSet::new()).await
}

/// Retain terminal events consumed while publishing a stopping checkpoint so
/// callers can subsequently wait for the remaining tasks without losing IDs.
async fn checkpoint_with_stop(
    ctx: &mut SmokeTestContext<'_>,
    epoch: u32,
    then_stop: bool,
    finished_tasks: &mut HashSet<(u32, u32)>,
) -> u64 {
    let checkpoint_started = std::time::Instant::now();
    let checkpoint_id = epoch as i64;
    let leader = leader_mode();
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    let paths = arroyo_state_protocol::ProtocolPaths::new(
        arroyo_types::PipelineId(Arc::new("pipe-test".into())),
        arroyo_types::JobId(ctx.job_id.clone()),
    );
    let generation = arroyo_state_protocol::types::Generation(0);
    if leader && epoch == 1 {
        use arroyo_state_protocol::workflow::{
            GenerationInitialization, InitializeGenerationRequest, initialize_generation,
        };
        let initialized = initialize_generation(
            storage.as_ref(),
            InitializeGenerationRequest {
                pipeline_id: paths.pipeline_id().clone(),
                job_id: paths.job_id().clone(),
                generation,
                updated_at: SystemTime::now(),
            },
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            initialized,
            GenerationInitialization::Initialized { .. }
        ));
    }
    let mut operators = Vec::new();
    let mut checkpoint_bytes = 0u64;
    let mut checkpoint_state = CheckpointState::new(
        ctx.job_id.clone(),
        checkpoint_id.to_string(),
        Epoch(epoch as u64),
        Epoch(0),
        ctx.program.clone(),
    );

    // trigger a checkpoint, pass the messages to the CheckpointState

    let barrier = CheckpointBarrier {
        epoch,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop,
    };

    for source in ctx.engine.source_controls() {
        source
            .send(ControlMessage::Checkpoint(barrier))
            .await
            .unwrap();
    }

    while !checkpoint_state.done() {
        let c: ControlResp = tokio::time::timeout(test_runtime_timeout(), ctx.control_rx.recv())
            .await
            .expect("checkpoint timed out")
            .expect("checkpoint workers stopped");

        match c {
            ControlResp::CheckpointEvent(c) => {
                let req = TaskCheckpointEventReq {
                    worker_context: Some(WorkerContext {
                        machine_id: "test".to_string(),
                        worker_id: 1,
                        pipeline_id: "pipe-test".to_string(),
                        job_id: (*ctx.job_id).clone(),
                        generation: 0,
                    }),
                    time: to_micros(c.time),
                    operator_id: c.operator_id,
                    subtask_idx: c.subtask_idx,
                    epoch: c.checkpoint_epoch,
                    event_type: c.event_type as i32,
                };
                checkpoint_state.checkpoint_event(req).unwrap();
            }
            ControlResp::CheckpointCompleted(c) => {
                checkpoint_bytes += c.subtask_metadata.bytes;
                let req = TaskCheckpointCompletedReq {
                    worker_context: Some(WorkerContext {
                        machine_id: "test".to_string(),
                        pipeline_id: "pipe-test".to_string(),
                        worker_id: 1,
                        job_id: (*ctx.job_id).clone(),
                        generation: 0,
                    }),
                    time: c.subtask_metadata.finish_time,
                    operator_id: c.operator_id,
                    epoch: c.checkpoint_epoch,
                    needs_commit: false,
                    metadata: Some(c.subtask_metadata),
                };
                if let Some(operator_metadata) = checkpoint_state.checkpoint_finished(req).unwrap()
                {
                    if leader {
                        operators.push(operator_metadata);
                    } else {
                        StateBackend::write_operator_checkpoint_metadata(
                            &StorageProviderFor::Worker,
                            operator_metadata,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            ControlResp::TaskFailed { error, .. } => panic!("checkpoint worker failed: {error:?}"),
            ControlResp::TaskFinished {
                task_id,
                subtask_idx,
            } => {
                finished_tasks.insert((task_id, subtask_idx));
            }
            _ => {}
        }
    }

    let checkpoint_metadata = checkpoint_state.build_metadata();
    if leader {
        use arroyo_state_protocol::store::read_json;
        use arroyo_state_protocol::types::GenerationManifest;
        use arroyo_state_protocol::workflow::{
            CheckpointPublication, PublishCheckpointRequest, publish_checkpoint,
        };
        let current: GenerationManifest =
            read_json(storage.as_ref(), &paths.generation_manifest(generation))
                .await
                .unwrap()
                .unwrap();
        let manifest = arroyo_rpc::grpc::rpc::CheckpointManifest {
            pipeline_id: "pipe-test".into(),
            job_id: checkpoint_metadata.job_id,
            generation: 0,
            epoch: epoch as u64,
            min_epoch: 0,
            start_time: checkpoint_metadata.start_time,
            finish_time: checkpoint_metadata.finish_time,
            needs_commit: false,
            operators,
            parent_checkpoint_ref: current.candidate_checkpoint_ref().map(ToString::to_string),
        };
        let checkpoint_ref = paths.checkpoint_manifest(generation, Epoch(epoch as u64));
        assert!(matches!(
            publish_checkpoint(
                storage.as_ref(),
                PublishCheckpointRequest {
                    generation_manifest: &current,
                    checkpoint_ref: &checkpoint_ref,
                    checkpoint: &manifest,
                    created_at: SystemTime::now(),
                }
            )
            .await
            .unwrap(),
            CheckpointPublication::Ready { .. }
        ));
    } else {
        StateBackend::write_checkpoint_metadata(&StorageProviderFor::Worker, checkpoint_metadata)
            .await
            .unwrap();
    }

    println!(
        "CHECKPOINT epoch={epoch} bytes={checkpoint_bytes} mode={} elapsed_seconds={:.3} rss_bytes={}",
        if leader { "leader" } else { "controller" },
        checkpoint_started.elapsed().as_secs_f64(),
        process_rss_bytes()
    );
    info!("Smoke test checkpoint completed");
    checkpoint_bytes
}

async fn compact(
    job_id: Arc<String>,
    running_engine: &RunningEngine,
    tasks_per_operator: HashMap<String, usize>,
    epoch: u32,
) {
    let operator_to_node = running_engine.operator_to_node();
    let operator_controls = running_engine.operator_controls();
    for (operator, _) in tasks_per_operator {
        if let Ok(compacted) = ParquetBackend::compact_operator(
            &StorageProviderFor::Worker,
            job_id.clone(),
            &operator,
            epoch,
        )
        .await
        {
            let node_id = operator_to_node.get(&operator).unwrap();
            let operator_controls = operator_controls.get(node_id).unwrap();
            for s in operator_controls {
                s.send(ControlMessage::LoadCompacted {
                    compacted: CompactionResult {
                        operator_id: operator.to_string(),
                        compacted_tables: compacted.clone(),
                    },
                })
                .await
                .unwrap();
            }
        }
    }
}

async fn advance(engine: &RunningEngine, count: i32) {
    // let the engine run for a bit, process some records
    for source in engine.source_controls() {
        for _ in 0..count {
            let _ = source.send(ControlMessage::NoOp).await;
        }
    }
}

fn test_runtime_timeout() -> Duration {
    Duration::from_secs(
        env::var("STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "120".into())
            .parse::<u64>()
            .expect("runtime timeout must be seconds"),
    )
}

async fn run_until_finished(engine: &RunningEngine, control_rx: &mut Receiver<ControlResp>) {
    tokio::time::timeout(test_runtime_timeout(), async {
        loop {
            advance(engine, 10).await;
            match control_rx.try_recv() {
                Ok(ControlResp::TaskFailed { error, .. }) => panic!("worker failed: {error:?}"),
                Ok(_) | Err(TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
    })
    .await
    .expect("worker runtime timed out");
}

fn set_internal_parallelism(graph: &mut Graph<LogicalNode, LogicalEdge>, parallelism: usize) {
    // Stateful SQL maps currently support a singleton operator only. These
    // fixtures still exercise checkpoint recovery, but cannot test rescaling.
    if graph.node_weights().any(|node| {
        node.operator_chain
            .iter()
            .any(|(config, _)| config.operator_name == OperatorName::StatefulProcessor)
    }) {
        return;
    }
    let watermark_nodes: HashSet<_> = graph
        .node_indices()
        .filter(|index| {
            graph
                .node_weight(*index)
                .unwrap()
                .operator_chain
                .iter()
                .any(|(c, _)| c.operator_name == OperatorName::ExpressionWatermark)
        })
        .collect();

    let indices: Vec<_> = graph
        .node_indices()
        .filter(|index| {
            !watermark_nodes.contains(index)
                && graph
                    .node_weight(*index)
                    .unwrap()
                    .operator_chain
                    .iter()
                    .any(|(c, _)| match c.operator_name {
                        OperatorName::ExpressionWatermark
                        | OperatorName::ConnectorSource
                        | OperatorName::ConnectorSink => false,
                        _ => {
                            for watermark_node in watermark_nodes.iter() {
                                if has_path_connecting(&*graph, *watermark_node, *index, None) {
                                    return true;
                                }
                            }
                            false
                        }
                    })
        })
        .collect();

    for node in indices {
        graph.node_weight_mut(node).unwrap().parallelism = parallelism;
    }

    if parallelism > 1 {
        let mut edges_to_make_shuffle = vec![];
        for node in graph.externals(Direction::Outgoing) {
            for edge in graph.edges_directed(node, Direction::Incoming) {
                edges_to_make_shuffle.push(edge.id());
            }
        }
        for node in graph.node_indices() {
            if graph
                .node_weight(node)
                .unwrap()
                .operator_chain
                .iter()
                .any(|(c, _)| c.operator_name == OperatorName::ExpressionWatermark)
            {
                for edge in graph.edges_directed(node, Direction::Outgoing) {
                    edges_to_make_shuffle.push(edge.id());
                }
            }
        }
        for edge in edges_to_make_shuffle {
            graph.edge_weight_mut(edge).unwrap().edge_type = LogicalEdgeType::Shuffle;
        }
    }
}

async fn run_and_checkpoint(
    job_id: Arc<String>,
    program: Program,
    logical_program: Arc<LogicalProgram>,
    tasks_per_operator: HashMap<String, usize>,
    control_rx: &mut Receiver<ControlResp>,
    checkpoint_interval: i32,
    output_location: &str,
) {
    let engine = Engine::for_local(program, "pipe-test".to_string(), job_id.to_string())
        .await
        .unwrap();
    let running_engine = engine.start().await;
    info!("Smoke test checkpointing enabled");

    unsafe {
        env::set_var(
            "ARROYO__CONTROLLER__COMPACTION__CHECKPOINTS_TO_COMPACT",
            "2",
        );
    }

    let ctx = &mut SmokeTestContext {
        job_id: job_id.clone(),
        engine: &running_engine,
        control_rx,
        program: logical_program,
    };

    // trigger a couple checkpoints
    advance(&running_engine, checkpoint_interval).await;
    checkpoint(ctx, 1).await;
    advance(&running_engine, checkpoint_interval).await;
    checkpoint(ctx, 2).await;
    advance(&running_engine, checkpoint_interval).await;

    if !leader_mode() {
        compact(job_id, &running_engine, tasks_per_operator.clone(), 2).await;
    }

    // trigger checkpoint 3, which will include the compacted files
    advance(&running_engine, checkpoint_interval).await;
    let captured_bytes = checkpoint(ctx, 3).await;
    if ctx.job_id.starts_with("milestone2_probe-") {
        assert!(
            captured_bytes >= 10 * 12 * 1024 * 1024,
            "selected checkpoint must exceed ten times assigned state budgets"
        );
        let cleanup_started = std::time::Instant::now();
        for _ in 0..2 {
            if leader_mode() {
                let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
                    .await
                    .unwrap();
                let paths = arroyo_state_protocol::ProtocolPaths::new(
                    arroyo_types::PipelineId::new("pipe-test"),
                    arroyo_types::JobId(ctx.job_id.clone()),
                );
                arroyo_state_protocol::gc::cleanup_leader_checkpoints(
                    storage.as_ref(),
                    &paths,
                    paths
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(3)),
                    Epoch(3),
                )
                .await
                .unwrap();
            } else {
                for operator in tasks_per_operator.keys() {
                    ParquetBackend::cleanup_operator(
                        &StorageProviderFor::Worker,
                        (*ctx.job_id).clone(),
                        operator.clone(),
                        1,
                        3,
                    )
                    .await
                    .unwrap();
                }
            }
        }
        println!(
            "QUALIFICATION_CLEANUP retained_epoch=3 retries=2 elapsed_seconds={:.3}",
            cleanup_started.elapsed().as_secs_f64()
        );
    }
    if std::env::var("STREAMR_TEST_CRASH").as_deref() == Ok("1") {
        // Flush at least one complete source batch so newer writes actually
        // reach the state operator before cancellation.
        let committed_length = tokio::fs::metadata(output_location).await.unwrap().len();
        advance(&running_engine, 40).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            while tokio::fs::metadata(output_location).await.unwrap().len() <= committed_length {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("post-checkpoint rows never reached the sink");
        let task_count: usize = running_engine
            .operator_controls()
            .values()
            .map(Vec::len)
            .sum();
        running_engine.abort_workers();
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stopped = HashSet::new();
            while stopped.len() < task_count {
                match control_rx.recv().await {
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
            assert_eq!(stopped.len(), task_count);
        })
        .await
        .expect("aborted workers did not terminate");
        return;
    }
    if std::env::var("STREAMR_TEST_CHECKPOINT_STOP").as_deref() == Ok("1") {
        run_until_finished(&running_engine, control_rx).await;
        return;
    }
    // shut down the engine
    for source in running_engine.source_controls() {
        source
            .send(ControlMessage::Stop {
                mode: StopMode::Graceful,
            })
            .await
            .unwrap();
    }
    run_until_finished(&running_engine, control_rx).await;
}

async fn finish_from_checkpoint(
    job_id: &str,
    program: Program,
    control_rx: &mut Receiver<ControlResp>,
) {
    let engine = Engine::for_local(program, "pipe-local".to_string(), job_id.to_string())
        .await
        .unwrap();
    let running_engine = engine.start().await;

    info!("Restored engine, running until finished");
    run_until_finished(&running_engine, control_rx).await;
}

fn tasks_per_operator(graph: &LogicalGraph) -> HashMap<String, usize> {
    graph
        .node_weights()
        .flat_map(|node| {
            node.operator_chain
                .iter()
                .map(|(op, _)| (op.operator_id.clone(), node.parallelism))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_and_assert_outputs(
    job_id: &str,
    mut graph: LogicalGraph,
    checkpoint_interval: i32,
    output_location: String,
    golden_output_location: String,
    udfs: &[LocalUdf],
    primary_keys: Option<&[&str]>,
) {
    // remove output_location before running the pipeline
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }

    println!("Running completely");
    let initial_started = std::time::Instant::now();

    let (control_tx, mut control_rx) = channel(128);
    run_completely(
        job_id,
        local_program(job_id, &graph, udfs, None, control_tx).await,
        output_location.clone(),
        golden_output_location.clone(),
        primary_keys,
        &mut control_rx,
    )
    .await;

    println!(
        "PHASE initial elapsed_seconds={:.3} rss_bytes={}",
        initial_started.elapsed().as_secs_f64(),
        process_rss_bytes()
    );
    // debezium sources can't be arbitrarily split because they are effectively stateful
    // and ordering matters
    if primary_keys.is_none() {
        set_internal_parallelism(&mut graph, 2);
    }

    let (control_tx, mut control_rx) = channel(128);

    println!("Run and checkpoint");
    let checkpoint_started = std::time::Instant::now();
    run_and_checkpoint(
        Arc::new(job_id.to_string()),
        local_program(job_id, &graph, udfs, None, control_tx).await,
        Arc::new(LogicalProgram::new(
            graph.clone(),
            ProgramConfig {
                udf_dylibs: Default::default(),
                python_udfs: Default::default(),
            },
        )),
        tasks_per_operator(&graph),
        &mut control_rx,
        checkpoint_interval,
        &output_location,
    )
    .await;

    println!(
        "PHASE checkpoint elapsed_seconds={:.3} rss_bytes={}",
        checkpoint_started.elapsed().as_secs_f64(),
        process_rss_bytes()
    );
    if primary_keys.is_none() {
        set_internal_parallelism(&mut graph, 3);
    }

    let (control_tx, mut control_rx) = channel(128);

    println!("Finish from checkpoint");
    let restore_started = std::time::Instant::now();
    finish_from_checkpoint(
        job_id,
        local_program(job_id, &graph, udfs, Some(3), control_tx).await,
        &mut control_rx,
    )
    .await;

    println!(
        "PHASE restore_and_replay elapsed_seconds={:.3} rss_bytes={}",
        restore_started.elapsed().as_secs_f64(),
        process_rss_bytes()
    );
    check_output_files(
        "resuming from checkpointing",
        output_location,
        golden_output_location,
        primary_keys,
    )
    .await;
}

async fn run_completely(
    job_id: &str,
    program: Program,
    output_location: String,
    golden_output_location: String,
    primary_keys: Option<&[&str]>,
    control_rx: &mut Receiver<ControlResp>,
) {
    let engine = Engine::for_local(program, "pipe-local".to_string(), job_id.to_string())
        .await
        .unwrap();
    let running_engine = engine.start().await;

    run_until_finished(&running_engine, control_rx).await;

    check_output_files(
        "initial run",
        output_location.clone(),
        golden_output_location,
        primary_keys,
    )
    .await;
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }
}

fn get_key(v: &Value, primary_keys: Option<&[&str]>) -> Vec<String> {
    match primary_keys {
        Some(pks) => pks
            .iter()
            .map(|pk| v.get(pk).expect("primary key not found in row").to_string())
            .collect::<Vec<_>>(),
        None => {
            vec![v.to_string()]
        }
    }
}

fn merge_debezium(rows: Vec<Value>, primary_keys: Option<&[&str]>) -> HashSet<Value> {
    let mut state = HashMap::new();
    for r in rows {
        let before = r.get("before").map(roundtrip);
        let after = r.get("after").map(roundtrip);
        let op = r.get("op").expect("no 'op' for debezium");

        match op.as_str().expect("op isn't string") {
            "c" => {
                let key = get_key(after.as_ref().expect("no after for c"), primary_keys);
                assert!(
                    state.insert(key, after.expect("no after for c")).is_none(),
                    "'c' for existing row"
                );
            }
            "u" => {
                let key = get_key(&before.expect("no before for 'u'"), primary_keys);
                assert!(
                    state.remove(&key).is_some(),
                    "'u' for non-existent row ({r})"
                );
                let key = get_key(after.as_ref().expect("no after for 'u'"), primary_keys);
                assert!(
                    state
                        .insert(key, after.expect("no after for 'u'"))
                        .is_none(),
                    "'u' overwrote existing row"
                );
            }
            "d" => {
                let key = get_key(&before.expect("no before for 'd'"), primary_keys);
                assert!(
                    state.remove(&key).is_some(),
                    "'d' for non-existent row: {r} ({primary_keys:?})"
                );
            }
            c => {
                panic!("unknown debezium op '{c}'");
            }
        }
    }

    state.into_values().collect()
}

fn is_debezium(value: &Value) -> bool {
    let Some(op) = value.get("op") else {
        return false;
    };
    op.as_str().is_some()
}

fn order_by_pk(lines: HashSet<Value>, pks: Option<&[&str]>) -> Vec<Value> {
    let mut lines: Vec<Value> = lines.into_iter().collect();

    let Some(pks) = pks else {
        return lines;
    };

    lines.sort_by_key(|v| {
        pks.iter()
            .map(|pk| v.get(pk).unwrap().to_string())
            .collect::<Vec<_>>()
    });

    lines
}

fn check_debezium(
    name: &str,
    output_location: String,
    golden_output_location: String,
    output_lines: Vec<Value>,
    golden_output_lines: Vec<Value>,
    primary_keys: Option<&[&str]>,
) {
    let output_merged = order_by_pk(merge_debezium(output_lines, primary_keys), primary_keys);
    let golden_output_merged = order_by_pk(
        merge_debezium(golden_output_lines, primary_keys),
        primary_keys,
    );

    similar_asserts::assert_eq!(
        output_merged,
        golden_output_merged,
        "Incorrect outputs for updating ({}) for\noutput: {}\ngolden: {}",
        name,
        output_location,
        golden_output_location
    );
}
fn roundtrip(v: &Value) -> Value {
    if v.is_null() {
        return Value::Null;
    }
    // round trip string through a btreemap to get consistent key ordering
    serde_json::to_value(serde_json::from_value::<BTreeMap<String, Value>>(v.clone()).unwrap())
        .unwrap()
}

async fn check_output_files(
    check_name: &str,
    output_location: String,
    golden_output_location: String,
    primary_keys: Option<&[&str]>,
) {
    let mut output_lines: Vec<Value> = read_to_string(output_location.clone())
        .await
        .unwrap_or_else(|_| panic!("output file not found at {output_location}"))
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();

    let mut golden_output_lines: Vec<Value> = read_to_string(golden_output_location.clone())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "golden output file not found at {golden_output_location}, want to compare to {output_location}"
            )
        })
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();

    let Some(first_output) = output_lines.first() else {
        panic!(
            "failed at check {}, output has 0 lines, expect {} lines.\noutput: {}\ngolden: {}",
            check_name,
            golden_output_lines.len(),
            output_location,
            golden_output_location
        );
    };
    if is_debezium(first_output) {
        check_debezium(
            check_name,
            output_location,
            golden_output_location,
            output_lines,
            golden_output_lines,
            primary_keys,
        );
        return;
    }

    if output_lines.len() != golden_output_lines.len() {
        panic!(
            "failed at check {}, output has {} lines, expect {} lines.\noutput: {}\ngolden: {}",
            check_name,
            output_lines.len(),
            golden_output_lines.len(),
            output_location,
            golden_output_location
        );
    }

    output_lines.sort_by_cached_key(|v| roundtrip(v).to_string());
    golden_output_lines.sort_by_cached_key(|v| roundtrip(v).to_string());
    output_lines
        .into_iter()
        .zip(golden_output_lines)
        .enumerate()
        .for_each(|(i, (output_line, golden_output_line))| {
            similar_asserts::assert_eq!(
                output_line,
                golden_output_line,
                "check {}: line {} of output and golden output differ\nactual:{}\nexpected:{})",
                check_name,
                i,
                output_location,
                golden_output_location
            )
        });
}

pub async fn correctness_run_codegen(
    test_name: impl Into<String>,
    query: impl Into<String>,
    primary_keys: Option<&[&str]>,
    checkpoint_interval: i32,
) -> Result<()> {
    let test_name = test_name.into();
    let parent_directory = std::env::current_dir()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Depending on run location the directory might end with arroyo-sql-testing.
    // If so, remove it.
    let parent_directory = if parent_directory.ends_with("arroyo-sql-testing") {
        parent_directory
            .strip_suffix("arroyo-sql-testing")
            .unwrap()
            .to_string()
    } else {
        parent_directory
    };

    // replace $input_file with the current directory and then inputs/query_name.json
    let physical_input_dir = format!("{parent_directory}/arroyo-sql-testing/inputs/",);

    let query_string = query.into().replace("$input_dir", &physical_input_dir);

    // replace $output_file with the current directory and then outputs/query_name.json
    let physical_output = format!("{parent_directory}/arroyo-sql-testing/outputs/{test_name}.json");

    let query_string = query_string.replace("$output_path", &physical_output);
    let golden_output_location =
        format!("{parent_directory}/arroyo-sql-testing/golden_outputs/{test_name}.json");

    let udfs = get_udfs();

    let logical_program = get_graph(query_string.clone(), &udfs).await?;
    let job_id = format!(
        "{}-{}-{}",
        test_name,
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    run_pipeline_and_assert_outputs(
        &job_id,
        logical_program.graph,
        checkpoint_interval,
        physical_output,
        golden_output_location,
        &udfs,
        primary_keys,
    )
    .await;
    Ok(())
}

async fn get_graph(query_string: String, udfs: &[LocalUdf]) -> Result<LogicalProgram> {
    let mut schema_provider = ArroyoSchemaProvider::new();
    for udf in udfs {
        schema_provider
            .add_rust_udf(udf.def, udf.config.name.as_str())
            .unwrap();
    }

    // TODO: test with higher parallelism
    let program = parse_and_get_arrow_program(
        query_string,
        schema_provider,
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await?
    .program;
    Ok(program)
}

fn leader_mode() -> bool {
    std::env::var("STREAMR_TEST_CHECKPOINT_MODE").as_deref() == Ok("leader")
}

async fn local_program(
    job_id: &str,
    graph: &LogicalGraph,
    udfs: &[LocalUdf],
    epoch: Option<u64>,
    control_tx: tokio::sync::mpsc::Sender<ControlResp>,
) -> Program {
    if !leader_mode() {
        return Program::local_from_logical(job_id.to_owned(), graph, udfs, epoch, control_tx)
            .await;
    }
    let pipeline_id = arroyo_types::PipelineId(Arc::new("pipe-test".into()));
    let paths = arroyo_state_protocol::ProtocolPaths::new(
        pipeline_id.clone(),
        arroyo_types::JobId(Arc::new(job_id.into())),
    );
    let generation = arroyo_state_protocol::types::Generation(0);
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    let manifest = if let Some(epoch) = epoch {
        use arroyo_state_protocol::store::{read_json, read_protobuf};
        use arroyo_state_protocol::types::GenerationManifest;
        use arroyo_state_protocol::workflow::{GenerationResolution, resolve_generation_manifest};
        let current: GenerationManifest =
            read_json(storage.as_ref(), &paths.generation_manifest(generation))
                .await
                .unwrap()
                .unwrap();
        let resolution = resolve_generation_manifest(storage.as_ref(), &current, generation)
            .await
            .unwrap();
        let checkpoint_ref = match resolution {
            GenerationResolution::Ready { checkpoint_ref } => checkpoint_ref,
            other => panic!("leader recovery not ready: {other:?}"),
        };
        assert_eq!(
            checkpoint_ref,
            paths.checkpoint_manifest(generation, Epoch(epoch))
        );
        Some(
            read_protobuf(storage.as_ref(), &checkpoint_ref)
                .await
                .unwrap()
                .unwrap(),
        )
    } else {
        None
    };
    let assignments = graph
        .node_weights()
        .flat_map(|node| {
            (0..node.parallelism).map(move |index| arroyo_rpc::grpc::rpc::TaskAssignment {
                task_id: node.node_id,
                subtask_idx: index as u32,
                worker_id: 0,
                worker_addr: String::new(),
                worker_rpc: String::new(),
            })
        })
        .collect();
    let mut registry = arroyo_planner::physical::new_registry();
    for udf in udfs {
        registry.add_local_udf(udf);
    }
    Program::from_logical(
        job_id,
        graph,
        &assignments,
        registry,
        None,
        manifest,
        arroyo_types::CheckpointFilePathLayout::Protocol {
            pipeline_id,
            generation: 0,
        },
        control_tx,
    )
    .await
    .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct CaptureCounts {
    input_rows_before_checkpoint: i32,
    expected_rows: usize,
    expected_checkpoint_rows: usize,
    checkpoint_epoch: u32,
}

impl CaptureCounts {
    fn from_env() -> std::result::Result<Self, String> {
        Self::parse(|name| env::var(name).map_err(|error| format!("{name}: {error}")))
    }

    fn parse(
        mut read: impl FnMut(&str) -> std::result::Result<String, String>,
    ) -> std::result::Result<Self, String> {
        fn number<T: std::str::FromStr>(name: &str, raw: String) -> std::result::Result<T, String> {
            if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(format!("{name} must be an unsigned decimal integer"));
            }
            raw.parse()
                .map_err(|_| format!("{name} is outside the supported integer range"))
        }
        let input_name = "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT";
        let epoch_name = "STREAMR_CAPTURE_CHECKPOINT_EPOCH";
        let input_rows_before_checkpoint = number(input_name, read(input_name)?)?;
        let checkpoint_epoch = number(epoch_name, read(epoch_name)?)?;
        if input_rows_before_checkpoint == 0 {
            return Err(format!(
                "{input_name} must be positive: the source reads its first row immediately"
            ));
        }
        if checkpoint_epoch == 0 {
            return Err(format!("{epoch_name} must be positive"));
        }
        let rows_name = "STREAMR_CAPTURE_EXPECTED_ROWS";
        let checkpoint_rows_name = "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS";
        Ok(Self {
            input_rows_before_checkpoint,
            expected_rows: number(rows_name, read(rows_name)?)?,
            expected_checkpoint_rows: number(checkpoint_rows_name, read(checkpoint_rows_name)?)?,
            checkpoint_epoch,
        })
    }
}

#[test]
fn capture_counts_require_explicit_bounded_parameters() {
    let parse = |input: &str, rows: &str, checkpoint_rows: &str, epoch: &str| {
        CaptureCounts::parse(|name| {
            Ok(match name {
                "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT" => input,
                "STREAMR_CAPTURE_EXPECTED_ROWS" => rows,
                "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS" => checkpoint_rows,
                "STREAMR_CAPTURE_CHECKPOINT_EPOCH" => epoch,
                _ => unreachable!(),
            }
            .to_owned())
        })
    };
    assert_eq!(
        parse("7", "3", "2", "9").unwrap(),
        CaptureCounts {
            input_rows_before_checkpoint: 7,
            expected_rows: 3,
            expected_checkpoint_rows: 2,
            checkpoint_epoch: 9
        }
    );
    assert!(parse("1", "0", "0", "1").is_ok());
    for invalid in ["", "-1", "+1", " 1", "1.5", "18446744073709551616"] {
        assert!(parse(invalid, "3", "2", "9").is_err());
        assert!(parse("7", invalid, "2", "9").is_err());
        assert!(parse("7", "3", invalid, "9").is_err());
        assert!(parse("7", "3", "2", invalid).is_err());
    }
    assert!(parse("0", "3", "2", "9").is_err());
    assert!(parse("2147483648", "3", "2", "9").is_err());
    assert!(parse("7", "3", "2", "0").is_err());
    assert!(parse("7", "3", "2", "4294967296").is_err());
    assert!(CaptureCounts::parse(|name| Err(format!("missing {name}"))).is_err());
}

/// Capture externally supplied SQL as JSONL before and after checkpoint recovery.
/// Requires a singleton graph and one control-waiting single-file source.
/// Input advancement and expected output counts are configured independently;
/// fixture preparation and business-output comparison belong to the caller.
/// Run alone because worker configuration and checkpoint storage are process-wide.
#[test_log(tokio::test)]
#[ignore = "opt-in external SQL output and checkpoint/recovery capture"]
async fn external_sql_checkpoint_capture() {
    tokio::time::timeout(
        test_runtime_timeout() * 4,
        external_sql_checkpoint_capture_inner(),
    )
    .await
    .expect("external SQL capture planning, startup, or recovery timed out");
}

async fn external_sql_checkpoint_capture_inner() {
    let capture = CaptureCounts::from_env().expect("invalid external SQL capture configuration");
    configure_test_worker();
    let selected_backend = env::var("STREAMR_TEST_BACKEND").unwrap_or_else(|_| "memory".into());
    let selected_checkpoint =
        env::var("STREAMR_TEST_CHECKPOINT_MODE").unwrap_or_else(|_| "controller".into());
    assert!(matches!(selected_backend.as_str(), "memory" | "rocksdb"));
    assert!(matches!(
        selected_checkpoint.as_str(),
        "controller" | "leader"
    ));
    assert_eq!(
        matches!(
            config::config().worker.sql_state_backend,
            arroyo_rpc::config::SqlStateBackend::Rocksdb
        ),
        selected_backend == "rocksdb"
    );
    println!(
        "CAPTURE_CONFIG backend={selected_backend} checkpoint_mode={selected_checkpoint} execution_resources={:?}",
        config::config().worker.execution_resources
    );
    let query_path = PathBuf::from(
        env::var("STREAMR_CAPTURE_QUERY").expect("STREAMR_CAPTURE_QUERY is required"),
    );
    let output_path = PathBuf::from(
        env::var("STREAMR_CAPTURE_OUTPUT").expect("STREAMR_CAPTURE_OUTPUT is required"),
    );
    assert!(query_path.is_absolute(), "query path must be absolute");
    assert!(output_path.is_absolute(), "output path must be absolute");
    let query = read_to_string(&query_path).await.unwrap();
    let udfs = get_udfs();
    let logical = Arc::new(
        tokio::time::timeout(test_runtime_timeout(), get_graph(query, &udfs))
            .await
            .expect("external SQL planning timed out")
            .expect("external SQL failed to plan"),
    );
    for node in logical.graph.node_weights() {
        assert_eq!(
            node.parallelism, 1,
            "external SQL capture requires singleton graph"
        );
        for (operator, _) in node.operator_chain.iter() {
            println!(
                "CAPTURE_OPERATOR node={} operator={} kind={:?} parallelism={}",
                node.node_id, operator.operator_id, operator.operator_name, node.parallelism
            );
        }
    }
    let sources: Vec<_> = logical
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::ConnectorSource)
        .collect();
    assert_eq!(sources.len(), 1, "capture requires one connector source");
    let source: arroyo_rpc::grpc::api::ConnectorOp =
        prost::Message::decode(sources[0].0.operator_config.as_slice())
            .expect("capture source config must decode");
    assert_eq!(
        source.connector, "single_file",
        "capture requires a single-file source"
    );
    let source_config: arroyo_rpc::OperatorConfig =
        serde_json::from_str(&source.config).expect("capture source connector config must decode");
    assert!(
        source_config
            .table
            .get("wait_for_control")
            .is_none_or(|value| value.is_null() || value.as_bool() == Some(true)),
        "capture source must wait for control after each input row"
    );
    let job_id = format!(
        "external-sql-capture-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let initial_path = output_path.with_extension("initial.jsonl");
    for path in [&output_path, &initial_path] {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot clear capture {}: {error}", path.display()),
        }
    }
    let (control_tx, mut control_rx) = channel(128);
    let program = local_program(&job_id, &logical.graph, &udfs, None, control_tx).await;
    let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    run_until_finished(&running, &mut control_rx).await;
    capture_rows(&output_path, capture.expected_rows).await;
    tokio::fs::rename(&output_path, &initial_path)
        .await
        .unwrap();
    println!(
        "CAPTURE_RESULT phase=initial rows={} path={}",
        capture.expected_rows,
        initial_path.display()
    );

    // The shared helper initializes leader generations for epoch 1. Other
    // configured first epochs require explicit generation initialization.
    if leader_mode() && capture.checkpoint_epoch != 1 {
        use arroyo_state_protocol::workflow::{
            GenerationInitialization, InitializeGenerationRequest, initialize_generation,
        };
        let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
            .await
            .unwrap();
        let initialized = initialize_generation(
            storage.as_ref(),
            InitializeGenerationRequest {
                pipeline_id: arroyo_types::PipelineId::new("pipe-test"),
                job_id: arroyo_types::JobId::new(job_id.clone()),
                generation: arroyo_state_protocol::types::Generation(0),
                updated_at: SystemTime::now(),
            },
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            initialized,
            GenerationInitialization::Initialized { .. }
        ));
    }
    let (control_tx, mut control_rx) = channel(128);
    let program = local_program(&job_id, &logical.graph, &udfs, None, control_tx).await;
    let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    // A control-waiting single-file source reads its first row immediately;
    // configured NoOps advance the remaining input rows. The barrier flushes
    // a partial source batch; output cardinality need not match input cardinality.
    assert_eq!(
        running.source_controls().len(),
        1,
        "capture requires one source"
    );
    advance(&running, capture.input_rows_before_checkpoint - 1).await;
    // Stop at the barrier: a normal checkpoint resumes the source and reads
    // another line, which could flush beyond the captured checkpoint prefix.
    let mut finished_tasks = HashSet::new();
    let checkpoint_bytes = checkpoint_with_stop(
        &mut SmokeTestContext {
            job_id: Arc::new(job_id.clone()),
            engine: &running,
            control_rx: &mut control_rx,
            program: logical.clone(),
        },
        capture.checkpoint_epoch,
        true,
        &mut finished_tasks,
    )
    .await;
    capture_rows(&output_path, capture.expected_checkpoint_rows).await;
    if leader_mode() {
        use arroyo_state_protocol::store::read_protobuf;
        let paths = arroyo_state_protocol::ProtocolPaths::new(
            arroyo_types::PipelineId::new("pipe-test"),
            arroyo_types::JobId::new(job_id.clone()),
        );
        let checkpoint_ref = paths.checkpoint_manifest(
            arroyo_state_protocol::types::Generation(0),
            Epoch(u64::from(capture.checkpoint_epoch)),
        );
        let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
            .await
            .unwrap();
        let metadata: arroyo_rpc::grpc::rpc::CheckpointManifest =
            read_protobuf(storage.as_ref(), &checkpoint_ref)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(metadata.epoch, u64::from(capture.checkpoint_epoch));
        assert_eq!(metadata.job_id, job_id);
        assert!(!metadata.operators.is_empty());
        println!("CAPTURE_CHECKPOINT path={checkpoint_ref} metadata={metadata:?}");
    } else {
        let metadata = StateBackend::load_checkpoint_metadata(
            &StorageProviderFor::Worker,
            &job_id,
            capture.checkpoint_epoch,
        )
        .await
        .unwrap();
        assert_eq!(metadata.epoch, capture.checkpoint_epoch);
        assert_eq!(metadata.job_id, job_id);
        assert!(!metadata.operator_ids.is_empty());
        println!(
            "CAPTURE_CHECKPOINT path={job_id}/checkpoints/checkpoint-{:07}/metadata metadata={metadata:?}",
            capture.checkpoint_epoch
        );
    }
    let task_count: usize = running.operator_controls().values().map(Vec::len).sum();
    running.abort_workers();
    tokio::time::timeout(test_runtime_timeout(), async {
        let mut stopped = finished_tasks;
        while stopped.len() < task_count {
            match control_rx.recv().await {
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
        assert_eq!(
            stopped.len(),
            task_count,
            "cancelled workers did not all terminate"
        );
    })
    .await
    .expect("external SQL worker cancellation timed out");
    drop(running);
    let (control_tx, mut control_rx) = channel(128);
    let program = local_program(
        &job_id,
        &logical.graph,
        &udfs,
        Some(u64::from(capture.checkpoint_epoch)),
        control_tx,
    )
    .await;
    let restored = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    run_until_finished(&restored, &mut control_rx).await;
    capture_rows(&output_path, capture.expected_rows).await;
    println!(
        "CAPTURE_RESULT phase=recovered checkpoint={} input_rows_before_checkpoint={} committed_rows={} rows={} bytes={checkpoint_bytes} path={} job={job_id}",
        capture.checkpoint_epoch,
        capture.input_rows_before_checkpoint,
        capture.expected_checkpoint_rows,
        capture.expected_rows,
        output_path.display()
    );
}

async fn capture_rows(path: &Path, expected: usize) {
    let captured = read_to_string(path)
        .await
        .expect("external SQL capture file missing");
    let rows: Vec<_> = captured.lines().collect();
    assert_eq!(
        rows.len(),
        expected,
        "unexpected capture row count in {}",
        path.display()
    );
    for row in rows {
        let value: Value =
            serde_json::from_str(row).expect("external SQL capture contains invalid JSON");
        assert!(
            value.is_object(),
            "external SQL capture must contain JSON objects"
        );
    }
}

/// Run separately: resources and RSS measurements belong to one worker process.
#[cfg(target_os = "linux")]
#[test_log(tokio::test)]
#[ignore = "dedicated-process larger-than-RAM qualification"]
async fn milestone2_larger_than_ram() {
    use std::io::Write;
    assert_eq!(
        std::env::var("STREAMR_TEST_BACKEND").as_deref(),
        Ok("rocksdb")
    );
    // Qualification runs alone: native fsync and full exports on slow test
    // storage need a larger deadline than the small fixture smoke tests.
    unsafe {
        env::set_var(
            "STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS",
            env::var("STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS").unwrap_or_else(|_| "600".into()),
        );
    }
    let cardinality = 32_768usize;
    let payload_bytes = 4096usize;
    let logical_bytes = cardinality * payload_bytes;
    let assigned_bytes = 12 * 1024 * 1024usize;
    assert!(logical_bytes >= 10 * assigned_bytes);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let input = root.join("inputs/milestone2_probe.json");
    let golden = root.join("golden_outputs/milestone2_probe.json");
    std::fs::create_dir_all(root.join("outputs")).unwrap();
    let query_path = root.join("outputs/milestone2_probe.sql");
    let mut source = std::io::BufWriter::new(std::fs::File::create(&input).unwrap());
    let mut expected = std::io::BufWriter::new(std::fs::File::create(&golden).unwrap());
    let payload = |key: usize| {
        let mut rng = (key as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
        (0..payload_bytes)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (b'a' + (rng % 26) as u8) as char
            })
            .collect::<String>()
    };
    // Grow uniformly, then mutate existing keys before skewed reads. Epoch 3
    // captures the first 32 updates; the crash advances into newer updates.
    // The expected previous version therefore detects accepting a local WAL
    // or losing committed updates, as well as missing unchanged older keys.
    let update_rows = 128usize;
    for phase in 0..2 {
        for index in 0..cardinality {
            let writing = phase == 0 || index < update_rows;
            let key = if phase == 0 {
                index
            } else if index % 4 == 0 {
                0
            } else if index >= update_rows && index % 4 == 1 {
                // Also revisit updated cold keys instead of checking only the
                // hot key and untouched tail of the original population.
                1 + ((index / 4) % (update_rows - 1))
            } else {
                index
            };
            let before_seed = if phase == 0 {
                None
            } else if index < update_rows {
                Some(if key == 0 && index > 0 {
                    cardinality + index - 4
                } else {
                    key
                })
            } else if key == 0 {
                Some(cardinality + update_rows - 4)
            } else if key < update_rows && key % 4 != 0 {
                Some(cardinality + key)
            } else {
                Some(key)
            };
            let after_seed = if phase == 1 && writing {
                cardinality + index
            } else {
                before_seed.unwrap_or(key)
            };
            let id = phase * cardinality + index;
            serde_json::to_writer(
                &mut source,
                &serde_json::json!({
                    "id": id,
                    "key": key.to_string(),
                    "payload": payload(after_seed),
                    "expected_before": before_seed.map(payload),
                    "writing": writing,
                }),
            )
            .unwrap();
            writeln!(source).unwrap();
            writeln!(expected, "{{\"id\":{id},\"matches\":true}}").unwrap();
        }
    }
    source.flush().unwrap();
    expected.flush().unwrap();
    std::fs::write(
        &query_path,
        format!(
            r#"--checkpoint-interval={}
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, expected_before TEXT, writing BOOLEAN)
WITH (connector='single_file',path='$input_dir/milestone2_probe.json',format='json',type='source');
CREATE TABLE output (id BIGINT, matches BOOLEAN)
WITH (connector='single_file',path='$output_path',format='json',type='sink');
INSERT INTO output
WITH before_step AS (
 SELECT id,key,payload,expected_before,writing,
  state_get('qualified',key) AS previous_value FROM events
), after_step AS (
 SELECT id,payload,expected_before,previous_value,
  CASE WHEN writing THEN state_put('qualified',key,payload)
  ELSE state_get('qualified',key) END AS final_value FROM before_step
)
SELECT id,
 (previous_value IS NOT DISTINCT FROM expected_before)
 AND final_value = payload AS matches FROM after_step;
"#,
            cardinality / 4 + 8
        ),
    )
    .unwrap();
    let envelope_bytes = std::env::var("STREAMR_TEST_RSS_MIB")
        .unwrap_or_else(|_| "768".into())
        .parse::<usize>()
        .unwrap()
        * 1024
        * 1024;
    let started = std::time::Instant::now();
    let probe_output = root.join("outputs/milestone2_probe.json");
    if probe_output.exists() {
        std::fs::remove_file(&probe_output).unwrap();
    }
    let monitor = tokio::spawn(async move {
        use std::io::{Read, Seek, SeekFrom};
        use std::os::unix::fs::MetadataExt;
        let mut inode = 0;
        let mut offset = 0;
        let mut records = 0usize;
        let mut next_sample = cardinality / 4;
        loop {
            let rss = process_rss_bytes();
            assert!(rss > 0, "qualification requires a working RSS measurement");
            assert!(
                rss <= envelope_bytes,
                "SQL process RSS {rss} exceeds envelope {envelope_bytes}"
            );
            if let Ok(mut file) = std::fs::File::open(&probe_output) {
                let metadata = file.metadata().unwrap();
                if inode != metadata.ino() || metadata.len() < offset {
                    inode = metadata.ino();
                    offset = 0;
                    records = 0;
                    next_sample = cardinality / 4;
                }
                file.seek(SeekFrom::Start(offset)).unwrap();
                let mut bytes = [0u8; 65536];
                for _ in 0..8 {
                    let read = file.read(&mut bytes).unwrap();
                    if read == 0 {
                        break;
                    }
                    records += bytes[..read].iter().filter(|&&b| b == b'\n').count();
                    offset += read as u64;
                }
                if records >= next_sample {
                    println!(
                        "GROWTH emitted_records={records} cardinality_upper={} rss_bytes={rss} local_state_disk_bytes={} elapsed_seconds={:.3}",
                        records.min(cardinality),
                        directory_bytes(Path::new("/tmp/streamr-sql-live")),
                        started.elapsed().as_secs_f64()
                    );
                    next_sample = records + cardinality / 4;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    run_smoketest(&query_path).await;
    let monitor_failed = monitor.is_finished();
    monitor.abort();
    if monitor_failed {
        monitor.await.unwrap();
    }
    let peak_rss = std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<usize>()
                .ok()
        })
        .unwrap()
        * 1024;
    assert!(
        peak_rss <= envelope_bytes,
        "peak RSS {peak_rss} exceeds {envelope_bytes}"
    );
    println!("PEAK_RSS bytes={peak_rss}");
    for family in prometheus::gather() {
        if family.name().starts_with("streamr_sql_state_") {
            for metric in family.get_metric() {
                let histogram = metric.get_histogram();
                let count = histogram.get_sample_count();
                if count == 0 {
                    continue;
                }
                let bound = |quantile: f64| {
                    histogram
                        .get_bucket()
                        .iter()
                        .find(|bucket| bucket.cumulative_count() as f64 >= count as f64 * quantile)
                        .map(|bucket| bucket.upper_bound())
                        .unwrap_or(f64::INFINITY)
                };
                println!(
                    "LATENCY metric={} samples={count} mean_seconds={:.6} p50_upper_seconds={:.6} p99_upper_seconds={:.6}",
                    family.name(),
                    histogram.get_sample_sum() / count as f64,
                    bound(0.5),
                    bound(0.99)
                );
            }
        }
    }
    println!(
        "QUALIFICATION mode={} cardinality={cardinality} payload_bytes={payload_bytes} logical_bytes={logical_bytes} assigned_state_bytes={assigned_bytes} rss_bytes={} envelope_bytes={envelope_bytes} elapsed_seconds={:.3} output_records={} skew=25%-hot-key",
        if leader_mode() {
            "leader"
        } else {
            "controller"
        },
        process_rss_bytes(),
        started.elapsed().as_secs_f64(),
        cardinality * 2
    );
    for file in [input, golden, query_path] {
        std::fs::remove_file(file).unwrap();
    }
}

fn process_rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<usize>()
                .ok()
        })
        .unwrap_or(0)
        * 1024
}

#[cfg(target_os = "linux")]
fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let Ok(metadata) = entry.metadata() else {
                return 0;
            };
            if metadata.is_dir() {
                directory_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn configure_test_worker() {
    config::config();
    config::update(|c| {
        // reduce the batch size to increase consistency
        c.pipeline.source_batch_size = 32;
        if let Ok(bytes) = std::env::var("STREAMR_TEST_EXECUTION_BYTES") {
            c.worker.execution_resources = Some(arroyo_rpc::config::ExecutionResourceConfig {
                memory_bytes: bytes.parse().expect("invalid execution memory limit"),
                max_batch_bytes: 1024 * 1024,
            });
        }
        if std::env::var("STREAMR_TEST_BACKEND").as_deref() == Ok("rocksdb") {
            use arroyo_rpc::config::{
                DiskSqlStateConfig, LiveStateResourceConfig, SqlStateBackend,
            };
            c.worker.sql_state_backend = SqlStateBackend::Rocksdb;
            c.worker.disk_sql_state = Some(DiskSqlStateConfig {
                directory: std::path::PathBuf::from("/tmp/streamr-sql-live"),
                max_row_bytes: 24 * 1024,
            });
            c.worker.live_state_resources = Some(LiveStateResourceConfig {
                block_cache_bytes: 8 * 1024 * 1024,
                memtable_bytes: 4 * 1024 * 1024,
                queued_write_bytes: 1024 * 1024,
                decoded_value_bytes: 1024 * 1024,
                scan_page_bytes: 2 * 1024 * 1024,
                max_blocking_operations: 2,
                max_snapshots: 2,
                max_open_databases: 2,
                disk_reserve_bytes: 64 * 1024 * 1024,
            });
        }
    });
}

#[path = "smoke_fault_tests.rs"]
mod fault_tests;
