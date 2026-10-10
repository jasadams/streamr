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
use tokio::fs::{File, read_to_string};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
    // These live SQL owners currently require unchanged singleton ownership.
    // The fixture still exercises checkpoint and replay; rescaling is not
    // qualified by this run. Legacy window/aggregate fixtures still rescale.
    let current = config::config();
    let worker = &current.worker;
    let native_window = worker.window_state.is_some();
    let native_aggregate = worker.aggregate_state.is_some();
    if graph.node_weights().any(|node| {
        node.operator_chain
            .iter()
            .any(|(operator, _)| match operator.operator_name {
                OperatorName::UpdatingAggregate => native_aggregate,
                OperatorName::SlidingWindowAggregate => native_window,
                OperatorName::SessionWindowAggregate => native_window,
                OperatorName::TumblingWindowAggregate if native_window => {
                    <arroyo_rpc::grpc::api::TumblingWindowAggregateOperator as prost::Message>::decode(
                        operator.operator_config.as_slice(),
                    )
                    .is_ok_and(|window| window.width_micros > 0)
                }
                _ => false,
            })
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
    checkpoint(ctx, 3).await;
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
    local_program_selected(job_id, graph, udfs, epoch, control_tx, false).await
}

// Retained commits are selected explicitly by the lifecycle harness. Ordinary
// recovery continues to require the generation's currently published commit.
async fn local_program_selected(
    job_id: &str,
    graph: &LogicalGraph,
    udfs: &[LocalUdf],
    epoch: Option<u64>,
    control_tx: tokio::sync::mpsc::Sender<ControlResp>,
    retained: bool,
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
        let checkpoint_ref = if retained {
            let selected = paths.checkpoint_manifest(generation, Epoch(epoch));
            let selected_metadata: arroyo_rpc::grpc::rpc::CheckpointManifest =
                read_protobuf(storage.as_ref(), &selected)
                    .await
                    .unwrap()
                    .expect("selected checkpoint was not published");
            assert_eq!(selected_metadata.job_id, job_id);
            assert_eq!(selected_metadata.epoch, epoch);
            assert!(
                !selected_metadata.needs_commit,
                "selected checkpoint requires commit"
            );
            let mut cursor = checkpoint_ref;
            let mut seen = HashSet::new();
            loop {
                assert!(
                    seen.insert(cursor.clone()),
                    "checkpoint history contains a cycle"
                );
                let committed: arroyo_rpc::grpc::rpc::CheckpointManifest =
                    read_protobuf(storage.as_ref(), &cursor)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(committed.job_id, job_id);
                assert!(
                    !committed.needs_commit,
                    "retained checkpoint requires commit"
                );
                if cursor == selected {
                    break;
                }
                cursor = arroyo_state_protocol::types::CheckpointRef::new(
                    committed
                        .parent_checkpoint_ref
                        .expect("selected epoch is not in committed history"),
                )
                .unwrap();
            }
            selected
        } else {
            assert_eq!(
                checkpoint_ref,
                paths.checkpoint_manifest(generation, Epoch(epoch))
            );
            checkpoint_ref
        };
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
    expected_initial_rows: usize,
    expected_checkpoint_rows: usize,
    checkpoint_epoch: u32,
}

impl CaptureCounts {
    fn from_env() -> std::result::Result<Self, String> {
        let initial_name = "STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS";
        let initial = match env::var(initial_name) {
            Ok(value) => Some(value),
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(format!("{initial_name}: {error}")),
        };
        Self::parse(
            |name| env::var(name).map_err(|error| format!("{name}: {error}")),
            initial.as_deref(),
        )
    }

    fn parse(
        mut read: impl FnMut(&str) -> std::result::Result<String, String>,
        initial_rows: Option<&str>,
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
        let expected_rows = number(rows_name, read(rows_name)?)?;
        let expected_initial_rows = match initial_rows {
            Some(value) => number("STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS", value.to_owned())?,
            None => expected_rows,
        };
        Ok(Self {
            input_rows_before_checkpoint,
            expected_rows,
            expected_initial_rows,
            expected_checkpoint_rows: number(checkpoint_rows_name, read(checkpoint_rows_name)?)?,
            checkpoint_epoch,
        })
    }
}

/// Opt-in pause after a real input row, with the source blocked on its
/// existing control channel. The ordinary capture path does not use this.
#[derive(Debug, PartialEq, Eq)]
struct CaptureIdle {
    source_row_target: i32,
    duration: Duration,
    min_pre_rows: usize,
    max_output_bytes: usize,
    pre_match: Option<CaptureIdlePreMatch>,
}

/// Optional, caller-owned value readiness for the last complete sink row.
#[derive(Debug, PartialEq, Eq)]
struct CaptureIdlePreMatch {
    pointer: String,
    expected: Value,
}

impl CaptureIdlePreMatch {
    fn parse(
        pointer: Option<&str>,
        value: Option<&str>,
    ) -> std::result::Result<Option<Self>, String> {
        const POINTER: &str = "STREAMR_CAPTURE_IDLE_PRE_MATCH_POINTER";
        const VALUE: &str = "STREAMR_CAPTURE_IDLE_PRE_MATCH_VALUE";
        let (Some(pointer), Some(value)) = (pointer, value) else {
            if pointer.is_some() || value.is_some() {
                return Err(format!("{POINTER} and {VALUE} must be set together"));
            }
            return Ok(None);
        };
        if pointer.len() > 1024 || value.len() > 4096 {
            return Err(format!(
                "{POINTER} and {VALUE} exceed configured size limits"
            ));
        }
        if !pointer.is_empty() && !pointer.starts_with('/') {
            return Err(format!("{POINTER} must be a JSON Pointer"));
        }
        let bytes = pointer.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == b'~' {
                if index + 1 >= bytes.len() || !matches!(bytes[index + 1], b'0' | b'1') {
                    return Err(format!("{POINTER} has an invalid escape"));
                }
                index += 1;
            }
            index += 1;
        }
        let expected = serde_json::from_str(value)
            .map_err(|error| format!("{VALUE} must be valid JSON: {error}"))?;
        Ok(Some(Self {
            pointer: pointer.to_owned(),
            expected,
        }))
    }

    fn matches(&self, row: &Value) -> bool {
        row.pointer(&self.pointer) == Some(&self.expected)
    }
}

impl CaptureIdle {
    fn from_env(capture: &CaptureCounts) -> std::result::Result<Option<Self>, String> {
        fn optional(name: &str) -> std::result::Result<Option<String>, String> {
            match env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(format!("{name}: {error}")),
            }
        }
        let target = optional("STREAMR_CAPTURE_IDLE_SOURCE_ROW_TARGET")?;
        let seconds = optional("STREAMR_CAPTURE_IDLE_SECONDS")?;
        let pre_rows = optional("STREAMR_CAPTURE_IDLE_MIN_PRE_ROWS")?;
        let max_bytes = optional("STREAMR_CAPTURE_IDLE_MAX_BYTES")?;
        let batch = optional("STREAMR_TEST_SOURCE_BATCH_ROWS")?;
        let pre_match = CaptureIdlePreMatch::parse(
            optional("STREAMR_CAPTURE_IDLE_PRE_MATCH_POINTER")?.as_deref(),
            optional("STREAMR_CAPTURE_IDLE_PRE_MATCH_VALUE")?.as_deref(),
        )?;
        let mut idle = Self::parse(
            target.as_deref(),
            seconds.as_deref(),
            pre_rows.as_deref(),
            max_bytes.as_deref(),
            batch.as_deref(),
            capture.input_rows_before_checkpoint,
            test_runtime_timeout(),
        )?;
        match idle.as_mut() {
            Some(idle) => idle.pre_match = pre_match,
            None if pre_match.is_some() => {
                return Err("idle pre-match requires an idle capture".into());
            }
            None => {}
        }
        Ok(idle)
    }

    fn parse(
        target: Option<&str>,
        seconds: Option<&str>,
        pre_rows: Option<&str>,
        max_bytes: Option<&str>,
        source_batch_rows: Option<&str>,
        checkpoint_prefix: i32,
        runtime_timeout: Duration,
    ) -> std::result::Result<Option<Self>, String> {
        const TARGET: &str = "STREAMR_CAPTURE_IDLE_SOURCE_ROW_TARGET";
        const SECONDS: &str = "STREAMR_CAPTURE_IDLE_SECONDS";
        const PRE_ROWS: &str = "STREAMR_CAPTURE_IDLE_MIN_PRE_ROWS";
        const MAX_BYTES: &str = "STREAMR_CAPTURE_IDLE_MAX_BYTES";
        let (Some(target), Some(seconds)) = (target, seconds) else {
            if target.is_some() || seconds.is_some() || pre_rows.is_some() || max_bytes.is_some() {
                return Err(format!(
                    "{TARGET}, {SECONDS}, {PRE_ROWS}, and {MAX_BYTES} must be set together"
                ));
            }
            return Ok(None);
        };
        let Some(pre_rows) = pre_rows else {
            return Err(format!("{PRE_ROWS} is required for idle capture"));
        };
        let Some(max_bytes) = max_bytes else {
            return Err(format!("{MAX_BYTES} is required for idle capture"));
        };
        fn decimal<T: std::str::FromStr>(name: &str, raw: &str) -> std::result::Result<T, String> {
            if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(format!("{name} must be an unsigned decimal integer"));
            }
            raw.parse()
                .map_err(|_| format!("{name} is outside the supported integer range"))
        }
        let source_row_target: i32 = decimal(TARGET, target)?;
        let idle_seconds: u64 = decimal(SECONDS, seconds)?;
        let min_pre_rows: usize = decimal(PRE_ROWS, pre_rows)?;
        let max_output_bytes: usize = decimal(MAX_BYTES, max_bytes)?;
        if source_row_target <= checkpoint_prefix {
            return Err(format!("{TARGET} must exceed the checkpoint input prefix"));
        }
        if idle_seconds == 0 || idle_seconds > 120 {
            return Err(format!("{SECONDS} must be in 1..=120"));
        }
        if min_pre_rows == 0 {
            return Err(format!("{PRE_ROWS} must be positive"));
        }
        if max_output_bytes == 0 || max_output_bytes > 64 * 1024 * 1024 {
            return Err(format!("{MAX_BYTES} must be in 1..=67108864"));
        }
        let duration = Duration::from_secs(idle_seconds);
        if duration > runtime_timeout {
            return Err(format!("{SECONDS} must fit within the runtime timeout"));
        }
        if source_batch_rows != Some("1") {
            return Err("STREAMR_TEST_SOURCE_BATCH_ROWS must be 1 for idle capture".into());
        }
        Ok(Some(Self {
            source_row_target,
            duration,
            min_pre_rows,
            max_output_bytes,
            pre_match: None,
        }))
    }

    /// Single-file sources read the first available row without a NoOp.
    fn additional_noops(&self, already_read: i32) -> i32 {
        assert!(already_read > 0 && already_read <= self.source_row_target);
        self.source_row_target - already_read
    }
}

async fn advance_idle_target(engine: &RunningEngine, noops: i32) {
    let sources = engine.source_controls();
    assert_eq!(sources.len(), 1, "idle capture requires one source");
    for _ in 0..noops {
        sources[0]
            .send(ControlMessage::NoOp)
            .await
            .expect("source ended before idle row target");
    }
}

fn check_idle_response(response: Option<ControlResp>) {
    match response {
        Some(ControlResp::TaskFailed { error, .. }) => {
            panic!("worker failed during idle hold: {error:?}")
        }
        Some(ControlResp::Error {
            message, details, ..
        }) => {
            panic!("worker error during idle hold: {message}: {details}")
        }
        Some(ControlResp::TaskFinished {
            task_id,
            subtask_idx,
        }) => {
            panic!("task {task_id}/{subtask_idx} finished during idle hold")
        }
        Some(_) => {}
        None => panic!("control channel closed during idle hold"),
    }
}

async fn hold_capture_idle(
    control_rx: &mut Receiver<ControlResp>,
    phase: &str,
    idle: &CaptureIdle,
) {
    let started = tokio::time::Instant::now();
    let deadline = started + idle.duration;
    println!(
        "CAPTURE_IDLE phase={phase} event=start source_position={} duration_ms={}",
        idle.source_row_target,
        idle.duration.as_millis()
    );
    loop {
        tokio::select! {
            response = control_rx.recv() => check_idle_response(response),
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    while let Ok(response) = control_rx.try_recv() {
        check_idle_response(Some(response));
    }
    assert!(
        !control_rx.is_closed(),
        "control channel closed during idle hold"
    );
    println!(
        "CAPTURE_IDLE phase={phase} event=end source_position={} elapsed_ms={}",
        idle.source_row_target,
        started.elapsed().as_millis()
    );
}

/// Snapshot complete sink rows while the control-waiting source remains live.
/// The single-file sink writes directly to a tokio File, without a BufWriter.
/// The caller compares these generic before/after byte prefixes with its own
/// value oracle; row count alone does not establish which input was processed.
fn complete_idle_jsonl_rows(bytes: &[u8]) -> Option<(usize, Value)> {
    if !bytes.ends_with(b"\n") {
        return None;
    }
    let mut rows = 0;
    let mut last = None;
    for line in bytes[..bytes.len() - 1].split(|&byte| byte == b'\n') {
        assert!(
            !line.is_empty(),
            "idle output contains a blank JSONL record"
        );
        let value: Value =
            serde_json::from_slice(line).expect("idle output contains malformed JSONL");
        assert!(value.is_object(), "idle output JSONL row must be an object");
        rows += 1;
        last = Some(value);
    }
    Some((rows, last.expect("complete JSONL has at least one row")))
}

async fn capture_idle_output(
    output_path: &Path,
    phase: &str,
    boundary: &str,
    minimum_rows: usize,
    max_bytes: usize,
    pre_match: Option<&CaptureIdlePreMatch>,
) -> (usize, Vec<u8>) {
    // Four snapshots across the two phases must fit inside the outer 4x
    // runtime timeout alongside the two bounded idle holds.
    let deadline = tokio::time::Instant::now() + test_runtime_timeout() / 4;
    loop {
        match File::open(output_path).await {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(u64::try_from(max_bytes).unwrap() + 1)
                    .read_to_end(&mut bytes)
                    .await
                    .expect("cannot read idle output");
                assert!(
                    bytes.len() <= max_bytes,
                    "idle output exceeds configured byte limit"
                );
                // A sink write may be between its value and newline.
                if let Some((rows, last)) = complete_idle_jsonl_rows(&bytes)
                    && rows >= minimum_rows
                    && pre_match.is_none_or(|expected| expected.matches(&last))
                {
                    let snapshot =
                        output_path.with_extension(format!("idle-{phase}-{boundary}.jsonl"));
                    tokio::fs::write(&snapshot, &bytes)
                        .await
                        .expect("failed to write idle output snapshot");
                    println!(
                        "CAPTURE_IDLE_OUTPUT phase={phase} boundary={boundary} rows={rows} bytes={} path={}",
                        bytes.len(),
                        snapshot.display()
                    );
                    return (rows, bytes);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot read idle output {}: {error}", output_path.display()),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle output did not reach {minimum_rows} complete rows before timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn capture_counts_require_explicit_bounded_parameters() {
    let parse = |input: &str, rows: &str, checkpoint_rows: &str, epoch: &str| {
        CaptureCounts::parse(
            |name| {
                Ok(match name {
                    "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT" => input,
                    "STREAMR_CAPTURE_EXPECTED_ROWS" => rows,
                    "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS" => checkpoint_rows,
                    "STREAMR_CAPTURE_CHECKPOINT_EPOCH" => epoch,
                    _ => unreachable!(),
                }
                .to_owned())
            },
            None,
        )
    };
    assert_eq!(
        parse("7", "3", "2", "9").unwrap(),
        CaptureCounts {
            input_rows_before_checkpoint: 7,
            expected_rows: 3,
            expected_initial_rows: 3,
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
    assert!(CaptureCounts::parse(|name| Err(format!("missing {name}")), None).is_err());
    let with_initial = |initial| {
        CaptureCounts::parse(
            |name| {
                Ok(match name {
                    "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT" => "2",
                    "STREAMR_CAPTURE_EXPECTED_ROWS" => "3",
                    "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS" => "1",
                    "STREAMR_CAPTURE_CHECKPOINT_EPOCH" => "1",
                    _ => unreachable!(),
                }
                .to_owned())
            },
            Some(initial),
        )
    };
    assert_eq!(with_initial("1").unwrap().expected_initial_rows, 1);
    assert_eq!(with_initial("0").unwrap().expected_initial_rows, 0);
    for invalid in ["", "-1", "+1", " 1", "1.5", "18446744073709551616"] {
        assert!(with_initial(invalid).is_err());
    }
}

#[test]
fn idle_capture_requires_one_row_batches_and_bounded_complete_configuration() {
    let parse = |target, seconds, pre_rows, max_bytes, batch, timeout| {
        CaptureIdle::parse(
            target,
            seconds,
            pre_rows,
            max_bytes,
            batch,
            4,
            Duration::from_secs(timeout),
        )
    };
    assert_eq!(parse(None, None, None, None, None, 120).unwrap(), None);
    assert_eq!(
        parse(
            Some("5"),
            Some("120"),
            Some("2"),
            Some("1024"),
            Some("1"),
            120
        )
        .unwrap(),
        Some(CaptureIdle {
            source_row_target: 5,
            duration: Duration::from_secs(120),
            min_pre_rows: 2,
            max_output_bytes: 1024,
            pre_match: None,
        })
    );
    assert!(parse(Some("5"), None, Some("2"), Some("1024"), Some("1"), 120).is_err());
    assert!(parse(None, Some("1"), Some("2"), Some("1024"), Some("1"), 120).is_err());
    assert!(parse(Some("5"), Some("1"), None, Some("1024"), Some("1"), 120).is_err());
    assert!(parse(Some("5"), Some("1"), Some("2"), None, Some("1"), 120).is_err());
    assert!(parse(None, None, Some("1"), Some("1024"), None, 120).is_err());
    assert!(
        parse(
            Some("4"),
            Some("1"),
            Some("2"),
            Some("1024"),
            Some("1"),
            120
        )
        .is_err()
    );
    assert!(
        parse(
            Some("5"),
            Some("0"),
            Some("2"),
            Some("1024"),
            Some("1"),
            120
        )
        .is_err()
    );
    assert!(
        parse(
            Some("5"),
            Some("121"),
            Some("2"),
            Some("1024"),
            Some("1"),
            600
        )
        .is_err()
    );
    assert!(
        parse(
            Some("5"),
            Some("11"),
            Some("2"),
            Some("1024"),
            Some("1"),
            10
        )
        .is_err()
    );
    assert!(parse(Some("5"), Some("1"), Some("2"), Some("0"), Some("1"), 120).is_err());
    assert!(
        parse(
            Some("5"),
            Some("1"),
            Some("2"),
            Some("67108865"),
            Some("1"),
            120
        )
        .is_err()
    );
    assert!(
        parse(
            Some("5"),
            Some("1"),
            Some("2"),
            Some("1024"),
            Some("8"),
            120
        )
        .is_err()
    );
    assert!(parse(Some("5"), Some("1"), Some("2"), Some("1024"), None, 120).is_err());
    for invalid in ["", "-1", "+5", "5x", "2147483648"] {
        assert!(
            parse(
                Some(invalid),
                Some("1"),
                Some("2"),
                Some("1024"),
                Some("1"),
                120
            )
            .is_err()
        );
    }
    for invalid in ["", "-1", "+1", "1.5", "18446744073709551616"] {
        assert!(
            parse(
                Some("5"),
                Some(invalid),
                Some("2"),
                Some("1024"),
                Some("1"),
                120
            )
            .is_err()
        );
        assert!(
            parse(
                Some("5"),
                Some("1"),
                Some(invalid),
                Some("1024"),
                Some("1"),
                120
            )
            .is_err()
        );
        assert!(
            parse(
                Some("5"),
                Some("1"),
                Some("2"),
                Some(invalid),
                Some("1"),
                120
            )
            .is_err()
        );
    }
}

#[test]
fn idle_control_counts_account_for_automatic_first_and_restored_suffix_rows() {
    let idle = CaptureIdle {
        source_row_target: 9,
        duration: Duration::from_secs(1),
        min_pre_rows: 1,
        max_output_bytes: 1024,
        pre_match: None,
    };
    assert_eq!(idle.additional_noops(1), 8); // initial reads row 1
    assert_eq!(idle.additional_noops(5), 4); // prefix 4, restore reads row 5
    assert_eq!(idle.additional_noops(9), 0); // target is first suffix row
}

#[test]
fn idle_snapshot_requires_complete_object_jsonl_rows() {
    assert_eq!(
        complete_idle_jsonl_rows(b"{\"x\":1}\n{\"x\":2}\n"),
        Some((2, serde_json::json!({"x": 2})))
    );
    assert_eq!(complete_idle_jsonl_rows(b"{\"x\":1}"), None);
    for invalid in [
        b"{\"x\":1}\n\n".as_slice(),
        b"{\"x\":1}\n[]\n".as_slice(),
        b"{\"x\":1}\n42\n".as_slice(),
    ] {
        assert!(std::panic::catch_unwind(|| complete_idle_jsonl_rows(invalid)).is_err());
    }
}

#[test]
fn idle_pre_match_requires_paired_bounded_json_pointer_and_value() {
    let parse = CaptureIdlePreMatch::parse;
    assert_eq!(parse(None, None).unwrap(), None);
    assert!(parse(Some("/after"), None).is_err());
    assert!(parse(None, Some("null")).is_err());
    assert!(parse(Some("after"), Some("null")).is_err());
    assert!(parse(Some("/after/~2"), Some("null")).is_err());
    assert!(parse(Some("/after/~"), Some("null")).is_err());
    assert!(parse(Some("/after"), Some("not JSON")).is_err());
    assert!(parse(Some(&format!("/{}", "x".repeat(1024))), Some("null")).is_err());
    assert!(parse(Some("/after"), Some(&" ".repeat(4097))).is_err());
    let escaped = parse(Some("/a~1b/~0"), Some("null")).unwrap().unwrap();
    assert!(escaped.matches(&serde_json::json!({"a/b": {"~": null}})));
    assert!(!escaped.matches(&serde_json::json!({"a/b": {}})));
    let root = parse(Some(""), Some("{\"x\":1}")).unwrap().unwrap();
    assert!(root.matches(&serde_json::json!({"x": 1})));
}

#[test]
fn idle_pre_match_checks_latest_complete_object_row() {
    let expected = CaptureIdlePreMatch::parse(Some("/after"), Some("{\"x\":2}"))
        .unwrap()
        .unwrap();
    let (rows, latest) =
        complete_idle_jsonl_rows(b"{\"after\":{\"x\":1}}\n{\"after\":{\"x\":2}}\n").unwrap();
    assert_eq!(rows, 2);
    assert!(expected.matches(&latest));
    let (_, latest) =
        complete_idle_jsonl_rows(b"{\"after\":{\"x\":2}}\n{\"after\":{\"x\":1}}\n").unwrap();
    assert!(!expected.matches(&latest));
    let null = CaptureIdlePreMatch::parse(Some("/after"), Some("null"))
        .unwrap()
        .unwrap();
    assert!(null.matches(&serde_json::json!({"after": null})));
    assert!(!null.matches(&serde_json::json!({})));
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

#[path = "smoke_schedule_tests.rs"]
mod smoke_schedule_tests;

fn validate_capture_source_count(count: usize, requires_single: bool) -> Result<()> {
    anyhow::ensure!((1..=8).contains(&count), "capture requires 1..=8 sources");
    anyhow::ensure!(
        count == 1 || !requires_single,
        "idle and schedule capture require exactly one source"
    );
    Ok(())
}

#[test]
fn capture_source_count_is_bounded_and_idle_schedule_remain_single_source() {
    for count in [1, 2, 8] {
        validate_capture_source_count(count, false).unwrap();
    }
    for count in [0, 9, usize::MAX] {
        assert!(validate_capture_source_count(count, false).is_err());
    }
    validate_capture_source_count(1, true).unwrap();
    for count in [0, 2, 8] {
        assert!(validate_capture_source_count(count, true).is_err());
    }
}

async fn external_sql_checkpoint_capture_inner() {
    let capture = CaptureCounts::from_env().expect("invalid external SQL capture configuration");
    let idle = CaptureIdle::from_env(&capture).expect("invalid external SQL idle configuration");
    let schedule = smoke_schedule_tests::CaptureSchedule::from_env()
        .expect("invalid external SQL initial schedule");
    assert!(
        idle.is_none() || schedule.is_none(),
        "idle and initial schedule captures are mutually exclusive"
    );
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
    validate_capture_source_count(sources.len(), idle.is_some() || schedule.is_some())
        .expect("invalid capture source count");
    println!(
        "CAPTURE_SOURCE_PREFIX sources={} rows_per_source={}",
        sources.len(),
        capture.input_rows_before_checkpoint
    );
    // The checkpoint prefix is applied independently to every source. All
    // sources must remain control-waiting until the common stopping barrier.
    for (operator, _) in &sources {
        let source: arroyo_rpc::grpc::api::ConnectorOp =
            prost::Message::decode(operator.operator_config.as_slice())
                .expect("capture source config must decode");
        assert_eq!(
            source.connector, "single_file",
            "capture requires a single-file source"
        );
        let source_config: arroyo_rpc::OperatorConfig = serde_json::from_str(&source.config)
            .expect("capture source connector config must decode");
        assert!(
            source_config
                .table
                .get("wait_for_control")
                .is_none_or(|value| value.is_null() || value.as_bool() == Some(true)),
            "capture source must wait for control after each input row"
        );
        if let Some(schedule) = &schedule {
            schedule
                .validate_input(
                    source_config
                        .table
                        .get("path")
                        .and_then(Value::as_str)
                        .expect("scheduled single-file source must have a path"),
                )
                .await
                .expect("invalid scheduled source input");
        }
    }
    let job_id = format!(
        "external-sql-capture-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let initial_path = output_path.with_extension("initial.jsonl");
    let mut stale_paths = vec![output_path.clone(), initial_path.clone()];
    if idle.is_some() {
        for phase in ["initial", "recovered"] {
            for boundary in ["before", "after"] {
                stale_paths
                    .push(output_path.with_extension(format!("idle-{phase}-{boundary}.jsonl")));
            }
        }
    }
    for path in &stale_paths {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot clear capture {}: {error}", path.display()),
        }
    }
    let (control_tx, mut control_rx) = channel(128);
    let program = local_program(&job_id, &logical.graph, &udfs, None, control_tx).await;
    crate::event_clock_probe::install(&program, &output_path, "initial");
    let initial_engine = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap();
    let initial_started = tokio::time::Instant::now();
    let running = initial_engine.start().await;
    if let Some(schedule) = &schedule {
        schedule
            .run(&running, &mut control_rx, &output_path, initial_started)
            .await;
    }
    if let Some(idle) = &idle {
        advance_idle_target(&running, idle.additional_noops(1)).await;
        let (before_rows, before_bytes) = capture_idle_output(
            &output_path,
            "initial",
            "before",
            idle.min_pre_rows,
            idle.max_output_bytes,
            idle.pre_match.as_ref(),
        )
        .await;
        hold_capture_idle(&mut control_rx, "initial", idle).await;
        let (after_rows, after_bytes) = capture_idle_output(
            &output_path,
            "initial",
            "after",
            before_rows,
            idle.max_output_bytes,
            None,
        )
        .await;
        assert!(after_bytes.starts_with(&before_bytes) && after_rows >= before_rows);
    }
    run_until_finished(&running, &mut control_rx).await;
    let initial_rows = capture_rows(
        &output_path,
        capture.expected_initial_rows,
        "STREAMR_CAPTURE_MAX_INITIAL_ROWS",
    )
    .await;
    tokio::fs::rename(&output_path, &initial_path)
        .await
        .unwrap();
    println!(
        "CAPTURE_RESULT phase=initial rows={} path={}",
        initial_rows,
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
    crate::event_clock_probe::install(&program, &output_path, "checkpoint");
    let running = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    // A control-waiting single-file source reads its first row immediately;
    // configured NoOps advance the remaining input rows. The barrier flushes
    // a partial source batch; output cardinality need not match input cardinality.
    assert_eq!(running.source_controls().len(), sources.len());
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
    let checkpoint_rows = capture_rows(
        &output_path,
        capture.expected_checkpoint_rows,
        "STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS",
    )
    .await;
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
    crate::event_clock_probe::install(&program, &output_path, "recovered");
    let restored = Engine::for_local(program, "pipe-test".into(), job_id.clone())
        .await
        .unwrap()
        .start()
        .await;
    if let Some(idle) = &idle {
        // Restore reads the first suffix row without a NoOp. The configured
        // target is an absolute source row count across the checkpoint.
        advance_idle_target(
            &restored,
            idle.additional_noops(capture.input_rows_before_checkpoint + 1),
        )
        .await;
        let (before_rows, before_bytes) = capture_idle_output(
            &output_path,
            "recovered",
            "before",
            idle.min_pre_rows,
            idle.max_output_bytes,
            idle.pre_match.as_ref(),
        )
        .await;
        hold_capture_idle(&mut control_rx, "recovered", idle).await;
        let (after_rows, after_bytes) = capture_idle_output(
            &output_path,
            "recovered",
            "after",
            before_rows,
            idle.max_output_bytes,
            None,
        )
        .await;
        assert!(after_bytes.starts_with(&before_bytes) && after_rows >= before_rows);
    }
    run_until_finished(&restored, &mut control_rx).await;
    let recovered_rows = capture_rows(
        &output_path,
        capture.expected_rows,
        "STREAMR_CAPTURE_MAX_ROWS",
    )
    .await;
    println!(
        "CAPTURE_RESULT phase=recovered checkpoint={} input_rows_before_checkpoint={} committed_rows={} rows={} bytes={checkpoint_bytes} path={} job={job_id}",
        capture.checkpoint_epoch,
        capture.input_rows_before_checkpoint,
        checkpoint_rows,
        recovered_rows,
        output_path.display()
    );
}

fn capture_max_rows(expected: usize, configured: Option<&str>, name: &str) -> usize {
    let Some(configured) = configured else {
        return expected;
    };
    assert!(
        !configured.is_empty() && configured.bytes().all(|byte| byte.is_ascii_digit()),
        "{name} must be an unsigned decimal integer"
    );
    let max: usize = configured
        .parse()
        .expect("capture row maximum is out of range");
    assert!(
        max >= expected,
        "{name} must be at least the expected row minimum"
    );
    max
}

#[test]
fn capture_max_rows_defaults_to_exact_and_rejects_invalid_ranges() {
    assert_eq!(capture_max_rows(2, None, "test"), 2);
    assert_eq!(capture_max_rows(2, Some("4"), "test"), 4);
    assert_eq!(capture_max_rows(0, None, "test"), 0);
    assert_eq!(capture_max_rows(0, Some("0"), "test"), 0);
    assert_eq!(capture_max_rows(0, Some("2"), "test"), 2);
    assert!(std::panic::catch_unwind(|| capture_max_rows(2, Some("1"), "test")).is_err());
    assert!(std::panic::catch_unwind(|| capture_max_rows(2, Some("1x"), "test")).is_err());
}

async fn capture_rows(path: &Path, expected: usize, max_name: &str) -> usize {
    let configured_max = match env::var(max_name) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(error) => panic!("{max_name}: {error}"),
    };
    let max = capture_max_rows(expected, configured_max.as_deref(), max_name);
    let file = File::open(path)
        .await
        .expect("external SQL capture file missing");
    let mut reader = BufReader::new(file);
    let mut row = String::new();
    let mut count = 0usize;
    loop {
        row.clear();
        if reader
            .read_line(&mut row)
            .await
            .expect("external SQL capture read failed")
            == 0
        {
            break;
        }
        count = count
            .checked_add(1)
            .expect("external SQL capture row count overflow");
        let value: Value =
            serde_json::from_str(&row).expect("external SQL capture contains invalid JSON");
        assert!(
            value.is_object(),
            "external SQL capture must contain JSON objects"
        );
    }
    assert!(
        (expected..=max).contains(&count),
        "unexpected capture row count in {}: {count} outside {expected}..={max}",
        path.display()
    );
    count
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

fn configure_test_worker() {
    config::config();
    config::update(|c| {
        // reduce the batch size to increase consistency
        c.pipeline.source_batch_size = 32;
        if env::var("STREAMR_CAPTURE_EVENT_CLOCK_PROBE").as_deref() == Ok("1") {
            // Keep focused clock fixture batches deterministic across backend startup.
            // Existing source barriers and EOF still flush partial batches.
            c.pipeline.source_batch_linger = Duration::from_secs(3600).into();
        }
        if let Ok(seconds) = std::env::var("STREAMR_TEST_AGGREGATE_FLUSH_SECONDS") {
            let seconds: u64 = seconds.parse().expect("invalid aggregate flush interval");
            assert!(seconds > 0, "aggregate flush interval must be positive");
            c.pipeline.update_aggregate_flush_interval = Duration::from_secs(seconds).into();
        }
        if let Some(rows) = std::env::var_os("STREAMR_TEST_SOURCE_BATCH_ROWS") {
            let rows: usize = rows
                .to_str()
                .expect("source batch rows must be Unicode")
                .parse()
                .expect("invalid source batch rows");
            assert!(rows > 0, "source batch rows must be positive");
            c.pipeline.source_batch_size = rows;
        }
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
        if std::env::var("STREAMR_TEST_TYPED_SQL").as_deref() == Ok("1") {
            c.worker.execution_resources.get_or_insert(
                arroyo_rpc::config::ExecutionResourceConfig {
                    memory_bytes: 16 * 1024 * 1024,
                    max_batch_bytes: 1024 * 1024,
                },
            );
            c.worker.typed_sql_state = Some(arroyo_rpc::config::TypedSqlStateConfig {
                key_bytes: 4096,
                row_bytes: 24 * 1024,
                decoded_bytes: 64 * 1024,
                scope_bytes: 256 * 1024,
                scope_operations: 128,
                page_bytes: 128 * 1024,
                page_entries: 64,
                max_working_event_bytes: 256 * 1024,
                max_captured_event_bytes: 128 * 1024,
                max_pending_output_rows: 64,
                max_pending_output_bytes: 512 * 1024,
                max_resident_bytes: 8 * 1024 * 1024,
            });
            let resources = c.worker.live_state_resources.get_or_insert(
                arroyo_rpc::config::LiveStateResourceConfig {
                    block_cache_bytes: 8 * 1024 * 1024,
                    memtable_bytes: 4 * 1024 * 1024,
                    queued_write_bytes: 1024 * 1024,
                    decoded_value_bytes: 1024 * 1024,
                    scan_page_bytes: 2 * 1024 * 1024,
                    max_blocking_operations: 2,
                    max_snapshots: 2,
                    max_open_databases: 2,
                    disk_reserve_bytes: 64 * 1024 * 1024,
                },
            );
            // Typed paging reserves up to three decoded copies of each page,
            // in addition to the live event scope and captured output buffers.
            resources.decoded_value_bytes = 16 * 1024 * 1024;
            // The typed scope also admits backend copies and operation metadata.
            resources.queued_write_bytes = 2 * 1024 * 1024;
        }
        if std::env::var("STREAMR_TEST_NATIVE_AGGREGATES").as_deref() == Ok("1") {
            c.worker.execution_resources = Some(arroyo_rpc::config::ExecutionResourceConfig {
                memory_bytes: 16 * 1024 * 1024,
                max_batch_bytes: 8 * 1024 * 1024,
            });
            c.worker.aggregate_state = Some(arroyo_rpc::config::AggregateStateConfig {
                key_bytes: 512,
                value_bytes: 32 * 1024,
                page_bytes: 128 * 1024,
                page_entries: 64,
                write_bytes: 2 * 1024 * 1024,
                write_operations: 128,
                overlay_bytes: 2 * 1024 * 1024,
                max_pending_output_rows: 64,
                max_pending_output_bytes: 512 * 1024,
                max_resident_bytes: 128 * 1024 * 1024,
            });
            let resources = c.worker.live_state_resources.get_or_insert(
                arroyo_rpc::config::LiveStateResourceConfig {
                    block_cache_bytes: 8 * 1024 * 1024,
                    memtable_bytes: 4 * 1024 * 1024,
                    queued_write_bytes: 32 * 1024 * 1024,
                    decoded_value_bytes: 16 * 1024 * 1024,
                    scan_page_bytes: 2 * 1024 * 1024,
                    max_blocking_operations: 2,
                    max_snapshots: 2,
                    max_open_databases: 2,
                    disk_reserve_bytes: 64 * 1024 * 1024,
                },
            );
            // Two native owners can each admit a complete 2 MiB write scope.
            resources.queued_write_bytes = 32 * 1024 * 1024;
            resources.decoded_value_bytes = 16 * 1024 * 1024;
        }
        if std::env::var("STREAMR_TEST_NATIVE_WINDOWS").as_deref() == Ok("1") {
            c.worker.execution_resources.get_or_insert(
                arroyo_rpc::config::ExecutionResourceConfig {
                    memory_bytes: 16 * 1024 * 1024,
                    max_batch_bytes: 1024 * 1024,
                },
            );
            c.worker.window_state = Some(arroyo_rpc::config::WindowStateConfig {
                key_bytes: 512,
                partial_bytes: 32 * 1024,
                page_bytes: 128 * 1024,
                page_entries: 64,
                write_bytes: 512 * 1024,
                write_operations: 64,
                max_resident_bytes: 128 * 1024 * 1024,
            });
            let resources = c.worker.live_state_resources.get_or_insert(
                arroyo_rpc::config::LiveStateResourceConfig {
                    block_cache_bytes: 8 * 1024 * 1024,
                    memtable_bytes: 4 * 1024 * 1024,
                    queued_write_bytes: 4 * 1024 * 1024,
                    decoded_value_bytes: 16 * 1024 * 1024,
                    scan_page_bytes: 2 * 1024 * 1024,
                    max_blocking_operations: 2,
                    max_snapshots: 2,
                    max_open_databases: 2,
                    disk_reserve_bytes: 64 * 1024 * 1024,
                },
            );
            resources.queued_write_bytes = resources.queued_write_bytes.max(4 * 1024 * 1024);
            resources.decoded_value_bytes = 16 * 1024 * 1024;
        }
        // A composition fixture can run several independent native state
        // owners. Keep the ordinary two-owner defaults unless the test asks
        // for a larger shared worker admission limit explicitly.
        let positive_limit = |name: &str| -> Option<usize> {
            std::env::var(name).ok().map(|value| {
                let count: usize = value.parse().unwrap_or_else(|_| panic!("invalid {name}"));
                assert!(count > 0, "{name} must be positive");
                count
            })
        };
        let databases = positive_limit("STREAMR_TEST_MAX_OPEN_DATABASES");
        let snapshots = positive_limit("STREAMR_TEST_MAX_SNAPSHOTS");
        let scan_page_bytes = positive_limit("STREAMR_TEST_SCAN_PAGE_BYTES");
        let queued_write_bytes = positive_limit("STREAMR_TEST_QUEUED_WRITE_BYTES");
        if databases.is_some()
            || snapshots.is_some()
            || scan_page_bytes.is_some()
            || queued_write_bytes.is_some()
        {
            let resources = c
                .worker
                .live_state_resources
                .as_mut()
                .expect("test live-state limit requires configured live-state resources");
            if let Some(databases) = databases {
                resources.max_open_databases = databases;
            }
            if let Some(snapshots) = snapshots {
                resources.max_snapshots = snapshots;
            }
            if let Some(scan_page_bytes) = scan_page_bytes {
                resources.scan_page_bytes = scan_page_bytes;
            }
            if let Some(queued_write_bytes) = queued_write_bytes {
                resources.queued_write_bytes = queued_write_bytes;
            }
        }
    });
}

#[path = "smoke_fault_tests.rs"]
mod fault_tests;

#[path = "smoke_state_table_lifecycle.rs"]
mod smoke_state_table_lifecycle;
