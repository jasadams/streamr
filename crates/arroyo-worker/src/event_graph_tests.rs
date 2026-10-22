use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_types::{
    SignalMessage,
    event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse},
};

fn schema() -> Arc<ArroyoSchema> {
    Arc::new(
        ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new(
                "_timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
        ])))
        .unwrap(),
    )
}
fn context() -> WorkerContext {
    WorkerContext {
        machine_id: MachineId(Arc::new("event-registration".into())),
        worker_id: WorkerId(11),
        pipeline_id: PipelineId(Arc::new("event-registration".into())),
        job_id: JobId(Arc::new("event-registration".into())),
        generation: 17,
    }
}
fn assignments() -> Vec<TaskAssignment> {
    [(2, 11), (3, 11), (20, 33), (30, 44)]
        .into_iter()
        .map(|(task_id, worker_id)| TaskAssignment {
            task_id,
            subtask_idx: 0,
            worker_id,
            worker_addr: "localhost:0".into(),
            worker_rpc: "http://localhost:0".into(),
        })
        .collect()
}
fn registration_program(
    capacity: u32,
) -> (
    Program,
    petgraph::graph::EdgeIndex,
    petgraph::graph::EdgeIndex,
) {
    let (commands, _command_rx) = channel::<ControlMessage>(16);
    let (responses, _response_rx) = channel::<ControlResp>(16);
    let mut graph = Graph::new();
    let nodes = [2, 3, 20, 30].map(|id| {
        let mut info = arroyo_types::get_test_task_info();
        info.operator_idx = id;
        info.task_index = 0;
        info.parallelism = 1;
        graph.add_node(SubtaskOrQueueNode::QueueNode(QueueNode {
            task_info: Arc::new(info),
            operator_ids: vec![format!("op-{id}")],
            tx: commands.clone(),
        }))
    });
    let spec = Arc::new(EventEdgeSpec {
        root_operator: 7,
        root_subtask: 0,
        schema: schema().schema.clone(),
        layout: EventLayout::Raw,
    });
    let edge = |src, dst, response| {
        let (tx, rx) = batch_bounded(capacity);
        PhysicalGraphEdge {
            edge_idx: 0,
            in_logical_idx: src,
            out_logical_idx: dst,
            schema: schema(),
            edge: LogicalEdgeType::Forward,
            tx: Some(tx),
            rx: Some(rx),
            response,
            response_wire: None,
        }
    };
    // The ordinary Quad 2->3 would collide if response task ids replaced wire indices.
    let response = graph.add_edge(nodes[0], nodes[1], edge(0, 1, Some(spec)));
    let ordinary = graph.add_edge(nodes[2], nodes[3], edge(2, 3, None));
    (
        Program {
            graph: Arc::new(RwLock::new(graph)),
            control_tx: Some(responses),
        },
        response,
        ordinary,
    )
}

#[tokio::test]
async fn event_engine_registration_uses_actual_assignments_without_renumbering_mixed_wire_quads()
-> Result<()> {
    let (program, response, ordinary) = registration_program(8);
    let engine = Engine::new(
        program,
        context(),
        NetworkManager::new(0).await?,
        assignments(),
    )?;
    let wire = {
        let graph = engine.program.graph.read().unwrap();
        assert!(graph[ordinary].response.is_none() && graph[ordinary].response_wire.is_none());
        graph[response].response_wire.clone().unwrap()
    };
    wire.require_quad(Quad {
        src_id: 0,
        src_idx: 0,
        dst_id: 1,
        dst_idx: 0,
    })?;
    assert!(
        wire.require_quad(Quad {
            src_id: 2,
            src_idx: 0,
            dst_id: 3,
            dst_idx: 0
        })
        .is_err()
    );
    let mut identity = Vec::new();
    wire.write_identity(&mut identity).await?;
    assert_eq!(
        identity,
        [
            17u64.to_le_bytes(),
            11u64.to_le_bytes(),
            11u64.to_le_bytes()
        ]
        .concat()
    );
    let signal = wire.encode_signal(&SignalMessage::Stop)?;
    identity.extend_from_slice(&signal);
    assert_eq!(
        wire.read_signal_payload(&mut identity.as_slice(), signal.len())
            .await?,
        SignalMessage::Stop
    );
    let packet = EventResponse {
        native_body: None,
        context: EventContext {
            version: EVENT_RESPONSE_VERSION,
            root_operator: 7,
            root_subtask: 0,
            attempt_nonce: [9; 16],
            sequence: 1,
            trigger_nanos: 5,
        },
        rows: None,
        touched_keys: None,
    };
    let encoded = wire.encode(&packet)?;
    let mut bytes = Vec::new();
    wire.write_payload(&mut bytes, &encoded).await?;
    assert_eq!(
        wire.read_payload(&mut bytes.as_slice(), 0).await?.context,
        packet.context
    );
    Ok(())
}

#[tokio::test]
async fn event_engine_registration_rejects_missing_duplicate_and_zero_capacity_before_scheduling()
-> Result<()> {
    for kind in [0, 1, 2, 3] {
        let (program, _, _) = registration_program(if kind == 2 { 0 } else { 8 });
        let mut tasks = assignments();
        if kind == 0 {
            tasks.remove(1);
        }
        if kind == 1 {
            tasks.push(tasks[0].clone());
        }
        if kind == 3 {
            tasks[1].worker_id = 22;
        }
        let error = match Engine::new(program, context(), NetworkManager::new(0).await?, tasks) {
            Err(error) => error,
            Ok(_) => panic!("invalid private registration must fail before scheduling"),
        };
        let diagnostic = if kind == 0 {
            "missing response task assignment"
        } else if kind == 1 {
            "duplicate response task assignment"
        } else if kind == 2 {
            "response queue capacity must be positive"
        } else {
            "private response edges require source and destination on the same worker"
        };
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
    Ok(())
}

#[tokio::test]
async fn event_engine_local_attempt_has_fresh_queues_and_old_identity_is_rejected() -> Result<()> {
    let (old_program, old_edge, _) = registration_program(8);
    let old = Engine::new(
        old_program,
        context(),
        NetworkManager::new(0).await?,
        assignments(),
    )?;
    let (new_program, new_edge, _) = registration_program(8);
    let mut next = context();
    next.generation = 18;
    let new = Engine::new(
        new_program,
        next,
        NetworkManager::new(0).await?,
        assignments(),
    )?;
    let old_wire = old.program.graph.read().unwrap()[old_edge]
        .response_wire
        .clone()
        .unwrap();
    let new_wire = new.program.graph.read().unwrap()[new_edge]
        .response_wire
        .clone()
        .unwrap();
    let mut bytes = Vec::new();
    old_wire.write_identity(&mut bytes).await?;
    // No Signal variable payload exists: failure must be fixed-identity mismatch.
    let error = new_wire
        .read_signal_payload(&mut bytes.as_slice(), 1)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("response execution assignment mismatch"),
        "{error}"
    );
    let tx = old.program.graph.read().unwrap()[old_edge]
        .tx
        .as_ref()
        .unwrap()
        .clone();
    tx.send(arroyo_types::ArrowMessage::Signal(SignalMessage::Stop))
        .await
        .unwrap();
    assert_eq!(
        old.program.graph.read().unwrap()[old_edge]
            .tx
            .as_ref()
            .unwrap()
            .capacity(),
        7
    );
    assert_eq!(
        new.program.graph.read().unwrap()[new_edge]
            .tx
            .as_ref()
            .unwrap()
            .capacity(),
        8
    );
    Ok(())
}

// Actual native construction tests deliberately require the same explicit native
// owner configuration as SQL integration. They do not mutate process-global config.
const SQL: &str = r#"
SET updating_ttl=NULL;
CREATE TABLE events(k TEXT NOT NULL,v BIGINT,eligible BOOLEAN NOT NULL,event_time TIMESTAMP NOT NULL,
 WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY)
 WITH(connector='single_file',path='/tmp/event-engine-registration-input.jsonl',format='json',type='source');
CREATE VIEW counts AS SELECT k,COUNT(*) AS total FROM events WHERE v>=0 GROUP BY k;
CREATE VIEW peaks AS SELECT k,MAX(v) AS peak FROM events WHERE eligible GROUP BY k;
CREATE VIEW coherent AS SELECT counts.k,counts.total,peaks.peak FROM counts LEFT JOIN peaks ON counts.k=peaks.k;
CREATE STATE TABLE totals(k TEXT PRIMARY KEY,total BIGINT,peak BIGINT) PARTITION BY k;
CREATE VIEW captured AS MERGE INTO totals AS previous USING coherent AS incoming ON previous.k=incoming.k
 WHEN MATCHED THEN UPDATE SET total=incoming.total,peak=incoming.peak
 WHEN NOT MATCHED THEN INSERT(k,total,peak) VALUES(incoming.k,incoming.total,incoming.peak)
 RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output WITH(connector='single_file',path='/tmp/event-engine-registration-output.jsonl',format='json',type='sink');
CREATE TABLE mirror WITH(connector='single_file',path='/tmp/event-engine-registration-mirror.jsonl',format='json',type='sink');
INSERT INTO output SELECT * FROM captured;
INSERT INTO mirror SELECT * FROM captured;
"#;
async fn native_program() -> Result<(Program, Vec<TaskAssignment>)> {
    ensure!(
        config().worker.aggregate_state.is_some()
            && config().worker.join_state.is_some()
            && config().worker.typed_sql_state.is_some(),
        "explicit native aggregate/JOIN/state-table limits required"
    );
    let compiled = arroyo_planner::parse_and_get_program(
        SQL,
        arroyo_planner::ArroyoSchemaProvider::new(),
        arroyo_planner::SqlConfig {
            default_parallelism: 1,
        },
    )
    .await?;
    let mut logical = compiled.program.graph;
    // Deliberately make actual task ids differ from ALL existing graph indices.
    for node in logical.node_weights_mut() {
        node.node_id += 1000;
        for (operator, _) in node.operator_chain.iter_mut() {
            match operator.operator_name {
                OperatorName::ExpressionWatermark => {
                    let mut cfg = api::ExpressionWatermarkConfig::decode(
                        operator.operator_config.as_slice(),
                    )?;
                    if let Some(id) = &mut cfg.response_root_operator {
                        *id += 1000;
                    }
                    operator.operator_config = cfg.encode_to_vec();
                }
                OperatorName::UpdatingAggregate => {
                    let mut cfg = api::UpdatingAggregateOperator::decode(
                        operator.operator_config.as_slice(),
                    )?;
                    if let Some(id) = &mut cfg.response_root_operator {
                        *id += 1000;
                    }
                    operator.operator_config = cfg.encode_to_vec();
                }
                OperatorName::Join => {
                    let mut cfg = api::JoinOperator::decode(operator.operator_config.as_slice())?;
                    if let Some(id) = &mut cfg.response_root_operator {
                        *id += 1000;
                    }
                    operator.operator_config = cfg.encode_to_vec();
                }
                _ => {}
            }
        }
    }
    let assignments = logical
        .node_weights()
        .map(|node| TaskAssignment {
            task_id: node.node_id,
            subtask_idx: 0,
            worker_id: 11,
            worker_addr: "localhost:0".into(),
            worker_rpc: "http://localhost:0".into(),
        })
        .collect::<Vec<_>>();
    let (control, _rx) = channel(128);
    let program = Program::from_logical(
        "native-event-graph",
        &logical,
        &assignments,
        new_registry(),
        None,
        None,
        CheckpointFilePathLayout::Legacy,
        control,
    )
    .await?;
    Ok((program, assignments))
}
#[tokio::test]
#[ignore = "explicit native SQL-state limits; run through maintained configured SQL qualification"]
async fn event_engine_actual_native_local_graph_binds_root_scalars_owners_and_join() -> Result<()> {
    let (program, assignments) = native_program().await?;
    let engine = Engine::new(
        program,
        context(),
        NetworkManager::new(0).await?,
        assignments,
    )?;
    assert_native_bound(&engine);
    Ok(())
}
#[tokio::test]
#[ignore = "explicit native SQL-state limits; run through maintained configured SQL qualification"]
async fn event_engine_actual_native_rejects_cross_worker_private_edge_before_tasks() -> Result<()> {
    let (program, mut assignments) = native_program().await?;
    let graph = program.graph.clone();
    // Choose an actual configured response edge, never alternating graph order.
    let destination = {
        let graph = graph.read().unwrap();
        let edge = graph
            .edge_references()
            .find(|edge| edge.weight().response.is_some())
            .context("actual native graph has no private edge")?;
        graph[edge.target()].id()
    };
    assignments
        .iter_mut()
        .find(|task| task.task_id == destination)
        .context("missing explicit destination assignment")?
        .worker_id = 22;
    let error = match Engine::new(
        program,
        context(),
        NetworkManager::new(0).await?,
        assignments,
    ) {
        Err(error) => error,
        Ok(_) => panic!("cross-worker private response must fail before tasks start"),
    };
    assert!(
        error
            .to_string()
            .contains("private response edges require source and destination on the same worker"),
        "{error}"
    );
    assert!(
        graph
            .read()
            .unwrap()
            .node_weights()
            .all(|node| matches!(node, SubtaskOrQueueNode::SubtaskNode(_))),
        "preflight must not schedule/take any native task"
    );
    Ok(())
}
fn assert_native_bound(engine: &Engine) {
    let graph = engine.program.graph.read().unwrap();
    let mut roles = Vec::new();
    let mut wires = 0;
    for node in graph.node_weights() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Chained(chain) = &node.node
        {
            roles.extend(
                chain
                    .iter()
                    .map(|(operator, _)| operator.event_response_role()),
            );
        }
    }
    assert_eq!(
        roles
            .iter()
            .filter(|role| matches!(role, EventResponseRole::Root(_)))
            .count(),
        1
    );
    assert_eq!(
        roles
            .iter()
            .filter(|role| matches!(role, EventResponseRole::Aggregate(_)))
            .count(),
        2
    );
    assert_eq!(
        roles
            .iter()
            .filter(|role| matches!(role, EventResponseRole::Join(_)))
            .count(),
        1
    );
    for edge in graph.edge_references() {
        if let Some(wire) = &edge.weight().response_wire {
            wires += 1;
            let quad = Quad {
                src_id: edge.weight().in_logical_idx,
                src_idx: graph[edge.source()].subtask_idx(),
                dst_id: edge.weight().out_logical_idx,
                dst_idx: graph[edge.target()].subtask_idx(),
            };
            wire.require_quad(quad).unwrap();
            assert_ne!(quad.src_id, graph[edge.source()].id() as usize);
        } else {
            assert!(edge.weight().response.is_none());
        }
    }
    assert!(
        wires >= 4,
        "actual shared root must fan out to both native aggregate owners then JOIN"
    );
}
