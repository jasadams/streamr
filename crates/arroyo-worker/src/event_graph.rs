//! Bind private response descriptors after physical construction, before tasks run.
//! Unconfigured ordinary components do not participate in the private route.
use super::*;
use anyhow::{Context, Result, ensure};
use arroyo_operator::operator::EventResponseRole;
use arroyo_types::event_edge::EventLayout;
use std::collections::HashSet;

type Graph = DiGraph<SubtaskOrQueueNode, PhysicalGraphEdge>;

pub(super) fn bind_constructed_graph(graph: &mut Graph) -> Result<()> {
    let roles: Vec<_> = graph
        .node_weights()
        .filter_map(|node| match node {
            SubtaskOrQueueNode::SubtaskNode(node) => match &node.node {
                OperatorNode::Chained(chain) => Some(
                    chain
                        .iter()
                        .map(|(operator, _)| operator.event_response_role())
                        .collect::<Vec<_>>(),
                ),
                OperatorNode::Source(_) => None,
            },
            _ => None,
        })
        .flatten()
        .collect();
    let roots: Vec<_> = roles
        .iter()
        .filter_map(|role| match role {
            EventResponseRole::Root(root) => Some(*root),
            _ => None,
        })
        .collect();
    let aggregate_roots: Vec<_> = roles
        .iter()
        .filter_map(|role| match role {
            EventResponseRole::Aggregate(root) => Some(*root),
            _ => None,
        })
        .collect();
    let join_roots: Vec<_> = roles
        .iter()
        .filter_map(|role| match role {
            EventResponseRole::Join(root) => Some(*root),
            _ => None,
        })
        .collect();
    let result_roots = roles
        .iter()
        .filter_map(|role| match role {
            EventResponseRole::Result(root) => Some(*root),
            _ => None,
        })
        .collect::<Vec<_>>();
    let unions = roles
        .iter()
        .filter(|role| matches!(role, EventResponseRole::Union))
        .count();
    if roots.is_empty()
        && aggregate_roots.is_empty()
        && join_roots.is_empty()
        && result_roots.is_empty()
        && unions == 0
    {
        return Ok(());
    }
    ensure!(
        roots.len() == 1
            && ((aggregate_roots.len() == 2
                && join_roots.len() == 1
                && result_roots.is_empty()
                && unions == 0)
                || ((1..=3).contains(&aggregate_roots.len())
                    && join_roots.is_empty()
                    && result_roots.len() == 1
                    && unions == usize::from(aggregate_roots.len() > 1))),
        "response wiring requires one shared root and a proven native JOIN or finite aggregate-result topology; independent ordinary components are unrestricted"
    );
    let root = roots[0];
    ensure!(
        aggregate_roots
            .iter()
            .chain(&join_roots)
            .chain(&result_roots)
            .all(|id| *id == root),
        "private response roles disagree on planned root id"
    );
    let order = petgraph::algo::toposort(&*graph, None)
        .map_err(|_| anyhow::anyhow!("response graph must be acyclic"))?;
    let mut root_location = None;
    let mut key_outputs =
        std::collections::HashMap::<NodeIndex, Option<Vec<arrow::datatypes::DataType>>>::new();
    for index in order {
        // Use exactly the queue input ordering in Engine::run_locally.
        let mut incoming = graph
            .edges_directed(index, Direction::Incoming)
            .map(|edge| {
                (
                    (edge.weight().edge, edge.weight().in_logical_idx),
                    edge.weight().response.clone(),
                    edge.weight().schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        incoming.sort_by_key(|(key, _, _)| *key);
        let count = incoming
            .iter()
            .filter(|(_, spec, _)| spec.is_some())
            .count();
        ensure!(
            count == 0 || count == incoming.len(),
            "mixed ordinary and response inputs are unsupported"
        );
        let mut current = incoming
            .iter()
            .filter_map(|(_, spec, _)| spec.clone())
            .collect::<Vec<_>>();
        let mut key_edges = graph
            .edges_directed(index, Direction::Incoming)
            .map(|edge| {
                (
                    (edge.weight().edge, edge.weight().in_logical_idx),
                    key_outputs.get(&edge.source()).cloned().flatten(),
                )
            })
            .collect::<Vec<_>>();
        key_edges.sort_by_key(|(key, _)| *key);
        let mut current_keys = key_edges
            .into_iter()
            .map(|(_, keys)| keys)
            .collect::<Vec<_>>();
        let outgoing = graph
            .edges_directed(index, Direction::Outgoing)
            .map(|edge| edge.id())
            .collect::<Vec<_>>();
        let node = match graph
            .node_weight_mut(index)
            .context("missing physical node")?
        {
            SubtaskOrQueueNode::SubtaskNode(node) => node,
            _ => anyhow::bail!("response binding must precede task scheduling"),
        };
        let parallelism = node.parallelism;
        let subtask_idx = node.subtask_idx;
        match &mut node.node {
            OperatorNode::Source(_) => {
                ensure!(current.is_empty(), "source cannot consume responses")
            }
            OperatorNode::Chained(chain) => {
                for (position, (operator, context)) in chain.iter_mut().enumerate() {
                    let role = operator.event_response_role();
                    let private = matches!(
                        role,
                        EventResponseRole::Root(_)
                            | EventResponseRole::Aggregate(_)
                            | EventResponseRole::Join(_)
                            | EventResponseRole::Result(_)
                            | EventResponseRole::Union
                    );
                    if !current.is_empty() || private {
                        ensure!(
                            parallelism == 1
                                && subtask_idx == 0
                                && context.task_info.parallelism == 1,
                            "private response execution is singleton"
                        );
                        if !matches!(
                            role,
                            EventResponseRole::Root(_)
                                | EventResponseRole::Join(_)
                                | EventResponseRole::Union
                        ) {
                            ensure!(
                                current.len() == 1
                                    && context.in_schemas.len() == 1
                                    && current[0].schema == context.in_schemas[0].schema,
                                "response input differs from actual constructed operator schema"
                            );
                        }
                        if matches!(role, EventResponseRole::Root(_)) {
                            ensure!(
                                root_location.replace((index, position)).is_none(),
                                "duplicate response root"
                            );
                        }
                        if matches!(role, EventResponseRole::Union) {
                            ensure!(
                                position == 0
                                    && (2..=3).contains(&incoming.len())
                                    && incoming
                                        .iter()
                                        .all(|(key, _, _)| key.0 == LogicalEdgeType::Forward),
                                "finite UNION must be a singleton chain head with registered Forward inputs"
                            );
                        }
                        if matches!(role, EventResponseRole::Join(_)) {
                            ensure!(
                                position == 0
                                    && incoming.len() == 2
                                    && incoming[0].0.0 == LogicalEdgeType::LeftJoin
                                    && incoming[1].0.0 == LogicalEdgeType::RightJoin,
                                "private JOIN must be a two-input chain head with one left and one right queue"
                            );
                        } else if !matches!(role, EventResponseRole::Root(_)) {
                            ensure!(
                                !current.is_empty(),
                                "configured response operator has no response input"
                            );
                        }
                        operator.bind_event_response_key_types(&current_keys)?;
                        if matches!(role, EventResponseRole::Union) {
                            ensure!(
                                current_keys.iter().all(|keys| keys == &current_keys[0])
                                    && current_keys[0].is_some(),
                                "UNION touched-key native layouts differ"
                            );
                        }
                        let keys = operator
                            .event_response_key_types()
                            .or_else(|| current_keys.first().cloned().flatten());
                        current = operator
                            .bind_event_response(&current, context)?
                            .into_iter()
                            .collect();
                        current_keys = if current.is_empty() {
                            Vec::new()
                        } else {
                            vec![keys]
                        };
                        if matches!(role, EventResponseRole::Root(_)) {
                            ensure!(
                                matches!(
                                    current.first().map(|spec| &spec.layout),
                                    Some(EventLayout::Raw)
                                ),
                                "root output must be raw response"
                            );
                        }
                    }
                }
            }
        }
        key_outputs.insert(index, current_keys.first().cloned().flatten());
        if let Some(spec) = current.first() {
            ensure!(
                current.len() == 1 && !outgoing.is_empty(),
                "response output must reach a configured consumer"
            );
            for edge_id in outgoing {
                let edge = graph.edge_weight_mut(edge_id).unwrap();
                ensure!(
                    edge.schema.schema == spec.schema,
                    "response fanout has inconsistent registered schemas"
                );
                ensure!(
                    matches!(
                        edge.edge,
                        LogicalEdgeType::Forward
                            | LogicalEdgeType::LeftJoin
                            | LogicalEdgeType::RightJoin
                    ),
                    "response edges cannot repartition"
                );
                ensure!(
                    edge.tx.as_ref().is_some_and(|tx| tx.size() > 0)
                        && edge.rx.as_ref().is_some_and(|rx| rx.size() > 0),
                    "response queue capacity must be positive"
                );
                edge.response = Some(spec.clone());
            }
        }
    }
    let (location, position) = root_location.context("configured response root not bound")?;
    validate_source_prefix(graph, location, position)?;
    Ok(())
}

/// Exact canonical expression provenance is checked by planner serialization.
/// Runtime independently refuses predicates/state/expansion before shared root.
fn validate_source_prefix(graph: &Graph, index: NodeIndex, root_position: usize) -> Result<()> {
    let node = match &graph[index] {
        SubtaskOrQueueNode::SubtaskNode(node) => node,
        _ => anyhow::bail!("source proof after scheduling"),
    };
    if let OperatorNode::Chained(chain) = &node.node {
        for (operator, _) in chain.iter().take(root_position) {
            operator.validate_event_source_plumbing()?;
        }
    }
    let mut current = index;
    let mut visited = HashSet::new();
    loop {
        ensure!(visited.insert(current), "cycle before response root");
        let incoming = graph
            .edges_directed(current, Direction::Incoming)
            .collect::<Vec<_>>();
        ensure!(
            incoming.len() == 1 && incoming[0].weight().edge == LogicalEdgeType::Forward,
            "response root requires one canonical source path"
        );
        current = incoming[0].source();
        let source = match &graph[current] {
            SubtaskOrQueueNode::SubtaskNode(node) => node,
            _ => anyhow::bail!("source proof after scheduling"),
        };
        ensure!(
            source.parallelism == 1 && source.subtask_idx == 0,
            "response source path must be singleton"
        );
        match &source.node {
            OperatorNode::Source(_) => {
                ensure!(
                    graph
                        .edges_directed(current, Direction::Incoming)
                        .next()
                        .is_none(),
                    "source has incoming queues"
                );
                return Ok(());
            }
            OperatorNode::Chained(chain) => {
                for (operator, _) in chain.iter() {
                    operator.validate_event_source_plumbing()?;
                }
            }
        }
    }
}

pub(super) fn bind_wires(
    graph: &mut Graph,
    generation: u64,
    assignments: &[TaskAssignment],
) -> Result<()> {
    // Keep ordinary and response frames in the SAME existing Quad namespace.
    let mut quads = HashMap::new();
    let ids = graph.edge_indices().collect::<Vec<_>>();
    // Validate all namespaces before mutating/registering any response descriptor.
    for id in &ids {
        let (source, target) = graph.edge_endpoints(*id).unwrap();
        let edge = &graph[*id];
        let quad = Quad {
            src_id: edge.in_logical_idx,
            src_idx: graph[source].subtask_idx(),
            dst_id: edge.out_logical_idx,
            dst_idx: graph[target].subtask_idx(),
        };
        let response = edge.response.is_some();
        if let Some(previous_response) = quads.insert(quad, response) {
            ensure!(
                !previous_response && !response,
                "duplicate response/ordinary wire Quad"
            );
        }
    }
    for id in ids {
        let (source, target) = graph.edge_endpoints(id).unwrap();
        let source = &graph[source];
        let target = &graph[target];
        let edge = &graph[id];
        let quad = Quad {
            src_id: edge.in_logical_idx,
            src_idx: source.subtask_idx(),
            dst_id: edge.out_logical_idx,
            dst_idx: target.subtask_idx(),
        };
        if let Some(spec) = &edge.response {
            ensure!(
                source.subtask_idx() == 0 && target.subtask_idx() == 0,
                "response endpoints must be singleton"
            );
            ensure!(
                edge.tx.as_ref().is_some_and(|tx| tx.size() > 0)
                    && edge.rx.as_ref().is_some_and(|rx| rx.size() > 0),
                "response queue capacity must be positive"
            );
            // Actual constructed owner limits are authoritative only when the
            // two endpoints run in this same worker configuration. Assignments
            // carry no remote quota authority; do not guess a remote limit.
            let source_worker = assignments
                .iter()
                .find(|task| {
                    task.task_id == source.id() && task.subtask_idx as usize == source.subtask_idx()
                })
                .context("missing response task assignment")?
                .worker_id;
            let destination_worker = assignments
                .iter()
                .find(|task| {
                    task.task_id == target.id() && task.subtask_idx as usize == target.subtask_idx()
                })
                .context("missing response task assignment")?
                .worker_id;
            ensure!(
                source_worker == destination_worker,
                "private response edges require source and destination on the same worker"
            );
            let wire = EventWireSpec::registered(
                generation,
                quad,
                [source.id(), target.id()],
                assignments,
                spec.clone(),
            )?;
            graph.edge_weight_mut(id).unwrap().response_wire = Some(Arc::new(wire));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "event_graph_tests.rs"]
mod tests;
