use crate::logical::{LogicalEdgeType, LogicalGraph};
use petgraph::prelude::*;
use petgraph::visit::NodeRef;
use std::mem;

pub trait Optimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool;

    fn optimize(&self, plan: &mut LogicalGraph) {
        loop {
            if !self.optimize_once(plan) {
                break;
            }
        }
    }
}

pub struct ChainingOptimizer {}

fn remove_in_place<N, E>(graph: &mut DiGraph<N, E>, node: NodeIndex) {
    let incoming = graph.edges_directed(node, Incoming).next().unwrap();

    let parent = incoming.source().id();
    let incoming = incoming.id();
    graph.remove_edge(incoming);

    // DiGraph swap-removes edge indices. Re-read the current outgoing edge
    // after every mutation so rewiring cannot invalidate another saved ID.
    while let Some((edge, target)) = graph
        .edges_directed(node, Outgoing)
        .next()
        .map(|edge| (edge.id(), edge.target().id()))
    {
        let weight = graph.remove_edge(edge).unwrap();
        graph.add_edge(parent, target, weight);
    }

    graph.remove_node(node);
}

impl Optimizer for ChainingOptimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool {
        let node_indices: Vec<NodeIndex> = plan.node_indices().collect();

        for &node_idx in &node_indices {
            let cur = plan.node_weight(node_idx).unwrap();

            // sources can't be chained
            if cur.operator_chain.is_source() {
                continue;
            }

            let mut successors = plan.edges_directed(node_idx, Outgoing).collect::<Vec<_>>();

            if successors.len() != 1 {
                continue;
            }

            let edge = successors.remove(0);
            let edge_type = edge.weight().edge_type;

            if edge_type != LogicalEdgeType::Forward {
                continue;
            }

            let successor_idx = edge.target();

            let successor_node = plan.node_weight(successor_idx).unwrap();

            // skip if parallelism doesn't match or successor is a sink
            if cur.parallelism != successor_node.parallelism
                || successor_node.operator_chain.is_sink()
            {
                continue;
            }

            // skip successors with multiple predecessors
            if plan.edges_directed(successor_idx, Incoming).count() > 1 {
                continue;
            }

            // construct the new node
            let mut new_cur = cur.clone();

            new_cur.description = format!("{} -> {}", cur.description, successor_node.description);

            new_cur
                .operator_chain
                .operators
                .extend(successor_node.operator_chain.operators.clone());

            new_cur
                .operator_chain
                .edges
                .push(edge.weight().schema.clone());

            mem::swap(&mut new_cur, plan.node_weight_mut(node_idx).unwrap());

            // remove the old successor
            remove_in_place(plan, successor_idx);
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removing_chained_fanout_preserves_every_successor_and_edge_weight() {
        for reverse in [false, true] {
            let mut graph = DiGraph::new();
            let parent = graph.add_node("parent");
            let removed = graph.add_node("removed");
            let a = graph.add_node("a");
            let b = graph.add_node("b");
            graph.add_edge(parent, removed, "incoming");
            let outgoing = if reverse {
                [(b, "to-b"), (a, "to-a")]
            } else {
                [(a, "to-a"), (b, "to-b")]
            };
            for (target, weight) in outgoing {
                graph.add_edge(removed, target, weight);
            }

            remove_in_place(&mut graph, removed);

            let mut actual: Vec<_> = graph
                .edge_references()
                .map(|edge| (graph[edge.source()], graph[edge.target()], *edge.weight()))
                .collect();
            actual.sort_unstable();
            assert_eq!(
                actual,
                vec![("parent", "a", "to-a"), ("parent", "b", "to-b")],
                "fanout must survive either insertion order (reverse={reverse})"
            );
            assert_eq!(graph.node_count(), 3);
            assert_eq!(graph.edge_count(), 2);
        }
    }

    #[test]
    fn removing_chained_fanout_preserves_parallel_edge_weight_multiset() {
        for weights in [["first", "second"], ["second", "first"]] {
            let mut graph = DiGraph::new();
            let parent = graph.add_node("parent");
            let removed = graph.add_node("removed");
            let target = graph.add_node("target");
            graph.add_edge(parent, removed, "incoming");
            for weight in weights {
                graph.add_edge(removed, target, weight);
            }

            remove_in_place(&mut graph, removed);

            let mut actual: Vec<_> = graph
                .edge_references()
                .map(|edge| (graph[edge.source()], graph[edge.target()], *edge.weight()))
                .collect();
            actual.sort_unstable();
            assert_eq!(
                actual,
                vec![
                    ("parent", "target", "first"),
                    ("parent", "target", "second")
                ],
                "parallel edges retain independent weights"
            );
            assert_eq!(graph.node_count(), 2);
            assert_eq!(graph.edge_count(), 2);
        }
    }
}
