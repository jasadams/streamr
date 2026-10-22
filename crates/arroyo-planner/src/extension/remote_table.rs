use std::{fmt::Formatter, sync::Arc};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    grpc::api::ValuePlanOperator,
};
use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err, plan_err};

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    multifield_partial_ord,
    physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec},
};

use super::{ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const REMOTE_TABLE_NAME: &str = "RemoteTableExtension";

/* Lightweight extension that allows us to segment the graph and merge nodes with the same name.
  An Extension Planner will be used to isolate computation to individual nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: TableReference,
    pub(crate) schema: DFSchemaRef,
    pub(crate) materialize: bool,
    pub(crate) accepted_result: Option<crate::merge_event_results::AcceptedResultRoute>,
    pub(crate) union_schema: bool,
}

multifield_partial_ord!(
    RemoteTableExtension,
    input,
    name,
    materialize,
    accepted_result,
    union_schema
);

impl ArroyoExtension for RemoteTableExtension {
    fn response_root_slot(&self) -> Option<u32> {
        match self.accepted_result {
            Some(crate::merge_event_results::AcceptedResultRoute::Response(route)) => {
                Some(route.slot)
            }
            _ => None,
        }
    }
    fn bind_response_root(&self, operator: u32) -> Result<Option<Box<dyn ArroyoExtension>>> {
        let mut next = self.clone();
        if let Some(crate::merge_event_results::AcceptedResultRoute::Response(route)) =
            &mut next.accepted_result
        {
            route.operator = Some(operator);
        }
        Ok(Some(Box::new(next)))
    }
    fn validate_response_scope(&self) -> Result<()> {
        crate::merge_event_branches::memo_slot(&self.input).map(|_| ())
    }
    fn node_name(&self) -> Option<NamedNode> {
        if self.materialize {
            let slot = match crate::merge_event_branches::memo_slot(&self.input) {
                Ok(slot) => slot,
                // Both graph-entry paths validate before consulting this key;
                // an invalid route must never fall back to an ordinary name.
                Err(_) => return None,
            };
            Some(match slot {
                Some(slot) => NamedNode::EventResponseRemoteTable(slot, self.name.to_owned()),
                None => NamedNode::RemoteTable(self.name.to_owned()),
            })
        } else {
            None
        }
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        match input_schemas.len() {
            0 => return plan_err!("RemoteTableExtension should have exactly one input"),
            1 => {}
            _multiple_inputs => {
                // check they are all the same
                let first = input_schemas[0].clone();
                for schema in input_schemas.iter().skip(1) {
                    if *schema != first {
                        return plan_err!(
                            "If a node has multiple inputs, they must all have the same schema"
                        );
                    }
                }
            }
        }
        // A UNION has one graph edge per materialized branch. Its inputs are
        // already evaluated before reaching this node, so evaluating a
        // physical UnionExec over the incoming batch would emit that batch
        // once per branch. Decode one memory reader instead: the worker calls
        // this value operator once for each arriving branch batch.
        let physical_plan = if let LogicalPlan::Union(union) = &self.input {
            if input_schemas.len() != union.inputs.len() {
                return plan_err!("UNION consolidation must have one input per branch");
            }
            let output_schema = self.output_schema();
            if input_schemas[0].schema.as_ref() != output_schema.schema.as_ref() {
                return plan_err!("UNION consolidation input/output Arrow schemas differ");
            }
            Arc::new(ArroyoMemExec::new(
                "union_output".to_string(),
                output_schema.schema,
            )) as Arc<dyn datafusion::physical_plan::ExecutionPlan>
        } else {
            planner.sync_plan(&self.input)?
        };
        let physical_plan = if self.union_schema {
            if input_schemas.len() != 1 || self.accepted_result.is_some() {
                return plan_err!("native UNION schema boundary requires one branch SELECT input");
            }
            Arc::new(crate::physical::UnionSchemaExec::try_new(
                physical_plan,
                self.output_schema().schema,
            )?) as Arc<dyn datafusion::physical_plan::ExecutionPlan>
        } else {
            physical_plan
        };
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let response_union_inputs = if let LogicalPlan::Union(union) = &self.input {
            if crate::merge_event_branches::memo_slot(&self.input)?.is_some() {
                u32::try_from(union.inputs.len()).map_err(|_| {
                    datafusion::common::plan_datafusion_err!("too many response UNION inputs")
                })?
            } else {
                0
            }
        } else {
            0
        };
        let accepted_result = self
            .accepted_result
            .map(|route| {
                route.source(&self.input)?;
                if input_schemas.len() != 1 {
                    return plan_err!("accepted native SELECT requires one result input");
                }
                Ok(arroyo_rpc::grpc::api::AcceptedResultBoundary {
                    producer: Some(match route {
                        crate::merge_event_results::AcceptedResultRoute::Response(route) =>
                            arroyo_rpc::grpc::api::accepted_result_boundary::Producer::ResponseRootOperator(
                                route.operator.ok_or_else(|| datafusion::common::plan_datafusion_err!("accepted result root is not bound"))?),
                        crate::merge_event_results::AcceptedResultRoute::FinalizedAppend =>
                            arroyo_rpc::grpc::api::accepted_result_boundary::Producer::FinalizedAppend(true),
                    }),
                    input_schema: Some(input_schemas[0].as_ref().clone().into()),
                    output_schema: Some(self.output_schema().into()),
                })
            })
            .transpose()?;
        let config = ValuePlanOperator {
            accepted_result,
            response_union_inputs,
            name: format!("value_calculation({})", self.name),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node = LogicalNode::single(
            index as u32,
            format!("value_{index}"),
            OperatorName::ArrowValue,
            config.encode_to_vec(),
            self.name.to_string(),
            1,
        );

        let edges = input_schemas
            .into_iter()
            .map(|schema| LogicalEdge::project_all(LogicalEdgeType::Forward, (*schema).clone()))
            .collect();
        Ok(NodeWithIncomingEdges { node, edges })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for RemoteTableExtension {
    fn name(&self) -> &str {
        REMOTE_TABLE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }

        Ok(Self {
            input: inputs[0].clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            materialize: self.materialize,
            accepted_result: self.accepted_result,
            union_schema: self.union_schema,
        })
    }
}
