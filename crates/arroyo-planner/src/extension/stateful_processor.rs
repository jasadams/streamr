use super::{ArroyoExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};
use crate::{DFField, fields_with_qualifiers, schema_from_df_fields};
use arrow_schema::DataType;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{StateOpType, StateOperation, StatefulProcessorOperator};
use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use prost::Message;
use std::collections::HashSet;

pub(crate) const STATEFUL_PROCESSOR_EXTENSION_NAME: &str = "StatefulProcessorExtension";

/// Description of a single state operation for the planner.
///
/// Each op corresponds to one state function call (e.g. `state_get('map', key)`)
/// that was extracted from the projection by the rewriter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct StatefulOpDesc {
    pub map_name: String,
    /// `StateOpType` as i32 for proto encoding
    pub op_type: i32,
    /// The key expression evaluated against the input batch
    pub key_expr: Expr,
    /// The value expression (for put/upsert/update)
    pub value_expr: Option<Expr>,
    /// The condition expression (for update)
    pub condition_expr: Option<Expr>,
    /// Name of the output column appended by the operator
    pub output_field: String,
}

/// Logical plan node representing a stateful processing step.
///
/// The rewriter extracts state function calls from a projection, replaces them
/// with column references to `output_field` names, and wraps the input plan in
/// this extension. At physical planning time (`plan_node`), the op expressions
/// are serialized to physical expressions and encoded into the proto config.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StatefulProcessorExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) ops: Vec<StatefulOpDesc>,
    /// The projection expressions with state function calls replaced by column
    /// references to the operator's result columns.
    pub(crate) final_exprs: Vec<Expr>,
    /// The schema produced by the final projection.
    pub(crate) final_schema: DFSchemaRef,
}

crate::multifield_partial_ord!(StatefulProcessorExtension, input, ops, final_exprs);

impl UserDefinedLogicalNodeCore for StatefulProcessorExtension {
    fn name(&self) -> &str {
        STATEFUL_PROCESSOR_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.final_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::new();
        for op in &self.ops {
            exprs.push(op.key_expr.clone());
            if let Some(ref v) = op.value_expr {
                exprs.push(v.clone());
            }
            if let Some(ref c) = op.condition_expr {
                exprs.push(c.clone());
            }
        }
        exprs.extend(self.final_exprs.iter().cloned());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "StatefulProcessorExtension: ops={}, schema=[{}]",
            self.ops.len(),
            self.final_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "StatefulProcessorExtension requires exactly one input, found {}",
                inputs.len()
            );
        }

        let mut idx = 0;
        let mut new_ops = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            let key_expr = exprs[idx].clone();
            idx += 1;
            let value_expr = if op.value_expr.is_some() {
                let v = exprs[idx].clone();
                idx += 1;
                Some(v)
            } else {
                None
            };
            let condition_expr = if op.condition_expr.is_some() {
                let c = exprs[idx].clone();
                idx += 1;
                Some(c)
            } else {
                None
            };
            new_ops.push(StatefulOpDesc {
                map_name: op.map_name.clone(),
                op_type: op.op_type,
                key_expr,
                value_expr,
                condition_expr,
                output_field: op.output_field.clone(),
            });
        }
        let new_final_exprs = exprs[idx..].to_vec();

        Ok(Self {
            input: inputs[0].clone(),
            ops: new_ops,
            final_exprs: new_final_exprs,
            final_schema: self.final_schema.clone(),
        })
    }
}

impl ArroyoExtension for StatefulProcessorExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            return plan_err!(
                "StatefulProcessorExtension requires exactly one input schema, found {}",
                input_schemas.len()
            );
        }

        let input_schema = input_schemas[0].clone();
        // Use the logical plan's DFSchema for serializing op expressions -- it
        // preserves table qualifiers (e.g. `nexmark.bid`) that the Arrow schema
        // drops.  The Arrow-derived DFSchema is still used for final_exprs which
        // reference unqualified intermediate columns.
        let input_dfschema = self.input.schema().as_ref().clone();

        // Collect unique map names, namespaced to avoid collision with
        // internal table names used by other operators.
        let map_names: Vec<String> = {
            let mut seen = HashSet::new();
            self.ops
                .iter()
                .filter_map(|op| {
                    let prefixed = format!("__sp_{}", op.map_name);
                    if seen.insert(prefixed.clone()) {
                        Some(prefixed)
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Serialize state operation expressions against the input schema
        let operations: Vec<StateOperation> = self
            .ops
            .iter()
            .map(|op| {
                let key_bytes =
                    planner.serialize_as_physical_expr(&op.key_expr, &input_dfschema)?;

                let value_bytes = match &op.value_expr {
                    Some(expr) => planner.serialize_as_physical_expr(expr, &input_dfschema)?,
                    None => vec![],
                };

                let condition_bytes = match &op.condition_expr {
                    Some(expr) => planner.serialize_as_physical_expr(expr, &input_dfschema)?,
                    None => vec![],
                };

                Ok(StateOperation {
                    map_name: format!("__sp_{}", op.map_name),
                    op_type: op.op_type,
                    key_expr: key_bytes,
                    value_expr: value_bytes,
                    condition_expr: condition_bytes,
                    output_field: op.output_field.clone(),
                })
            })
            .collect::<Result<_>>()?;

        // Build the intermediate schema: input fields + one result column per op.
        // The operator appends these columns; final_exprs project them to the user schema.
        let mut intermediate_fields = fields_with_qualifiers(self.input.schema());
        for op in &self.ops {
            let dt = match StateOpType::try_from(op.op_type).unwrap_or(StateOpType::StateGet) {
                StateOpType::StateGet | StateOpType::StatePut | StateOpType::StateUpsert => {
                    DataType::Utf8
                }
                StateOpType::StateUpdate | StateOpType::StateDelete => DataType::Boolean,
            };
            intermediate_fields.push(DFField::new(None, &op.output_field, dt, true));
        }
        let intermediate_dfschema = schema_from_df_fields(&intermediate_fields)?;

        // Serialize final_exprs against the intermediate schema
        let final_exprs_bytes = self
            .final_exprs
            .iter()
            .map(|e| planner.serialize_as_physical_expr(e, &intermediate_dfschema))
            .collect::<Result<Vec<_>>>()?;

        let config = StatefulProcessorOperator {
            name: "StatefulProcessor".to_string(),
            operations,
            map_names,
            input_schema: Some((*input_schema).clone().into()),
            final_exprs: final_exprs_bytes,
        };

        let node = LogicalNode::single(
            index as u32,
            format!("stateful_processor_{index}"),
            OperatorName::StatefulProcessor,
            config.encode_to_vec(),
            "StatefulProcessor".to_string(),
            1,
        );

        let edge = LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema).clone());

        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_fields(
            self.final_schema
                .fields()
                .iter()
                .map(|f| (**f).clone())
                .collect(),
        )
    }
}
