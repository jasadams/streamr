use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::multifield_partial_ord;
use crate::schemas::add_timestamp_field;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::SOURCE_ENVELOPE_FIELD;
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::ExpressionWatermarkConfig;
use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;
use std::fmt::Formatter;
use std::sync::Arc;

pub(crate) const WATERMARK_NODE_NAME: &str = "WatermarkNode";
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatermarkNode {
    pub input: LogicalPlan,
    pub qualifier: TableReference,
    pub watermark_expression: Expr,
    pub schema: DFSchemaRef,
    timestamp_index: usize,
    pub(crate) preserve_source_envelope: bool,
    pub(crate) response_root_slot: Option<u32>,
}

multifield_partial_ord!(
    WatermarkNode,
    input,
    qualifier,
    watermark_expression,
    timestamp_index,
    preserve_source_envelope,
    response_root_slot
);

impl UserDefinedLogicalNodeCore for WatermarkNode {
    fn name(&self) -> &str {
        WATERMARK_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.watermark_expression.clone()]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WatermarkNode({}): {}", self.qualifier, self.schema)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }
        if exprs.len() != 1 {
            return internal_err!("expected one expression; found {}", exprs.len());
        }

        let timestamp_index = self
            .schema
            .index_of_column_by_name(Some(&self.qualifier), "_timestamp")
            .ok_or_else(|| DataFusionError::Plan("missing timestamp column".to_string()))?;

        Ok(Self {
            input: inputs[0].clone(),
            qualifier: self.qualifier.clone(),
            watermark_expression: exprs.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            timestamp_index,
            preserve_source_envelope: self.preserve_source_envelope,
            response_root_slot: self.response_root_slot,
        })
    }
}

impl ArroyoExtension for WatermarkNode {
    fn node_name(&self) -> Option<NamedNode> {
        if let Some(slot) = self.response_root_slot {
            Some(NamedNode::EventResponseRoot(slot))
        } else {
            (!self.preserve_source_envelope).then(|| NamedNode::Watermark(self.qualifier.clone()))
        }
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        let expression =
            planner.create_physical_expr(&self.watermark_expression, self.input.schema())?;
        let expression = serialize_physical_expr(&expression, &DefaultPhysicalExtensionCodec {})?;
        let planned_id = u32::try_from(index)
            .map_err(|_| DataFusionError::Plan("watermark graph id exceeds u32".to_owned()))?;
        let node = LogicalNode::single(
            planned_id,
            format!("watermark_{index}"),
            OperatorName::ExpressionWatermark,
            ExpressionWatermarkConfig {
                response_root_operator: None,
                preserve_source_envelope: self.preserve_source_envelope,
                period_micros: 1_000_000,
                idle_time_micros: None,
                expression: expression.encode_to_vec(),
                input_schema: Some(input_schemas[0].as_ref().clone().into()),
                source_envelope_index: self
                    .input
                    .schema()
                    .index_of_column_by_name(Some(&self.qualifier), SOURCE_ENVELOPE_FIELD)
                    .map(|i| i as u32),
            }
            .encode_to_vec(),
            if self.response_root_slot.is_some() {
                format!("event_root_{planned_id}")
            } else {
                "watermark".to_owned()
            },
            1,
        );

        let incoming_edge =
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schemas[0].as_ref().clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![incoming_edge],
        })
    }
    fn output_schema(&self) -> ArroyoSchema {
        self.arroyo_schema()
    }
}

impl WatermarkNode {
    pub(crate) fn new(
        input: LogicalPlan,
        qualifier: TableReference,
        watermark_expression: Expr,
    ) -> Result<Self> {
        Self::with_source_envelope(input, qualifier, watermark_expression, false)
    }

    pub(crate) fn with_source_envelope(
        input: LogicalPlan,
        qualifier: TableReference,
        watermark_expression: Expr,
        preserve_source_envelope: bool,
    ) -> Result<Self> {
        let fields = crate::fields_with_qualifiers(input.schema())
            .into_iter()
            .filter(|field| preserve_source_envelope || field.field.name() != SOURCE_ENVELOPE_FIELD)
            .collect::<Vec<_>>();
        let schema = add_timestamp_field(
            Arc::new(crate::schema_from_df_fields_with_metadata(
                &fields,
                input.schema().metadata().clone(),
            )?),
            Some(qualifier.clone()),
        )?;
        let timestamp_index = schema
            .index_of_column_by_name(None, "_timestamp")
            .ok_or_else(|| DataFusionError::Plan("missing _timestamp column".to_string()))?;
        Ok(Self {
            input,
            qualifier,
            watermark_expression,
            schema,
            timestamp_index,
            preserve_source_envelope,
            response_root_slot: None,
        })
    }
    pub(crate) fn arroyo_schema(&self) -> ArroyoSchema {
        ArroyoSchema::new_unkeyed(Arc::new(self.schema.as_ref().into()), self.timestamp_index)
    }
}
