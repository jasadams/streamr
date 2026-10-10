use super::{ArroyoExtension, IsRetractExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};
use crate::functions::multi_hash;
use crate::physical::ArroyoPhysicalExtensionCodec;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::config::config;
use arroyo_rpc::{df::ArroyoSchema, grpc::api::UpdatingAggregateOperator};
use datafusion::common::{DFSchemaRef, Result, TableReference, ToDFSchema, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore, col, lit,
};
use datafusion::prelude::named_struct;
use datafusion::scalar::ScalarValue;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{
    PhysicalPlanNode, physical_expr_node::ExprType, physical_plan_node::PhysicalPlanType,
};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const UPDATING_AGGREGATE_EXTENSION_NAME: &str = "UpdatingAggregateExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct UpdatingAggregateExtension {
    pub(crate) aggregate: LogicalPlan,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
    pub(crate) timestamp_qualifier: Option<TableReference>,
    pub(crate) ttl: Option<Duration>,
    pub(crate) calendar_aggregates: Vec<crate::plan::CalendarAggregate>,
}

impl UpdatingAggregateExtension {
    pub fn new(
        aggregate: LogicalPlan,
        key_fields: Vec<usize>,
        timestamp_qualifier: Option<TableReference>,
        ttl: Option<Duration>,
        calendar_aggregates: Vec<crate::plan::CalendarAggregate>,
    ) -> Result<Self> {
        let final_calculation = LogicalPlan::Extension(Extension {
            node: Arc::new(IsRetractExtension::new(
                aggregate.clone(),
                timestamp_qualifier.clone(),
            )),
        });

        Ok(Self {
            aggregate,
            key_fields,
            final_calculation,
            timestamp_qualifier,
            ttl,
            calendar_aggregates,
        })
    }
}

impl UserDefinedLogicalNodeCore for UpdatingAggregateExtension {
    fn name(&self) -> &str {
        UPDATING_AGGREGATE_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.aggregate]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.final_calculation.schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UpdatingAggregateExtension")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Self::new(
            inputs[0].clone(),
            self.key_fields.clone(),
            self.timestamp_qualifier.clone(),
            self.ttl,
            self.calendar_aggregates.clone(),
        )
    }
}

impl ArroyoExtension for UpdatingAggregateExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<arroyo_rpc::df::ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            return plan_err!(
                "UpdatingAggregateExtension requires exactly one input schema, found {}",
                input_schemas.len()
            );
        }

        let input_schema = input_schemas[0].clone();
        let input_dfschema = input_schema.schema.clone().to_dfschema()?;

        let aggregate_exec = PhysicalPlanNode::try_from_physical_plan(
            planner.sync_plan(&self.aggregate)?,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;

        let key_exprs: Vec<Expr> = self
            .key_fields
            .iter()
            .map(|&i| col(input_schema.schema.field(i).name()))
            .collect();
        let hash_expr = if key_exprs.is_empty() {
            Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(vec![0; 16])), None)
        } else {
            Expr::ScalarFunction(ScalarFunction {
                func: multi_hash(),
                args: key_exprs,
            })
        };

        let updating_meta_expr =
            named_struct(vec![lit("is_retract"), lit(false), lit("id"), hash_expr]);

        let collection_output_limits = self
            .aggregate
            .schema()
            .metadata()
            .iter()
            .filter_map(|(key, value)| {
                Some((
                    key.strip_prefix(crate::plan::COLLECTION_LIMIT_PREFIX)?
                        .parse::<u32>()
                        .ok()?,
                    value.parse::<u64>().ok()?,
                ))
            })
            .collect();
        // Calendar expressions retain source qualifiers through aliases and
        // projections. Arrow wire schemas have no relation qualifiers; resolve
        // these columns against the logical aggregate input to preserve their
        // actual ordinal instead of stripping or guessing their source names.
        let LogicalPlan::Aggregate(logical_aggregate) = &self.aggregate else {
            return plan_err!("updating aggregate requires a logical aggregate plan");
        };
        let calendar_schema = logical_aggregate.input.schema();
        if !self.calendar_aggregates.is_empty()
            && (calendar_schema.fields().len() != input_schema.schema.fields().len()
                || calendar_schema
                    .fields()
                    .iter()
                    .zip(input_schema.schema.fields())
                    .any(|(logical, wire)| {
                        logical.name() != wire.name() || logical.data_type() != wire.data_type()
                    }))
        {
            return plan_err!("calendar aggregate logical and wire input schema ordinals differ");
        }
        let calendar_aggregates = self
            .calendar_aggregates
            .iter()
            .map(|calendar| {
                // Argument coercion belongs to physical aggregate planning.
                // Persist the expressions actually consumed by the accumulator,
                // so shared-family identities include implicit numeric casts.
                let Some(PhysicalPlanType::Aggregate(physical)) =
                    &aggregate_exec.physical_plan_type
                else {
                    return plan_err!("calendar descriptor requires a physical aggregate plan");
                };
                let Some(ExprType::AggregateExpr(expression)) = physical
                    .aggr_expr
                    .get(calendar.aggregate_index)
                    .and_then(|expression| expression.expr_type.as_ref())
                else {
                    return plan_err!(
                        "calendar descriptor ordinal does not identify a physical aggregate"
                    );
                };
                if expression.expr.len() != 1 {
                    return plan_err!(
                        "calendar descriptor requires one physical aggregate argument"
                    );
                }
                let static_filter = physical
                    .filter_expr
                    .get(calendar.aggregate_index)
                    .and_then(|filter| filter.expr.as_ref())
                    .map(Message::encode_to_vec);
                Ok(arroyo_rpc::grpc::api::CalendarAggregateDescriptor {
                    aggregate_index: calendar.aggregate_index as u32,
                    argument: expression.expr[0].encode_to_vec(),
                    static_filter,
                    contribution_date: planner
                        .serialize_as_physical_expr(&calendar.contribution_date, calendar_schema)?,
                    reference_date: planner
                        .serialize_as_physical_expr(&calendar.reference_date, calendar_schema)?,
                    horizon_days: calendar.horizon_days,
                    context_id: calendar.reference_date.to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let config = UpdatingAggregateOperator {
            calendar_aggregates,
            collection_output_limits,
            name: "UpdatingAggregate".to_string(),
            input_schema: Some((*input_schema).clone().into()),
            final_schema: Some(self.output_schema().into()),
            aggregate_exec: aggregate_exec.encode_to_vec(),
            metadata_expr: planner
                .serialize_as_physical_expr(&updating_meta_expr, &input_dfschema)?,
            flush_interval_micros: config()
                .pipeline
                .update_aggregate_flush_interval
                .as_micros() as u64,
            ttl_micros: self.ttl.map(|ttl| ttl.as_micros() as u64).unwrap_or(0),
            retain_indefinitely: self.ttl.is_none().then_some(true),
        };

        let node = LogicalNode::single(
            index as u32,
            format!("updating_aggregate_{index}"),
            OperatorName::UpdatingAggregate,
            config.encode_to_vec(),
            "UpdatingAggregate".to_string(),
            1,
        );

        let edge = LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*input_schema).clone());

        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> arroyo_rpc::df::ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into())).unwrap()
    }
}
