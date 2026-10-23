use super::{ArroyoExtension, IsRetractExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};
use crate::functions::multi_hash;
use crate::physical::ArroyoPhysicalExtensionCodec;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::config::config;
use arroyo_rpc::{df::ArroyoSchema, grpc::api::UpdatingAggregateOperator};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UpdatingAggregateExtension {
    pub(crate) aggregate: LogicalPlan,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
    pub(crate) timestamp_qualifier: Option<TableReference>,
    pub(crate) ttl: Option<Duration>,
    pub(crate) calendar_aggregates: Vec<crate::plan::CalendarAggregate>,
    pub(crate) emit_per_event: bool,
    pub(crate) response_route: Option<crate::merge_event_branches::ResponseRoute>,
    pub(crate) accepted_schema: Option<DFSchemaRef>,
    pub(crate) response_key_fields: Vec<usize>,
    pub(crate) response_finalized_append_input: bool,
}

// DFSchema has exact equality/hash but no ordering. Different accepted schema
// contracts are incomparable; never report Equal after omitting that proof.
impl PartialOrd for UpdatingAggregateExtension {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.accepted_schema != other.accepted_schema
            || self.response_finalized_append_input != other.response_finalized_append_input
        {
            return None;
        }
        (
            &self.aggregate,
            &self.key_fields,
            &self.final_calculation,
            &self.timestamp_qualifier,
            &self.ttl,
            &self.calendar_aggregates,
            &self.emit_per_event,
            &self.response_route,
            &self.response_key_fields,
        )
            .partial_cmp(&(
                &other.aggregate,
                &other.key_fields,
                &other.final_calculation,
                &other.timestamp_qualifier,
                &other.ttl,
                &other.calendar_aggregates,
                &other.emit_per_event,
                &other.response_route,
                &other.response_key_fields,
            ))
    }
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
            emit_per_event: false,
            response_route: None,
            accepted_schema: None,
            response_key_fields: vec![],
            response_finalized_append_input: false,
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
        self.accepted_schema
            .as_ref()
            .unwrap_or_else(|| self.final_calculation.schema())
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UpdatingAggregateExtension")?;
        if let LogicalPlan::Aggregate(aggregate) = &self.aggregate
            && aggregate
                .aggr_expr
                .iter()
                .any(|expr| expr.to_string().contains("__streamr_grouped_"))
        {
            write!(
                f,
                ": aggr=[{}]",
                aggregate
                    .aggr_expr
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let mut next = Self::new(
            inputs[0].clone(),
            self.key_fields.clone(),
            self.timestamp_qualifier.clone(),
            self.ttl,
            self.calendar_aggregates.clone(),
        )?;
        if self.emit_per_event {
            next.emit_per_event = true;
            next.final_calculation = next.aggregate.clone();
        }
        next.response_route = self.response_route;
        next.accepted_schema = self.accepted_schema.clone();
        next.response_key_fields = self.response_key_fields.clone();
        next.response_finalized_append_input = self.response_finalized_append_input;
        Ok(next)
    }
}

impl ArroyoExtension for UpdatingAggregateExtension {
    fn response_root_slot(&self) -> Option<u32> {
        self.response_route.map(|route| route.slot)
    }
    fn bind_response_root(&self, operator: u32) -> Result<Option<Box<dyn ArroyoExtension>>> {
        let mut next = self.clone();
        if let Some(route) = &mut next.response_route {
            route.operator = Some(operator);
        }
        Ok(Some(Box::new(next)))
    }
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
        if self.emit_per_event
            && input_schema
                .schema
                .index_of(arroyo_rpc::UPDATING_META_FIELD)
                .is_ok()
        {
            // Extension boundaries become separate memory readers in physical
            // plans. Check EVERY segment down to the source, not just the last.
            self.aggregate.apply(|node| {
                if let LogicalPlan::Extension(extension) = node {
                    let input = if let Some(watermark) = extension
                        .node
                        .as_any()
                        .downcast_ref::<super::watermark_node::WatermarkNode>(
                    ) {
                        if !watermark.preserve_source_envelope {
                            return plan_err!(
                                "per-event correction watermark drops source envelope"
                            );
                        }
                        Some(&watermark.input)
                    } else if let Some(remote) = extension
                        .node
                        .as_any()
                        .downcast_ref::<super::remote_table::RemoteTableExtension>(
                    ) {
                        Some(&remote.input)
                    } else {
                        extension
                            .node
                            .as_any()
                            .downcast_ref::<super::key_calculation::KeyCalculationExtension>()
                            .map(|key| &key.input)
                    };
                    if let Some(input) = input {
                        crate::physical::validate_complete_source_batch(
                            planner.sync_plan(input)?.as_ref(),
                        )?;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let LogicalPlan::Aggregate(aggregate) = &self.aggregate else {
                return plan_err!("per-event correction requires aggregate input");
            };
            crate::physical::validate_complete_source_batch(
                planner.sync_plan(&aggregate.input)?.as_ref(),
            )?;
        }
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
            response_finalized_append_input: self.response_finalized_append_input,
            response_root_operator: self
                .response_route
                .map(|route| {
                    route.operator.ok_or_else(|| {
                        datafusion::common::plan_datafusion_err!(
                            "response aggregate has no bound graph root"
                        )
                    })
                })
                .transpose()?,
            emit_per_event: self.emit_per_event,
            calendar_aggregates,
            collection_output_limits,
            name: "UpdatingAggregate".to_string(),
            input_schema: Some((*input_schema).clone().into()),
            final_schema: Some(
                ArroyoSchema::from_schema_unkeyed(Arc::new(
                    self.final_calculation.schema().as_ref().into(),
                ))?
                .into(),
            ),
            accepted_output_schema: self
                .accepted_schema
                .as_ref()
                .map(|schema| {
                    ArroyoSchema::from_schema_unkeyed(Arc::new(schema.as_ref().into()))
                        .map(Into::into)
                })
                .transpose()?,
            response_key_fields: self
                .response_key_fields
                .iter()
                .map(|index| *index as u64)
                .collect(),
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

        let edge = LogicalEdge::project_all(
            if self.response_route.is_some() {
                LogicalEdgeType::Forward
            } else {
                LogicalEdgeType::Shuffle
            },
            (*input_schema).clone(),
        );

        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> arroyo_rpc::df::ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into())).unwrap()
    }
}
