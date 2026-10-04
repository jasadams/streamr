//! Effectful retained table access metadata. Execution requires STR-41's fused
//! event owner; these nodes are never ordinary independent batch-stage state.
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{
    StateTableDefinition, StateTableFieldValue, StateTableMutationClause, StateTableOperator,
};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, JoinType, Result, TableReference, internal_err, plan_err,
};
use datafusion::logical_expr::{
    BinaryExpr, Expr, Extension, Join, LogicalPlan, Operator, UserDefinedLogicalNodeCore,
};
use prost::Message;

use super::{ArroyoExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};
use crate::multifield_partial_ord;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateTableDescriptor {
    pub name: String,
    pub table_identity: String,
    pub schema_identity: String,
    pub schema: SchemaRef,
    pub primary_key: Vec<usize>,
    pub partition_key: Vec<usize>,
    pub parallelism: usize,
}
multifield_partial_ord!(
    StateTableDescriptor,
    table_identity,
    schema_identity,
    primary_key,
    partition_key,
    parallelism
);

impl From<&crate::state_tables::StateTable> for StateTableDescriptor {
    fn from(table: &crate::state_tables::StateTable) -> Self {
        Self {
            name: table.name.clone(),
            table_identity: table.table_identity.clone(),
            schema_identity: table.schema_identity.clone(),
            schema: table.schema.clone(),
            primary_key: table.primary_key.clone(),
            partition_key: table.partition_key.clone(),
            parallelism: table.parallelism,
        }
    }
}

impl StateTableDescriptor {
    fn ownership_fields(&self) -> Vec<(&str, &arrow_schema::DataType)> {
        self.partition_key
            .iter()
            .map(|&i| {
                (
                    self.schema.field(i).name().as_str(),
                    self.schema.field(i).data_type(),
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateTableClause {
    pub matched: bool,
    pub predicate: Option<Expr>,
    pub action: String,
    pub values: Vec<(usize, Expr)>,
}
multifield_partial_ord!(StateTableClause, matched, predicate, action, values);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateTableAccess {
    pub input: LogicalPlan,
    pub table: StateTableDescriptor,
    pub keys: Vec<Expr>,
    /// Input fields followed by nullable old-target fields. Clause expressions
    /// refer to this schema and are evaluated lazily, in declared clause order.
    pub expression_schema: DFSchemaRef,
    pub schema: DFSchemaRef,
    pub result_name: Option<String>,
    pub clauses: Vec<StateTableClause>,
    pub lookup_join: Option<JoinType>,
    /// Qualified event timestamp position in the output. A target may have
    /// its own `_timestamp` field, so a name-only search is ambiguous.
    pub event_timestamp_index: Option<usize>,
    pub event_scope_id: String,
    pub ownership_bindings: Vec<String>,
}
multifield_partial_ord!(
    StateTableAccess,
    input,
    table,
    keys,
    result_name,
    clauses,
    event_scope_id,
    ownership_bindings,
    event_timestamp_index
);

impl StateTableAccess {
    // Keep the complete planned access together at its construction boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        input: LogicalPlan,
        table: StateTableDescriptor,
        keys: Vec<Expr>,
        expression_schema: DFSchemaRef,
        schema: DFSchemaRef,
        result_name: Option<String>,
        clauses: Vec<StateTableClause>,
        lookup_join: Option<JoinType>,
    ) -> Result<Self> {
        let ownership_bindings = table
            .partition_key
            .iter()
            .map(|index| {
                let key_position = table.primary_key.iter().position(|i| i == index).unwrap();
                canonical_expression(&keys[key_position], &input)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut source_names = Vec::new();
        let mut related = vec![];
        input.apply(|node| {
            if let LogicalPlan::Extension(e) = node {
                if let Some(access) = e.node.as_any().downcast_ref::<Self>() {
                    related.push(access.clone());
                }
                if let Ok(extension) = <&dyn ArroyoExtension>::try_from(e.node.as_ref())
                    && let Some(NamedNode::Source(name)) = extension.node_name()
                {
                    source_names.push(name.to_string());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if source_names.len() != 1 {
            return plan_err!(
                "state-table access requires one input event source; unrelated source joins/unions cannot define one serial event scope"
            );
        }
        let event_scope_id = format!("state-event-v1:{}", source_names.remove(0));
        for access in related {
            if access.event_scope_id != event_scope_id
                || table.parallelism != access.table.parallelism
                || table.ownership_fields() != access.table.ownership_fields()
                || ownership_bindings != access.ownership_bindings
            {
                return plan_err!(
                    "related state-table accesses require compatible partition ownership and the same input-event ownership expressions"
                );
            }
        }
        let event_timestamp_index = if lookup_join.is_none() {
            let index = schema.fields().len().checked_sub(1).ok_or_else(|| {
                datafusion::common::plan_datafusion_err!("MERGE result has no event timestamp")
            })?;
            if schema.field(index).name() != arroyo_rpc::TIMESTAMP_FIELD {
                return plan_err!("MERGE result must end with the event timestamp");
            }
            Some(index)
        } else {
            None
        };
        Ok(Self {
            input,
            table,
            keys,
            expression_schema,
            schema,
            result_name,
            clauses,
            lookup_join,
            event_timestamp_index,
            event_scope_id,
            ownership_bindings,
        })
    }
}

/// Preserve field lineage across aliases/projections and a captured MERGE source
/// struct, so related table ownership is checked against the original event,
/// rather than against coincidentally equal column names at different stages.
fn column_at(schema: &DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

fn canonical_expression(expr: &Expr, plan: &LogicalPlan) -> Result<String> {
    match expr {
        Expr::Alias(alias) => canonical_expression(&alias.expr, plan),
        Expr::Column(column) => {
            let index = plan.schema().index_of_column(column)?;
            match plan {
                LogicalPlan::Projection(p) => canonical_expression(&p.expr[index], &p.input),
                LogicalPlan::SubqueryAlias(p) => canonical_expression(
                    &Expr::Column(column_at(p.input.schema(), index)),
                    &p.input,
                ),
                LogicalPlan::Extension(e) => {
                    if let Some(remote) = e
                        .node
                        .as_any()
                        .downcast_ref::<super::remote_table::RemoteTableExtension>()
                    {
                        return canonical_expression(
                            &Expr::Column(column_at(remote.input.schema(), index)),
                            &remote.input,
                        );
                    }
                    if e.node.as_any().is::<StateTableAccess>() {
                        return plan_err!(
                            "state ownership must be bound to captured source fields, not previous/resulting target rows"
                        );
                    }
                    Ok(format!(
                        "{}:{}",
                        e.node.name(),
                        plan.schema().field(index).name()
                    ))
                }
                LogicalPlan::Filter(p) => canonical_expression(expr, &p.input),
                _ => plan_err!(
                    "state-table ownership expressions require unambiguous input-event field lineage"
                ),
            }
        }
        Expr::ScalarFunction(function)
            if function.func.name() == "get_field" && function.args.len() == 2 =>
        {
            let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(field)), _) =
                &function.args[1]
            else {
                return plan_err!("state ownership struct field must have a constant field name");
            };
            captured_field_origin(&function.args[0], field, plan)
        }
        Expr::Literal(_, _) => Ok(expr.to_string()),
        Expr::ScalarFunction(f)
            if f.func.signature().volatility == datafusion::logical_expr::Volatility::Volatile =>
        {
            plan_err!("volatile expressions cannot define state-table ownership")
        }
        _ => {
            let canonical = expr
                .clone()
                .map_children(|child| {
                    Ok(Transformed::yes(Expr::Literal(
                        datafusion::common::ScalarValue::Utf8(Some(canonical_expression(
                            &child, plan,
                        )?)),
                        None,
                    )))
                })?
                .data;
            Ok(canonical.to_string())
        }
    }
}

fn captured_field_origin(expr: &Expr, field: &str, plan: &LogicalPlan) -> Result<String> {
    let column = match expr {
        Expr::Alias(alias) => return captured_field_origin(&alias.expr, field, plan),
        Expr::Column(column) => column,
        _ => {
            return plan_err!("ambiguous captured source ownership expression: {expr}");
        }
    };
    let index = plan.schema().index_of_column(column)?;
    match plan {
        LogicalPlan::Projection(p) => captured_field_origin(&p.expr[index], field, &p.input),
        LogicalPlan::SubqueryAlias(p) => captured_field_origin(
            &Expr::Column(column_at(p.input.schema(), index)),
            field,
            &p.input,
        ),
        LogicalPlan::Extension(e) => {
            if let Some(access) = e.node.as_any().downcast_ref::<StateTableAccess>()
                && access.result_name.is_some()
                && index == 0
            {
                let column = {
                    let (qualifier, field) = access
                        .input
                        .schema()
                        .qualified_field_with_unqualified_name(field)?;
                    Column::new(qualifier.cloned(), field.name())
                };
                return canonical_expression(&Expr::Column(column), &access.input);
            }
            if let Some(remote) = e
                .node
                .as_any()
                .downcast_ref::<super::remote_table::RemoteTableExtension>()
            {
                return captured_field_origin(
                    &Expr::Column(column_at(remote.input.schema(), index)),
                    field,
                    &remote.input,
                );
            }
            plan_err!("state ownership must use the captured MERGE source, not old/new/action")
        }
        _ => plan_err!("ambiguous captured source ownership expression"),
    }
}

impl ArroyoExtension for StateTableAccess {
    fn node_name(&self) -> Option<NamedNode> {
        self.result_name
            .as_ref()
            .map(|name| NamedNode::StateTableMerge(TableReference::parse_str(name)))
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            return plan_err!("state-table access requires one event input");
        }
        let clauses = self
            .clauses
            .iter()
            .map(|clause| {
                Ok(StateTableMutationClause {
                    matched: clause.matched,
                    predicate: clause
                        .predicate
                        .as_ref()
                        .map(|e| planner.serialize_as_physical_expr(e, &self.expression_schema))
                        .transpose()?,
                    action: clause.action.clone(),
                    values: clause
                        .values
                        .iter()
                        .map(|(index, expr)| {
                            Ok(StateTableFieldValue {
                                field_index: *index as u64,
                                expression: planner
                                    .serialize_as_physical_expr(expr, &self.expression_schema)?,
                            })
                        })
                        .collect::<Result<_>>()?,
                })
            })
            .collect::<Result<_>>()?;
        let config = StateTableOperator {
            table: Some(StateTableDefinition {
                name: self.table.name.clone(),
                table_identity: self.table.table_identity.clone(),
                schema_identity: self.table.schema_identity.clone(),
                schema_json: serde_json::to_string(self.table.schema.as_ref())
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?,
                primary_key: self.table.primary_key.iter().map(|i| *i as u64).collect(),
                partition_key: self.table.partition_key.iter().map(|i| *i as u64).collect(),
                parallelism: self.table.parallelism as u32,
            }),
            event_scope_id: self.event_scope_id.clone(),
            requires_fused_serial_owner: true,
            captured_result_name: self.result_name.clone(),
            input_schema: Some(input_schemas[0].as_ref().clone().into()),
            output_schema: Some(self.output_schema().into()),
            expression_schema_json: serde_json::to_string(self.expression_schema.as_arrow())
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?,
            key_expressions: self
                .keys
                .iter()
                .map(|e| planner.serialize_as_physical_expr(e, self.input.schema()))
                .collect::<Result<_>>()?,
            clauses,
            lookup_join_type: self.lookup_join.map(|j| j.to_string()),
            ownership_bindings: self.ownership_bindings.clone(),
        };
        Ok(NodeWithIncomingEdges {
            node: LogicalNode::single(
                index as u32,
                format!("state_table_{index}"),
                OperatorName::StateTable,
                config.encode_to_vec(),
                format!(
                    "current-row state table {} (requires fused event owner)",
                    self.table.name
                ),
                1,
            ),
            edges: vec![LogicalEdge::project_all(
                LogicalEdgeType::Forward,
                input_schemas[0].as_ref().clone(),
            )],
        })
    }
    fn output_schema(&self) -> ArroyoSchema {
        let arrow = Arc::new(self.schema.as_ref().into());
        match self.event_timestamp_index {
            Some(index) => ArroyoSchema::new_unkeyed(arrow, index),
            None => ArroyoSchema::from_schema_unkeyed(arrow).unwrap(),
        }
    }
}

impl UserDefinedLogicalNodeCore for StateTableAccess {
    fn name(&self) -> &str {
        "StateTableAccess"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    // Mutation predicates/value expressions remain private to the effect node:
    // generic optimizer rules cannot eagerly extract inactive-clause effects.
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StateTableAccess: table={} scope={} capture={:?}; requires fused serial event owner",
            self.table.name, self.event_scope_id, self.result_name
        )
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!(
                "state-table access expressions are private; exactly one input required"
            );
        }
        let mut next = self.clone();
        next.input = inputs[0].clone();
        if next.input.schema() != self.input.schema() {
            return plan_err!("optimizer cannot change state-table input schema");
        }
        Ok(next)
    }
}

/// A catalog placeholder, legal only on the target side of a keyed lookup. It
/// never becomes a graph source, historical join, or independently read scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateTableScan {
    pub table: StateTableDescriptor,
    pub schema: DFSchemaRef,
}
multifield_partial_ord!(StateTableScan, table);
impl UserDefinedLogicalNodeCore for StateTableScan {
    fn name(&self) -> &str {
        "StateTableScan"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StateTableScan: {} requires keyed event lookup",
            self.table.name
        )
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("state table scan has no expressions or inputs");
        }
        Ok(self.clone())
    }
}

pub(crate) fn plan_lookup(join: &Join) -> Result<Option<LogicalPlan>> {
    fn has_scan(plan: &LogicalPlan) -> Result<bool> {
        plan.exists(|p| {
            Ok(matches!(p, LogicalPlan::Extension(e) if e.node.as_any().is::<StateTableScan>()))
        })
    }
    if has_scan(&join.left)? {
        return plan_err!(
            "state tables must be on the right of an input-event INNER or LEFT keyed lookup; target scans are unsupported"
        );
    }
    if !has_scan(&join.right)? {
        return Ok(None);
    }
    if !matches!(join.join_type, JoinType::Inner | JoinType::Left) {
        return plan_err!(
            "state-table lookup requires INNER/LEFT JOIN and complete primary-key equality without residual target predicates"
        );
    }
    let mut right = join.right.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = right {
        right = alias.input.as_ref();
    }
    let LogicalPlan::Extension(e) = right else {
        return plan_err!(
            "state-table target scans/filters/projections are unsupported; use direct keyed JOIN"
        );
    };
    let Some(scan) = e.node.as_any().downcast_ref::<StateTableScan>() else {
        return plan_err!("state-table lookup requires a direct declared target");
    };
    if join.right.schema().fields().len() != scan.table.schema.fields().len() {
        return plan_err!("state-table lookup requires the complete declared target schema");
    }
    let mut conditions = join
        .on
        .iter()
        .map(|(l, r)| {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l.clone()),
                Operator::Eq,
                Box::new(r.clone()),
            ))
        })
        .collect::<Vec<_>>();
    if let Some(filter) = &join.filter {
        conditions.push(filter.clone());
    }
    let on = conditions
        .into_iter()
        .reduce(|l, r| Expr::BinaryExpr(BinaryExpr::new(Box::new(l), Operator::And, Box::new(r))))
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "state-table lookup requires complete primary-key equality"
            )
        })?;
    let keys = crate::continuous_merge::bind_keys(
        &on,
        join.right.schema(),
        join.left.schema(),
        &scan.table,
    )?;
    let mut fields = join
        .left
        .schema()
        .iter()
        .map(|(q, f)| (q.cloned(), f.clone()))
        .collect::<Vec<_>>();
    fields.extend(
        join.right
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::new(f.as_ref().clone().with_nullable(true)))),
    );
    let expression_schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);
    let (timestamp_qualifier, timestamp_field) = join
        .left
        .schema()
        .qualified_field_with_unqualified_name(arroyo_rpc::TIMESTAMP_FIELD)?;
    let timestamp_qualifier = timestamp_qualifier.cloned();
    let mut output_schema = join.schema.clone();
    let left_timestamp_index = join
        .left
        .schema()
        .iter()
        .position(|(qualifier, field)| {
            qualifier.cloned() == timestamp_qualifier && field.as_ref() == timestamp_field
        })
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "event timestamp is absent from the lookup left input"
            )
        })?;
    let left_complete = output_schema
        .iter()
        .take(join.left.schema().fields().len())
        .eq(join.left.schema().iter());
    let event_timestamp_index = if left_complete {
        left_timestamp_index
    } else {
        let left_without_timestamp = join
            .left
            .schema()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != left_timestamp_index)
            .map(|(_, field)| field)
            .collect::<Vec<_>>();
        if !output_schema
            .iter()
            .take(left_without_timestamp.len())
            .eq(left_without_timestamp.iter().copied())
        {
            return plan_err!("lookup JOIN changed the event-field order before its target fields");
        }
        let event_index = left_without_timestamp.len() + join.right.schema().fields().len();
        if output_schema.fields().len() == event_index + 1 {
            let (qualifier, field) = output_schema.qualified_field(event_index);
            if qualifier.cloned() != timestamp_qualifier || field != timestamp_field {
                return plan_err!("lookup JOIN has an unexpected field after target columns");
            }
            event_index
        } else if output_schema.fields().len() == event_index {
            // The logical JOIN may omit the source's internal event timestamp.
            // Append that exact qualified field even if the target itself has
            // an unrelated column named `_timestamp`.
            output_schema = Arc::new(output_schema.join(&DFSchema::new_with_metadata(
                vec![(timestamp_qualifier.clone(), timestamp_field.clone().into())],
                HashMap::new(),
            )?)?);
            output_schema.fields().len() - 1
        } else {
            return plan_err!("lookup JOIN field count differs from event and target schemas");
        }
    };
    let mut access = StateTableAccess::new(
        join.left.as_ref().clone(),
        scan.table.clone(),
        keys,
        expression_schema,
        output_schema,
        None,
        vec![],
        Some(join.join_type),
    )?;
    access.event_timestamp_index = Some(event_timestamp_index);
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(access),
    })))
}
