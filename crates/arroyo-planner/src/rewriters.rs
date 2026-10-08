use crate::builder::NamedNode;
use crate::extension::ArroyoExtension;
use crate::extension::debezium::DebeziumUnrollingExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::sink::SinkExtension;
use crate::extension::table_source::TableSourceExtension;
use crate::extension::watermark_node::WatermarkNode;
use crate::schemas::add_timestamp_field;
use crate::tables::ConnectorTable;
use crate::tables::FieldSpec;
use crate::tables::Table;
use crate::{
    ASYNC_RESULT_FIELD, ArroyoSchemaProvider, DFField, fields_with_qualifiers,
    schema_from_df_fields,
};

use arrow_schema::DataType;
use arroyo_rpc::TIMESTAMP_FIELD;
use arroyo_rpc::UPDATING_META_FIELD;
use datafusion::logical_expr::UserDefinedLogicalNode;

use crate::extension::AsyncUDFExtension;
use crate::extension::lookup::LookupSource;
use crate::extension::stateful_processor::{StatefulOpDesc, StatefulProcessorExtension};
use arroyo_rpc::grpc::api::StateOpType;
use arroyo_udf_host::parse::{AsyncOptions, UdfType};
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{
    Column, DataFusionError, Result as DFResult, ScalarValue, TableReference, plan_err,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, ColumnUnnestList, Expr, Extension, LogicalPlan, Projection, TableScan, Unnest,
};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Rewrites a logical plan to move projections out of table scans
/// and into a separate projection node which may include virtual fields,
/// and adds a watermark node.
pub struct SourceRewriter<'a> {
    pub(crate) schema_provider: &'a ArroyoSchemaProvider,
}

impl SourceRewriter<'_> {
    fn watermark_expression(table: &ConnectorTable) -> DFResult<Expr> {
        let expr = match table.watermark_field.clone() {
            Some(watermark_field) => table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &watermark_field {
                        return match f {
                            FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                                Some(Expr::Column(Column {
                                    relation: None,
                                    name: field.name().to_string(),
                                    spans: Default::default(),
                                }))
                            }
                            FieldSpec::Virtual { expression, .. } => Some(*expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Watermark field {watermark_field} not found"))
                })?,
            None => Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "_timestamp".to_string(),
                    spans: Default::default(),
                })),
                op: logical_expr::Operator::Minus,
                right: Box::new(Expr::Literal(
                    ScalarValue::DurationNanosecond(Some(Duration::from_secs(1).as_nanos() as i64)),
                    None,
                )),
            }),
        };
        Ok(expr)
    }

    fn projection_expressions(
        table: &ConnectorTable,
        qualifier: &TableReference,
        projection: &Option<Vec<usize>>,
    ) -> DFResult<Vec<Expr>> {
        let mut expressions = table
            .fields
            .iter()
            .map(|field| match field {
                FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                    Expr::Column(Column {
                        relation: Some(qualifier.clone()),
                        name: field.name().to_string(),
                        spans: Default::default(),
                    })
                }
                FieldSpec::Virtual { field, expression } => expression
                    .clone()
                    .alias_qualified(Some(qualifier.clone()), field.name().to_string()),
            })
            .collect::<Vec<_>>();

        if let Some(projection) = projection {
            expressions = projection.iter().map(|i| expressions[*i].clone()).collect();
        }

        // Add event time field if present
        if let Some(event_time_field) = table.event_time_field.clone() {
            let event_time_field = table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &event_time_field {
                        return match f {
                            FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                                Some(Expr::Column(Column {
                                    relation: Some(qualifier.clone()),
                                    name: field.name().to_string(),
                                    spans: Default::default(),
                                }))
                            }
                            FieldSpec::Virtual { expression, .. } => Some(*expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Event time field {event_time_field} not found"))
                })?;

            let event_time_field =
                event_time_field.alias_qualified(Some(qualifier.clone()), "_timestamp".to_string());
            expressions.push(event_time_field);
        } else {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                TIMESTAMP_FIELD,
            )))
        }
        if table.is_updating() {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                UPDATING_META_FIELD,
            )))
        }
        Ok(expressions)
    }

    fn projection(&self, table_scan: &TableScan, table: &ConnectorTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        let table_source_extension = LogicalPlan::Extension(Extension {
            node: Arc::new(TableSourceExtension::new(
                qualifier.to_owned(),
                table.clone(),
            )),
        });

        let (projection_input, projection) = if table.is_updating() {
            let mut projection_offsets = table_scan.projection.clone();
            if let Some(offsets) = projection_offsets.as_mut() {
                offsets.push(table.fields.len())
            }
            (
                LogicalPlan::Extension(Extension {
                    node: Arc::new(DebeziumUnrollingExtension::try_new(
                        table_source_extension,
                        table.primary_keys.clone(),
                    )?),
                }),
                None,
            )
        } else {
            (table_source_extension, table_scan.projection.clone())
        };

        Ok(LogicalPlan::Projection(Projection::try_new(
            Self::projection_expressions(table, &qualifier, &projection)?,
            Arc::new(projection_input),
        )?))
    }

    fn mutate_connector_table(
        &self,
        table_scan: &TableScan,
        table: &ConnectorTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let input = self.projection(table_scan, table)?;

        let schema = input.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                input,
                name: table_scan.table_name.to_owned(),
                schema,
                materialize: true,
            }),
        });

        let watermark_node = WatermarkNode::new(
            remote,
            table_scan.table_name.clone(),
            Self::watermark_expression(table)?,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!("failed to create watermark expression: {err}"))
        })?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        })))
    }

    fn mutate_lookup_table(
        &self,
        table_scan: &TableScan,
        table: &ConnectorTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(LookupSource {
                table: table.clone(),
                schema: table_scan.projected_schema.clone(),
            }),
        })))
    }

    fn mutate_table_from_query(
        &self,
        table_scan: &TableScan,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let column_expressions: Vec<_> = if let Some(projection) = &table_scan.projection {
            fields_with_qualifiers(logical_plan.schema())
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if projection.contains(&i) {
                        Some(Expr::Column(Column::new(
                            f.qualifier().cloned(),
                            f.name().to_string(),
                        )))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            fields_with_qualifiers(logical_plan.schema())
                .iter()
                .map(|f| Expr::Column(Column::new(f.qualifier().cloned(), f.name().to_string())))
                .collect()
        };
        let expressions = column_expressions
            .into_iter()
            .zip(fields_with_qualifiers(&table_scan.projected_schema))
            .map(|(expr, field)| {
                expr.alias_qualified(field.qualifier().cloned(), field.name().to_string())
            })
            .collect();
        let projection = LogicalPlan::Projection(Projection::try_new_with_schema(
            expressions,
            Arc::new(logical_plan.clone()),
            table_scan.projected_schema.clone(),
        )?);
        Ok(Transformed::yes(projection))
    }
}

impl TreeNodeRewriter for SourceRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::TableScan(mut table_scan) = node else {
            return Ok(Transformed::no(node));
        };

        if let Some(table) = self
            .schema_provider
            .get_state_table_reference(&table_scan.table_name)
        {
            if table_scan.projection.is_some() || !table_scan.filters.is_empty() {
                return plan_err!(
                    "state-table lookup requires a direct complete keyed JOIN; target scans, projections and filters are unsupported"
                );
            }
            return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(crate::extension::state_table::StateTableScan {
                    table: crate::extension::state_table::StateTableDescriptor::from(table),
                    schema: table_scan.projected_schema,
                }),
            })));
        }

        let table_name = &table_scan.table_name;
        let table = self
            .schema_provider
            .get_table_reference(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table {table_name} not found")))?;

        match table {
            Table::ConnectorTable(table) => self.mutate_connector_table(&table_scan, table),
            Table::LookupTable(table) => self.mutate_lookup_table(&table_scan, table),
            Table::MemoryTable {
                name,
                fields: _,
                logical_plan,
            } => {
                let Some(logical_plan) = logical_plan else {
                    return plan_err!(
                        "Can't query from memory table {} without first inserting into it.",
                        name
                    );
                };
                // this can only be done here, otherwise the query planner will be upset about the timestamp column.
                table_scan.projected_schema = add_timestamp_field(
                    table_scan.projected_schema.clone(),
                    Some(table_scan.table_name.clone()),
                )?;

                self.mutate_table_from_query(&table_scan, logical_plan)
            }
            Table::TableFromQuery {
                name: _,
                logical_plan,
            } => self.mutate_table_from_query(&table_scan, logical_plan),
            Table::PreviewSink { .. } => Err(DataFusionError::Plan(
                "can't select from a preview sink".to_string(),
            )),
        }
    }
}

pub const UNNESTED_COL: &str = "__unnested";

pub struct UnnestRewriter {}

impl UnnestRewriter {
    fn split_unnest(expr: Expr) -> DFResult<(Expr, Option<Expr>)> {
        let mut c: Option<Expr> = None;

        let expr = expr.transform_up(&mut |e| {
            if let Expr::ScalarFunction(ScalarFunction { func: udf, args }) = &e
                && udf.name() == "unnest"
            {
                match args.len() {
                    1 => {
                        if c.replace(args[0].clone()).is_some() {
                            return Err(DataFusionError::Plan(
                                "Multiple unnests in expression, which is not allowed".to_string(),
                            ));
                        };

                        return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                            UNNESTED_COL,
                        ))));
                    }
                    n => {
                        panic!("Unnest has wrong number of arguments (expected 1, found {n})");
                    }
                }
            };
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, c))
    }
}

impl TreeNodeRewriter for UnnestRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(projection) = &node else {
            if node.expressions().iter().any(|e| {
                let e = Self::split_unnest(e.clone());
                e.is_err() || e.unwrap().1.is_some()
            }) {
                return plan_err!("unnest is only supported in SELECT statements");
            }
            return Ok(Transformed::no(node));
        };

        let mut unnest = None;
        let exprs = projection
            .expr
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                let (expr, opt) = Self::split_unnest(expr)?;
                let typ = if let Some(e) = opt {
                    if let Some(prev) = unnest.replace((e, i))
                        && &prev != unnest.as_ref().unwrap()
                    {
                        return plan_err!(
                            "Projection contains multiple unnests, which is not currently supported"
                        );
                    }
                    true
                } else {
                    false
                };

                Ok((expr, typ))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some((unnest_inner, unnest_idx)) = unnest {
            let produce_list = Arc::new(LogicalPlan::Projection(
                Projection::try_new(
                    exprs
                        .iter()
                        .cloned()
                        .map(|(e, is_unnest)| {
                            if is_unnest {
                                unnest_inner.clone().alias(UNNESTED_COL)
                            } else {
                                e
                            }
                        })
                        .collect(),
                    projection.input.clone(),
                )
                .unwrap(),
            ));

            let unnest_fields = fields_with_qualifiers(produce_list.schema())
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i == unnest_idx {
                        let DataType::List(inner) = f.data_type() else {
                            return plan_err!(
                                "Argument '{}' to unnest is not a List",
                                f.qualified_name()
                            );
                        };

                        Ok(DFField::new_unqualified(
                            UNNESTED_COL,
                            inner.data_type().clone(),
                            inner.is_nullable(),
                        ))
                    } else {
                        Ok((*f).clone())
                    }
                })
                .collect::<DFResult<Vec<_>>>()?;

            let unnest_node = LogicalPlan::Unnest(Unnest {
                exec_columns: vec![
                    DFField::from(produce_list.schema().qualified_field(unnest_idx))
                        .qualified_column(),
                ],
                input: produce_list,
                list_type_columns: vec![(
                    unnest_idx,
                    ColumnUnnestList {
                        output_column: Column::new_unqualified(UNNESTED_COL),
                        depth: 1,
                    },
                )],
                struct_type_columns: vec![],
                dependency_indices: vec![],
                schema: Arc::new(schema_from_df_fields(&unnest_fields).unwrap()),
                options: Default::default(),
            });

            let output_node = LogicalPlan::Projection(Projection::try_new(
                exprs
                    .iter()
                    .enumerate()
                    .map(|(i, (expr, has_unnest))| {
                        if *has_unnest {
                            expr.clone()
                        } else {
                            Expr::Column(
                                DFField::from(unnest_node.schema().qualified_field(i))
                                    .qualified_column(),
                            )
                        }
                    })
                    .collect(),
                Arc::new(unnest_node),
            )?);

            Ok(Transformed::yes(output_node))
        } else {
            Ok(Transformed::no(LogicalPlan::Projection(projection.clone())))
        }
    }
}

pub struct AsyncUdfRewriter<'a> {
    provider: &'a ArroyoSchemaProvider,
}

type AsyncSplitResult = (String, AsyncOptions, Vec<Expr>);

impl<'a> AsyncUdfRewriter<'a> {
    pub fn new(provider: &'a ArroyoSchemaProvider) -> Self {
        Self { provider }
    }

    fn split_async(
        expr: Expr,
        provider: &ArroyoSchemaProvider,
    ) -> DFResult<(Expr, Option<AsyncSplitResult>)> {
        let mut c: Option<(String, AsyncOptions, Vec<Expr>)> = None;
        let expr = expr.transform_up(&mut |e| {
            if let Expr::ScalarFunction(ScalarFunction { func: udf, args }) = &e
                && let Some(UdfType::Async(opts)) =
                    provider.udf_defs.get(udf.name()).map(|udf| udf.udf_type)
            {
                if c.replace((udf.name().to_string(), opts, args.clone()))
                    .is_some()
                {
                    return plan_err!(
                        "multiple async calls in the same expression, which is not allowed"
                    );
                }
                return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    ASYNC_RESULT_FIELD,
                ))));
            }
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, c))
    }
}

impl TreeNodeRewriter for AsyncUdfRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(mut projection) = node else {
            for e in node.expressions() {
                if let (_, Some((udf, _, _))) = Self::split_async(e.clone(), self.provider)? {
                    return plan_err!(
                        "async UDFs are only supported in projections, but {udf} was called in another context"
                    );
                }
            }
            return Ok(Transformed::no(node));
        };

        let mut args = None;

        for e in projection.expr.iter_mut() {
            let (new_e, Some(udf)) = Self::split_async(e.clone(), self.provider)? else {
                continue;
            };

            if let Some((prev, _, _)) = args.replace(udf) {
                return plan_err!(
                    "Projection contains multiple async UDFs, which is not supported \
                    \n(hint: two async UDFs calls, {} and {}, appear in the same SELECT statement)",
                    prev,
                    args.unwrap().0
                );
            }

            *e = new_e;
        }

        let Some((name, opts, args)) = args else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };

        let udf = self.provider.dylib_udfs.get(&name).unwrap().clone();

        let input = if matches!(*projection.input, LogicalPlan::Projection(..)) {
            // if our input is a projection, we need to plan it separately -- this happens
            // for subqueries

            Arc::new(LogicalPlan::Extension(Extension {
                node: Arc::new(RemoteTableExtension {
                    input: (*projection.input).clone(),
                    name: TableReference::bare("subquery_projection"),
                    schema: projection.input.schema().clone(),
                    materialize: false,
                }),
            }))
        } else {
            projection.input
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(AsyncUDFExtension {
                input,
                name,
                udf,
                arg_exprs: args,
                final_exprs: projection.expr,
                ordered: opts.ordered,
                max_concurrency: opts.max_concurrency,
                timeout: opts.timeout,
                final_schema: projection.schema,
            }),
        })))
    }
}

pub struct SourceMetadataVisitor<'a> {
    schema_provider: &'a ArroyoSchemaProvider,
    pub connection_ids: HashSet<i64>,
}

impl<'a> SourceMetadataVisitor<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        Self {
            schema_provider,
            connection_ids: HashSet::new(),
        }
    }
}

impl SourceMetadataVisitor<'_> {
    fn get_connection_id(&self, node: &LogicalPlan) -> Option<i64> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return None;
        };
        // extract the name if it is a sink or source.
        let table_name = match node.name() {
            "TableSourceExtension" => {
                let TableSourceExtension { name, .. } =
                    node.as_any().downcast_ref::<TableSourceExtension>()?;
                name
            }
            "SinkExtension" => {
                let SinkExtension { name, .. } = node.as_any().downcast_ref::<SinkExtension>()?;
                name
            }
            _ => return None,
        };
        let table = self.schema_provider.get_table_reference(table_name)?;
        match table {
            Table::ConnectorTable(table) => table.id,
            _ => None,
        }
    }
}

impl TreeNodeVisitor<'_> for SourceMetadataVisitor<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Some(id) = self.get_connection_id(node) {
            self.connection_ids.insert(id);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

struct TimeWindowExprChecker {}

pub struct TimeWindowUdfChecker {}

pub fn is_time_window(expr: &Expr) -> Option<&str> {
    if let Expr::ScalarFunction(ScalarFunction { func, args: _ }) = expr {
        match func.name() {
            "tumble" | "hop" | "session" => {
                return Some(func.name());
            }
            _ => {}
        }
    }
    None
}

impl TreeNodeVisitor<'_> for TimeWindowExprChecker {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Some(w) = is_time_window(node) {
            return plan_err!(
                "time window function {} is not allowed in this context. Are you missing a GROUP BY clause?",
                w
            );
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

impl TreeNodeVisitor<'_> for TimeWindowUdfChecker {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        node.expressions().iter().try_for_each(|expr| {
            let mut checker = TimeWindowExprChecker {};
            expr.visit(&mut checker)?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(TreeNodeRecursion::Continue)
    }
}

pub struct TimeWindowNullCheckRemover {}

impl TreeNodeRewriter for TimeWindowNullCheckRemover {
    type Node = Expr;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let Expr::IsNotNull(expr) = &node
            && is_time_window(expr).is_some()
        {
            return Ok(Transformed::yes(Expr::Literal(
                ScalarValue::Boolean(Some(true)),
                None,
            )));
        }

        Ok(Transformed::no(node))
    }
}

pub struct RowTimeRewriter {}

impl TreeNodeRewriter for RowTimeRewriter {
    type Node = Expr;
    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let Expr::ScalarFunction(func) = &node
            && func.name() == "row_time"
        {
            let transformed = Expr::Column(Column {
                relation: None,
                name: "_timestamp".to_string(),
                spans: Default::default(),
            })
            .alias("row_time()");
            return Ok(Transformed::yes(transformed));
        }
        Ok(Transformed::no(node))
    }
}

type SinkInputs = HashMap<NamedNode, Vec<LogicalPlan>>;

pub(crate) struct SinkInputRewriter<'a> {
    sink_inputs: &'a mut SinkInputs,
    pub was_removed: bool,
}

impl<'a> SinkInputRewriter<'a> {
    pub(crate) fn new(sink_inputs: &'a mut SinkInputs) -> Self {
        Self {
            sink_inputs,
            was_removed: false,
        }
    }
}

impl TreeNodeRewriter for SinkInputRewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let LogicalPlan::Extension(extension) = &node
            && let Some(sink_node) = extension.node.as_any().downcast_ref::<SinkExtension>()
            && let Some(named_node) = sink_node.node_name()
        {
            if let Some(inputs) = self.sink_inputs.remove(&named_node) {
                let extension = LogicalPlan::Extension(Extension {
                    node: sink_node.with_exprs_and_inputs(vec![], inputs)?,
                });
                return Ok(Transformed::new(extension, true, TreeNodeRecursion::Jump));
            } else {
                self.was_removed = true;
            }
        }
        Ok(Transformed::no(node))
    }
}

/// Rewrites projections containing state function calls (`state_get`, `state_put`,
/// `state_upsert`, `state_update`, `state_delete`) into `StatefulProcessorExtension`
/// plan nodes. Each state function call is extracted as a `StatefulOpDesc` and the
/// function call in the projection is replaced by a column reference to the
/// operator's result column.
#[derive(Default)]
pub struct StatefulProcessorRewriter {
    counter: usize,
}

impl StatefulProcessorRewriter {
    pub fn new() -> Self {
        Self::default()
    }

    fn is_state_function(name: &str) -> bool {
        matches!(
            name,
            "state_get" | "state_put" | "state_upsert" | "state_update" | "state_delete"
        )
    }

    fn state_op_type(name: &str) -> i32 {
        match name {
            "state_get" => StateOpType::StateGet as i32,
            "state_put" => StateOpType::StatePut as i32,
            "state_upsert" => StateOpType::StateUpsert as i32,
            "state_update" => StateOpType::StateUpdate as i32,
            "state_delete" => StateOpType::StateDelete as i32,
            _ => unreachable!("state_op_type called with non-state function: {name}"),
        }
    }
}

/// Returns true if the expression tree contains any state function call.
pub(crate) fn contains_state_function(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::ScalarFunction(ScalarFunction { func, .. })
                if StatefulProcessorRewriter::is_state_function(func.name())
        ))
    })
    .unwrap_or(false)
}

fn contains_volatile_scalar(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::ScalarFunction(function)
        if !StatefulProcessorRewriter::is_state_function(function.func.name())
        && function.func.signature().volatility == datafusion::logical_expr::Volatility::Volatile))
    })
    .unwrap_or(false)
}

type FusedStateInput = (LogicalPlan, Vec<StatefulOpDesc>, Vec<Expr>);

/// Inline linear SQL stages into one ordered map owner. Columns from a CTE are
/// substituted with their defining expressions; state-result columns remain
/// references to earlier operations in the same owner.
fn flatten_state_input(plan: &LogicalPlan) -> DFResult<Option<FusedStateInput>> {
    let (input, projection) = match plan {
        LogicalPlan::Projection(p) => (p.input.as_ref(), Some(p.expr.clone())),
        LogicalPlan::SubqueryAlias(a) => (a.input.as_ref(), None),
        LogicalPlan::Extension(e) => {
            if let Some(s) = e.node.as_any().downcast_ref::<StatefulProcessorExtension>() {
                if s.final_exprs.iter().any(contains_volatile_scalar) {
                    return plan_err!(
                        "volatile expressions after a stateful stage cannot be fused without changing evaluation count; compute them before the first stateful SELECT"
                    );
                }
                return Ok(Some((
                    s.input.clone(),
                    s.ops.clone(),
                    s.final_exprs.clone(),
                )));
            }
            if let Some(r) = e.node.as_any().downcast_ref::<RemoteTableExtension>() {
                (&r.input, None)
            } else {
                return Ok(None);
            }
        }
        _ => return Ok(None),
    };
    let Some((base, ops, mapping)) = flatten_state_input(input)? else {
        return Ok(None);
    };
    if projection
        .as_ref()
        .is_some_and(|exprs| exprs.iter().any(contains_volatile_scalar))
    {
        return plan_err!(
            "volatile expressions between stateful stages are unsupported; compute them before the first stateful SELECT"
        );
    }
    let mapping = match projection {
        Some(exprs) => exprs
            .into_iter()
            .map(|e| substitute_state_columns(e, input.schema(), &mapping))
            .collect::<DFResult<Vec<_>>>()?,
        None => mapping,
    };
    Ok(Some((base, ops, mapping)))
}

fn substitute_state_columns(
    expr: Expr,
    schema: &datafusion::common::DFSchema,
    mapping: &[Expr],
) -> DFResult<Expr> {
    Ok(expr
        .transform_up(&mut |e| {
            if let Expr::Column(c) = &e {
                let index = schema.index_of_column(c)?;
                // Strip projection aliases before embedding their expressions.
                let mut replacement = mapping[index].clone();
                while let Expr::Alias(a) = replacement {
                    replacement = *a.expr;
                }
                Ok(Transformed::yes(replacement))
            } else {
                Ok(Transformed::no(e))
            }
        })?
        .data)
}

fn rewrite_state_calls(
    expr: Expr,
    ops: &mut Vec<StatefulOpDesc>,
    counter: &mut usize,
) -> DFResult<Expr> {
    rewrite_guarded_state_calls(expr, ops, counter, None)
}

fn rewrite_guarded_state_calls(
    expr: Expr,
    ops: &mut Vec<StatefulOpDesc>,
    counter: &mut usize,
    guard: Option<Expr>,
) -> DFResult<Expr> {
    if !contains_state_function(&expr) {
        return Ok(expr);
    }
    let expr = expr.transform_down(&mut |e| {
        if let Expr::Case(mut case) = e {
            if case.expr.as_ref().is_some_and(|e| contains_state_function(e)) || case.when_then_expr.iter().any(|(w, _)| contains_state_function(w)) {
                return plan_err!("state functions in CASE conditions are unsupported; compute the condition in an earlier SELECT stage");
            }
            if case.expr.as_ref().is_some_and(|expr| contains_volatile_scalar(expr)) || case.when_then_expr.iter().any(|(when, _)| contains_volatile_scalar(when)) {
                return plan_err!("volatile CASE conditions with state functions are unsupported; materialize the condition before the first stateful SELECT");
            }
            let mut remaining = guard.clone().unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true)), None));
            for (when, then) in &mut case.when_then_expr {
                let condition = match &case.expr { Some(base) => (**base).clone().eq((**when).clone()), None => (**when).clone() };
                let selected = Expr::IsTrue(Box::new(condition));
                **then = rewrite_guarded_state_calls((**then).clone(), ops, counter, Some(remaining.clone().and(selected.clone())))?;
                remaining = remaining.and(Expr::Not(Box::new(selected)));
            }
            if let Some(otherwise) = &mut case.else_expr {
                **otherwise = rewrite_guarded_state_calls((**otherwise).clone(), ops, counter, Some(remaining))?;
            }
            // State calls were already replaced in each selected branch.
            return Ok(Transformed::yes(Expr::Case(case)));
        }
        if let Expr::BinaryExpr(mut binary) = e {
            if matches!(binary.op, datafusion::logical_expr::Operator::And | datafusion::logical_expr::Operator::Or) && contains_state_function(&Expr::BinaryExpr(binary.clone())) {
                *binary.left = rewrite_guarded_state_calls((*binary.left).clone(), ops, counter, guard.clone())?;
                if contains_volatile_scalar(&binary.left) {
                    return plan_err!("volatile AND/OR conditions with state functions are unsupported; materialize the condition before the first stateful SELECT");
                }
                let needed = match binary.op {
                    datafusion::logical_expr::Operator::And => Expr::IsNotFalse(Box::new((*binary.left).clone())),
                    _ => Expr::IsNotTrue(Box::new((*binary.left).clone())),
                };
                let right_guard = match &guard { Some(outer) => outer.clone().and(needed), None => needed };
                *binary.right = rewrite_guarded_state_calls((*binary.right).clone(), ops, counter, Some(right_guard))?;
                return Ok(Transformed::yes(Expr::BinaryExpr(binary)));
            }
            return Ok(Transformed::no(Expr::BinaryExpr(binary)));
        }
        if let Expr::ScalarFunction(f) = &e
            && matches!(f.func.name().to_ascii_lowercase().as_str(), "coalesce" | "nvl" | "ifnull" | "if" | "iif" | "nvl2") && f.args.iter().any(contains_state_function) {
            return plan_err!("state functions inside {} are unsupported; use CASE with a state-free condition", f.func.name().to_ascii_uppercase());
        }
        Ok(Transformed::no(e))
    })?.data;
    let result = expr.transform_up(&mut |e| {
        let Expr::ScalarFunction(ScalarFunction { ref func, ref args }) = e else {
            return Ok(Transformed::no(e));
        };

        if !StatefulProcessorRewriter::is_state_function(func.name()) {
            return Ok(Transformed::no(e));
        }

        let func_name = func.name().to_string();
        let output_field = format!("__state_result_{}", counter);
        *counter += 1;

        // First argument must be a string literal: the map name
        let map_name = match &args[0] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => {
                return plan_err!(
                    "first argument to {} must be a string literal (the map name)",
                    func_name
                );
            }
        };

        if map_name.is_empty() || map_name.len() > 249 || !map_name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')) {
            return plan_err!("state map name must be 1-249 ASCII letters, digits, underscores, or hyphens; path separators and relative paths are unsupported");
        }

        // Second argument: the key expression
        let key_expr = match &guard {
            Some(guard) => Expr::Case(datafusion::logical_expr::expr::Case {
                expr: None,
                when_then_expr: vec![(Box::new(guard.clone()), Box::new(args[1].clone()))],
                else_expr: Some(Box::new(Expr::Literal(ScalarValue::Utf8(None), None))),
            }),
            None => args[1].clone(),
        };

        // Third argument (for put/upsert/update): the value expression
        let value_expr = if args.len() > 2 {
            Some(args[2].clone())
        } else {
            None
        };

        // Fourth argument (for update only): the condition expression
        let condition_expr = if args.len() > 3 {
            Some(args[3].clone())
        } else {
            None
        };

        ops.push(StatefulOpDesc {
            map_name,
            op_type: StatefulProcessorRewriter::state_op_type(&func_name),
            key_expr,
            value_expr,
            condition_expr,
            output_field: output_field.clone(),
        });

        Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            output_field,
        ))))
    })?;

    Ok(result.data)
}

impl TreeNodeRewriter for StatefulProcessorRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(projection) = node else {
            // State functions are only valid in SELECT projections.
            // Check non-projection nodes for misuse and produce a clear error.
            for e in node.expressions() {
                if contains_state_function(&e) {
                    return plan_err!(
                        "state functions (state_get, state_put, etc.) \
                         are only supported in SELECT projections"
                    );
                }
            }
            return Ok(Transformed::no(node));
        };

        let flattened = flatten_state_input(&projection.input)?;
        let (state_input, mut ops, exprs) = if let Some((base, ops, mapping)) = flattened {
            let exprs = projection
                .expr
                .iter()
                .cloned()
                .map(|e| substitute_state_columns(e, projection.input.schema(), &mapping))
                .collect::<DFResult<Vec<_>>>()?;
            (base, ops, exprs)
        } else {
            ((*projection.input).clone(), vec![], projection.expr.clone())
        };
        if ops.is_empty() && projection.expr.iter().any(contains_state_function) {
            let mut found = false;
            projection.input.apply(|plan| {
                if let LogicalPlan::Extension(e) = plan
                    && e.node.as_any().is::<StatefulProcessorExtension>()
                {
                    found = true;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if found {
                return plan_err!(
                    "stateful stages separated by filters, joins, unions, or other non-projection operators cannot share ordered maps; use a linear CTE projection chain"
                );
            }
        }
        let original_ops = ops.len();

        // Rewrite each projection expression, extracting state function calls.
        // Uses self.counter (shared across all projections in the plan) so that
        // __state_result_N names are unique when multiple CTEs each contain state calls.
        let mut new_exprs = Vec::with_capacity(projection.expr.len());
        for expr in exprs {
            new_exprs.push(rewrite_state_calls(expr, &mut ops, &mut self.counter)?);
        }

        if ops.is_empty() {
            // No state functions found -- reconstruct unchanged projection
            return Ok(Transformed::no(LogicalPlan::Projection(
                Projection::try_new_with_schema(new_exprs, projection.input, projection.schema)?,
            )));
        }

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(StatefulProcessorExtension {
                // Materialize the ordinary input plan before evaluating state calls.
                // Graph traversal only creates operators for extensions; without this
                // boundary filters and computed CTE columns would be skipped and the
                // worker would receive the upstream extension's different schema.
                input: if original_ops > 0 {
                    state_input
                } else {
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(RemoteTableExtension {
                            input: state_input,
                            name: TableReference::bare(format!("__state_input_{}", self.counter)),
                            schema: projection.input.schema().clone(),
                            materialize: false,
                        }),
                    })
                },
                ops,
                final_exprs: new_exprs,
                final_schema: projection.schema,
            }),
        })))
    }
}

#[cfg(test)]
mod stateful_processor_tests {
    use super::*;
    use arroyo_rpc::grpc::api::StateOpType;
    use datafusion::prelude::col;

    #[test]
    fn test_is_state_function_positive() {
        assert!(StatefulProcessorRewriter::is_state_function("state_get"));
        assert!(StatefulProcessorRewriter::is_state_function("state_put"));
        assert!(StatefulProcessorRewriter::is_state_function("state_upsert"));
        assert!(StatefulProcessorRewriter::is_state_function("state_update"));
        assert!(StatefulProcessorRewriter::is_state_function("state_delete"));
    }

    #[test]
    fn test_is_state_function_negative() {
        assert!(!StatefulProcessorRewriter::is_state_function("my_udf"));
        assert!(!StatefulProcessorRewriter::is_state_function("get_state"));
        assert!(!StatefulProcessorRewriter::is_state_function("state_"));
        assert!(!StatefulProcessorRewriter::is_state_function(""));
        assert!(!StatefulProcessorRewriter::is_state_function("STATE_GET"));
    }

    #[test]
    fn test_state_op_type_mapping() {
        assert_eq!(
            StatefulProcessorRewriter::state_op_type("state_get"),
            StateOpType::StateGet as i32
        );
        assert_eq!(
            StatefulProcessorRewriter::state_op_type("state_put"),
            StateOpType::StatePut as i32
        );
        assert_eq!(
            StatefulProcessorRewriter::state_op_type("state_upsert"),
            StateOpType::StateUpsert as i32
        );
        assert_eq!(
            StatefulProcessorRewriter::state_op_type("state_update"),
            StateOpType::StateUpdate as i32
        );
        assert_eq!(
            StatefulProcessorRewriter::state_op_type("state_delete"),
            StateOpType::StateDelete as i32
        );
    }

    #[test]
    fn test_contains_state_function_column_expr() {
        let expr = col("some_column");
        assert!(!contains_state_function(&expr));
    }

    #[test]
    fn test_contains_state_function_literal_expr() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())), None);
        assert!(!contains_state_function(&expr));
    }

    #[test]
    fn test_contains_state_function_binary_expr() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: logical_expr::Operator::Eq,
            right: Box::new(col("b")),
        });
        assert!(!contains_state_function(&expr));
    }

    #[test]
    fn test_rewrite_state_calls_no_state_functions() {
        let expr = col("some_column");
        let mut ops = vec![];
        let mut counter = 0;
        let result = rewrite_state_calls(expr.clone(), &mut ops, &mut counter).unwrap();
        assert!(ops.is_empty());
        assert_eq!(counter, 0);
        assert_eq!(result, expr);
    }
}
