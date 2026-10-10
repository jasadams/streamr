use arroyo_datastream::WindowType;
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_field};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DataFusionError, Result, Spans, TableReference, plan_err,
    tree_node::{TreeNode, TreeNodeRewriter, TreeNodeVisitor},
};
use std::{collections::HashSet, sync::Arc};

use aggregate::AggregateRewriter;
pub(crate) use aggregate::{COLLECTION_LIMIT_PREFIX, CalendarAggregate};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, Filter, LogicalPlan, Projection, SubqueryAlias, expr::Alias, lit,
    when,
};
use datafusion::prelude::named_struct;
use datafusion::scalar::ScalarValue;
use join::JoinRewriter;

use self::window_fn::WindowFunctionRewriter;
use crate::functions::multi_hash;
use crate::rewriters::TimeWindowNullCheckRemover;
use crate::{
    ArroyoSchemaProvider, DFField, WindowBehavior,
    extension::{
        aggregate::{AGGREGATE_EXTENSION_NAME, AggregateExtension},
        join::JOIN_NODE_NAME,
    },
    fields_with_qualifiers, find_window,
    rewriters::SourceRewriter,
    rewriters::{
        EVENT_CLOCK_PROVENANCE, EventClockRewriter, RowTimeRewriter, depends_on_event_clock,
        event_timestamp_index,
    },
    schema_from_df_fields_with_metadata,
    schemas::{add_timestamp_field, has_timestamp_field},
};
use crate::{
    extension::{ArroyoExtension, remote_table::RemoteTableExtension},
    rewriters::AsyncUdfRewriter,
};

mod aggregate;
pub(crate) mod current_result;
mod join;
mod window_fn;

#[derive(Debug, Default)]
struct WindowDetectingVisitor {
    window: Option<WindowType>,
    fields: HashSet<DFField>,
}

impl WindowDetectingVisitor {
    fn get_window(logical_plan: &LogicalPlan) -> Result<Option<WindowType>> {
        let mut visitor = WindowDetectingVisitor {
            window: None,
            fields: HashSet::new(),
        };
        logical_plan.visit_with_subqueries(&mut visitor)?;
        Ok(visitor.window.take())
    }
}

fn extract_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(Alias { expr, .. }) => extract_column(expr),
        _ => None,
    }
}

// An updating row ID identifies a row within its producer, not across UNION
// inputs. Scope it at the edge so two identical inputs (including a shared CTE)
// still contribute two distinct bag members. The domain and ordinal are stable
// for a given logical UNION plan, and a nested UNION adds another scope.
fn scope_union_updating_id(input: Arc<LogicalPlan>, branch: usize) -> Result<Arc<LogicalPlan>> {
    let schema = input.schema().clone();
    let metadata_index = schema.index_of_column(&Column::from_name(UPDATING_META_FIELD))?;
    let metadata = schema.field(metadata_index);
    if metadata.data_type() != updating_meta_field().data_type() || metadata.is_nullable() {
        return plan_err!("UNION input has incompatible updating metadata");
    }
    let metadata_column =
        Expr::Column(fields_with_qualifiers(&schema)[metadata_index].qualified_column());
    let old_id = get_field(metadata_column.clone(), "id");
    let scoped_id = multi_hash().call(vec![
        lit("streamr.union.updating-id.v1"),
        lit(i64::try_from(branch).map_err(|_| {
            DataFusionError::Plan("UNION has too many updating branches".to_string())
        })?),
        old_id.clone(),
    ]);
    let scoped_id = when(
        old_id.is_null(),
        lit(ScalarValue::FixedSizeBinary(16, None)),
    )
    .otherwise(scoped_id)?;
    let scoped_fields = named_struct(vec![
        lit("is_retract"),
        get_field(metadata_column.clone(), "is_retract"),
        lit("id"),
        scoped_id,
    ]);
    let scoped_metadata =
        when(metadata_column.clone().is_null(), metadata_column).otherwise(scoped_fields)?;
    let expressions = fields_with_qualifiers(&schema)
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            if index == metadata_index {
                scoped_metadata.clone().alias(UPDATING_META_FIELD)
            } else {
                Expr::Column(field.qualified_column())
            }
        })
        .collect();
    Ok(Arc::new(LogicalPlan::Projection(
        Projection::try_new_with_schema(expressions, input, schema)?,
    )))
}

impl TreeNodeVisitor<'_> for WindowDetectingVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        // handle Join in the pre-join, as each side needs to be checked separately.
        if node.name() == JOIN_NODE_NAME {
            let input_windows: HashSet<_> = node
                .inputs()
                .iter()
                .map(|input| Self::get_window(input))
                .collect::<Result<HashSet<_>>>()?;
            if input_windows.len() > 1 {
                return Err(DataFusionError::Plan(
                    "can't handle mixed windowing between left and right".to_string(),
                ));
            }
            self.window = input_windows
                .into_iter()
                .next()
                .expect("join has at least one input");
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Projection(projection) => {
                let window_expressions = projection
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = projection
                                .input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => {
                                    return Some(Err(err));
                                }
                            };
                            if self.fields.contains(
                                &(column.relation.clone(), Arc::new(input_field.clone())).into(),
                            ) {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                // A finalized window result is an ordinary append relation once
                // this projection stops carrying its complete window field.
                // A scalar such as `window.end` is data, not window scope for
                // an aggregate further downstream.
                if window_expressions.is_empty() {
                    self.window = None;
                }
                self.fields.clear();
                for (index, window) in window_expressions {
                    // if there's already a window they should match
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return plan_err!(
                                "can't window by both {:?} and {:?}",
                                existing_window,
                                window
                            );
                        }
                        self.fields
                            .insert(projection.schema.qualified_field(index).into());
                    } else {
                        // If the input doesn't have an input window, we shouldn't be creating a window.
                        return plan_err!(
                            "can't call a windowing function without grouping by it in an aggregate"
                        );
                    }
                }
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                // translate the fields to the output schema
                self.fields = self
                    .fields
                    .drain()
                    .map(|field| {
                        Ok(subquery_alias
                            .schema
                            .qualified_field(
                                subquery_alias
                                    .input
                                    .schema()
                                    .index_of_column(&field.qualified_column())?,
                            )
                            .into())
                    })
                    .collect::<Result<HashSet<_>>>()?;
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr: _,
                schema,
                ..
            }) => {
                let window_expressions = group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => {
                                    return Some(Err(err));
                                }
                            };
                            if self
                                .fields
                                .contains(&(column.relation.as_ref(), input_field).into())
                            {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.fields.clear();
                for (index, window) in window_expressions {
                    // if there's already a window they should match
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window);
                    }
                    self.fields.insert(schema.qualified_field(index).into());
                }
            }
            LogicalPlan::Extension(Extension { node })
                if node.name() == AGGREGATE_EXTENSION_NAME =>
            {
                let aggregate_extension = node
                    .as_any()
                    .downcast_ref::<AggregateExtension>()
                    .expect("should be aggregate extension");

                match &aggregate_extension.window_behavior {
                    WindowBehavior::FromOperator {
                        window,
                        window_field,
                        window_index: _,
                        is_nested,
                    } => {
                        if self.window.is_some() && !*is_nested {
                            return Err(DataFusionError::Plan(
                                    "aggregate node should not be recalculating window, as input is windowed.".to_string(),
                                ));
                        }
                        self.window = Some(window.clone());
                        self.fields.insert(window_field.clone());
                    }
                    WindowBehavior::InData => {
                        let input_fields = self.fields.clone();
                        self.fields.clear();
                        for field in fields_with_qualifiers(node.schema()) {
                            if input_fields.contains(&field) {
                                self.fields.insert(field);
                            }
                        }
                        if self.fields.is_empty() {
                            return Err(DataFusionError::Plan(
                                    "must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input".to_string(),
                                ));
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

// This is one rewriter so that we can rely on inputs having already been rewritten
// ensuring they have _timestamp field, amongst other things.
pub struct ArroyoRewriter<'a> {
    pub(crate) schema_provider: &'a ArroyoSchemaProvider,
}

impl<'a> ArroyoRewriter<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        Self { schema_provider }
    }
}

impl TreeNodeRewriter for ArroyoRewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        aggregate::bound_collection_outputs(node)
    }

    fn f_up(&mut self, mut node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::Projection(ref mut projection) => {
                let clock_outputs = projection
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expression)| {
                        depends_on_event_clock(expression, projection.input.schema())
                            .then_some(index)
                    })
                    .collect::<Vec<_>>();
                if !clock_outputs.is_empty() {
                    let mut output_fields = fields_with_qualifiers(&projection.schema);
                    for index in clock_outputs {
                        let mut metadata = output_fields[index].metadata().clone();
                        metadata.insert(EVENT_CLOCK_PROVENANCE.to_string(), "trigger".to_string());
                        output_fields[index] = output_fields[index].clone().with_metadata(metadata);
                    }
                    projection.schema = Arc::new(schema_from_df_fields_with_metadata(
                        &output_fields,
                        projection.schema.metadata().clone(),
                    )?);
                }
                projection.expr = projection
                    .expr
                    .iter()
                    .map(|expr| {
                        Ok(expr
                            .clone()
                            .rewrite(&mut EventClockRewriter {
                                input: &projection.input,
                            })?
                            .data)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if projection.input.exists(|plan| {
                    Ok(matches!(plan, LogicalPlan::Extension(extension)
                        if extension.node.as_any().is::<crate::extension::state_table::StateTableScan>()))
                })? {
                    return plan_err!(
                        "state-table scans require an input-event keyed INNER or LEFT JOIN; standalone target scans are unsupported"
                    );
                }
                let event_index = event_timestamp_index(&projection.input);
                let retained_timestamp_in_lineage = projection.input.exists(|plan| {
                    Ok(matches!(plan, LogicalPlan::Extension(extension)
                        if extension.node.as_any().downcast_ref::<crate::extension::state_table::StateTableAccess>()
                            .is_some_and(|access| access.table.schema.fields().iter().any(|field| field.name() == TIMESTAMP_FIELD))))
                })?;
                if retained_timestamp_in_lineage {
                    for (position, expression) in projection.expr.iter().enumerate() {
                        if projection.schema.field(position).name() != TIMESTAMP_FIELD {
                            continue;
                        }
                        let column = match expression {
                            Expr::Column(column) => Some(column),
                            Expr::Alias(alias) => match alias.expr.as_ref() {
                                Expr::Column(column) => Some(column),
                                _ => None,
                            },
                            _ => None,
                        };
                        if event_index.is_none_or(|event_index| {
                            column.is_none_or(|column| {
                                projection.input.schema().index_of_column(column).ok()
                                    != Some(event_index)
                            })
                        }) {
                            return plan_err!(
                                "projected state-table _timestamp conflicts with event time; alias the retained value to a different output name"
                            );
                        }
                    }
                }
                if !has_timestamp_field(&projection.schema) {
                    let timestamp_field: DFField = if let Some(index) = event_index {
                        // A lookup target may also declare `_timestamp`. Use the
                        // access's qualified event-time ordinal, not an
                        // unqualified name lookup that can select the target.
                        projection.input.schema().qualified_field(index).into()
                    } else {
                        projection.input.schema().qualified_field_with_unqualified_name(TIMESTAMP_FIELD).map_err(|_| {
                            DataFusionError::Plan(format!("No timestamp field found in projection input ({}). Query should've been rewritten", projection.input.display()))
                        })?.into()
                    };
                    projection.schema = add_timestamp_field(
                        projection.schema.clone(),
                        timestamp_field.qualifier().cloned(),
                    )
                    .expect("in projection");
                    projection.expr.push(Expr::Column(Column {
                        relation: timestamp_field.qualifier().cloned(),
                        name: "_timestamp".to_string(),
                        spans: Spans::default(),
                    }));
                }
                if projection
                    .input
                    .schema()
                    .has_column_with_unqualified_name(UPDATING_META_FIELD)
                    && !projection
                        .schema
                        .has_column_with_unqualified_name(UPDATING_META_FIELD)
                {
                    let field: DFField = projection
                        .input
                        .schema()
                        .qualified_field_with_unqualified_name(UPDATING_META_FIELD)?
                        .into();
                    let mut output_fields = fields_with_qualifiers(&projection.schema);
                    output_fields.push(field.clone());
                    projection.schema = Arc::new(schema_from_df_fields_with_metadata(
                        &output_fields,
                        projection.schema.metadata().clone(),
                    )?);
                    projection.expr.push(Expr::Column(field.qualified_column()));
                }

                let rewritten = projection
                    .expr
                    .iter()
                    .map(|expr| expr.clone().rewrite(&mut RowTimeRewriter {}))
                    .collect::<Result<Vec<_>>>()?;
                if rewritten.iter().any(|r| r.transformed) {
                    projection.expr = rewritten.into_iter().map(|r| r.data).collect();
                }

                return AsyncUdfRewriter::new(self.schema_provider).f_up(node);
            }
            LogicalPlan::Aggregate(aggregate) => {
                return AggregateRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Aggregate(aggregate));
            }
            LogicalPlan::Join(join) => {
                if let Some(lookup) = crate::extension::state_table::plan_lookup(&join)? {
                    return Ok(Transformed::yes(lookup));
                }
                return JoinRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Join(join));
            }
            LogicalPlan::TableScan(table_scan) => {
                return SourceRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::TableScan(table_scan));
            }
            LogicalPlan::Filter(mut f) => {
                f.predicate = f
                    .predicate
                    .rewrite(&mut EventClockRewriter { input: &f.input })?
                    .data;

                // Joins with windows in the join condition can cause IS NOT NULL predicates to get
                // pushed down to the table scan; however windows can never be null, and they can't
                // be evaluated in filters—so we just remove them
                let expr = f
                    .predicate
                    .clone()
                    .rewrite(&mut TimeWindowNullCheckRemover {})?;
                return Ok(if expr.transformed {
                    Transformed::yes(LogicalPlan::Filter(Filter::try_new(expr.data, f.input)?))
                } else {
                    Transformed::no(LogicalPlan::Filter(f))
                });
            }
            LogicalPlan::Window(_) => {
                return WindowFunctionRewriter {}.f_up(node);
            }
            LogicalPlan::Sort(_) => {
                return plan_err!("ORDER BY is not currently supported ({})", node.display());
            }
            LogicalPlan::Repartition(_) => {
                return plan_err!(
                    "Repartitions are not currently supported ({})",
                    node.display()
                );
            }
            LogicalPlan::Union(mut union) => {
                let metadata_positions = union
                    .inputs
                    .iter()
                    .map(|input| {
                        input
                            .schema()
                            .has_column_with_unqualified_name(UPDATING_META_FIELD)
                            .then(|| {
                                input
                                    .schema()
                                    .index_of_column(&Column::from_name(UPDATING_META_FIELD))
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                if let Some(position) = metadata_positions.iter().copied().flatten().next() {
                    if metadata_positions
                        .iter()
                        .any(|candidate| *candidate != Some(position))
                    {
                        return plan_err!(
                            "UNION updating metadata must be present at the same column in every input"
                        );
                    }
                    for (branch, input) in union.inputs.iter_mut().enumerate() {
                        *input = scope_union_updating_id(input.clone(), branch)?;
                    }
                }
                // Input rewrites can add the event timestamp and changelog
                // metadata after DataFusion first derived the UNION schema.
                // Keep the first rewritten input's names, qualifiers, types,
                // and metadata, but a column is nullable if *any* branch can
                // produce NULL. The materializing branch extensions and the
                // union edge must agree on that widened Arrow contract.
                let first = union
                    .inputs
                    .first()
                    .ok_or_else(|| DataFusionError::Plan("UNION has no inputs".to_string()))?;
                let first_schema = first.schema();
                let width = first_schema.fields().len();
                for input in union.inputs.iter().skip(1) {
                    if input.schema().fields().len() != width {
                        return plan_err!("rewritten UNION inputs have different column counts");
                    }
                }
                let fields = (0..width)
                    .map(|index| {
                        let (qualifier, field) = first_schema.qualified_field(index);
                        let nullable = union
                            .inputs
                            .iter()
                            .any(|input| input.schema().field(index).is_nullable());
                        (
                            qualifier.cloned(),
                            Arc::new(field.clone().with_nullable(nullable)),
                        )
                    })
                    .collect();
                union.schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    first_schema.metadata().clone(),
                )?);

                // Need all the elements of the union to be materialized
                for input in union.inputs.iter_mut() {
                    if let LogicalPlan::Extension(Extension { node }) = input.as_ref() {
                        let arroyo_extension: &dyn ArroyoExtension = node.try_into().unwrap();
                        if !arroyo_extension.transparent() {
                            continue;
                        }
                    }
                    let remote_table_extension = Arc::new(RemoteTableExtension {
                        input: input.as_ref().clone(),
                        name: TableReference::bare("union_input"),
                        schema: union.schema.clone(),
                        materialize: false,
                    });
                    *input = Arc::new(LogicalPlan::Extension(Extension {
                        node: remote_table_extension,
                    }));
                }

                // A logical UNION has no graph node of its own. Without this
                // boundary its branch nodes become separate inputs to the next
                // operator, which may require exactly one input. The remote
                // table's UNION case forwards each incoming batch once; the
                // per-branch projections above still scope updating row IDs.
                let union = LogicalPlan::Union(union);
                let schema = union.schema().clone();
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(RemoteTableExtension {
                        input: union,
                        name: TableReference::bare("union_output"),
                        schema,
                        materialize: false,
                    }),
                })));
            }
            LogicalPlan::EmptyRelation(_) => {}
            LogicalPlan::Subquery(_) => {}
            LogicalPlan::SubqueryAlias(sa) => {
                // recreate from our children, in case the schemas have changed
                return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(sa.input, sa.alias)?,
                )));
            }
            LogicalPlan::Limit(_) => {
                return plan_err!("LIMIT is not currently supported ({})", node.display());
            }
            LogicalPlan::Statement(s) => {
                return plan_err!("Unsupported statement: {}", s.display());
            }
            LogicalPlan::Values(_) => {}
            LogicalPlan::Explain(_) => {
                return plan_err!("EXPLAIN is not supported ({})", node.display());
            }
            LogicalPlan::Analyze(_) => {
                return plan_err!("ANALYZE is not supported ({})", node.display());
            }
            LogicalPlan::Extension(_) => {}
            LogicalPlan::Distinct(_) => {}
            LogicalPlan::Dml(_) => {}
            LogicalPlan::Ddl(_) => {}
            LogicalPlan::Copy(_) => {
                return plan_err!("COPY is not supported ({})", node.display());
            }
            LogicalPlan::DescribeTable(_) => {
                return plan_err!("DESCRIBE is not supported ({})", node.display());
            }
            LogicalPlan::Unnest(_) => {}
            LogicalPlan::RecursiveQuery(_) => {
                return plan_err!("Recursive CTEs are not supported ({})", node.display());
            }
        }
        Ok(Transformed::no(node))
    }
}

#[cfg(test)]
mod union_id_tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::logical_expr::{EmptyRelation, Union};

    #[test]
    fn branch_hash_distinguishes_equal_source_ids_and_is_stable() {
        let old_id = ScalarValue::FixedSizeBinary(16, Some(vec![7; 16]));
        let hash = crate::functions::MultiHashFunction::default();
        let scoped = |branch| {
            let value = hash
                .invoke(&[
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        "streamr.union.updating-id.v1".to_string(),
                    ))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(branch))),
                    ColumnarValue::Scalar(old_id.clone()),
                ])
                .unwrap();
            let ColumnarValue::Scalar(value) = value else {
                panic!("constant branch hash must be a scalar");
            };
            value
        };
        assert_ne!(scoped(0), scoped(1));
        assert_eq!(scoped(0), scoped(0));
    }

    fn updating_input() -> Arc<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            updating_meta_field().as_ref().clone(),
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]);
        let schema = Arc::new(
            DFSchema::try_from_qualified_schema(TableReference::bare("shared"), &schema).unwrap(),
        );
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        }))
    }

    fn union_branches(plan: LogicalPlan) -> Vec<Arc<LogicalPlan>> {
        let LogicalPlan::Extension(extension) = plan else {
            panic!("expected consolidated UNION");
        };
        let remote = extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
            .expect("UNION must have a graph consolidation boundary");
        assert_eq!(remote.name, TableReference::bare("union_output"));
        assert!(!remote.materialize);
        let LogicalPlan::Union(union) = &remote.input else {
            panic!("consolidation boundary must contain the UNION");
        };
        union.inputs.clone()
    }

    fn scoped_projection(branch: &LogicalPlan) -> &Projection {
        let LogicalPlan::Extension(extension) = branch else {
            panic!("each updating UNION edge must be materialized");
        };
        let remote = extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
            .expect("each UNION edge uses a distinct remote-table materialization");
        assert!(!remote.materialize);
        let LogicalPlan::Projection(projection) = &remote.input else {
            panic!("branch identity must be projected at the UNION edge");
        };
        projection
    }

    #[test]
    fn identical_updating_inputs_get_stable_distinct_branch_ids() {
        let provider = ArroyoSchemaProvider::new();
        let input = updating_input();
        let union = LogicalPlan::Union(Union {
            inputs: vec![input.clone(), input.clone()],
            schema: input.schema().clone(),
        });
        let rewrite = || {
            ArroyoRewriter::new(&provider)
                .f_up(union.clone())
                .unwrap()
                .data
        };
        let first = union_branches(rewrite());
        let second = union_branches(rewrite());
        let left = scoped_projection(&first[0]);
        let right = scoped_projection(&first[1]);
        assert_eq!(left.schema.as_ref(), input.schema().as_ref());
        assert_eq!(right.schema.as_ref(), input.schema().as_ref());
        assert_eq!(left.expr[0], right.expr[0]);
        assert_eq!(left.expr[2], right.expr[2]);
        assert_ne!(left.expr[1], right.expr[1]);
        assert_eq!(left.expr[1], scoped_projection(&second[0]).expr[1]);
        assert_eq!(right.expr[1], scoped_projection(&second[1]).expr[1]);
        for projection in [left, right] {
            let metadata = projection.expr[1].to_string();
            assert!(metadata.contains("multi_hash"));
            assert!(metadata.contains("streamr.union.updating-id.v1"));
            assert!(metadata.contains("is_retract"));
            assert!(metadata.contains("IS NULL"));
        }
    }

    #[test]
    fn nested_union_adds_a_second_branch_scope() {
        let provider = ArroyoSchemaProvider::new();
        let input = updating_input();
        let inner = ArroyoRewriter::new(&provider)
            .f_up(LogicalPlan::Union(Union {
                inputs: vec![input.clone(), input.clone()],
                schema: input.schema().clone(),
            }))
            .unwrap()
            .data;
        let outer = ArroyoRewriter::new(&provider)
            .f_up(LogicalPlan::Union(Union {
                inputs: vec![Arc::new(inner.clone()), Arc::new(inner)],
                schema: input.schema().clone(),
            }))
            .unwrap()
            .data;
        let branches = union_branches(outer);
        assert_ne!(
            scoped_projection(&branches[0]).expr[1],
            scoped_projection(&branches[1]).expr[1]
        );
        for branch in branches {
            let projection = scoped_projection(&branch);
            assert!(matches!(
                projection.input.as_ref(),
                LogicalPlan::Extension(_)
            ));
        }
    }
}
