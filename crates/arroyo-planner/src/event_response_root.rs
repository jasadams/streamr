//! Canonical shared source extraction for validated private response routes.
use crate::builder::Planner;
use crate::extension::debezium::DebeziumUnrollingExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::table_source::TableSourceExtension;
use crate::extension::watermark_node::WatermarkNode;
use crate::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DebeziumUnrollingExec};
use crate::rewriters::SourceRewriter;
use datafusion::common::{Column, Result, plan_err};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, Projection, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(crate) struct SharedSourceRoot {
    pub root: LogicalPlan,
    pub branches: Vec<LogicalPlan>,
    pub source_columns: Vec<usize>,
}

fn extension(plan: &LogicalPlan) -> Option<&dyn UserDefinedLogicalNode> {
    match plan {
        LogicalPlan::Extension(e) => Some(e.node.as_ref()),
        _ => None,
    }
}
pub(crate) fn source_projection(
    plan: &LogicalPlan,
) -> Option<(&Projection, &TableSourceExtension)> {
    let LogicalPlan::Projection(projection) = plan else {
        return None;
    };
    let input = extension(&projection.input)?;
    if let Some(source) = input.as_any().downcast_ref::<TableSourceExtension>() {
        return Some((projection, source));
    }
    let unroll = input
        .as_any()
        .downcast_ref::<DebeziumUnrollingExtension>()?;
    let inputs = unroll.inputs();
    let input = extension(inputs.first().copied()?)?;
    Some((
        projection,
        input.as_any().downcast_ref::<TableSourceExtension>()?,
    ))
}
fn scalar_input(plan: &LogicalPlan) -> Result<&LogicalPlan> {
    if let Some(node) = extension(plan) {
        let allowed = node.as_any().is::<RemoteTableExtension>()
            || node.as_any().is::<WatermarkNode>()
            || node.as_any().is::<KeyCalculationExtension>();
        if !allowed {
            return plan_err!("response root requires a direct single-source scalar branch");
        }
    } else if !matches!(
        plan,
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_)
    ) {
        return plan_err!("response root requires a direct single-source scalar branch");
    }
    let inputs = plan.inputs();
    if inputs.len() != 1 {
        return plan_err!("response root requires one scalar input");
    }
    Ok(inputs[0])
}
pub(crate) fn find_source(plan: &LogicalPlan) -> Result<(&Projection, &TableSourceExtension)> {
    if let Some(found) = source_projection(plan) {
        return Ok(found);
    }
    find_source(scalar_input(plan)?)
}
pub(crate) fn projection_columns(
    projection: &Projection,
    source: &TableSourceExtension,
) -> Result<Vec<usize>> {
    let indices = source
        .table
        .fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            projection
                .schema
                .index_of_column_by_name(Some(&source.name), field.field().name())
                .map(|_| index)
        })
        .collect::<Vec<_>>();
    let expected =
        SourceRewriter::canonical_projection(&source.table, &source.name, Some(indices.clone()))?;
    let LogicalPlan::Projection(expected) = expected else {
        unreachable!()
    };
    if projection.expr != expected.expr || projection.schema != expected.schema {
        return plan_err!("response root source projection is not canonical source plumbing");
    }
    Ok(indices)
}
/// Canonical source-generated columns may be shared only when their native
/// expression is repeatable. Source clock/watermark expressions retain their
/// existing dedicated admission proof; branch expressions never move here.
fn validate_source_virtuals(
    source: &TableSourceExtension,
    indices: impl IntoIterator<Item = usize>,
) -> Result<()> {
    for index in indices {
        if let crate::tables::FieldSpec::Virtual { field, expression } = &source.table.fields[index]
            && source.table.event_time_field.as_ref() != Some(field.name())
            && source.table.watermark_field.as_ref() != Some(field.name())
        {
            crate::continuous_merge::event_source::validate_repeatable(
                expression,
                source.schema(),
            )?;
        }
    }
    Ok(())
}

fn replace_source(
    plan: &LogicalPlan,
    root: &LogicalPlan,
    source: &TableSourceExtension,
) -> Result<LogicalPlan> {
    if source_projection(plan).is_some() {
        // Preserve each branch's exact old field order/qualifiers. Extra union
        // columns exist only on the shared edge, never silently in branch SQL.
        let exprs = crate::fields_with_qualifiers(plan.schema())
            .iter()
            .map(|field| {
                Expr::Column(Column::new(
                    field.qualifier().cloned(),
                    field.name().to_owned(),
                ))
            })
            .collect();
        return Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(root.clone()),
        )?));
    }
    let input = replace_source(scalar_input(plan)?, root, source)?;
    if let Some(node) = extension(plan) {
        if let Some(watermark) = node.as_any().downcast_ref::<WatermarkNode>() {
            if watermark.qualifier != source.name
                || watermark.watermark_expression
                    != SourceRewriter::watermark_expression(&source.table)?
                || watermark.response_root_slot.is_some()
            {
                return plan_err!(
                    "response root cannot replace a different source admission boundary"
                );
            }
            let fields = crate::fields_with_qualifiers(plan.schema());
            let expressions = fields
                .iter()
                .map(|f| Expr::Column(Column::new(f.qualifier().cloned(), f.name().to_owned())))
                .collect();
            return Ok(LogicalPlan::Projection(Projection::try_new(
                expressions,
                Arc::new(input),
            )?));
        }
        if let Some(remote) = node.as_any().downcast_ref::<RemoteTableExtension>() {
            // This is the old source capture, possibly enclosing pushed branch
            // predicates. Its computation moves above the new root. Other named
            // captures remain named and continue to execute once.
            if remote.name == source.name {
                return Ok(input);
            }
        }
    }
    plan.with_new_exprs(plan.expressions(), vec![input])
}

/// route_slot is only a planner memoization key. The transport root_operator is
/// the checked final graph node_id assigned by WatermarkNode::plan_node, never
/// this slot and never a hash/parse of the runtime operator display string.
pub(crate) fn extract_shared_source_root<'a>(
    branches: impl AsRef<[&'a LogicalPlan]>,
    route_slot: u32,
) -> Result<SharedSourceRoot> {
    let branches = branches.as_ref();
    if !(1..=3).contains(&branches.len()) {
        return plan_err!("response extraction requires one to three native branches");
    }
    let (_, left_source) = find_source(branches[0])?;
    let mut union = BTreeSet::new();
    for branch in branches {
        let (projection, source) = find_source(branch)?;
        if source != left_source {
            return plan_err!("response branches require the same physical source definition");
        }
        union.extend(projection_columns(projection, source)?);
    }
    // Source watermark expressions may need a source-generated virtual column
    // even if neither branch projects it. It is source admission, not branch SQL.
    if let Some(name) = &left_source.table.watermark_field {
        let Some(index) = left_source
            .table
            .fields
            .iter()
            .position(|f| f.field().name() == name)
        else {
            return plan_err!("response root watermark field is absent");
        };
        union.insert(index);
    }
    for column in SourceRewriter::watermark_expression(&left_source.table)?.column_refs() {
        if column.name != arroyo_rpc::TIMESTAMP_FIELD {
            let Some(index) = left_source
                .table
                .fields
                .iter()
                .position(|f| f.field().name() == &column.name)
            else {
                return plan_err!("response root watermark dependency is not a source column");
            };
            union.insert(index);
        }
    }
    validate_source_virtuals(left_source, union.iter().copied())?;
    let source_columns = union.into_iter().collect::<Vec<_>>();
    let source_projection = SourceRewriter::canonical_projection(
        &left_source.table,
        &left_source.name,
        Some(source_columns.clone()),
    )?;
    let source_calc = LogicalPlan::Extension(Extension {
        node: Arc::new(RemoteTableExtension {
            accepted_result: None,
            union_schema: false,
            schema: source_projection.schema().clone(),
            input: source_projection,
            name: left_source.name.clone(),
            materialize: false,
        }),
    });
    let mut watermark = WatermarkNode::with_source_envelope(
        source_calc,
        left_source.name.clone(),
        SourceRewriter::watermark_expression(&left_source.table)?,
        left_source.table.is_updating(),
    )?;
    watermark.response_root_slot = Some(route_slot);
    let root = LogicalPlan::Extension(Extension {
        node: Arc::new(watermark),
    });
    let result = SharedSourceRoot {
        branches: branches
            .iter()
            .map(|branch| replace_source(branch, &root, left_source))
            .collect::<Result<Vec<_>>>()?,
        root,
        source_columns,
    };
    Ok(result)
}

/// Check actual physical source execution, not just logical scalar labels.
/// Compare with regenerated canonical source plumbing as well as whitelisting:
/// a projection containing a branch expression must not pass as source work.
pub(crate) fn validate_root_physical(plan: &dyn ExecutionPlan) -> Result<()> {
    if plan.as_any().is::<ArroyoMemExec>() {
        if !plan.children().is_empty() {
            return plan_err!("response source reader must be a leaf");
        }
        return Ok(());
    }
    if !plan
        .as_any()
        .is::<datafusion::physical_plan::projection::ProjectionExec>()
        && !plan.as_any().is::<DebeziumUnrollingExec>()
    {
        return plan_err!(
            "response root physical source contains branch work: {}",
            plan.name()
        );
    }
    let children = plan.children();
    if children.len() != 1 {
        return plan_err!("response root physical source requires one input");
    }
    validate_root_physical(children[0].as_ref())
}
/// Recover the existing source-scope name only after exact canonical admission
/// validation. The name is not the carrier identity or the root descriptor.
pub(crate) fn source_for_response_admission(
    root: &WatermarkNode,
) -> Result<datafusion::common::TableReference> {
    let slot = root.response_root_slot.ok_or_else(|| {
        datafusion::common::plan_datafusion_err!(
            "accepted response boundary has no private source root"
        )
    })?;
    let remote = extension(&root.input)
        .and_then(|node| node.as_any().downcast_ref::<RemoteTableExtension>())
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!(
                "accepted response boundary lacks canonical source calculation"
            )
        })?;
    let (projection, source) = source_projection(&remote.input).ok_or_else(|| {
        datafusion::common::plan_datafusion_err!(
            "accepted response boundary contains non-source work below admission"
        )
    })?;
    let indices = projection_columns(projection, source)?;
    validate_source_virtuals(source, indices.iter().copied())?;
    let expected =
        SourceRewriter::canonical_projection(&source.table, &source.name, Some(indices))?;
    if remote.materialize
        || remote.union_schema
        || remote.accepted_result.is_some()
        || remote.name != source.name
        || &remote.schema != remote.input.schema()
        || remote.input != expected
    {
        return plan_err!("accepted response boundary source calculation is not canonical");
    }
    let mut expected_root = WatermarkNode::with_source_envelope(
        root.input.clone(),
        source.name.clone(),
        SourceRewriter::watermark_expression(&source.table)?,
        source.table.is_updating(),
    )?;
    expected_root.response_root_slot = Some(slot);
    if root != &expected_root {
        return plan_err!(
            "accepted response boundary source schema or clock differs from canonical admission"
        );
    }
    Ok(source.name.clone())
}

#[cfg(test)]
pub(crate) fn prove_root_physical(root: &SharedSourceRoot, planner: &Planner) -> Result<()> {
    let watermark = extension(&root.root)
        .and_then(|n| n.as_any().downcast_ref::<WatermarkNode>())
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("missing response source admission")
        })?;
    let remote = extension(&watermark.input)
        .and_then(|n| n.as_any().downcast_ref::<RemoteTableExtension>())
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("missing response source calculation")
        })?;
    let (_, source) = find_source(&remote.input)?;
    let expected = SourceRewriter::canonical_projection(
        &source.table,
        &source.name,
        Some(root.source_columns.clone()),
    )?;
    let actual = planner.sync_plan(&remote.input)?;
    let expected = planner.sync_plan(&expected)?;
    validate_root_physical(actual.as_ref())?;
    let codec = ArroyoPhysicalExtensionCodec::default();
    let actual = PhysicalPlanNode::try_from_physical_plan(actual, &codec)?.encode_to_vec();
    let expected = PhysicalPlanNode::try_from_physical_plan(expected, &codec)?.encode_to_vec();
    if actual != expected {
        return plan_err!("response root physical source differs from canonical source plumbing");
    }
    Ok(())
}

pub(crate) fn prove_admission_physical(watermark: &WatermarkNode, planner: &Planner) -> Result<()> {
    let remote = extension(&watermark.input)
        .and_then(|node| node.as_any().downcast_ref::<RemoteTableExtension>())
        .ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("response root lacks canonical source capture")
        })?;
    let (projection, source) = find_source(&remote.input)?;
    let indices = projection_columns(projection, source)?;
    let expected =
        SourceRewriter::canonical_projection(&source.table, &source.name, Some(indices))?;
    let actual = planner.sync_plan(&remote.input)?;
    let expected = planner.sync_plan(&expected)?;
    validate_root_physical(actual.as_ref())?;
    let codec = ArroyoPhysicalExtensionCodec::default();
    if PhysicalPlanNode::try_from_physical_plan(actual, &codec)?.encode_to_vec()
        != PhysicalPlanNode::try_from_physical_plan(expected, &codec)?.encode_to_vec()
    {
        return plan_err!("response root physical source differs from canonical source plumbing");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArroyoSchemaProvider;
    use crate::builder::PlanToGraphVisitor;
    use crate::tables::Table;
    use arroyo_datastream::logical::OperatorName;
    use datafusion::common::TableReference;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::Filter;
    use datafusion::prelude::{col, lit};

    fn source() -> (ArroyoSchemaProvider, TableSourceExtension) {
        let mut provider = ArroyoSchemaProvider::new();
        let statement = crate::parse_sql("CREATE TABLE events (id BIGINT NOT NULL, l BIGINT, r BIGINT) WITH (connector='single_file',path='/tmp/str64-root-proof.json',format='json',type='source');").unwrap().remove(0);
        let table = Table::try_from_statement(&statement, &provider)
            .unwrap()
            .unwrap();
        let Table::ConnectorTable(table_definition) = &table else {
            panic!("expected connector source")
        };
        let source =
            TableSourceExtension::new(TableReference::bare("events"), table_definition.clone());
        provider.insert_table(table, vec!["events".to_owned()]);
        (provider, source)
    }
    fn branch(
        source: &TableSourceExtension,
        indices: Vec<usize>,
        predicate: Expr,
    ) -> Result<LogicalPlan> {
        let base =
            SourceRewriter::canonical_projection(&source.table, &source.name, Some(indices))?;
        // Deliberately model an optimizer-pushed predicate BEFORE the old
        // watermark. Merely tagging that old watermark would fail this case.
        let filtered = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(base))?);
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                accepted_result: None,
                union_schema: false,
                schema: filtered.schema().clone(),
                input: filtered,
                name: source.name.clone(),
                materialize: true,
            }),
        });
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(WatermarkNode::new(
                remote,
                source.name.clone(),
                SourceRewriter::watermark_expression(&source.table)?,
            )?),
        }))
    }
    #[test]
    fn accepted_boundary_requires_identical_canonical_source_admission() -> Result<()> {
        let (_, source) = source();
        let left = branch(&source, vec![0, 1], lit(false))?;
        let right = branch(&source, vec![0, 2], lit(true))?;
        let shared = extract_shared_source_root([&left, &right], 41)?;
        let root = extension(&shared.root)
            .and_then(|node| node.as_any().downcast_ref::<WatermarkNode>())
            .expect("canonical admission root");
        assert_eq!(
            crate::merge_event_branches::common_admission_source(root, root, 41)?,
            source.name
        );
        let mut different_slot = root.clone();
        different_slot.response_root_slot = Some(42);
        assert!(
            crate::merge_event_branches::common_admission_source(root, &different_slot, 41)
                .expect_err("different route must reject")
                .to_string()
                .contains("identical canonical response admission")
        );
        let mut unmarked = root.clone();
        unmarked.response_root_slot = None;
        assert!(
            source_for_response_admission(&unmarked)
                .expect_err("unmarked root must reject")
                .to_string()
                .contains("no private source root")
        );
        let mut changed_clock = root.clone();
        changed_clock.watermark_expression = lit(55i64);
        assert!(
            crate::merge_event_branches::common_admission_source(
                &changed_clock,
                &changed_clock,
                41
            )
            .expect_err("matching malformed roots must reject")
            .to_string()
            .contains("source schema or clock differs")
        );
        Ok(())
    }

    #[tokio::test]
    async fn event_response_root_precedes_false_first_branch_and_unions_columns() -> Result<()> {
        let (provider, source) = source();
        let left = branch(&source, vec![0, 1], lit(false))?;
        let right = branch(&source, vec![0, 2], col("r").gt(lit(0i64)))?;
        let root = extract_shared_source_root([&left, &right], 41)?;
        assert_eq!(root.source_columns, vec![0, 1, 2]);
        let mut root_filters = 0;
        root.root.apply(|node| {
            root_filters += usize::from(matches!(node, LogicalPlan::Filter(_)));
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(root_filters, 0);
        for branch in &root.branches {
            let mut filters = 0;
            branch.apply(|node| {
                filters += usize::from(matches!(node, LogicalPlan::Filter(_)));
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(filters, 1);
        }
        assert_eq!(root.branches[0].schema(), left.schema());
        assert_eq!(root.branches[1].schema(), right.schema());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build();
        prove_root_physical(&root, &Planner::new(&provider, &state))?;
        let mut visitor = PlanToGraphVisitor::new(&provider, &state);
        for (i, branch) in root.branches.iter().enumerate() {
            visitor.add_plan(LogicalPlan::Extension(Extension {
                node: Arc::new(RemoteTableExtension {
                    accepted_result: None,
                    union_schema: false,
                    input: branch.clone(),
                    schema: branch.schema().clone(),
                    name: TableReference::bare(format!("branch_{i}")),
                    materialize: true,
                }),
            }))?;
        }
        let graph = visitor.into_graph();
        assert_eq!(graph.externals(petgraph::Direction::Incoming).count(), 1);
        let roots = graph
            .node_weights()
            .filter(|n| n.description.starts_with("event_root_"))
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].parallelism, 1);
        assert_eq!(
            roots[0].description,
            format!("event_root_{}", roots[0].node_id)
        );
        let ids = graph
            .node_weights()
            .map(|n| n.node_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), graph.node_count());
        assert_eq!(
            graph
                .node_weights()
                .filter(|n| n
                    .operator_chain
                    .iter()
                    .any(|(op, _)| op.operator_name == OperatorName::ConnectorSource))
                .count(),
            1
        );
        Ok(())
    }
    #[tokio::test]
    async fn event_response_root_rejects_predicate_or_replaced_expression_before_admission()
    -> Result<()> {
        let (provider, source) = source();
        let left = branch(&source, vec![0, 1], lit(false))?;
        let right = branch(&source, vec![0, 2], lit(true))?;
        let mut root = extract_shared_source_root([&left, &right], 41)?;
        let LogicalPlan::Extension(root_extension) = &root.root else {
            unreachable!()
        };
        let mut watermark = root_extension
            .node
            .as_any()
            .downcast_ref::<WatermarkNode>()
            .unwrap()
            .clone();
        let remote = extension(&watermark.input)
            .unwrap()
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
            .unwrap();
        let mut remote = remote.clone();
        remote.input = LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(remote.input))?);
        watermark.input = LogicalPlan::Extension(Extension {
            node: Arc::new(remote),
        });
        root.root = LogicalPlan::Extension(Extension {
            node: Arc::new(watermark),
        });
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build();
        let error = prove_root_physical(&root, &Planner::new(&provider, &state)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("physical source contains branch work")
        );
        let mut replaced = extract_shared_source_root([&left, &right], 42)?;
        let mut watermark = extension(&replaced.root)
            .unwrap()
            .as_any()
            .downcast_ref::<WatermarkNode>()
            .unwrap()
            .clone();
        let mut remote = extension(&watermark.input)
            .unwrap()
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
            .unwrap()
            .clone();
        let LogicalPlan::Projection(mut projection) = remote.input else {
            unreachable!()
        };
        projection.expr[1] = lit(55i64).alias_qualified(Some(source.name.clone()), "l");
        remote.input = LogicalPlan::Projection(projection);
        watermark.input = LogicalPlan::Extension(Extension {
            node: Arc::new(remote),
        });
        replaced.root = LogicalPlan::Extension(Extension {
            node: Arc::new(watermark),
        });
        let error = prove_root_physical(&replaced, &Planner::new(&provider, &state)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from canonical source plumbing")
        );
        Ok(())
    }

    #[tokio::test]
    async fn event_response_root_preserves_one_named_scalar_capture_after_root() -> Result<()> {
        let (provider, source) = source();
        let input = branch(&source, vec![0, 1, 2], lit(true))?;
        let named = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                accepted_result: None,
                union_schema: false,
                input: input.clone(),
                schema: input.schema().clone(),
                name: TableReference::bare("shared_projection"),
                materialize: true,
            }),
        });
        let select = |name: &str| -> Result<LogicalPlan> {
            Ok(LogicalPlan::Projection(Projection::try_new(
                vec![col("id"), col(name), col(arroyo_rpc::TIMESTAMP_FIELD)],
                Arc::new(named.clone()),
            )?))
        };
        let left = select("l")?;
        let right = select("r")?;
        let root = extract_shared_source_root([&left, &right], 43)?;
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build();
        let mut visitor = PlanToGraphVisitor::new(&provider, &state);
        for (i, branch) in root.branches.iter().enumerate() {
            visitor.add_plan(LogicalPlan::Extension(Extension {
                node: Arc::new(RemoteTableExtension {
                    accepted_result: None,
                    union_schema: false,
                    input: branch.clone(),
                    schema: branch.schema().clone(),
                    name: TableReference::bare(format!("result_{i}")),
                    materialize: true,
                }),
            }))?;
        }
        let graph = visitor.into_graph();
        let shared = graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .filter(|(op, _)| {
                op.operator_name == OperatorName::ArrowValue
                    && arroyo_rpc::grpc::api::ValuePlanOperator::decode(
                        op.operator_config.as_slice(),
                    )
                    .is_ok_and(|config| config.name == "value_calculation(shared_projection)")
            })
            .count();
        assert_eq!(shared, 1);
        assert_eq!(
            graph
                .node_weights()
                .filter(|n| n.description.starts_with("event_root_"))
                .count(),
            1
        );
        Ok(())
    }
}
