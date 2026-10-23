//! A finalized native producer is an append result, not an original-event response.
use crate::extension::{
    aggregate::AggregateExtension,
    key_calculation::{KeyCalculationExtension, KeysOrExprs},
    remote_table::RemoteTableExtension,
    table_source::TableSourceExtension,
};
use crate::merge_event_branches;
use crate::merge_event_results::AcceptedResultRoute;
use crate::{WindowBehavior, WindowType};
use datafusion::common::{Result, TableReference, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan};
use std::sync::Arc;

fn producer(plan: &LogicalPlan) -> Option<&AggregateExtension> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(aggregate) = extension.node.as_any().downcast_ref::<AggregateExtension>()
    {
        return matches!(
            aggregate.window_behavior,
            WindowBehavior::FromOperator {
                window: WindowType::Session { .. },
                is_nested: false,
                ..
            }
        )
        .then_some(aggregate);
    }
    match plan {
        LogicalPlan::Projection(projection) => producer(&projection.input),
        LogicalPlan::Filter(filter) => producer(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => producer(&alias.input),
        LogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
            .and_then(|remote| producer(&remote.input)),
        _ => None,
    }
}
fn scalar_input(plan: &LogicalPlan) -> Result<&LogicalPlan> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(remote) = extension
            .node
            .as_any()
            .downcast_ref::<RemoteTableExtension>()
    {
        if remote.accepted_result.is_some() || remote.union_schema {
            return plan_err!("finalized result crossed another accepted boundary");
        }
        return Ok(&remote.input);
    }
    if let LogicalPlan::Extension(extension) = plan
        && let Some(key) = extension
            .node
            .as_any()
            .downcast_ref::<KeyCalculationExtension>()
    {
        // Native GROUP BY introduces an ordinal-only routing adapter before
        // its owner. This changes storage-key metadata, never row fields or
        // their clock. Alias rewrites may qualify synthetic routing columns;
        // ordinal-only adapters do not evaluate those logical qualifiers.
        // Computed and trimming key adapters are not transparent.
        let KeysOrExprs::Keys(indices) = &key.keys else {
            return plan_err!("finalized SELECT key adapter must copy existing fields");
        };
        let unique = indices
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if key.name.is_some()
            || key.schema.as_arrow() != key.input.schema().as_arrow()
            || indices.is_empty()
            || unique.len() != indices.len()
            || indices
                .iter()
                .any(|index| *index >= key.input.schema().fields().len())
        {
            return plan_err!(
                "finalized SELECT key adapter must preserve the complete row schema: name={:?}, indices={:?}, declared={:?}, input={:?}",
                key.name,
                indices,
                key.schema,
                key.input.schema()
            );
        }
        return Ok(&key.input);
    }
    match plan {
        LogicalPlan::Projection(projection) => {
            for (expression, field) in projection.expr.iter().zip(projection.schema.fields()) {
                if field.name() == arroyo_rpc::TIMESTAMP_FIELD
                    && merge_event_branches::bare_column(expression)
                        .ok()
                        .is_none_or(|column| column.name != arroyo_rpc::TIMESTAMP_FIELD)
                {
                    return plan_err!("finalized SELECT must preserve the native producer clock");
                }
            }
            Ok(&projection.input)
        }
        LogicalPlan::Filter(filter) => Ok(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => Ok(&alias.input),
        _ => plan_err!("finalized result requires native scalar SELECT work"),
    }
}
fn append_source(plan: &LogicalPlan) -> Result<TableReference> {
    if plan
        .schema()
        .has_column_with_unqualified_name(arroyo_rpc::UPDATING_META_FIELD)
    {
        return plan_err!("finalized SESSION requires append input, not changelog rows");
    }
    if let Some((projection, source)) = crate::event_response_root::source_projection(plan) {
        if source.table.is_updating() {
            return plan_err!("finalized SESSION requires an append source");
        }
        // Source admission creates _timestamp from its declared FOR clock.
        // Prove that exact canonical projection before allowing its alias;
        // ordinary user projections must still forward the hidden clock.
        crate::event_response_root::projection_columns(projection, source)?;
        return Ok(source.name.clone());
    }
    if let LogicalPlan::Extension(extension) = plan {
        if let Some(source) = extension
            .node
            .as_any()
            .downcast_ref::<TableSourceExtension>()
        {
            if source.table.is_updating() {
                return plan_err!("finalized SESSION requires an append source");
            }
            return Ok(source.name.clone());
        }
        if let Some(watermark) = extension
            .node
            .as_any()
            .downcast_ref::<crate::extension::watermark_node::WatermarkNode>()
        {
            return append_source(&watermark.input);
        }
        if let Some(key) = extension
            .node
            .as_any()
            .downcast_ref::<crate::extension::key_calculation::KeyCalculationExtension>(
        ) {
            return append_source(&key.input);
        }
    }
    append_source(scalar_input(plan)?)
}
pub(crate) fn accepted_source(plan: &LogicalPlan) -> Result<TableReference> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(aggregate) = extension.node.as_any().downcast_ref::<AggregateExtension>()
    {
        if !matches!(
            aggregate.window_behavior,
            WindowBehavior::FromOperator {
                window: WindowType::Session { .. },
                is_nested: false,
                ..
            }
        ) {
            return plan_err!("finalized append result requires the native SESSION producer");
        }
        let LogicalPlan::Aggregate(native) = &aggregate.aggregate else {
            return plan_err!("finalized SESSION native aggregate missing");
        };
        return append_source(&native.input);
    }
    accepted_source(scalar_input(plan)?)
}
pub(crate) fn resolve(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    if producer(plan).is_none() {
        return Ok(None);
    }
    accepted_source(plan)?;
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(RemoteTableExtension {
            input: plan.clone(),
            schema: plan.schema().clone(),
            name: TableReference::bare("accepted_native_result"),
            materialize: false,
            union_schema: false,
            accepted_result: Some(AcceptedResultRoute::FinalizedAppend),
        }),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::logical_plan::EmptyRelation;

    #[test]
    fn finalized_routing_adapter_preserves_fields_and_rejects_computation_or_trimming() {
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::try_from(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "key",
                    arrow_schema::DataType::Int64,
                    false,
                )]))
                .unwrap(),
            ),
        });
        let key = KeyCalculationExtension::new(input.clone(), KeysOrExprs::Keys(vec![0]));
        let wrap = |key| {
            LogicalPlan::Extension(Extension {
                node: Arc::new(key),
            })
        };
        assert_eq!(scalar_input(&wrap(key.clone())).unwrap(), &input);
        let mut aliased = key.clone();
        aliased.schema = Arc::new(
            DFSchema::try_from_qualified_schema(
                TableReference::bare("aliased"),
                input.schema().as_arrow(),
            )
            .unwrap(),
        );
        assert_eq!(scalar_input(&wrap(aliased)).unwrap(), &input);
        let mut retyped = key.clone();
        retyped.schema = Arc::new(
            DFSchema::try_from(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "key",
                arrow_schema::DataType::Utf8,
                false,
            )]))
            .unwrap(),
        );
        assert!(scalar_input(&wrap(retyped)).is_err());

        let mut named = key.clone();
        named.name = Some("trimmed".into());
        assert!(scalar_input(&wrap(named)).is_err());
        let mut duplicate = key.clone();
        duplicate.keys = KeysOrExprs::Keys(vec![0, 0]);
        assert!(scalar_input(&wrap(duplicate)).is_err());
        let mut out_of_bounds = key.clone();
        out_of_bounds.keys = KeysOrExprs::Keys(vec![1]);
        assert!(scalar_input(&wrap(out_of_bounds)).is_err());
        let mut computed = key.clone();
        computed.keys = KeysOrExprs::Exprs(vec![]);
        assert!(scalar_input(&wrap(computed)).is_err());
        let mut trimmed = key;
        trimmed.schema = Arc::new(DFSchema::empty());
        assert!(scalar_input(&wrap(trimmed)).is_err());
    }

    #[test]
    fn finalized_routing_adapter_rejects_clock_and_ordinal_schema_changes() {
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        let fields = vec![
            Field::new("key", DataType::Int64, false),
            Field::new(
                arroyo_rpc::TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ];
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(Schema::new(fields.clone())).unwrap()),
        });
        let key = KeyCalculationExtension::new(input, KeysOrExprs::Keys(vec![0]));
        let mut reversed = fields.clone();
        reversed.reverse();
        let mut wrong_unit = fields.clone();
        wrong_unit[1] = Field::new(
            arroyo_rpc::TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        );
        let mut nullable_clock = fields.clone();
        nullable_clock[1] = fields[1].clone().with_nullable(true);
        for fields in [
            vec![fields[0].clone()],
            reversed,
            wrong_unit,
            nullable_clock,
        ] {
            let mut changed = key.clone();
            changed.schema = Arc::new(DFSchema::try_from(Schema::new(fields)).unwrap());
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(changed),
            });
            assert!(scalar_input(&plan).is_err());
        }
    }
}
