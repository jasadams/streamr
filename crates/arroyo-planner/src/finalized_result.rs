//! A finalized native producer is an append result, not an original-event response.
use crate::extension::{
    aggregate::AggregateExtension, remote_table::RemoteTableExtension,
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
