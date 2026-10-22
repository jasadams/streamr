//! Physical adapter for the shared grouped selector kernel. Analytic SQL keeps
//! DataFusion's built-ins; updating aggregates call the same kernel directly.
use crate::grouped_selector::{
    ORDINAL_MARKER, SelectorBudget, SelectorCandidate, SelectorKind, SelectorSpec, SelectorTie,
};
use arrow_array::{Array, ArrayRef, BinaryArray, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::{
    common::{Result, ScalarValue, exec_err, plan_err},
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarUDF, Signature,
        Volatility, create_udf,
    },
    physical_expr::aggregate::AggregateFunctionExpr,
};
use std::{any::Any, sync::Arc};

#[derive(Debug)]
struct GroupedSelector {
    kind: SelectorKind,
    declared: bool,
    signature: Signature,
    budget: Option<SelectorBudget>,
}
impl GroupedSelector {
    fn name_for(kind: SelectorKind, declared: bool) -> &'static str {
        match (kind, declared) {
            (SelectorKind::First, true) => "__streamr_grouped_first_for_v1",
            (SelectorKind::Last, true) => "__streamr_grouped_last_for_v1",
            (SelectorKind::First, false) => "__streamr_grouped_first_explicit_v1",
            (SelectorKind::Last, false) => "__streamr_grouped_last_explicit_v1",
        }
    }
}
pub fn grouped_selector(kind: SelectorKind, declared: bool) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(GroupedSelector {
        kind,
        declared,
        signature: Signature::any(2, Volatility::Immutable),
        budget: None,
    }))
}
pub fn classify(function: &AggregateUDF) -> Option<(SelectorKind, bool)> {
    function
        .inner()
        .as_any()
        .downcast_ref::<GroupedSelector>()
        .map(|function| (function.kind, function.declared))
}
pub fn ordinal_marker() -> Arc<ScalarUDF> {
    Arc::new(create_udf(
        ORDINAL_MARKER,
        vec![],
        DataType::UInt64,
        Volatility::Volatile,
        Arc::new(|_: &[ColumnarValue]| {
            exec_err!("grouped selector write ordinal requires lifecycle binding")
        }),
    ))
}
pub fn register(registry: &mut dyn datafusion::execution::FunctionRegistry) {
    for kind in [SelectorKind::First, SelectorKind::Last] {
        for declared in [false, true] {
            registry
                .register_udaf(grouped_selector(kind, declared))
                .unwrap();
        }
    }
    registry.register_udf(ordinal_marker()).unwrap();
}
fn spec(
    kind: SelectorKind,
    declared: bool,
    value_type: DataType,
    order_types: Vec<DataType>,
    options: Vec<arrow_schema::SortOptions>,
    ignore_nulls: bool,
) -> Result<SelectorSpec> {
    let tie = if kind == SelectorKind::Last && (declared || order_types.is_empty()) {
        SelectorTie::LastWrite
    } else {
        SelectorTie::FirstWrite
    };
    SelectorSpec::new(value_type, order_types, options, kind, tie, ignore_nulls)
}
pub fn selector_spec(aggregate: &AggregateFunctionExpr, schema: &Schema) -> Result<SelectorSpec> {
    let Some((kind, declared)) = classify(aggregate.fun()) else {
        return plan_err!("not a grouped selector descriptor");
    };
    let args = aggregate.expressions();
    if args.len() != 2 {
        return plan_err!("grouped selector requires value and UInt64 write ordinal");
    }
    let ordinal_column = args[1]
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Column>();
    let ordinal_marker = args[1]
        .as_any()
        .downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>();
    let valid_column = ordinal_column.is_some_and(|column| {
        column.name() == crate::grouped_selector::ORDINAL_FIELD
            && schema.fields().get(column.index()).is_some_and(|field| {
                field.name() == crate::grouped_selector::ORDINAL_FIELD
                    && field.data_type() == &DataType::UInt64
                    && !field.is_nullable()
            })
    });
    let valid_marker = ordinal_marker
        .is_some_and(|function| function.name() == ORDINAL_MARKER && function.args().is_empty());
    if (!valid_column && !valid_marker) || args[1].data_type(schema)? != DataType::UInt64 {
        return plan_err!("grouped selector write ordinal is not the native lifecycle binding");
    }
    let orders = aggregate.order_bys().cloned().unwrap_or_default();
    if declared {
        let Some(first) = orders.first() else {
            return plan_err!("declared grouped selector lacks raw FOR primary order");
        };
        let Some(column) = first
            .expr
            .as_any()
            .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        else {
            return plan_err!("declared grouped selector raw FOR order must be a column");
        };
        if column.name() != arroyo_rpc::TIMESTAMP_FIELD
            || schema.index_of(arroyo_rpc::TIMESTAMP_FIELD).ok() != Some(column.index())
            || schema
                .fields()
                .get(column.index())
                .is_none_or(|field| field.name() != arroyo_rpc::TIMESTAMP_FIELD)
            || first.options.descending
            || first.options.nulls_first
            || !matches!(first.expr.data_type(schema)?, DataType::Timestamp(_, _))
        {
            return plan_err!(
                "declared grouped selector requires raw FOR timestamp ASC NULLS LAST"
            );
        }
    }
    spec(
        kind,
        declared,
        args[0].data_type(schema)?,
        orders
            .iter()
            .map(|s| s.expr.data_type(schema))
            .collect::<Result<Vec<_>>>()?,
        orders.iter().map(|s| s.options).collect(),
        aggregate.ignore_nulls(),
    )
}
impl AggregateUDFImpl for GroupedSelector {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        Self::name_for(self.kind, self.declared)
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 2 || args[1] != DataType::UInt64 {
            return plan_err!("grouped selector requires value and UInt64 ordinal");
        }
        Ok(args[0].clone())
    }
    fn supports_null_handling_clause(&self) -> bool {
        true
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(self))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[selector_v1]", args.name),
            DataType::Binary,
            true,
        ))])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let orders = args.ordering_req;
        let order_types = orders
            .iter()
            .map(|s| s.expr.data_type(args.schema))
            .collect::<Result<Vec<_>>>()?;
        if let Some(budget) = self.budget {
            SelectorSpec::preflight_types(args.return_type(), &order_types, budget)?;
        }
        let spec = spec(
            self.kind,
            self.declared,
            args.return_type().clone(),
            order_types,
            orders.iter().map(|s| s.options).collect(),
            args.ignore_nulls,
        )?;
        Ok(Box::new(GroupedAccumulator {
            spec,
            winner: None,
            budget: self.budget,
        }))
    }
}
/// Bind the existing window lifecycle allowance to accumulator instances. This
/// is deliberately internal and does not read global config or add a new pool.
pub fn bind_window_budget(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    budget: SelectorBudget,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_plan::aggregates::AggregateExec;
    plan.transform_up(|node| {
        let Some(aggregate) = node.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(node));
        };
        let mut changed = false;
        let expressions = aggregate
            .aggr_expr()
            .iter()
            .map(|expression| {
                let Some((kind, declared)) = classify(expression.fun()) else {
                    return Ok(expression.clone());
                };
                let input_schema = aggregate.input_schema();
                let args = expression.expressions();
                let value_type = args[0].data_type(&input_schema)?;
                let order_types = expression
                    .order_bys()
                    .into_iter()
                    .flatten()
                    .map(|order| order.expr.data_type(&input_schema))
                    .collect::<Result<Vec<_>>>()?;
                SelectorSpec::preflight_types(&value_type, &order_types, budget)?;
                let spec = selector_spec(expression, &input_schema)?;
                spec.validate_budget(budget)?;
                let udf = Arc::new(AggregateUDF::new_from_impl(GroupedSelector {
                    kind,
                    declared,
                    signature: Signature::any(2, Volatility::Immutable),
                    budget: Some(budget),
                }));
                changed = true;
                Ok(Arc::new(
                    AggregateExprBuilder::new(udf, expression.expressions())
                        .schema(aggregate.input_schema())
                        .alias(expression.name())
                        .human_display(expression.human_display().to_string())
                        .order_by(expression.order_bys().cloned().unwrap_or_default())
                        .with_distinct(expression.is_distinct())
                        .with_ignore_nulls(expression.ignore_nulls())
                        .build()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if changed {
            Ok(Transformed::yes(
                Arc::new(aggregate.with_new_aggr_exprs(expressions))
                    as Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            ))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|result| result.data)
}
#[derive(Debug)]
struct GroupedAccumulator {
    spec: SelectorSpec,
    winner: Option<SelectorCandidate>,
    budget: Option<SelectorBudget>,
}
impl GroupedAccumulator {
    fn budget(&self) -> Result<SelectorBudget> {
        self.budget.ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "grouped selector requires its native lifecycle budget binding".into(),
            )
        })
    }
}
impl Accumulator for GroupedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() < 2 {
            return exec_err!("grouped selector missing write ordinal");
        }
        let Some(ordinals) = values[1].as_any().downcast_ref::<UInt64Array>() else {
            return exec_err!("grouped selector write ordinal is not UInt64");
        };
        let budget = self.budget()?;
        for row in 0..values[0].len() {
            if ordinals.is_null(row) {
                return exec_err!("grouped selector write ordinal is null");
            }
            let mut args = vec![values[0].slice(row, 1)];
            args.extend(values[2..].iter().map(|array| array.slice(row, 1)));
            if let Some(candidate) = self.spec.encode(&args, ordinals.value(row), budget)? {
                self.spec.choose(&mut self.winner, candidate);
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 1 {
            return exec_err!("grouped selector expects one versioned binary state");
        }
        let Some(states) = states[0].as_any().downcast_ref::<BinaryArray>() else {
            return exec_err!("grouped selector state is not Binary");
        };
        let budget = self.budget()?;
        for row in 0..states.len() {
            if !states.is_null(row) {
                let candidate = self.spec.decode_candidate(states.value(row), budget)?;
                self.spec.choose(&mut self.winner, candidate);
            }
        }
        Ok(())
    }
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let state = self
            .winner
            .as_ref()
            .map(|candidate| self.spec.encode_candidate(candidate, self.budget()?))
            .transpose()?;
        Ok(vec![ScalarValue::Binary(state)])
    }
    fn evaluate(&self) -> Result<ScalarValue> {
        self.spec.evaluate(self.winner.as_ref(), self.budget()?)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .winner
                .as_ref()
                .map_or(0, |c| c.key.capacity() + c.args.capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::SortOptions;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
    fn budget() -> SelectorBudget {
        SelectorBudget {
            key_bytes: 4096,
            value_bytes: 16384,
            decoded_bytes: 1024 * 1024,
        }
    }
    fn accumulator(kind: SelectorKind, declared: bool) -> GroupedAccumulator {
        GroupedAccumulator {
            spec: spec(
                kind,
                declared,
                DataType::Utf8,
                vec![DataType::Int64],
                vec![SortOptions::default()],
                false,
            )
            .unwrap(),
            winner: None,
            budget: Some(budget()),
        }
    }
    #[test]
    fn partial_state_merge_retains_original_arrival_and_null() {
        let mut first = accumulator(SelectorKind::Last, true);
        let mut second = accumulator(SelectorKind::Last, true);
        let input = |text, ordinal| {
            vec![
                Arc::new(StringArray::from(vec![text])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![ordinal])) as ArrayRef,
                Arc::new(Int64Array::from(vec![20])) as ArrayRef,
            ]
        };
        first.update_batch(&input(Some("old"), 0)).unwrap();
        second.update_batch(&input(None, 1)).unwrap();
        let state = second.state().unwrap()[0].to_array().unwrap();
        first.merge_batch(&[state]).unwrap();
        assert_eq!(first.evaluate().unwrap(), ScalarValue::Utf8(None));
        let persisted = first.state().unwrap();
        let mut restored = accumulator(SelectorKind::Last, true);
        restored
            .merge_batch(&[persisted[0].to_array().unwrap()])
            .unwrap();
        restored
            .update_batch(&input(Some("earlier-arrival"), 0))
            .unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::Utf8(None));
        let mut explicit = accumulator(SelectorKind::Last, false);
        explicit.update_batch(&input(Some("first"), 0)).unwrap();
        explicit.update_batch(&input(Some("equal"), 1)).unwrap();
        assert_eq!(
            explicit.evaluate().unwrap(),
            ScalarValue::Utf8(Some("first".into()))
        );
    }
    #[test]
    fn physical_descriptor_rejects_unbound_ordinal_and_wrong_for_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, true),
            Field::new(
                "_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                crate::grouped_selector::ORDINAL_FIELD,
                DataType::UInt64,
                false,
            ),
        ]));
        let expression = |ordinal, descending| {
            AggregateExprBuilder::new(
                grouped_selector(SelectorKind::Last, true),
                vec![Arc::new(Column::new("value", 0)), ordinal],
            )
            .schema(schema.clone())
            .alias("selected")
            .order_by(
                vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("_timestamp", 1)),
                    options: SortOptions {
                        descending,
                        nulls_first: false,
                    },
                }]
                .into(),
            )
            .build()
            .unwrap()
        };
        let valid = expression(
            Arc::new(Column::new(crate::grouped_selector::ORDINAL_FIELD, 2)),
            false,
        );
        selector_spec(&valid, &schema).unwrap();
        assert!(
            selector_spec(
                &expression(
                    Arc::new(Column::new(crate::grouped_selector::ORDINAL_FIELD, 2)),
                    true
                ),
                &schema
            )
            .is_err()
        );
        assert!(
            selector_spec(
                &expression(
                    Arc::new(datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::UInt64(Some(0))
                    )),
                    false
                ),
                &schema
            )
            .is_err()
        );
    }
}
