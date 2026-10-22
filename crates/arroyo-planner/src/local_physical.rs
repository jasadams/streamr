//! Native finite query execution for scalar and EXISTS expressions.
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    execution::{FunctionRegistry, TaskContext},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    physical_expr::{
        PhysicalExpr, ScalarFunctionExpr,
        expressions::{CaseExpr, Literal, try_cast},
    },
    physical_plan::{ExecutionPlan, execute_stream},
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use futures::{StreamExt, future::BoxFuture};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    sync::{Arc, RwLock},
};

const BOUND: &str = "__streamr_local_bound";
const QUERY: &str = "__streamr_local_query";
fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug)]
pub struct BoundValueUdf {
    pub index: usize,
    pub data_type: DataType,
    value: RwLock<Option<ScalarValue>>,
    signature: Signature,
}
impl BoundValueUdf {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self {
            index,
            data_type,
            value: RwLock::new(None),
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}
impl ScalarUDFImpl for BoundValueUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        BOUND
    }
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| std::ptr::eq(self, other))
    }
    fn hash_value(&self) -> u64 {
        self as *const Self as usize as u64
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.value
            .read()
            .map_err(|_| error("local query binding lock poisoned"))?
            .clone()
            .map(ColumnarValue::Scalar)
            .ok_or_else(|| error("local query binding is unset"))
    }
}

#[derive(Debug)]
pub struct LocalQueryUdf {
    pub plan: Arc<dyn ExecutionPlan>,
    pub bindings: Vec<Arc<BoundValueUdf>>,
    pub result_type: DataType,
    pub exists: bool,
    pub array: bool,
    signature: Signature,
    execution: tokio::sync::Mutex<()>,
}
impl LocalQueryUdf {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        bindings: Vec<Arc<BoundValueUdf>>,
        result_type: DataType,
        exists: bool,
        array: bool,
    ) -> Self {
        let signature = Signature::exact(
            bindings.iter().map(|b| b.data_type.clone()).collect(),
            Volatility::Volatile,
        );
        Self {
            plan,
            bindings,
            result_type,
            exists,
            array,
            signature,
            execution: tokio::sync::Mutex::new(()),
        }
    }
    async fn evaluate(&self, arguments: Vec<ScalarValue>) -> Result<ScalarValue> {
        let _execution = self.execution.lock().await;
        if arguments.len() != self.bindings.len() {
            return Err(error("local query binding arity mismatch"));
        }
        struct Clear<'a>(&'a [Arc<BoundValueUdf>]);
        impl Drop for Clear<'_> {
            fn drop(&mut self) {
                for binding in self.0 {
                    if let Ok(mut value) = binding.value.write() {
                        *value = None;
                    }
                }
            }
        }
        let _clear = Clear(&self.bindings);
        for (binding, value) in self.bindings.iter().zip(arguments) {
            if binding.data_type != value.data_type() {
                return Err(error("local query binding type mismatch"));
            }
            *binding
                .value
                .write()
                .map_err(|_| error("local query binding lock poisoned"))? = Some(value);
        }
        self.plan.reset()?;
        let mut stream = execute_stream(
            wrap_local_execution(self.plan.clone())?,
            Arc::new(TaskContext::default()),
        )?;
        let mut value = None;
        let mut members = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            if self.exists {
                return Ok(ScalarValue::Boolean(Some(true)));
            }
            if self.array {
                if batch.num_columns() != 1 {
                    return Err(error("ARRAY(query) must return one column"));
                }
                for row in 0..batch.num_rows() {
                    members.push(ScalarValue::try_from_array(batch.column(0), row)?);
                }
                continue;
            }
            if batch.num_columns() != 1 {
                return Err(error("scalar subquery must return one column"));
            }
            if batch.num_rows() > 1 || value.is_some() {
                return Err(error(format!(
                    "scalar subquery returned more than one row: {}",
                    datafusion::physical_plan::displayable(self.plan.as_ref()).indent(true)
                )));
            }
            value = Some(ScalarValue::try_from_array(batch.column(0), 0)?);
        }
        if self.array {
            let DataType::List(element) = &self.result_type else {
                return Err(error("ARRAY(query) output must be a List"));
            };
            return Ok(ScalarValue::List(ScalarValue::new_list(
                &members,
                element.data_type(),
                true,
            )));
        }
        if self.exists {
            Ok(ScalarValue::Boolean(Some(false)))
        } else {
            value
                .map(Ok)
                .unwrap_or_else(|| ScalarValue::try_from(&self.result_type))
        }
    }
}
impl ScalarUDFImpl for LocalQueryUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        QUERY
    }
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| std::ptr::eq(self, other))
    }
    fn hash_value(&self) -> u64 {
        self as *const Self as usize as u64
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.result_type.clone())
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(error(
            "local query requires asynchronous expression evaluation",
        ))
    }
}

fn scalar(expression: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<ScalarValue> {
    match expression.evaluate(batch)? {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
    }
}
/// Resolve only the selected CASE branch, preserving scalar cardinality errors.
pub fn resolve_expression(
    expression: Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> BoxFuture<'_, Result<Arc<dyn PhysicalExpr>>> {
    Box::pin(async move {
        if batch.num_rows() != 1 {
            return Err(error("local expression evaluation requires one outer row"));
        }
        if let Some(case) = expression.as_any().downcast_ref::<CaseExpr>() {
            // Returning a selected child must retain CASE's declared result
            // type. In particular, a chosen untyped NULL is a typed null value,
            // as in DataFusion's native CASE evaluator.
            let result_type = expression.data_type(batch.schema().as_ref())?;
            let base = if let Some(base) = case.expr() {
                Some(scalar(
                    &resolve_expression(base.clone(), batch).await?,
                    batch,
                )?)
            } else {
                None
            };
            for (when, then) in case.when_then_expr() {
                let condition = scalar(&resolve_expression(when.clone(), batch).await?, batch)?;
                let matches = match &base {
                    Some(base) => !base.is_null() && !condition.is_null() && base == &condition,
                    None => condition == ScalarValue::Boolean(Some(true)),
                };
                if matches {
                    return try_cast(
                        resolve_expression(then.clone(), batch).await?,
                        batch.schema().as_ref(),
                        result_type,
                    );
                }
            }
            return match case.else_expr() {
                Some(other) => try_cast(
                    resolve_expression(other.clone(), batch).await?,
                    batch.schema().as_ref(),
                    result_type,
                ),
                None => Ok(Arc::new(Literal::new(ScalarValue::try_from(&result_type)?))
                    as Arc<dyn PhysicalExpr>),
            };
        }
        let mut children = Vec::new();
        for child in expression.children() {
            children.push(resolve_expression(child.clone(), batch).await?);
        }
        let expression = expression.with_new_children(children)?;
        if let Some(function) = expression.as_any().downcast_ref::<ScalarFunctionExpr>()
            && let Some(query) = function
                .fun()
                .inner()
                .as_any()
                .downcast_ref::<LocalQueryUdf>()
        {
            let arguments = function
                .args()
                .iter()
                .map(|arg| scalar(arg, batch))
                .collect::<Result<Vec<_>>>()?;
            return Ok(
                Arc::new(Literal::new(query.evaluate(arguments).await?)) as Arc<dyn PhysicalExpr>
            );
        }
        Ok(expression)
    })
}

#[derive(Serialize, Deserialize)]
struct QueryEncoding {
    plan: Vec<u8>,
    bindings: Vec<DataType>,
    result_type: DataType,
    exists: bool,
    array: bool,
}
#[derive(Serialize, Deserialize)]
struct BindingEncoding {
    index: usize,
    data_type: DataType,
}

pub fn encode_local_udf(
    udf: &ScalarUDF,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Vec<u8>>> {
    let bytes = if let Some(bound) = udf.inner().as_any().downcast_ref::<BoundValueUdf>() {
        serde_json::to_vec(&BindingEncoding {
            index: bound.index,
            data_type: bound.data_type.clone(),
        })
    } else if let Some(query) = udf.inner().as_any().downcast_ref::<LocalQueryUdf>() {
        validate_local_physical(&query.plan, &query.bindings)?;
        let plan =
            PhysicalPlanNode::try_from_physical_plan(query.plan.clone(), codec)?.encode_to_vec();
        serde_json::to_vec(&QueryEncoding {
            plan,
            bindings: query.bindings.iter().map(|b| b.data_type.clone()).collect(),
            result_type: query.result_type.clone(),
            exists: query.exists,
            array: query.array,
        })
    } else {
        return Ok(None);
    };
    bytes
        .map(Some)
        .map_err(|e| error(format!("local query serialization: {e}")))
}

#[derive(Debug)]
struct BindingCodec<'a> {
    parent: &'a dyn PhysicalExtensionCodec,
    bindings: Vec<Arc<BoundValueUdf>>,
}
impl PhysicalExtensionCodec for BindingCodec<'_> {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.parent.try_decode(buf, inputs, registry)
    }
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.parent.try_encode(node, buf)
    }
    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.parent.try_encode_udf(node, buf)
    }
    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == BOUND {
            let encoded: BindingEncoding = serde_json::from_slice(buf)
                .map_err(|e| error(format!("local binding decode: {e}")))?;
            let binding = self
                .bindings
                .get(encoded.index)
                .ok_or_else(|| error("local binding index out of range"))?;
            if binding.data_type != encoded.data_type {
                return Err(error("local binding serialized type mismatch"));
            }
            return Ok(Arc::new(ScalarUDF::new_from_shared_impl(binding.clone())));
        }
        self.parent.try_decode_udf(name, buf)
    }
}
pub fn decode_local_udf(
    name: &str,
    buf: &[u8],
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Arc<ScalarUDF>>> {
    if name != QUERY {
        return Ok(None);
    }
    let encoded: QueryEncoding =
        serde_json::from_slice(buf).map_err(|e| error(format!("local query decode: {e}")))?;
    let bindings = encoded
        .bindings
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| Arc::new(BoundValueUdf::new(index, data_type)))
        .collect::<Vec<_>>();
    let context = TaskContext::default();
    let mut registry = datafusion::prelude::SessionContext::new();
    crate::register_functions(&mut registry);
    let plan = PhysicalPlanNode::decode(encoded.plan.as_slice())
        .map_err(|e| error(format!("local plan decode: {e}")))?
        .try_into_physical_plan(
            &registry,
            context.runtime_env().as_ref(),
            &BindingCodec {
                parent: codec,
                bindings: bindings.clone(),
            },
        )?;
    validate_local_physical(&plan, &bindings)?;
    if encoded.array {
        let DataType::List(element) = &encoded.result_type else {
            return Err(error("ARRAY(query) serialized output must be a List"));
        };
        if encoded.exists
            || plan.schema().fields().len() != 1
            || plan.schema().field(0).data_type() != element.data_type()
        {
            return Err(error("ARRAY(query) serialized element schema mismatch"));
        }
    }
    if encoded.exists && encoded.result_type != DataType::Boolean {
        return Err(error("EXISTS result type must be boolean"));
    }
    if !encoded.exists
        && !encoded.array
        && (plan.schema().fields().len() != 1
            || plan.schema().field(0).data_type() != &encoded.result_type)
    {
        return Err(error("scalar subquery serialized output type mismatch"));
    }
    Ok(Some(Arc::new(ScalarUDF::new_from_impl(
        LocalQueryUdf::new(
            plan,
            bindings,
            encoded.result_type,
            encoded.exists,
            encoded.array,
        ),
    ))))
}

pub fn contains_local(expression: &Arc<dyn PhysicalExpr>) -> bool {
    expression
        .as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|f| f.fun().inner().as_any().is::<LocalQueryUdf>())
        || expression.children().into_iter().any(contains_local)
}

pub fn plan_contains_local(plan: &dyn ExecutionPlan) -> bool {
    use datafusion::physical_plan::{filter::FilterExec, projection::ProjectionExec};
    plan.as_any()
        .downcast_ref::<ProjectionExec>()
        .is_some_and(|projection| {
            projection
                .expr()
                .iter()
                .any(|(expression, _)| contains_local(expression))
        })
        || plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .is_some_and(|filter| contains_local(filter.predicate()))
        || plan
            .children()
            .into_iter()
            .any(|child| plan_contains_local(child.as_ref()))
}

pub fn validate_local_expression(
    expression: &Arc<dyn PhysicalExpr>,
    schema: &arrow_schema::Schema,
) -> Result<()> {
    if expression
        .as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| {
            function
                .fun()
                .inner()
                .as_any()
                .is::<crate::like_regex::LikeRegexFunc>()
        })
    {
        crate::physical::validate_json_table_physical_expression(expression, schema)?;
    }
    if let Some(function) = expression.as_any().downcast_ref::<ScalarFunctionExpr>()
        && let Some(query) = function
            .fun()
            .inner()
            .as_any()
            .downcast_ref::<LocalQueryUdf>()
    {
        if function.return_type() != &query.result_type
            || function.args().len() != query.bindings.len()
        {
            return Err(error(
                "local query physical result type or binding arity mismatch",
            ));
        }
        for (argument, binding) in function.args().iter().zip(&query.bindings) {
            if argument.data_type(schema)? != binding.data_type {
                return Err(error("local query physical binding type mismatch"));
            }
        }
        validate_local_physical(&query.plan, &query.bindings)?;
    }
    for child in expression.children() {
        validate_local_expression(child, schema)?;
    }
    Ok(())
}

/// VALUES is a single finite relation. Its runtime projections/UNION ALL must
/// retain that distribution rather than creating a probe partition per row.
pub fn normalize_finite_partitions(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| normalize_finite_partitions(child.clone()))
        .collect::<Result<Vec<_>>>()?;
    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };
    if plan
        .as_any()
        .is::<datafusion::physical_plan::union::UnionExec>()
    {
        Ok(Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(plan),
        ))
    } else {
        Ok(plan)
    }
}

/// Install asynchronous evaluation only where a native projection/filter contains
/// finite subqueries. All relational work remains in native DataFusion operators.
pub fn wrap_local_execution(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::{filter::FilterExec, projection::ProjectionExec};
    let children = plan
        .children()
        .into_iter()
        .map(|child| wrap_local_execution(child.clone()))
        .collect::<Result<Vec<_>>>()?;
    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };
    let projection = plan
        .as_any()
        .downcast_ref::<ProjectionExec>()
        .filter(|p| p.expr().iter().any(|(e, _)| contains_local(e)));
    let filter = plan
        .as_any()
        .downcast_ref::<FilterExec>()
        .filter(|p| contains_local(p.predicate()));
    let (input, expressions, predicate, projection) = if let Some(projection) = projection {
        (
            projection.input().clone(),
            projection.expr().iter().map(|(e, _)| e.clone()).collect(),
            None,
            None,
        )
    } else if let Some(filter) = filter {
        (
            filter.input().clone(),
            Vec::new(),
            Some(filter.predicate().clone()),
            filter.projection().cloned(),
        )
    } else {
        return Ok(plan);
    };
    Ok(Arc::new(LocalExpressionExec {
        properties: plan.properties().clone(),
        input,
        expressions,
        predicate,
        projection,
    }))
}

#[derive(Debug)]
struct LocalExpressionExec {
    properties: datafusion::physical_plan::PlanProperties,
    input: Arc<dyn ExecutionPlan>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<Vec<usize>>,
}
impl datafusion::physical_plan::DisplayAs for LocalExpressionExec {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "LocalExpressionExec")
    }
}
impl ExecutionPlan for LocalExpressionExec {
    fn name(&self) -> &str {
        "LocalExpressionExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(error("local expression executor requires one input"));
        }
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            input: children[0].clone(),
            expressions: self.expressions.clone(),
            predicate: self.predicate.clone(),
            projection: self.projection.clone(),
        }))
    }
    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output_schema = schema.clone();
        let expressions = self.expressions.clone();
        let predicate = self.predicate.clone();
        let projection = self.projection.clone();
        let stream = futures::stream::try_unfold(
            (input, None::<RecordBatch>, 0usize),
            move |(mut input, mut batch, mut row)| {
                let schema = output_schema.clone();
                let expressions = expressions.clone();
                let predicate = predicate.clone();
                let projection = projection.clone();
                async move {
                    loop {
                        if batch.as_ref().is_none_or(|batch| row >= batch.num_rows()) {
                            match input.next().await {
                                Some(next) => {
                                    batch = Some(next?);
                                    row = 0;
                                }
                                None => return Ok(None),
                            }
                        }
                        let current = batch.as_ref().expect("batch set above");
                        if row >= current.num_rows() {
                            continue;
                        }
                        let current = current.slice(row, 1);
                        row += 1;
                        if let Some(predicate) = &predicate {
                            let resolved = resolve_expression(predicate.clone(), &current).await?;
                            if scalar(&resolved, &current)? != ScalarValue::Boolean(Some(true)) {
                                continue;
                            }
                            let current = match &projection {
                                Some(indices) => current.project(indices)?,
                                None => current,
                            };
                            return Ok(Some((current, (input, batch, row))));
                        }
                        let mut columns = Vec::with_capacity(expressions.len());
                        for expression in &expressions {
                            let resolved = resolve_expression(expression.clone(), &current).await?;
                            columns.push(resolved.evaluate(&current)?.into_array(1)?);
                        }
                        let output = RecordBatch::try_new(schema, columns)?;
                        return Ok(Some((output, (input, batch, row))));
                    }
                }
            },
        );
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        ))
    }
}

/// Qualify the concrete implementations, including after decoding untrusted
/// physical metadata. Names and serialized volatility alone are insufficient.
pub fn validate_local_physical(
    plan: &Arc<dyn ExecutionPlan>,
    bindings: &[Arc<BoundValueUdf>],
) -> Result<()> {
    use datafusion::physical_expr::expressions::*;
    use datafusion::physical_plan::{
        aggregates::AggregateExec,
        coalesce_batches::CoalesceBatchesExec,
        coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec,
        filter::FilterExec,
        joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec},
        limit::{GlobalLimitExec, LocalLimitExec},
        placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
        sorts::sort::SortExec,
        union::UnionExec,
        unnest::UnnestExec,
    };
    let state = datafusion::prelude::SessionContext::new().state();
    fn expression(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &arrow_schema::Schema,
        bindings: &[Arc<BoundValueUdf>],
        state: &datafusion::execution::SessionState,
    ) -> Result<()> {
        let any = expr.as_any();
        if let Some(function) = any.downcast_ref::<ScalarFunctionExpr>() {
            let udf = function.fun().inner();
            let implementation = udf.as_any();
            if let Some(bound) = implementation.downcast_ref::<BoundValueUdf>() {
                if !function.args().is_empty()
                    || !bindings
                        .get(bound.index)
                        .is_some_and(|expected| std::ptr::eq(expected.as_ref(), bound))
                {
                    return Err(error("local query contains an unowned binding"));
                }
                if function.return_type() != &bound.data_type {
                    return Err(error("local binding physical return type mismatch"));
                }
            } else if let Some(query) = implementation.downcast_ref::<LocalQueryUdf>() {
                validate_local_physical(&query.plan, &query.bindings)?;
                if function.args().len() != query.bindings.len()
                    || function.return_type() != &query.result_type
                {
                    return Err(error("nested local query physical signature mismatch"));
                }
                for (argument, binding) in function.args().iter().zip(&query.bindings) {
                    if argument.data_type(schema)? != binding.data_type {
                        return Err(error("nested local query argument type mismatch"));
                    }
                }
            } else {
                use crate::sql_json::kernels::{
                    JsonExistsFunc, JsonObjectFunc, JsonQueryFunc, JsonTableRowsFunc, JsonValueFunc,
                };
                let native = state.scalar_functions().values().any(|candidate| {
                    candidate.inner().as_any().type_id() == implementation.type_id()
                });
                let json = implementation.is::<crate::like_regex::LikeRegexFunc>()
                    || implementation.is::<JsonValueFunc>()
                    || implementation.is::<JsonQueryFunc>()
                    || implementation.is::<JsonExistsFunc>()
                    || implementation.is::<JsonObjectFunc>()
                    || implementation.is::<JsonTableRowsFunc>();
                if (!native && !json) || udf.signature().volatility != Volatility::Immutable {
                    return Err(error(format!(
                        "local query does not permit scalar implementation {}",
                        udf.name()
                    )));
                }
                let arg_fields = function
                    .args()
                    .iter()
                    .map(|arg| arg.return_field(schema))
                    .collect::<Result<Vec<_>>>()?;
                let scalar_arguments = function
                    .args()
                    .iter()
                    .map(|arg| arg.as_any().downcast_ref::<Literal>().map(Literal::value))
                    .collect::<Vec<_>>();
                let field =
                    udf.return_field_from_args(datafusion::logical_expr::ReturnFieldArgs {
                        arg_fields: &arg_fields,
                        scalar_arguments: &scalar_arguments,
                    })?;
                if field.data_type() != function.return_type()
                    || field.is_nullable() != function.nullable()
                {
                    return Err(error("local scalar physical return field mismatch"));
                }
                if json {
                    crate::physical::validate_json_table_physical_expression(expr, schema)?;
                }
            }
        } else if !(any.is::<Column>()
            || any.is::<Literal>()
            || any.is::<BinaryExpr>()
            || any.is::<CastExpr>()
            || any.is::<TryCastExpr>()
            || any.is::<CaseExpr>()
            || any.is::<NotExpr>()
            || any.is::<NegativeExpr>()
            || any.is::<IsNullExpr>()
            || any.is::<IsNotNullExpr>()
            || any.is::<InListExpr>()
            || any.is::<LikeExpr>())
        {
            return Err(error(
                "local query contains an unsupported physical expression",
            ));
        }
        for child in expr.children() {
            expression(child, schema, bindings, state)?;
        }
        Ok(())
    }
    fn synchronous_expression(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &arrow_schema::Schema,
        bindings: &[Arc<BoundValueUdf>],
        state: &datafusion::execution::SessionState,
    ) -> Result<()> {
        if contains_local(expr) {
            return Err(error(
                "local subquery in a synchronous native expression requires an input projection",
            ));
        }
        expression(expr, schema, bindings, state)
    }
    fn visit(
        plan: &Arc<dyn ExecutionPlan>,
        bindings: &[Arc<BoundValueUdf>],
        state: &datafusion::execution::SessionState,
    ) -> Result<()> {
        let any = plan.as_any();
        if let Some(projection) = any.downcast_ref::<ProjectionExec>() {
            for (expr, _) in projection.expr() {
                expression(expr, projection.input().schema().as_ref(), bindings, state)?;
            }
        } else if let Some(filter) = any.downcast_ref::<FilterExec>() {
            expression(
                filter.predicate(),
                filter.input().schema().as_ref(),
                bindings,
                state,
            )?;
        } else if let Some(aggregate) = any.downcast_ref::<AggregateExec>() {
            let schema = aggregate.input().schema();
            for (expr, _) in aggregate
                .group_expr()
                .expr()
                .iter()
                .chain(aggregate.group_expr().null_expr())
            {
                synchronous_expression(expr, schema.as_ref(), bindings, state)?;
            }
            for function in aggregate.aggr_expr() {
                let udf = function.fun().inner();
                if udf.signature().volatility != Volatility::Immutable
                    || !state.aggregate_functions().values().any(|candidate| {
                        candidate.inner().as_any().type_id() == udf.as_any().type_id()
                    })
                {
                    return Err(error(
                        "local query contains an unsupported aggregate implementation",
                    ));
                }
                for expr in function.expressions() {
                    synchronous_expression(&expr, schema.as_ref(), bindings, state)?;
                }
                if let Some(ordering) = function.order_bys() {
                    for order in ordering {
                        synchronous_expression(&order.expr, schema.as_ref(), bindings, state)?;
                    }
                }
            }
            for expr in aggregate.filter_expr().iter().flatten() {
                synchronous_expression(expr, schema.as_ref(), bindings, state)?;
            }
        } else if let Some(sort) = any.downcast_ref::<SortExec>() {
            for order in sort.expr() {
                synchronous_expression(
                    &order.expr,
                    sort.input().schema().as_ref(),
                    bindings,
                    state,
                )?;
            }
        } else if let Some(join) = any.downcast_ref::<HashJoinExec>() {
            for (left, right) in join.on() {
                synchronous_expression(left, join.left().schema().as_ref(), bindings, state)?;
                synchronous_expression(right, join.right().schema().as_ref(), bindings, state)?;
            }
            if let Some(filter) = join.filter() {
                synchronous_expression(filter.expression(), filter.schema(), bindings, state)?;
            }
        } else if let Some(join) = any.downcast_ref::<NestedLoopJoinExec>() {
            if let Some(filter) = join.filter() {
                synchronous_expression(filter.expression(), filter.schema(), bindings, state)?;
            }
        } else if !(any.is::<CrossJoinExec>()
            || any.is::<EmptyExec>()
            || any.is::<PlaceholderRowExec>()
            || any.is::<GlobalLimitExec>()
            || any.is::<LocalLimitExec>()
            || any.is::<UnionExec>()
            || any.is::<UnnestExec>()
            || any.is::<CoalesceBatchesExec>()
            || any.is::<CoalescePartitionsExec>())
        {
            return Err(error(format!(
                "local query does not permit physical operator {}",
                plan.name()
            )));
        }
        for child in plan.children() {
            visit(child, bindings, state)?;
        }
        Ok(())
    }
    for (index, binding) in bindings.iter().enumerate() {
        if binding.index != index {
            return Err(error("local binding indices must be contiguous"));
        }
    }
    visit(plan, bindings, &state)?;
    crate::physical::validate_local_json_table_execution_plan(plan.as_ref())
}

#[derive(Debug)]
pub struct ArrayQueryMarker {
    signature: Signature,
}
impl Default for ArrayQueryMarker {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}
impl ScalarUDFImpl for ArrayQueryMarker {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "__streamr_array_query"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            args[0].clone(),
            true,
        ))))
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(error("ARRAY(query) was not bound during planning"))
    }
}

#[cfg(test)]
mod integrity_tests {
    use super::*;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::{placeholder_row::PlaceholderRowExec, sorts::sort::SortExec};

    fn json_sort(result_type: DataType, path: Arc<dyn PhysicalExpr>) -> Arc<dyn ExecutionPlan> {
        let udf = Arc::new(ScalarUDF::from(
            crate::sql_json::kernels::JsonValueFunc::new(
                crate::sql_json::kernels::Returning::Varchar,
            ),
        ));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            "json_value",
            udf,
            vec![
                Arc::new(Literal::new(ScalarValue::Utf8(Some("{}".into())))),
                path,
            ],
            Arc::new(arrow_schema::Field::new("json_value", result_type, true)),
        ));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr,
            options: Default::default(),
        }]);
        Arc::new(SortExec::new(
            ordering,
            Arc::new(PlaceholderRowExec::new(Arc::new(
                arrow_schema::Schema::empty(),
            ))),
        ))
    }

    #[test]
    fn json_sort_decode_checks_return_field_and_literal_path() {
        let path = Arc::new(Literal::new(ScalarValue::Utf8(Some("$".into()))));
        let valid = json_sort(DataType::Utf8, path.clone());
        validate_local_physical(&valid, &[]).unwrap();
        let malformed = json_sort(DataType::Boolean, path);
        assert!(
            validate_local_physical(&malformed, &[])
                .unwrap_err()
                .to_string()
                .contains("return field mismatch")
        );
        let null_path = json_sort(
            DataType::Utf8,
            Arc::new(Literal::new(ScalarValue::Utf8(None))),
        );
        assert!(
            validate_local_physical(&null_path, &[])
                .unwrap_err()
                .to_string()
                .contains("non-null UTF8 literal")
        );
    }
}
