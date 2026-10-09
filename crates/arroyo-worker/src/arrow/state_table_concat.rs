//! Admission of stock UTF8 CONCAT's and the ANSI SQL/JSON kernels'
//! additional backing allocations inside one fused event operation. This
//! extends the owner's existing Arrow backing-byte contract; it is not
//! accounting for every allocator/metadata allocation in previously admitted
//! scalar expressions.
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    functions::string::concat::ConcatFunc,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, interval_arithmetic::Interval,
        sort_properties::ExprProperties,
    },
    physical_expr::{PhysicalExpr, ScalarFunctionExpr},
    physical_plan::{ExecutionPlan, filter::FilterExec, projection::ProjectionExec},
};

use arroyo_planner::sql_json::kernels::{
    JsonExistsFunc, JsonObjectFunc, JsonQueryFunc, JsonValueFunc,
};

fn exhausted() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "state-table CONCAT exceeds remaining working-event backing-byte allowance".into(),
    )
}

#[derive(Debug, Default)]
pub(super) struct ConcatAllowance {
    remaining: Mutex<Option<usize>>,
    #[cfg(test)]
    invocations: std::sync::atomic::AtomicUsize,
}

impl ConcatAllowance {
    pub(super) fn begin(self: &Arc<Self>, bytes: usize) -> Result<ConcatScope> {
        let mut remaining = self.remaining.lock().unwrap();
        if remaining.is_some() {
            return Err(DataFusionError::Internal(
                "CONCAT operation already active".into(),
            ));
        }
        *remaining = Some(bytes);
        Ok(ConcatScope(Arc::clone(self)))
    }

    fn charge(&self, bytes: usize) -> Result<()> {
        let mut remaining = self.remaining.lock().unwrap();
        let available = remaining.as_mut().ok_or_else(|| {
            DataFusionError::Internal("CONCAT evaluated outside its event operation".into())
        })?;
        *available = available.checked_sub(bytes).ok_or_else(exhausted)?;
        Ok(())
    }
}

pub(super) struct ConcatScope(Arc<ConcatAllowance>);
impl Drop for ConcatScope {
    fn drop(&mut self) {
        *self.0.remaining.lock().unwrap() = None;
    }
}

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn round64(bytes: usize) -> Result<usize> {
    Ok(add(bytes, 63)? & !63)
}

/// DF48's UTF8 array branch preallocates SUM(values().len()), not visible
/// string lengths. Scalar arguments contribute their byte length * row count.
/// With <=1 row this capacity covers every append, so Arrow never grows it.
///
/// The scalar branch grows a Rust String: at most max(8, 2*payload) capacity,
/// and at most 3*max(8,payload) backing bytes during a realloc. Also admit one
/// scalar->one-row Arrow conversion (round64(payload) + one 64-byte offsets
/// buffer + StringArray object), the same backing units used by the owner.
fn backing_charge(args: &[ColumnarValue], rows: usize) -> Result<usize> {
    if rows > 1 {
        return Err(DataFusionError::Execution(
            "fused CONCAT requires at most one event row".into(),
        ));
    }
    let arrays = args
        .iter()
        .any(|arg| matches!(arg, ColumnarValue::Array(_)));
    let mut bytes = 0usize;
    for arg in args {
        let length = match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                let length = value.as_ref().map_or(0, String::len);
                if arrays {
                    length.checked_mul(rows).ok_or_else(exhausted)?
                } else {
                    length
                }
            }
            ColumnarValue::Array(array) if array.len() == rows => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("fused CONCAT admits only UTF8 arguments".into())
                })?
                .values()
                .len(),
            _ => {
                return Err(DataFusionError::Execution(
                    "fused CONCAT admits only UTF8 arguments with the event row count".into(),
                ));
            }
        };
        bytes = add(bytes, length)?;
    }
    finish_charge(bytes, arrays)
}

fn finish_charge(bytes: usize, arrays: bool) -> Result<usize> {
    // The stock UTF8 builder casts its final offset to i32. Refuse before
    // allocation rather than allowing an offset overflow or unchecked sum.
    if bytes > i32::MAX as usize {
        return Err(exhausted());
    }
    let arrow = add(add(round64(bytes)?, 64)?, size_of::<StringArray>())?;
    if arrays {
        return Ok(arrow);
    }
    let growth = if bytes == 0 {
        0
    } else {
        bytes.max(8).checked_mul(3).ok_or_else(exhausted)?
    };
    add(add(growth, arrow)?, size_of::<ScalarValue>())
}

#[derive(Debug)]
struct AdmittedConcat {
    function: ScalarFunctionExpr,
    allowance: Arc<ConcatAllowance>,
}
impl PartialEq for AdmittedConcat {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function && Arc::ptr_eq(&self.allowance, &other.allowance)
    }
}
impl Eq for AdmittedConcat {}
impl Hash for AdmittedConcat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        Arc::as_ptr(&self.allowance).hash(state);
    }
}
impl fmt::Display for AdmittedConcat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.function)
    }
}
impl PhysicalExpr for AdmittedConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn return_field(&self, schema: &Schema) -> Result<FieldRef> {
        self.function.return_field(schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Same evaluation order as ScalarFunctionExpr. Every nested CONCAT
        // has already been wrapped and charges this same cumulative allowance
        // before its own invocation; no child charge is returned prematurely.
        let args = self
            .function
            .args()
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        self.allowance
            .charge(backing_charge(&args, batch.num_rows())?)?;
        let arg_fields = self
            .function
            .args()
            .iter()
            .map(|expr| expr.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;
        #[cfg(test)]
        self.allowance
            .invocations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.function.fun().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: self.function.return_field(batch.schema_ref())?,
        })
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.function.children()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            function: ScalarFunctionExpr::new(
                self.function.name(),
                Arc::new(self.function.fun().clone()),
                children,
                self.function.return_field(&Schema::empty())?,
            ),
            allowance: Arc::clone(&self.allowance),
        }))
    }
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.function.evaluate_bounds(children)
    }
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.function.propagate_constraints(interval, children)
    }
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        self.function.get_properties(children)
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.function.fmt_sql(f)
    }
}

/// Which SQL/JSON kernel an admitted wrapper surrounds; selects the
/// pre-invocation charge model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SqlJsonKind {
    /// JSON_VALUE / JSON_QUERY / JSON_EXISTS: one document and one path in,
    /// a scalar (or at most one serialized document) out.
    Extraction,
    /// JSON_OBJECT: metadata plus per-pair values in, one constructed
    /// document out (FORMAT JSON values may embed their input whole, and
    /// string escaping may expand bytes).
    Construction,
}

fn sql_json_exhausted() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "state-table SQL/JSON evaluation exceeds remaining working-event backing-byte allowance"
            .into(),
    )
}

fn charge_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(sql_json_exhausted)
}

/// Charge the working-event allowance for a SQL/JSON kernel before it
/// allocates, accounting the input bytes, the temporary parsed-document
/// copy, and a worst-case bound on the output:
///
/// * extraction kernels: the document is parsed into a temporary value
///   (~input bytes) and the output never exceeds the document text
///   (a subtree serialization of valid JSON), so `3 * doc + path`;
/// * JSON_OBJECT: values may be re-escaped (up to 6x for control
///   characters) and FORMAT JSON embeds input whole, so values are charged
///   at `7 * bytes` plus metadata and per-pair overhead.
///
/// The charge covers the full set the kernels execute: UTF8 text by byte
/// length, fixed-width scalars at 16 bytes, fixed-width arrays at
/// `rows × element size`, and bare/typed SQL nulls at 16 bytes. Only types
/// the kernels themselves reject (nested lists, structs, …) fail here.
///
/// The fused owner evaluates one event at a time; a multi-row batch means
/// the plan expanded the event and is rejected. Over-reservation is safe
/// (the scope releases on drop); under-reservation is not.
fn sql_json_charge(args: &[ColumnarValue], rows: usize, kind: SqlJsonKind) -> Result<usize> {
    if rows > 1 {
        return Err(DataFusionError::Execution(
            "fused SQL/JSON evaluation requires at most one event row".into(),
        ));
    }
    let mut bytes = 256usize; // fixed builder/parse overhead
    for arg in args {
        let length = sql_json_arg_bytes(arg, rows)?;
        bytes = match kind {
            SqlJsonKind::Extraction => charge_add(bytes, length.saturating_mul(3))?,
            SqlJsonKind::Construction => charge_add(bytes, length.saturating_mul(7))?,
        };
    }
    Ok(bytes)
}

/// Backing bytes attributed to one SQL/JSON argument, matching the types the
/// kernels execute: UTF8/LargeUtf8 text by byte length, every fixed-width
/// scalar (boolean, integer, float, bare/typed null) at 16 bytes, and
/// fixed-width arrays at `rows × element size`. Anything else is a type the
/// kernels reject, and fails here with the same clarity.
fn sql_json_arg_bytes(arg: &ColumnarValue, rows: usize) -> Result<usize> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(value))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => {
            Ok(value.as_ref().map_or(0, String::len))
        }
        ColumnarValue::Scalar(scalar) if is_fixed_width_scalar(scalar) => Ok(16),
        ColumnarValue::Scalar(other) => Err(DataFusionError::Execution(format!(
            "fused SQL/JSON evaluation does not admit scalar argument type {}",
            other.data_type()
        ))),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "fused SQL/JSON evaluation admits only UTF8 arguments".into(),
                    )
                })?
                .values()
                .len()),
            DataType::LargeUtf8 => Ok(array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "fused SQL/JSON evaluation admits only UTF8 arguments".into(),
                    )
                })?
                .values()
                .len()),
            other => fixed_width(other)
                .map(|width| rows.saturating_mul(width))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "fused SQL/JSON evaluation does not admit argument type {other:?}"
                    ))
                }),
        },
    }
}

/// True for every non-null or typed-null scalar the JSON_OBJECT kernel
/// renders (bare `ScalarValue::Null` included).
fn is_fixed_width_scalar(scalar: &ScalarValue) -> bool {
    matches!(
        scalar,
        ScalarValue::Null
            | ScalarValue::Boolean(_)
            | ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
    )
}

/// Element size of a fixed-width arrow type the kernels execute.
fn fixed_width(data_type: &DataType) -> Option<usize> {
    Some(match data_type {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 8,
        _ => return None,
    })
}

/// A SQL/JSON kernel admitted into a fused owner step. Like
/// [`AdmittedConcat`], every invocation charges the owner's cumulative
/// working-event allowance *before* the kernel parses, traverses or
/// constructs, so an oversized evaluation fails before any state write and
/// the reservation is released when the per-event scope drops.
#[derive(Debug)]
struct AdmittedSqlJson {
    function: ScalarFunctionExpr,
    allowance: Arc<ConcatAllowance>,
    kind: SqlJsonKind,
}
impl PartialEq for AdmittedSqlJson {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.kind == other.kind
            && Arc::ptr_eq(&self.allowance, &other.allowance)
    }
}
impl Eq for AdmittedSqlJson {}
impl Hash for AdmittedSqlJson {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        self.kind.hash(state);
        Arc::as_ptr(&self.allowance).hash(state);
    }
}
impl fmt::Display for AdmittedSqlJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.function)
    }
}
impl PhysicalExpr for AdmittedSqlJson {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn return_field(&self, schema: &Schema) -> Result<FieldRef> {
        self.function.return_field(schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let args = self
            .function
            .args()
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        self.allowance
            .charge(sql_json_charge(&args, batch.num_rows(), self.kind)?)?;
        let arg_fields = self
            .function
            .args()
            .iter()
            .map(|expr| expr.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;
        self.function.fun().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: self.function.return_field(batch.schema_ref())?,
        })
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.function.children()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            function: ScalarFunctionExpr::new(
                self.function.name(),
                Arc::new(self.function.fun().clone()),
                children,
                self.function.return_field(&Schema::empty())?,
            ),
            allowance: Arc::clone(&self.allowance),
            kind: self.kind,
        }))
    }
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.function.evaluate_bounds(children)
    }
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.function.propagate_constraints(interval, children)
    }
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        self.function.get_properties(children)
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.function.fmt_sql(f)
    }
}

/// Recognize an admitted SQL/JSON kernel by implementation type (never by
/// name) and report its charge model.
fn sql_json_kind(function: &ScalarFunctionExpr) -> Option<SqlJsonKind> {
    let implementation = function.fun().inner();
    if implementation.as_any().is::<JsonValueFunc>()
        || implementation.as_any().is::<JsonQueryFunc>()
        || implementation.as_any().is::<JsonExistsFunc>()
    {
        Some(SqlJsonKind::Extraction)
    } else if implementation.as_any().is::<JsonObjectFunc>() {
        Some(SqlJsonKind::Construction)
    } else {
        None
    }
}

pub(super) fn guard_expression(
    expression: Arc<dyn PhysicalExpr>,
    allowance: &Arc<ConcatAllowance>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let children = expression
        .children()
        .into_iter()
        .map(|child| guard_expression(Arc::clone(child), allowance))
        .collect::<Result<Vec<_>>>()?;
    let unchanged = expression
        .children()
        .iter()
        .zip(&children)
        .all(|(old, new)| Arc::ptr_eq(old, new));
    let expression = if unchanged {
        expression
    } else {
        expression.with_new_children(children)?
    };
    let Some(function) = expression.as_any().downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(expression);
    };
    let stock = function.fun().inner().as_any().is::<ConcatFunc>();
    if let Some(kind) = sql_json_kind(function) {
        return Ok(Arc::new(AdmittedSqlJson {
            function: ScalarFunctionExpr::new(
                function.name(),
                Arc::new(function.fun().clone()),
                function.args().to_vec(),
                function.return_field(&Schema::empty())?,
            ),
            allowance: Arc::clone(allowance),
            kind,
        }));
    }
    if !stock && !function.name().eq_ignore_ascii_case("concat") {
        return Ok(expression);
    }
    if !stock || function.return_type() != &DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "fused CONCAT requires the stock UTF8 implementation".into(),
        ));
    }
    Ok(Arc::new(AdmittedConcat {
        function: ScalarFunctionExpr::new(
            function.name(),
            Arc::new(function.fun().clone()),
            function.args().to_vec(),
            function.return_field(&Schema::empty())?,
        ),
        allowance: Arc::clone(allowance),
    }))
}

/// Only the already admitted Projection/Filter chain is rewritten. No new
/// execution node, public codec, function registration, or state format exists.
pub(super) fn guard_plan(
    plan: Arc<dyn ExecutionPlan>,
    allowance: &Arc<ConcatAllowance>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let input = guard_plan(Arc::clone(projection.input()), allowance)?;
        let expressions = projection
            .expr()
            .iter()
            .map(|(expr, name)| Ok((guard_expression(Arc::clone(expr), allowance)?, name.clone())))
            .collect::<Result<Vec<_>>>()?;
        if Arc::ptr_eq(&input, projection.input())
            && expressions
                .iter()
                .zip(projection.expr())
                .all(|((new, _), (old, _))| Arc::ptr_eq(new, old))
        {
            return Ok(plan);
        }
        return Ok(Arc::new(ProjectionExec::try_new(expressions, input)?));
    }
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        let input = guard_plan(Arc::clone(filter.input()), allowance)?;
        let predicate = guard_expression(Arc::clone(filter.predicate()), allowance)?;
        if Arc::ptr_eq(&input, filter.input()) && Arc::ptr_eq(&predicate, filter.predicate()) {
            return Ok(plan);
        }
        return Ok(Arc::new(
            FilterExec::try_new(predicate, input)?
                .with_default_selectivity(filter.default_selectivity())?
                .with_projection(filter.projection().cloned())?,
        ));
    }
    if !plan.children().is_empty() {
        return Err(DataFusionError::Plan(
            "unsupported fused scalar plan while guarding CONCAT".into(),
        ));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, RecordBatchOptions};
    use arrow_schema::Field;
    use datafusion::{
        execution::TaskContext,
        logical_expr::{ScalarUDF, Volatility, create_udf},
        physical_expr::expressions::{Column, Literal, cast},
        physical_plan::placeholder_row::PlaceholderRowExec,
    };
    use futures::StreamExt;
    use std::sync::atomic::Ordering;

    fn literal(value: Option<&str>) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Utf8(value.map(str::to_owned))))
    }
    fn concat(args: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            "concat",
            Arc::new(ConcatFunc::new().into()),
            args,
            Arc::new(Field::new("concat", DataType::Utf8, false)),
        ))
    }
    fn row() -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap()
    }
    fn text(value: ColumnarValue) -> String {
        let array = value.into_array(1).unwrap();
        let values = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!values.is_null(0));
        values.value(0).to_owned()
    }

    #[test]
    fn stock_null_empty_integer_and_nested_values_are_preserved() {
        let allowance = Arc::new(ConcatAllowance::default());
        for (arguments, expected) in [
            (vec![literal(None), literal(None)], ""),
            (vec![literal(Some("")), literal(Some(""))], ""),
            (
                vec![literal(Some("a")), literal(None), literal(Some("é"))],
                "aé",
            ),
            (
                vec![
                    concat(vec![literal(Some("a")), literal(Some(":"))]),
                    literal(Some("b")),
                ],
                "a:b",
            ),
        ] {
            let stock = concat(arguments);
            let guarded = guard_expression(Arc::clone(&stock), &allowance).unwrap();
            let _scope = allowance.begin(64 * 1024).unwrap();
            assert_eq!(text(stock.evaluate(&row()).unwrap()), expected);
            assert_eq!(text(guarded.evaluate(&row()).unwrap()), expected);
        }
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        for number in [i64::MIN, i64::MAX] {
            let input = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![number]))],
            )
            .unwrap();
            let expr = concat(vec![
                literal(Some("n:")),
                cast(Arc::new(Column::new("n", 0)), &schema, DataType::Utf8).unwrap(),
            ]);
            let guarded = guard_expression(expr, &allowance).unwrap();
            let _scope = allowance.begin(64 * 1024).unwrap();
            assert_eq!(
                text(guarded.evaluate(&input).unwrap()),
                format!("n:{number}")
            );
        }
    }

    #[test]
    fn stock_case_coalesce_nullif_and_zero_row_selection_are_preserved() {
        use datafusion::physical_expr::expressions::{CaseExpr, IsNullExpr};
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("s", 0));
        let nullif: Arc<dyn PhysicalExpr> = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion::functions::core::nullif(),
                vec![column, literal(Some(""))],
                &schema,
            )
            .unwrap(),
        );
        let coalesce: Arc<dyn PhysicalExpr> = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion::functions::core::coalesce(),
                vec![nullif.clone(), literal(Some("blank"))],
                &schema,
            )
            .unwrap(),
        );
        let stock: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(
                    Arc::new(IsNullExpr::new(nullif)),
                    concat(vec![literal(Some("missing:")), coalesce]),
                )],
                Some(concat(vec![
                    literal(Some("value:")),
                    Arc::new(Column::new("s", 0)),
                ])),
            )
            .unwrap(),
        );
        let allowance = Arc::new(ConcatAllowance::default());
        let guarded = guard_expression(stock.clone(), &allowance).unwrap();
        for (value, expected) in [
            (None, "missing:blank"),
            (Some(""), "missing:blank"),
            (Some("v"), "value:v"),
        ] {
            let input = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec![value]))],
            )
            .unwrap();
            let _scope = allowance.begin(64 * 1024).unwrap();
            assert_eq!(text(stock.evaluate(&input).unwrap()), expected);
            assert_eq!(text(guarded.evaluate(&input).unwrap()), expected);
        }
        let empty = RecordBatch::new_empty(schema);
        let _scope = allowance.begin(64 * 1024).unwrap();
        let expected = stock.evaluate(&empty).unwrap().into_array(0).unwrap();
        let actual = guarded.evaluate(&empty).unwrap().into_array(0).unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());
    }

    #[test]
    fn cumulative_nested_charge_rejects_parent_before_invocation_and_resets() {
        let allowance = Arc::new(ConcatAllowance::default());
        let inner = concat(vec![literal(Some(&"x".repeat(512)))]);
        let outer = concat(vec![inner, literal(Some(&"y".repeat(512)))]);
        let guarded = guard_expression(outer, &allowance).unwrap();
        // Enough for either individual call, but not both held together.
        let parent = backing_charge(
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".repeat(512)))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("y".repeat(512)))),
            ],
            1,
        )
        .unwrap();
        {
            let _scope = allowance.begin(parent).unwrap();
            assert!(matches!(
                guarded.evaluate(&row()),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_eq!(
                allowance.invocations.load(Ordering::Relaxed),
                1,
                "the nested call ran, but the parent builder must not run"
            );
        }
        assert!(allowance.remaining.lock().unwrap().is_none());
        let _scope = allowance.begin(64 * 1024).unwrap();
        assert_eq!(
            text(guarded.evaluate(&row()).unwrap()),
            format!("{}{}", "x".repeat(512), "y".repeat(512))
        );
    }

    #[test]
    fn sliced_and_null_backing_buffers_are_admitted_before_the_stock_builder() {
        let values = StringArray::from(vec![Some("z".repeat(32 * 1024)), None, Some("a".into())]);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let allowance = Arc::new(ConcatAllowance::default());
        let guarded = guard_expression(
            concat(vec![Arc::new(Column::new("s", 0)), literal(Some("!"))]),
            &allowance,
        )
        .unwrap();
        for index in [1, 2] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(values.slice(index, 1))])
                    .unwrap();
            {
                let _scope = allowance.begin(1024).unwrap();
                assert!(matches!(
                    guarded.evaluate(&batch),
                    Err(DataFusionError::ResourcesExhausted(_))
                ));
            }
            assert_eq!(allowance.invocations.load(Ordering::Relaxed), 0);
        }
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values.slice(2, 1))]).unwrap();
        let _scope = allowance.begin(128 * 1024).unwrap();
        assert_eq!(text(guarded.evaluate(&batch).unwrap()), "a!");
    }

    #[test]
    fn named_spoof_and_unsupported_string_type_are_not_invoked() {
        let allowance = Arc::new(ConcatAllowance::default());
        let udf: ScalarUDF = create_udf(
            "concat",
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|_| panic!("spoof must never execute")),
        );
        let spoof = Arc::new(ScalarFunctionExpr::new(
            "concat",
            Arc::new(udf),
            vec![literal(Some("x"))],
            Arc::new(Field::new("x", DataType::Utf8, false)),
        ));
        assert!(guard_expression(spoof, &allowance).is_err());
        let wide = Arc::new(ScalarFunctionExpr::new(
            "concat",
            Arc::new(ConcatFunc::new().into()),
            vec![],
            Arc::new(Field::new("x", DataType::LargeUtf8, false)),
        ));
        assert!(guard_expression(wide, &allowance).is_err());
        assert!(round64(usize::MAX).is_err());
        assert!(add(usize::MAX, 1).is_err());
        assert!(backing_charge(&[], 2).is_err());
        assert!(finish_charge(i32::MAX as usize + 1, true).is_err());
        assert!(finish_charge(usize::MAX, false).is_err());
    }

    #[tokio::test]
    async fn value_projection_filter_chain_uses_one_allowance_and_drop_disarms_it() {
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
        let first: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(concat(vec![literal(Some("a")), literal(None)]), "s".into())],
                source,
            )
            .unwrap(),
        );
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExec::try_new(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                first,
            )
            .unwrap()
            .with_default_selectivity(63)
            .unwrap()
            .with_projection(Some(vec![0]))
            .unwrap(),
        );
        let last: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    concat(vec![Arc::new(Column::new("s", 0)), literal(Some("b"))]),
                    "s".into(),
                )],
                filtered,
            )
            .unwrap(),
        );
        let schema = last.schema();
        let allowance = Arc::new(ConcatAllowance::default());
        let plan = guard_plan(last, &allowance).unwrap();
        assert_eq!(schema, plan.schema());
        let filter = plan
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .unwrap()
            .input()
            .as_any()
            .downcast_ref::<FilterExec>()
            .unwrap();
        assert_eq!(filter.default_selectivity(), 63);
        assert_eq!(filter.projection(), Some(&vec![0]));
        {
            let _scope = allowance.begin(64 * 1024).unwrap();
            let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
            let output = stream.next().await.unwrap().unwrap();
            assert_eq!(text(ColumnarValue::Array(output.column(0).clone())), "ab");
            assert_eq!(allowance.invocations.load(Ordering::Relaxed), 2);
            drop(stream);
        }
        assert!(allowance.remaining.lock().unwrap().is_none());
        // Cancellation while an operation/stream is owned follows the same
        // scope drop path and must not retain an active allowance on retry.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let held = allowance.clone();
        let task = tokio::spawn(async move {
            let _scope = held.begin(64 * 1024).unwrap();
            let _stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
            entered_tx.send(()).unwrap();
            futures::future::pending::<()>().await;
        });
        entered_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let _retry = allowance.begin(64 * 1024).unwrap();
    }

    fn json_call(
        udf: Arc<datafusion::logical_expr::ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn PhysicalExpr> {
        let name = udf.name().to_string();
        // The SQL/JSON kernels' return types never depend on argument types,
        // so placeholder argument types are enough to build the call.
        let return_type = udf.return_type(&vec![DataType::Utf8; args.len()]).unwrap();
        Arc::new(ScalarFunctionExpr::new(
            name.as_str(),
            udf,
            args,
            Arc::new(Field::new("out", return_type, true)),
        ))
    }

    #[test]
    fn sql_json_kernels_are_wrapped_and_charge_before_invocation() {
        let allowance = Arc::new(ConcatAllowance::default());
        let doc = literal(Some(r#"{"a":1}"#));
        let path = literal(Some("$.a"));

        // Every kernel is wrapped by implementation type.
        for udf in [
            arroyo_planner::sql_json::kernels::json_value(),
            arroyo_planner::sql_json::kernels::json_query(),
            arroyo_planner::sql_json::kernels::json_exists(),
        ] {
            let call = json_call(udf, vec![doc.clone(), path.clone()]);
            let guarded = guard_expression(call, &allowance).unwrap();
            assert!(
                guarded.as_any().is::<AdmittedSqlJson>(),
                "kernel must be wrapped for allowance charging"
            );
            // A sufficient allowance lets it evaluate normally.
            {
                let _scope = allowance.begin(64 * 1024).unwrap();
                let _ = guarded.evaluate(&row()).unwrap();
            }
            assert!(allowance.remaining.lock().unwrap().is_none());
        }

        // JSON_OBJECT (construction) is wrapped too.
        let object = json_call(
            arroyo_planner::sql_json::kernels::json_object(),
            vec![literal(Some(r#"[["k",false]]"#)), literal(Some("v"))],
        );
        let guarded = guard_expression(object, &allowance).unwrap();
        assert!(guarded.as_any().is::<AdmittedSqlJson>());
        {
            let _scope = allowance.begin(64 * 1024).unwrap();
            assert_eq!(text(guarded.evaluate(&row()).unwrap()), r#"{"k":"v"}"#);
        }

        // An allowance too small to cover the charge fails BEFORE the kernel
        // runs (the reservation is enforced ahead of allocation).
        let guarded = guard_expression(
            json_call(
                arroyo_planner::sql_json::kernels::json_query(),
                vec![doc, path],
            ),
            &allowance,
        )
        .unwrap();
        {
            let _scope = allowance.begin(1).unwrap();
            assert!(matches!(
                guarded.evaluate(&row()),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        // The scope drop disarms the allowance for retry.
        let _retry = allowance.begin(64 * 1024).unwrap();
    }

    #[test]
    fn sql_json_charge_admits_every_kernel_executed_type() {
        let allowance = Arc::new(ConcatAllowance::default());
        let scalar =
            |value: ScalarValue| -> Arc<dyn PhysicalExpr> { Arc::new(Literal::new(value)) };

        // Int64 scalar, Boolean scalar and bare Null values through
        // JSON_OBJECT: these are fully kernel-supported and must charge
        // (not reject) at the guard.
        let object = json_call(
            arroyo_planner::sql_json::kernels::json_object(),
            vec![
                literal(Some(r#"[["n",false],["b",false],["z",false]]"#)),
                scalar(ScalarValue::Int64(Some(7))),
                scalar(ScalarValue::Boolean(Some(true))),
                scalar(ScalarValue::Null),
            ],
        );
        let guarded = guard_expression(object, &allowance).unwrap();
        {
            let _scope = allowance.begin(64 * 1024).unwrap();
            assert_eq!(
                text(guarded.evaluate(&row()).unwrap()),
                r#"{"n":7,"b":true,"z":null}"#
            );
        }

        // Int64 *array* arguments (a column-backed JSON_OBJECT value) must
        // also charge and execute through the guard.
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![42]))])
                .unwrap();
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("n", 0));
        let object = json_call(
            arroyo_planner::sql_json::kernels::json_object(),
            vec![literal(Some(r#"[["n",false]]"#)), column],
        );
        let guarded = guard_expression(object, &allowance).unwrap();
        {
            let _scope = allowance.begin(64 * 1024).unwrap();
            let out = guarded.evaluate(&batch).unwrap();
            assert_eq!(text(out), r#"{"n":42}"#);
        }
    }

    #[test]
    fn same_named_non_kernel_functions_are_not_wrapped() {
        // Admission matches implementation type, never name: a spoof named
        // like a SQL/JSON builtin is left untouched by the guard (planner
        // admission rejects it before it can reach a fused owner).
        let allowance = Arc::new(ConcatAllowance::default());
        let spoof: Arc<datafusion::logical_expr::ScalarUDF> = Arc::new(create_udf(
            "json_query",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|_| panic!("spoof must never execute")),
        ));
        let call = json_call(spoof, vec![literal(Some("{}")), literal(Some("$.a"))]);
        let guarded = guard_expression(call, &allowance).unwrap();
        assert!(!guarded.as_any().is::<AdmittedSqlJson>());
    }
}
