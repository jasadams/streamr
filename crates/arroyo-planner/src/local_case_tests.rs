//! Local CASE resolution must retain the upstream physical expression's result type.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::{
    PhysicalExpr,
    expressions::{CaseExpr, CastExpr, Column, Literal},
};

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("key", DataType::Int64, false),
            Field::new("clock", timestamp_type(), false),
        ])),
        vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(TimestampNanosecondArray::from(vec![50_000_000_000])),
        ],
    )
    .unwrap()
}

fn literal(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(value))
}

fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn case(
    base: Option<Arc<dyn PhysicalExpr>>,
    branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    otherwise: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(CaseExpr::try_new(base, branches, otherwise).unwrap())
}

async fn compare_upstream(expression: Arc<dyn PhysicalExpr>, expected_type: DataType) -> ArrayRef {
    let batch = batch();
    assert_eq!(
        expression.data_type(batch.schema().as_ref()).unwrap(),
        expected_type
    );
    let upstream = expression.evaluate(&batch).unwrap().into_array(1).unwrap();
    let resolved = crate::local_physical::resolve_expression(expression, &batch)
        .await
        .unwrap();
    assert_eq!(
        resolved.data_type(batch.schema().as_ref()).unwrap(),
        expected_type
    );
    let actual = resolved.evaluate(&batch).unwrap().into_array(1).unwrap();
    assert_eq!(actual.data_type(), &expected_type);
    assert_eq!(actual.to_data(), upstream.to_data());
    actual
}

#[tokio::test]
async fn searched_case_selected_untyped_null_retains_timestamp_type() {
    let expression = case(
        None,
        vec![
            (column("flag", 0), literal(ScalarValue::Null)),
            (
                literal(ScalarValue::Boolean(Some(false))),
                column("clock", 2),
            ),
        ],
        Some(column("clock", 2)),
    );
    assert!(
        compare_upstream(expression, timestamp_type())
            .await
            .is_null(0)
    );
}

#[tokio::test]
async fn simple_case_selected_untyped_null_retains_timestamp_type() {
    let expression = case(
        Some(column("key", 1)),
        vec![
            (
                literal(ScalarValue::Int64(Some(7))),
                literal(ScalarValue::Null),
            ),
            (literal(ScalarValue::Int64(Some(8))), column("clock", 2)),
        ],
        Some(column("clock", 2)),
    );
    assert!(
        compare_upstream(expression, timestamp_type())
            .await
            .is_null(0)
    );
}

#[tokio::test]
async fn nested_case_selected_untyped_null_retains_timestamp_type() {
    let inner = case(
        None,
        vec![(column("flag", 0), literal(ScalarValue::Null))],
        Some(column("clock", 2)),
    );
    let outer = case(
        None,
        vec![(column("flag", 0), inner)],
        Some(column("clock", 2)),
    );
    assert!(compare_upstream(outer, timestamp_type()).await.is_null(0));
}

#[tokio::test]
async fn case_else_is_coerced_to_declared_result_type() {
    let expression = case(
        None,
        vec![(literal(ScalarValue::Boolean(Some(false))), column("key", 1))],
        Some(literal(ScalarValue::Int32(Some(42)))),
    );
    let actual = compare_upstream(expression, DataType::Int64).await;
    assert_eq!(
        actual
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[tokio::test]
async fn case_unchosen_failing_timestamp_branch_stays_lazy() {
    let failing: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        literal(ScalarValue::Utf8(Some("not a timestamp".into()))),
        timestamp_type(),
        None,
    ));
    assert!(failing.evaluate(&batch()).is_err());
    let expression = case(
        None,
        vec![
            (column("flag", 0), column("clock", 2)),
            (literal(ScalarValue::Boolean(Some(true))), failing),
        ],
        None,
    );
    let actual = compare_upstream(expression, timestamp_type()).await;
    assert_eq!(
        actual
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        50_000_000_000
    );
}
