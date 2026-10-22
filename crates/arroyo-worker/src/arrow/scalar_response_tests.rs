//! Actual native physical execution tests; no route or JOIN activation.
use super::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int64Array, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_rpc::errors::{DataflowError, ErrorDomain, RetryHint, TaskError};
use arroyo_types::event_edge::EventLayout;
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext};
use arroyo_types::{TaskInfo, Watermark};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::physical_plan::{filter::FilterExec, projection::ProjectionExec};

#[derive(Default)]
struct Captured {
    responses: Vec<EventResponse>,
    data: Vec<RecordBatch>,
}
#[async_trait::async_trait]
impl Collector for Captured {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        self.data.push(batch);
        Ok(())
    }
    async fn collect_response(&mut self, response: EventResponse) -> DataflowResult<()> {
        self.responses.push(response);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}
fn context() -> EventContext {
    EventContext {
        version: EVENT_RESPONSE_VERSION,
        root_operator: 7,
        root_subtask: 0,
        attempt_nonce: [9; 16],
        sequence: 1,
        trigger_nanos: 5,
    }
}
fn response(rows: Option<RecordBatch>, keys: Option<RecordBatch>) -> EventResponse {
    EventResponse {
        context: context(),
        rows,
        touched_keys: keys,
        native_body: None,
    }
}
fn raw(schema: SchemaRef) -> Arc<EventEdgeSpec> {
    Arc::new(EventEdgeSpec {
        root_operator: 7,
        root_subtask: 0,
        schema,
        layout: EventLayout::Raw,
    })
}
fn native(schema: SchemaRef, allowance: usize) -> Arc<EventEdgeSpec> {
    Arc::new(EventEdgeSpec {
        root_operator: 7,
        root_subtask: 0,
        schema,
        layout: EventLayout::Native {
            max_rows: 4,
            owner_output_bytes: allowance,
            encoded_key_bytes: 64,
        },
    })
}
fn source(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(arroyo_planner::physical::ArroyoMemExec::new(
        "event_input".into(),
        schema,
    ))
}
fn executor(plan: Arc<dyn ExecutionPlan>) -> StatelessPhysicalExecutor {
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let plan = PhysicalPlanNode::try_from_physical_plan(plan, &codec).unwrap();
    StatelessPhysicalExecutor::new(
        &plan.encode_to_vec(),
        &arroyo_planner::physical::new_registry(),
    )
    .unwrap()
}
fn value(plan: Arc<dyn ExecutionPlan>, spec: Arc<EventEdgeSpec>) -> ValueExecutionOperator {
    let mut operator = ValueExecutionOperator {
        name: "value-response-test".into(),
        executor: executor(plan),
        event_output: None,
    };
    operator.register_response_output(spec).unwrap();
    operator
}
fn key(plan: Arc<dyn ExecutionPlan>, spec: Arc<EventEdgeSpec>) -> KeyExecutionOperator {
    let mut operator = KeyExecutionOperator {
        name: "key-response-test".into(),
        executor: executor(plan),
        key_fields: vec![0],
        event_output: None,
    };
    operator.register_response_output(spec).unwrap();
    operator
}
fn projection(
    schema: SchemaRef,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    spec: Arc<EventEdgeSpec>,
) -> ProjectionOperator {
    let mut operator = ProjectionOperator {
        name: "projection-response-test".into(),
        output_schema: ArroyoSchema::new_unkeyed(schema, 0),
        exprs: expressions,
        event_output: None,
    };
    operator.register_response_output(spec).unwrap();
    operator
}
async fn operator_context() -> OperatorContext {
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    OperatorContext::new(
        Arc::new(TaskInfo {
            job_id: format!(
                "scalar-response-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ),
            operator_idx: 97,
            operator_name: "scalar-response-test".into(),
            operator_id: "scalar-response".into(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        }),
        None,
        tx,
        1,
        vec![],
        None,
        std::collections::HashMap::new(),
    )
    .await
}
fn integers(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}
fn filtered(schema: SchemaRef, predicate: Arc<dyn PhysicalExpr>) -> Arc<dyn ExecutionPlan> {
    Arc::new(FilterExec::try_new(predicate, source(schema)).unwrap())
}
async fn callback(operator: &mut dyn ArrowOperator, input: EventResponse) -> Captured {
    let mut output = Captured::default();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        operator.process_response_index(0, 1, input, &mut operator_context().await, &mut output),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        output.responses.len(),
        1,
        "exactly one response, including zero SQL outputs"
    );
    assert!(
        output.data.is_empty(),
        "typed callback must not emit ordinary Data"
    );
    output
}

#[tokio::test]
async fn scalar_response_actual_filter_false_true_emits_one_for_value_and_key() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for keep in [false, true] {
            let input = integers(vec![Some(10)]);
            let plan = filtered(
                input.schema(),
                Arc::new(Literal::new(ScalarValue::Boolean(Some(keep)))),
            );
            let spec = raw(plan.schema());
            for mut operator in [
                Box::new(value(plan.clone(), spec.clone())) as Box<dyn ArrowOperator>,
                Box::new(key(plan.clone(), spec.clone())),
            ] {
                let output = callback(operator.as_mut(), response(Some(input.clone()), None)).await;
                let output = &output.responses[0];
                assert_eq!(output.context, context());
                assert_eq!(output.rows.as_ref(), keep.then_some(&input));
                assert!(output.touched_keys.is_none());
            }
        }
    })
    .await
    .expect("finite scalar response scenario timed out");
}
#[tokio::test]
async fn scalar_response_absence_skips_failing_physical_execution_and_null_is_present() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let schema = integers(vec![Some(1)]).schema();
        let failing: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "invalid integer".into(),
            )))),
            DataType::Int64,
            None,
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(vec![(failing, "v".into())], source(schema.clone())).unwrap(),
        );
        let mut operator = value(plan.clone(), raw(plan.schema()));
        let output = callback(&mut operator, response(None, None)).await;
        assert!(output.responses[0].rows.is_none());
        assert!(!operator.executor.active.load(Ordering::Acquire));
        let failed =
            callback_error(&mut operator, response(Some(integers(vec![Some(1)])), None)).await;
        assert!(
            failed.to_string().contains("invalid integer"),
            "present row must reach the actual failing cast: {failed}"
        );
        assert!(
            !operator.executor.active.load(Ordering::Acquire),
            "failed native stream must release executor input guard"
        );
        assert!(matches!(&failed, DataflowError::DataFusionError(_)));
        let task_error = TaskError::from(&failed);
        assert_eq!(task_error.domain, ErrorDomain::User);
        assert_eq!(task_error.retry_hint, RetryHint::NoRetry);
        let null: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(None)));
        let mut operator = projection(schema.clone(), vec![null], raw(schema));
        let output = callback(&mut operator, response(Some(integers(vec![Some(1)])), None)).await;
        let rows = output.responses[0].rows.as_ref().unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(rows.column(0).data_type(), &DataType::Int64);
        assert!(rows.column(0).is_null(0));
    })
    .await
    .expect("finite scalar response scenario timed out");
}
async fn callback_error(operator: &mut dyn ArrowOperator, input: EventResponse) -> DataflowError {
    let mut output = Captured::default();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        operator.process_response_index(0, 1, input, &mut operator_context().await, &mut output),
    )
    .await
    .unwrap();
    assert!(output.responses.is_empty());
    result.unwrap_err()
}
fn cdc(before: i64, after: i64) -> RecordBatch {
    let id = vec![8u8; 16];
    let ids =
        FixedSizeBinaryArray::try_from_iter([id.as_slice(), id.as_slice()].into_iter()).unwrap();
    let metadata = StructArray::new(
        arroyo_rpc::updating_meta_fields(),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(ids),
        ],
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Arc::new(Field::new("v", DataType::Int64, true)),
            arroyo_rpc::updating_meta_field(),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![before, after])),
            Arc::new(metadata),
        ],
    )
    .unwrap()
}
fn touched() -> RecordBatch {
    RecordBatch::try_new(
        EventEdgeSpec::keys_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![b"group".as_slice()]))],
    )
    .unwrap()
}
#[tokio::test]
async fn scalar_response_native_before_after_filter_crossings_retain_identity_and_keys() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for (before, after, indices) in [
            (-1, 2, vec![1]),
            (2, -1, vec![0]),
            (2, 3, vec![0, 1]),
            (-1, -2, vec![]),
        ] {
            let input = cdc(before, after);
            let keys = touched();
            let predicate = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("v", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
            ));
            let plan = filtered(input.schema(), predicate);
            let mut operator = value(plan.clone(), native(plan.schema(), 32768));
            let output = callback(
                &mut operator,
                response(Some(input.clone()), Some(keys.clone())),
            )
            .await;
            let output = &output.responses[0];
            assert_eq!(output.context, context());
            assert_eq!(output.touched_keys, Some(keys));
            if indices.is_empty() {
                assert!(output.rows.is_none());
                continue;
            }
            let actual = output.rows.as_ref().unwrap();
            assert_eq!(actual.num_rows(), indices.len());
            for (row, index) in indices.into_iter().enumerate() {
                assert_eq!(
                    actual.slice(row, 1),
                    input.slice(index, 1),
                    "native before/after flag and row identity must survive physical WHERE"
                );
            }
        }
    })
    .await
    .expect("finite scalar response scenario timed out");
}
fn decoded_near_allowance() -> (EventResponse, Arc<EventEdgeSpec>, RecordBatch) {
    let text = "x".repeat(2048);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let original = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![text.as_str(), text.as_str()])) as ArrayRef,
            Arc::new(StringArray::from(vec![text.as_str(), text.as_str()])),
        ],
    )
    .unwrap();
    let spec = native(schema, original.get_array_memory_size());
    let codec = crate::event_response_codec::ResponseSpec::from_edge(&spec).unwrap();
    let decoded = codec
        .decode(
            &codec
                .encode(&response(Some(original.clone()), None))
                .unwrap(),
        )
        .unwrap();
    spec.validate(&decoded).unwrap();
    assert!(decoded.native_body.is_some());
    let without_proof = EventResponse {
        native_body: None,
        ..decoded.clone()
    };
    assert!(
        spec.validate(&without_proof).is_err(),
        "fixture must actually expose decoded shared-body double accounting near owner allowance"
    );
    (decoded, spec, original)
}
#[tokio::test]
async fn scalar_response_alias_subset_slice_uses_checked_native_projection_or_compaction() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let (decoded, input_spec, original) = decoded_near_allowance();
        let aliases = Arc::new(Schema::new(vec![
            Field::new("right", DataType::Utf8, true),
            Field::new("left", DataType::Utf8, true),
        ]));
        let output_spec = native(aliases.clone(), original.get_array_memory_size());
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (
                        Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
                        "right".into(),
                    ),
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        "left".into(),
                    ),
                ],
                source(original.schema()),
            )
            .unwrap(),
        );
        for mut operator in [
            Box::new(value(plan.clone(), output_spec.clone())) as Box<dyn ArrowOperator>,
            Box::new(key(plan, output_spec.clone())),
        ] {
            let output = callback(operator.as_mut(), decoded.clone()).await;
            output_spec.validate(&output.responses[0]).unwrap();
            assert_eq!(
                output.responses[0].rows.as_ref().unwrap().column(0),
                decoded.rows.as_ref().unwrap().column(1)
            );
        }
        let mut operator = projection(
            aliases,
            vec![Arc::new(Column::new("b", 1)), Arc::new(Column::new("a", 0))],
            output_spec.clone(),
        );
        let output = callback(&mut operator, decoded.clone()).await;
        output_spec.validate(&output.responses[0]).unwrap();
        assert_eq!(
            output.responses[0].rows.as_ref().unwrap().column(0),
            decoded.rows.as_ref().unwrap().column(1)
        );
        let sliced = crate::event_response_codec::rebind_response_rows(
            &decoded,
            Some(decoded.rows.as_ref().unwrap().slice(1, 1)),
            &input_spec,
        )
        .unwrap();
        let output = callback(&mut operator, sliced).await;
        assert_eq!(output.responses[0].rows.as_ref().unwrap().num_rows(), 1);
        output_spec.validate(&output.responses[0]).unwrap();
        let subset = Arc::new(Schema::new(vec![Field::new("only", DataType::Utf8, true)]));
        let subset_spec = native(subset.clone(), original.column(0).get_array_memory_size());
        let mut operator = projection(
            subset,
            vec![Arc::new(Column::new("a", 0))],
            subset_spec.clone(),
        );
        let output = callback(&mut operator, decoded.clone()).await;
        subset_spec.validate(&output.responses[0]).unwrap();
        assert_eq!(
            output.responses[0].rows.as_ref().unwrap().column(0),
            original.column(0)
        );
        assert!(
            output.responses[0].retained_bytes() < decoded.retained_bytes(),
            "narrow output must compact when old backing exceeds next stage bound"
        );
    })
    .await
    .expect("finite scalar response scenario timed out");
}
#[tokio::test]
async fn scalar_response_computed_oversize_cannot_borrow_decoded_proof() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let (decoded, _, _) = decoded_near_allowance();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "computed",
            DataType::Utf8,
            false,
        )]));
        let mut operator = projection(
            schema.clone(),
            vec![Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "z".repeat(16384),
            ))))],
            native(schema, 1024),
        );
        let error = callback_error(&mut operator, decoded).await;
        assert!(error.to_string().contains("original owner admission"));
        assert!(matches!(&error, DataflowError::ArgumentError(_)));
        let task = TaskError::from(&error);
        assert_eq!(task.domain, ErrorDomain::User);
        assert_eq!(task.retry_hint, RetryHint::NoRetry);
    })
    .await
    .expect("finite scalar response scenario timed out");
}
#[tokio::test]
async fn scalar_response_registration_is_explicit_schema_checked_and_ordinary_data_unchanged() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let input = integers(vec![Some(10)]);
        let plan = source(input.schema());
        let mut operator = ValueExecutionOperator {
            name: "ordinary-value".into(),
            executor: executor(plan),
            event_output: None,
        };
        let error = callback_error(&mut operator, response(None, None)).await;
        assert!(error.to_string().contains("not explicitly registered"));
        let task = TaskError::from(&error);
        assert_eq!(task.domain, ErrorDomain::User);
        assert_eq!(task.retry_hint, RetryHint::NoRetry);
        let wrong = raw(Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )])));
        assert!(operator.register_response_output(wrong).is_err());
        let mut out = Captured::default();
        operator
            .process_batch(input.clone(), &mut operator_context().await, &mut out)
            .await
            .unwrap();
        assert_eq!(out.data, vec![input.clone()]);
        assert!(out.responses.is_empty());
        operator
            .register_response_output(raw(input.schema()))
            .unwrap();
        assert!(
            operator
                .register_response_output(raw(input.schema()))
                .is_err()
        );
    })
    .await
    .expect("finite scalar response scenario timed out");
}
#[tokio::test]
async fn scalar_response_one_physical_callback_fans_out_same_captured_packet() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        use arroyo_operator::context::{ArrowCollector, batch_bounded};
        use arroyo_types::{ArrowMessage, ChainInfo};
        let input = integers(vec![Some(10)]);
        let spec = raw(input.schema());
        let mut operator = value(source(input.schema()), spec.clone());
        let (left, mut left_rx) = batch_bounded(1);
        let (right, mut right_rx) = batch_bounded(1);
        let info = Arc::new(ChainInfo {
            job_id: format!("scalar-fanout-{}", std::process::id()),
            task_id: 97,
            description: "scalar-fanout".into(),
            task_index: 0,
        });
        let mut collector = ArrowCollector::new(
            info,
            Some(Arc::new(ArroyoSchema::new_unkeyed(input.schema(), 0))),
            vec![vec![left], vec![right]],
        );
        collector.register_event_output(spec).unwrap();
        let event = response(Some(input), None);
        operator
            .process_response_index(
                0,
                1,
                event.clone(),
                &mut operator_context().await,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(
            left_rx.recv().await,
            Some(ArrowMessage::Event(event.clone()))
        );
        assert_eq!(right_rx.recv().await, Some(ArrowMessage::Event(event)));
        assert!(
            !operator.executor.active.load(Ordering::Acquire),
            "native stream must drain before callback delivery completes"
        );
        // Actual named MERGE effect cardinality remains existing smoke_str64 SQL
        // coverage; this unit only proves scalar callback/transport fanout once.
    })
    .await
    .expect("finite scalar response scenario timed out");
}

struct FailingDelivery;
#[async_trait::async_trait]
impl Collector for FailingDelivery {
    async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
        panic!("typed scalar callback emitted Data");
    }
    async fn collect_response(&mut self, _: EventResponse) -> DataflowResult<()> {
        Err(DataflowError::ConnectorError {
            domain: ErrorDomain::External,
            retry: RetryHint::NoRetry,
            error: "typed scalar delivery failure".into(),
            source: Some(anyhow::anyhow!("delivery cause")),
        }
        .with_operator("collector-boundary".into()))
    }
    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}
#[tokio::test]
async fn scalar_response_preserves_native_and_delivery_task_error_classification() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let input = integers(vec![Some(1)]);
        let failing: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "invalid integer".into(),
            )))),
            DataType::Int64,
            None,
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(vec![(failing, "v".into())], source(input.schema())).unwrap(),
        );
        let spec = raw(plan.schema());
        for mut operator in [
            Box::new(value(plan.clone(), spec.clone())) as Box<dyn ArrowOperator>,
            Box::new(key(plan, spec)),
        ] {
            let error =
                callback_error(operator.as_mut(), response(Some(input.clone()), None)).await;
            assert!(
                matches!(&error, DataflowError::DataFusionError(_)),
                "native stream error must retain structured DataFusionError: {error:?}"
            );
            let task = TaskError::from(&error);
            assert_eq!(task.domain, ErrorDomain::User);
            assert_eq!(task.retry_hint, RetryHint::NoRetry);
            assert_eq!(task.operator_id, None);
            assert_eq!(task.details, None);
            assert!(task.message.contains("invalid integer"));
        }
        for mut operator in [
            Box::new(value(source(input.schema()), raw(input.schema()))) as Box<dyn ArrowOperator>,
            Box::new(key(source(input.schema()), raw(input.schema()))),
        ] {
            let error = operator
                .process_response_index(
                    0,
                    1,
                    response(Some(input.clone()), None),
                    &mut operator_context().await,
                    &mut FailingDelivery,
                )
                .await
                .unwrap_err();
            assert!(matches!(&error, DataflowError::WithOperator { .. }));
            let task = TaskError::from(&error);
            assert_eq!(task.domain, ErrorDomain::External);
            assert_eq!(task.retry_hint, RetryHint::NoRetry);
            assert_eq!(task.operator_id.as_deref(), Some("collector-boundary"));
            assert_eq!(task.details.as_deref(), Some("delivery cause"));
            assert_eq!(
                task.message,
                "error in connector: typed scalar delivery failure"
            );
        }
    })
    .await
    .expect("finite structured scalar error scenario");
}

struct BlockedDelivery {
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Collector for BlockedDelivery {
    async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
        panic!("typed scalar callback emitted Data");
    }
    async fn collect_response(&mut self, response: EventResponse) -> DataflowResult<()> {
        self.reached.notify_one();
        self.release.notified().await;
        drop(response);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}
#[tokio::test]
async fn scalar_response_releases_filtered_and_compacted_input_before_blocked_delivery() {
    tokio::time::timeout(std::time::Duration::from_secs(15),async {
        // Both executor callbacks plus Projection; shared output is a positive
        // control and may legitimately retain its immutable source backing.
        for scenario in ["value-filtered","key-filtered","projection-compacted","projection-shared"] {
            let (mut operator,input,weak,output_keeps_array):
                (Box<dyn ArrowOperator>,EventResponse,std::sync::Weak<dyn Array>,bool) =
                if scenario.ends_with("filtered") {
                    let input=integers(vec![Some(10)]);
                    let weak=Arc::downgrade(input.column(0));
                    let plan=filtered(input.schema(),Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))));
                    let spec=native(plan.schema(),32768);
                    let operator:Box<dyn ArrowOperator>=if scenario=="value-filtered" {
                        Box::new(value(plan,spec))
                    } else { Box::new(key(plan,spec)) };
                    (operator,response(Some(input),None),weak,false)
                } else {
                    let (decoded,_,original)=decoded_near_allowance();
                    let weak=Arc::downgrade(decoded.rows.as_ref().unwrap().column(0));
                    let compact=scenario=="projection-compacted";
                    let (schema,expressions,allowance):(_,Vec<Arc<dyn PhysicalExpr>>,_) = if compact {
                        (Arc::new(Schema::new(vec![Field::new("only",DataType::Utf8,true)])),
                         vec![Arc::new(Column::new("a",0))],original.column(0).get_array_memory_size())
                    } else {
                        (Arc::new(Schema::new(vec![Field::new("alias_a",DataType::Utf8,true),Field::new("alias_b",DataType::Utf8,true)])),
                         vec![Arc::new(Column::new("a",0)),Arc::new(Column::new("b",1))],original.get_array_memory_size())
                    };
                    (Box::new(projection(schema.clone(),expressions,native(schema,allowance))),decoded,weak,!compact)
                };
            let reached=Arc::new(tokio::sync::Notify::new());let release=Arc::new(tokio::sync::Notify::new());
            let mut collector=BlockedDelivery {reached:reached.clone(),release:release.clone()};
            let mut context=operator_context().await;
            let mut delivery=Box::pin(operator.process_response_index(0,1,input,&mut context,&mut collector));
            tokio::time::timeout(std::time::Duration::from_secs(3),async {
                tokio::select! {
                    _=reached.notified()=>{},
                    result=&mut delivery=>panic!("collector was not held for {scenario}: {result:?}"),
                }
            }).await.expect("real scalar collector did not reach blocked boundary");
            assert_eq!(weak.upgrade().is_some(),output_keeps_array,"obsolete input retention at actual output await: {scenario}");
            release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(3),delivery).await.unwrap().unwrap();
            assert!(weak.upgrade().is_none(),"collector completed but obsolete array still retained: {scenario}");
        }
    }).await.expect("finite scalar input lifetime scenario");
}
