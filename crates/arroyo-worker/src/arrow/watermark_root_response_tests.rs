//! Source admission/root boundary tests, not Engine/SQL composition proof.
use super::*;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::errors::DataflowError;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};

fn schema(markers: bool) -> arrow_schema::SchemaRef {
    let mut fields = vec![
        Field::new(
            "event_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("k", DataType::Utf8, false),
        Field::new("eligible", DataType::Boolean, false),
    ];
    if markers {
        fields.push(Field::new(
            arroyo_rpc::SOURCE_ENVELOPE_FIELD,
            DataType::UInt64,
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}
fn batch(seconds: &[i64], markers: Option<&[u64]>) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(
            seconds
                .iter()
                .map(|second| second * 1_000_000_000)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["opaque"; seconds.len()])),
        Arc::new(BooleanArray::from(
            (0..seconds.len()).map(|row| row != 0).collect::<Vec<_>>(),
        )),
    ];
    if let Some(markers) = markers {
        columns.push(Arc::new(UInt64Array::from(markers.to_vec())));
    }
    RecordBatch::try_new(schema(markers.is_some()), columns).unwrap()
}
fn watermark() -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("event_time", 0)),
        Operator::Minus,
        Arc::new(Literal::new(ScalarValue::new_interval_dt(1, 0))),
    ))
}
fn generator(markers: bool, preserve: bool) -> WatermarkGenerator {
    let mut generator = WatermarkGenerator::expression(
        std::time::Duration::from_secs(1),
        None,
        watermark(),
        0,
        markers.then_some(3),
    );
    generator.preserve_source_envelope = preserve;
    let output = if markers && !preserve {
        Arc::new(schema(true).project(&[0, 1, 2]).unwrap())
    } else {
        schema(markers)
    };
    generator.response_root = Some(ResponseRoot::new(7, schema(markers), output));
    generator
}
#[derive(Default)]
struct Captures {
    events: Vec<EventResponse>,
    data: Vec<RecordBatch>,
    watermarks: Vec<Watermark>,
}
#[async_trait]
impl Collector for Captures {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        self.data.push(batch);
        Ok(())
    }
    async fn collect_response(&mut self, response: EventResponse) -> DataflowResult<()> {
        self.events.push(response);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.watermarks.push(watermark);
        Ok(())
    }
}
fn clocks(response: &EventResponse) -> Vec<i64> {
    response
        .rows
        .as_ref()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .values()
        .to_vec()
}
#[tokio::test]
async fn root_response_admitted_append_10_20_5_precedes_any_branch_where() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for cap in [1, 8] {
            let input = batch(&[10, 20, 5], None);
            let mut root = generator(false, false);
            let mut captured = Captures::default();
            for offset in (0..3).step_by(cap) {
                root.admit_batch(input.slice(offset, cap.min(3 - offset)), 0, &mut captured)
                    .await
                    .unwrap();
            }
            assert!(captured.data.is_empty());
            assert_eq!(captured.events.len(), 3);
            let nonce = captured.events[0].context.attempt_nonce;
            for (event, (sequence, seconds)) in
                captured.events.iter().zip([(1, 10), (2, 20), (3, 5)])
            {
                assert_eq!(
                    event.context,
                    EventContext {
                        version: EVENT_RESPONSE_VERSION,
                        root_operator: 7,
                        root_subtask: 0,
                        attempt_nonce: nonce,
                        sequence,
                        trigger_nanos: seconds * 1_000_000_000
                    }
                );
                assert_eq!(clocks(event), vec![seconds * 1_000_000_000]);
                assert!(event.touched_keys.is_none() && event.native_body.is_none());
            }
            // First source event cannot qualify a future eligible=true WHERE. It
            // still gets original sequence1 before any branch expression runs.
            assert!(
                !captured.events[0]
                    .rows
                    .as_ref()
                    .unwrap()
                    .column(2)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0)
            );
        }
    })
    .await
    .expect("finite root admission scenario");
}
#[tokio::test]
async fn root_response_complete_cdc_operations_use_marker_not_equal_keys_or_clock() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for preserve in [false, true] {
            let mut root = generator(true, preserve);
            let mut captured = Captures::default();
            root.admit_batch(
                batch(&[20, 5, 20, 30, 15], Some(&[0, 0, 1, 2, 2])),
                0,
                &mut captured,
            )
            .await
            .unwrap();
            assert_eq!(
                captured
                    .events
                    .iter()
                    .map(|event| event.rows.as_ref().unwrap().num_rows())
                    .collect::<Vec<_>>(),
                vec![2, 1, 2]
            );
            assert_eq!(
                captured
                    .events
                    .iter()
                    .map(|event| event.context.sequence)
                    .collect::<Vec<_>>(),
                vec![1, 2, 3]
            );
            assert_eq!(
                captured
                    .events
                    .iter()
                    .map(|event| event.context.trigger_nanos)
                    .collect::<Vec<_>>(),
                vec![5, 20, 15]
                    .into_iter()
                    .map(|second| second * 1_000_000_000)
                    .collect::<Vec<_>>()
            );
            for event in &captured.events {
                assert!(
                    clocks(event)
                        .iter()
                        .all(|clock| *clock == event.context.trigger_nanos)
                );
                assert_eq!(
                    event
                        .rows
                        .as_ref()
                        .unwrap()
                        .column_by_name(arroyo_rpc::SOURCE_ENVELOPE_FIELD)
                        .is_some(),
                    preserve
                );
            }
            root.admit_batch(batch(&[50, 7], Some(&[0, 0])), 0, &mut captured)
                .await
                .unwrap();
            assert_eq!(
                captured.events[3].context.sequence, 4,
                "batch-local marker reuse cannot reset root sequence"
            );
            assert_eq!(clocks(&captured.events[3]), vec![7_000_000_000; 2]);
        }
    })
    .await
    .expect("finite root admission scenario");
}
#[tokio::test]
async fn root_response_uses_existing_whole_operation_admission_floor() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut root = generator(true, true);
        root.state_cache.emitted_watermark = Some(10_000_000_000);
        root.state_cache.max_watermark = Some(10_000_000_000);
        root.state_cache.last_watermark_emitted_at = Some(10_000_000_000);
        let mut captured = Captures::default();
        root.admit_batch(
            batch(&[1, 12, 1, 2, 11], Some(&[0, 0, 1, 1, 2])),
            0,
            &mut captured,
        )
        .await
        .unwrap();
        assert_eq!(captured.events.len(), 2);
        assert_eq!(captured.events[0].context.sequence, 1);
        assert_eq!(clocks(&captured.events[0]), vec![12_000_000_000; 2]);
        assert_eq!(captured.events[1].context.sequence, 2);
        assert_eq!(clocks(&captured.events[1]), vec![11_000_000_000]);
        assert_eq!(root.response_root.as_ref().unwrap().next_sequence, Some(3));
    })
    .await
    .expect("finite root admission scenario");
}
#[tokio::test]
async fn root_response_sequence_exhaustion_never_wraps_or_emits_zero() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut root = generator(false, false);
        root.response_root.as_mut().unwrap().next_sequence = Some(u64::MAX);
        let mut captured = Captures::default();
        let error = root
            .admit_batch(batch(&[10, 20], None), 0, &mut captured)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("sequence exhausted"));
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.events[0].context.sequence, u64::MAX);
        assert_eq!(root.response_root.as_ref().unwrap().next_sequence, None);
        // This is a failed whole attempt, not permission to continue that owner.
    })
    .await
    .expect("finite root admission scenario");
}
#[tokio::test]
async fn root_response_new_owner_has_fresh_nonce_sequence_and_ordinary_checkpoint_state() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut old = generator(false, false);
        let mut captured = Captures::default();
        old.admit_batch(batch(&[10, 20], None), 0, &mut captured)
            .await
            .unwrap();
        let old_nonce = captured.events[0].context.attempt_nonce;
        let encoded = bincode::encode_to_vec(old.state_cache, bincode::config::standard()).unwrap();
        let (state, used) = bincode::decode_from_slice::<WatermarkGeneratorState, _>(
            &encoded,
            bincode::config::standard(),
        )
        .unwrap();
        assert_eq!(used, encoded.len());
        let mut fresh = generator(false, false);
        fresh.state_cache = state;
        let mut restored = Captures::default();
        fresh
            .admit_batch(batch(&[5], None), 0, &mut restored)
            .await
            .unwrap();
        assert_eq!(restored.events.len(), 1);
        assert_eq!(restored.events[0].context.sequence, 1);
        assert_ne!(restored.events[0].context.attempt_nonce, old_nonce);
        assert_eq!(restored.events[0].context.trigger_nanos, 5_000_000_000);
        assert_eq!(
            fresh.state_cache.emitted_watermark,
            old.state_cache.emitted_watermark
        );
        // State serialization/fresh object only; no Engine restore or link teardown claim.
    })
    .await
    .expect("finite root admission scenario");
}
async fn task_context(
    output: arrow_schema::SchemaRef,
    parallelism: u32,
    index: u32,
) -> OperatorContext {
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    OperatorContext::new(
        Arc::new(arroyo_types::TaskInfo {
            job_id: format!(
                "root-generator-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ),
            operator_idx: 999,
            operator_name: "expression_watermark_generator".into(),
            operator_id: "opaque-chained-context".into(),
            task_index: index,
            parallelism,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        }),
        None,
        tx,
        1,
        vec![Arc::new(ArroyoSchema::new_unkeyed(output.clone(), 0))],
        Some(Arc::new(ArroyoSchema::new_unkeyed(output, 0))),
        HashMap::new(),
    )
    .await
}
#[tokio::test]
async fn root_response_validates_explicit_raw_schema_singleton_not_physical_task_id() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let root = generator(false, false);
        let state = root.response_root.as_ref().unwrap();
        state
            .validate_task(&task_context(schema(false), 1, 0).await)
            .unwrap();
        assert!(
            state
                .validate_task(&task_context(schema(false), 2, 0).await)
                .is_err()
        );
        assert!(
            state
                .validate_task(&task_context(schema(false), 1, 1).await)
                .is_err()
        );
        let wrong = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        assert!(
            state
                .validate_task(&task_context(wrong, 1, 0).await)
                .is_err()
        );
        let mut root = generator(false, false);
        let mut captured = Captures::default();
        assert!(
            root.admit_batch(batch(&[10], None), 1, &mut captured)
                .await
                .is_err()
        );
        assert!(captured.events.is_empty());
    })
    .await
    .expect("finite root admission scenario");
}
#[test]
fn root_response_rpc_constructor_is_explicit_and_validates_declared_clock() {
    use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
    let expression = serialize_physical_expr(&watermark(), &DefaultPhysicalExtensionCodec {})
        .unwrap()
        .encode_to_vec();
    let valid = ExpressionWatermarkConfig {
        response_root_operator: Some(u32::MAX),
        period_micros: 1_000_000,
        idle_time_micros: None,
        input_schema: Some(ArroyoSchema::new_unkeyed(schema(false), 0).into()),
        expression,
        source_envelope_index: None,
        preserve_source_envelope: false,
    };
    let constructor = WatermarkGeneratorConstructor;
    assert!(
        constructor
            .with_config(valid.clone(), Arc::new(Registry::default()))
            .is_ok()
    );
    let mut missing = valid.clone();
    missing.input_schema = None;
    assert!(
        constructor
            .with_config(missing, Arc::new(Registry::default()))
            .is_err()
    );
    let mut wrong = valid;
    wrong.input_schema = Some(
        ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![Field::new(
                "event_time",
                DataType::Int64,
                false,
            )])),
            0,
        )
        .into(),
    );
    match constructor.with_config(wrong, Arc::new(Registry::default())) {
        Err(error) => assert!(
            error
                .to_string()
                .contains("response root FOR clock must be Timestamp(Nanosecond,None)")
        ),
        Ok(_) => panic!("response root admitted a non-timestamp FOR clock"),
    }
}
#[tokio::test]
async fn root_response_disabled_mode_preserves_ordinary_data_and_watermarks() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut ordinary = WatermarkGenerator::expression(
            std::time::Duration::from_secs(1),
            None,
            watermark(),
            0,
            None,
        );
        let input = batch(&[10, 20, 5], None);
        let mut captured = Captures::default();
        ordinary
            .admit_batch(input.clone(), 0, &mut captured)
            .await
            .unwrap();
        assert_eq!(captured.data, vec![input]);
        assert!(captured.events.is_empty());
        assert_eq!(captured.watermarks.len(), 1);
    })
    .await
    .expect("finite root admission scenario");
}
#[tokio::test]
async fn root_response_actual_fanout_queue_one_finishes_event_prefix_before_later_barrier() {
    tokio::time::timeout(std::time::Duration::from_secs(15),async {
        use arroyo_operator::context::{ArrowCollector,batch_bounded};
        use arroyo_types::{ArrowMessage,ChainInfo};
        let mut root=generator(false,false);let input=batch(&[10,20],None);
        let spec=root.response_root.as_ref().unwrap().spec.clone();
        let (left,mut left_rx)=batch_bounded(1);let (right,mut right_rx)=batch_bounded(1);
        let later_left=left.clone();let later_right=right.clone();
        right.send(ArrowMessage::Signal(SignalMessage::Stop)).await.unwrap();
        let info=Arc::new(ChainInfo {job_id:format!("root-fanout-{}",std::process::id()),task_id:999,description:"root-fanout".into(),task_index:0});
        let mut collector=ArrowCollector::new(info,Some(Arc::new(ArroyoSchema::new_unkeyed(schema(false),0))),vec![vec![left],vec![right]]);
        collector.register_event_output(spec).unwrap();
        let checkpoint=CheckpointBarrier {
            completed_base_epoch: None,epoch:1,min_epoch:0,timestamp:SystemTime::UNIX_EPOCH,then_stop:false};
        let mut prefix=Box::pin(async {
            root.admit_batch(input,0,&mut collector).await?;
            // Same existing FIFO queues AFTER callback returns, just as the
            // operator loop broadcasts a later aligned checkpoint barrier.
            later_left.send(ArrowMessage::Signal(SignalMessage::Barrier(checkpoint))).await.unwrap();
            later_right.send(ArrowMessage::Signal(SignalMessage::Barrier(checkpoint))).await.unwrap();
            Ok::<_,DataflowError>(())
        });
        let first=tokio::select! {event=left_rx.recv()=>event.unwrap(),result=&mut prefix=>panic!("root escaped held fanout: {result:?}")};
        assert!(matches!(&first,ArrowMessage::Event(event) if event.context.sequence==1));
        let mut next_left=Box::pin(left_rx.recv());assert!(futures::poll!(&mut next_left).is_pending());drop(next_left);
        assert_eq!(right_rx.recv().await,Some(ArrowMessage::Signal(SignalMessage::Stop)));
        let mut l=vec![first];let mut r=vec![];let(mut ld,mut rd,mut finished)=(false,false,false);
        while !ld || !rd || !finished {
            tokio::select! {
                item=left_rx.recv(),if !ld=>{let item=item.unwrap();ld=matches!(&item,ArrowMessage::Signal(SignalMessage::Barrier(_)));l.push(item);},
                item=right_rx.recv(),if !rd=>{let item=item.unwrap();rd=matches!(&item,ArrowMessage::Signal(SignalMessage::Barrier(_)));r.push(item);},
                result=&mut prefix,if !finished=>{result.unwrap();finished=true;},
            }
        }
        for branch in [&l,&r] {
            assert_eq!(branch.len(),4);
            assert!(matches!(&branch[0],ArrowMessage::Event(event) if event.context.sequence==1));
            assert!(matches!(&branch[1],ArrowMessage::Event(event) if event.context.sequence==2));
            assert!(matches!(&branch[2],ArrowMessage::Signal(SignalMessage::Watermark(_))));
            assert!(matches!(&branch[3],ArrowMessage::Signal(SignalMessage::Barrier(barrier)) if barrier.epoch==1));
        }
        assert_eq!(l,r,"whole fanout has identical contexts and data");
    }).await.expect("finite root fanout prefix scenario");
}
