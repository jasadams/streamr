// Direct calendar snapshots use native event/maintenance kernels, never the
// bounded branch response fanout. Helpers in the parent exercise real config.
use super::*;

const DAY: i64 = 86_400_000_000_000;

fn calendar_config(corrections: bool) -> (UpdatingAggregateOperator, Arc<Registry>) {
    let (mut config, registry) = if corrections {
        native_correction_config()
    } else {
        native_event_config()
    };
    let date: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::CastExpr::new(
            Arc::new(Column::new(TIMESTAMP_FIELD, 3)), DataType::Date32, None,
        ),
    );
    let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
    let codec = DefaultPhysicalExtensionCodec {};
    let date = serialize_physical_expr(&date, &codec).unwrap().encode_to_vec();
    config.calendar_aggregates.push(arroyo_rpc::grpc::api::CalendarAggregateDescriptor {
        aggregate_index: 0, horizon_days: 1,
        argument: serialize_physical_expr(&one, &codec).unwrap().encode_to_vec(),
        static_filter: None, contribution_date: date.clone(), reference_date: date,
        context_id: "native-UTC-day".into(),
    });
    (config, registry)
}

async fn operator(corrections: bool) -> (IncrementalAggregatingFunc, OperatorContext) {
    let (config, registry) = calendar_config(corrections);
    let mut ctx = native_event_context(&config).await;
    ctx.out_schema = Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let config = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
    let mut limits = native_test_config();
    // A quiet rollover of three keys must drain several bounded outputs.
    limits.max_pending_output_rows = 2;
    let mut operator = IncrementalAggregatingConstructor::build_with_native_config(config, registry, Some(limits)).unwrap();
    operator.native_store = Some(native_test_store());
    (operator, ctx)
}

fn snapshots(collector: &AggregateCollector) -> Vec<(String, i64, i64)> {
    collector.batches.iter().flat_map(|batch| {
        assert!(batch.schema().index_of(UPDATING_META_FIELD).is_err());
        (0..batch.num_rows()).map(|row| (
            batch.column(0).as_string::<i32>().value(row).to_owned(),
            batch.column(1).as_primitive::<arrow_array::types::Int64Type>().value(row),
            batch.column(3).as_primitive::<arrow_array::types::TimestampNanosecondType>().value(row),
        )).collect::<Vec<_>>()
    }).collect()
}

#[tokio::test]
async fn calendar_merge_three_quiet_keys_keep_native_clock_and_suppress_unchanged_rollover() {
    let (mut operator, mut ctx) = operator(false).await;
    let mut output = AggregateCollector::default();
    let times = [200 * DAY + 10, 200 * DAY + 20, 200 * DAY + 30];
    operator.process_batch(batch_at(&[Some("a"), Some("b"), Some("c")], &[1, 2, 3], &times), &mut ctx, &mut output).await.unwrap();
    operator.handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut output).await.unwrap();
    assert_eq!(snapshots(&output), vec![
        ("a".into(), 1, times[0]), ("b".into(), 1, times[1]), ("c".into(), 1, times[2]),
        ("a".into(), 0, times[0]), ("b".into(), 0, times[1]), ("c".into(), 0, times[2]),
    ]);
    // Advancing more than a day must still run native references/due ownership,
    // but equal relation values, idle and terminal EOF must not invent rows.
    for watermark in [calendar_dispatch_watermark(203), Watermark::Idle, Watermark::EventTime(arroyo_types::from_nanos(u64::MAX as u128))] {
        operator.handle_watermark(watermark, &mut ctx, &mut output).await.unwrap();
    }
    assert_eq!(snapshots(&output).len(), 6);
    let before = correction_inventory(operator.native_store.as_ref().unwrap()).await;
    let error = operator.process_batch(batch_at(&[Some("a")], &[9], &[202 * DAY]), &mut ctx, &mut output).await.unwrap_err();
    assert!(error.to_string().contains("reference precedes admitted watermark context"), "{error}");
    assert_eq!(correction_inventory(operator.native_store.as_ref().unwrap()).await, before);
    assert_eq!(snapshots(&output).len(), 6);
}

#[tokio::test]
async fn calendar_merge_correction_moves_and_retirement_do_not_publish_intermediate_absence() {
    let (mut operator, mut ctx) = operator(true).await;
    let mut output = AggregateCollector::default();
    operator.process_batch(correction_batch(&[Some("a")], &[10], &[200 * DAY], &[false], &[[1; 16]], &[0]), &mut ctx, &mut output).await.unwrap();
    operator.handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut output).await.unwrap();
    // Before image carries the new operation clock; native J identity must
    // retract the original day200 contribution, never subtract a day201 value.
    operator.process_batch(correction_batch(&[Some("a"), Some("b")], &[10, 20], &[201 * DAY + 10; 2], &[true, false], &[[1; 16]; 2], &[0, 0]), &mut ctx, &mut output).await.unwrap();
    assert_eq!(snapshots(&output), vec![("a".into(), 1, 200 * DAY), ("a".into(), 0, 200 * DAY), ("b".into(), 1, 201 * DAY + 10)]);
    operator.process_batch(correction_batch(&[Some("b")], &[20], &[201 * DAY + 20], &[true], &[[1; 16]], &[0]), &mut ctx, &mut output).await.unwrap();
    assert_eq!(snapshots(&output).len(), 3, "source absence is no implicit target DELETE");
    operator.handle_tick(0, &mut ctx, &mut output).await.unwrap();
    let store = operator.native_store.as_ref().unwrap();
    let scope = store.begin().await.unwrap();
    for prefix in [b"G".as_slice(), b"C", b"B", b"J", b"U", b"H"] {
        assert!(scope.first(prefix).await.unwrap().is_none(), "retired calendar state remains at {prefix:?}");
    }
    drop(scope);
    operator.process_batch(correction_batch(&[Some("a")], &[7], &[201 * DAY + 30], &[false], &[[2; 16]], &[0]), &mut ctx, &mut output).await.unwrap();
    assert_eq!(snapshots(&output).last().unwrap(), &("a".into(), 1, 201 * DAY + 30));
}


#[tokio::test]
async fn calendar_merge_sum_uses_native_nullable_rollover() {
    let (mut config, mut registry) = calendar_config(false);
    let sum = datafusion::functions_aggregate::sum::sum_udaf();
    Arc::get_mut(&mut registry).unwrap().register_udaf(sum.clone()).unwrap();
    let amount: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sequence", 1));
    let expression = Arc::new(AggregateExprBuilder::new(sum, vec![amount.clone()])
        .schema(input_schema()).alias("recent_sum").build().unwrap());
    let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
    let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else { panic!("native aggregate fixture") };
    aggregate.aggr_expr[0] = serialize_physical_aggr_expr(expression.clone(), &DefaultPhysicalExtensionCodec {}).unwrap();
    aggregate.aggr_expr_name[0] = "recent_sum".into();
    config.aggregate_exec = plan.encode_to_vec();
    config.calendar_aggregates[0].argument = serialize_physical_expr(&amount, &DefaultPhysicalExtensionCodec {}).unwrap().encode_to_vec();
    let old: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
    let mut fields = old.schema.fields().to_vec();
    fields[1] = expression.field();
    config.final_schema = Some(ArroyoSchema::new(Arc::new(Schema::new(fields)), old.timestamp_index, old.storage_keys().cloned(), old.routing_keys().cloned()).into());
    let config = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
    let mut ctx = native_event_context(&config).await;
    ctx.out_schema = Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let mut operator = IncrementalAggregatingConstructor::build_with_native_config(config, registry, Some(native_test_config())).unwrap();
    operator.native_store = Some(native_test_store());
    let mut output = AggregateCollector::default();
    operator.process_batch(batch_at(&[Some("a"), Some("a")], &[2, 5], &[200 * DAY + 10, 200 * DAY + 20]), &mut ctx, &mut output).await.unwrap();
    operator.handle_watermark(calendar_dispatch_watermark(201), &mut ctx, &mut output).await.unwrap();
    let values = output.batches.iter().flat_map(|batch| {
        assert!(batch.schema().index_of(UPDATING_META_FIELD).is_err());
        let values = batch.column(1).as_primitive::<arrow_array::types::Int64Type>();
        (0..batch.num_rows()).map(|row| if values.is_null(row) { None } else { Some(values.value(row)) }).collect::<Vec<_>>()
    }).collect::<Vec<_>>();
    assert_eq!(values, vec![Some(2), Some(7), None], "SUM of an empty day is native SQL NULL");
    operator.handle_watermark(calendar_dispatch_watermark(203), &mut ctx, &mut output).await.unwrap();
    assert_eq!(output.batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3, "unchanged native NULL does not recapture");
}
