// Common operation/result preparation exercised through the real constructor.
use super::*;

async fn array_owner(corrections: bool) -> (IncrementalAggregatingFunc, OperatorContext) {
    let (mut config, mut registry) = if corrections { native_correction_config() } else { native_event_config() };
    Arc::get_mut(&mut registry).unwrap().register_udaf(array_agg_udaf()).unwrap();
    let old: ArroyoSchema = config.input_schema.take().unwrap().try_into().unwrap();
    let mut fields = old.schema.fields().to_vec();
    fields[1] = Arc::new(fields[1].as_ref().clone().with_nullable(true));
    let input = Arc::new(Schema::new(fields));
    config.input_schema = Some(ArroyoSchema::new(input.clone(), old.timestamp_index, old.storage_keys().cloned(), old.routing_keys().cloned()).into());
    let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sequence", 1));
    let expression = Arc::new(AggregateExprBuilder::new(array_agg_udaf(), vec![value.clone()])
        .schema(input).alias("items").order_by(LexOrdering::new(vec![PhysicalSortExpr::new(value, SortOptions { descending:false, nulls_first:false })])).build().unwrap());
    let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
    let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else { panic!("native aggregate fixture") };
    aggregate.aggr_expr[0] = serialize_physical_aggr_expr(expression.clone(), &DefaultPhysicalExtensionCodec{}).unwrap();
    aggregate.aggr_expr_name[0] = "items".into();
    config.aggregate_exec = plan.encode_to_vec();
    let old: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
    let mut fields = old.schema.fields().to_vec(); fields[1] = expression.field();
    config.final_schema = Some(ArroyoSchema::new(Arc::new(Schema::new(fields)), old.timestamp_index, old.storage_keys().cloned(), old.routing_keys().cloned()).into());
    let config = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
    let mut ctx = native_event_context(&config).await;
    ctx.out_schema = Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let mut owner = IncrementalAggregatingConstructor::build_with_native_config(config, registry, Some(native_test_config())).unwrap();
    owner.native_store = Some(native_test_store());
    (owner, ctx)
}
fn input(ctx: &OperatorContext, groups: &[Option<&str>], values: &[Option<i64>], times: &[i64], changes: Option<(&[bool], &[[u8;16]], &[u64])>) -> RecordBatch {
    let mut columns = if let Some((retracts, ids, markers)) = changes {
        correction_batch(groups, &vec![0;groups.len()], times, retracts, ids, markers).columns().to_vec()
    } else { batch_at(groups, &vec![0;groups.len()], times).columns().to_vec() };
    columns[1] = Arc::new(Int64Array::from(values.to_vec()));
    RecordBatch::try_new(ctx.in_schemas[0].schema.clone(), columns).unwrap()
}
fn items(output: &AggregateCollector) -> Vec<Vec<Option<i64>>> {
    output.batches.iter().flat_map(|batch| {
        assert!(batch.schema().index_of(UPDATING_META_FIELD).is_err());
        let lists = batch.column(1).as_list::<i32>();
        (0..batch.num_rows()).map(|row| lists.value(row).as_primitive::<arrow_array::types::Int64Type>().iter().collect()).collect::<Vec<_>>()
    }).collect()
}
#[tokio::test]
async fn native_result_array_append_prefixes_order_null_and_original_clock() {
    let (mut owner, mut ctx) = array_owner(false).await;
    assert!(owner.native_operation_needs_snapshot(), "ARRAY replay needs a bounded index snapshot");
    let mut output = AggregateCollector::default();
    owner.process_batch(input(&ctx, &[Some("a");3], &[Some(3), Some(1), None], &[10,20,5], None), &mut ctx, &mut output).await.unwrap();
    assert_eq!(items(&output), vec![vec![Some(3)], vec![Some(1), Some(3)], vec![Some(1), Some(3), None]]);
    let clocks = output.batches.iter().flat_map(|batch| batch.column(3).as_primitive::<arrow_array::types::TimestampNanosecondType>().values().to_vec()).collect::<Vec<_>>();
    assert_eq!(clocks, vec![10,20,5]);
    owner.handle_tick(0,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).len(),3,"ordinary ticks do not fabricate direct accepted results");
}
#[tokio::test]
async fn native_result_array_cdc_whole_replacement_absence_and_recreate() {
    let (mut owner, mut ctx) = array_owner(true).await;
    let mut output = AggregateCollector::default();
    owner.process_batch(input(&ctx,&[Some("a")],&[Some(3)],&[10],Some((&[false],&[[1;16]],&[0]))),&mut ctx,&mut output).await.unwrap();
    owner.process_batch(input(&ctx,&[Some("a"),Some("a")],&[Some(3),Some(1)],&[5,5],Some((&[true,false],&[[1;16];2],&[0,0]))),&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output),vec![vec![Some(3)],vec![Some(1)]],"replacement publishes its final native collection once");
    owner.process_batch(input(&ctx,&[Some("a")],&[Some(1)],&[6],Some((&[true],&[[1;16]],&[0]))),&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).len(),2,"absence is no implicit target delete row");
    let scope=owner.native_store.as_ref().unwrap().begin().await.unwrap();
    for prefix in [b"G".as_slice(),b"M",b"R",b"L"] { assert!(scope.first(prefix).await.unwrap().is_none(),"retired collection retains {prefix:?}"); }
    drop(scope);
    owner.process_batch(input(&ctx,&[Some("a")],&[Some(7)],&[7],Some((&[false],&[[2;16]],&[0]))),&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).last().unwrap(),&vec![Some(7)]);
}
#[tokio::test]
async fn native_result_array_output_refusal_is_before_live_commit() {
    let (mut owner, mut ctx) = array_owner(true).await;
    let mut output=AggregateCollector::default();
    owner.process_batch(input(&ctx,&[Some("a")],&[Some(1)],&[10],Some((&[false],&[[1;16]],&[0]))),&mut ctx,&mut output).await.unwrap();
    let before=correction_inventory(owner.native_store.as_ref().unwrap()).await;
    owner.native_config.as_mut().unwrap().max_pending_output_bytes=1;
    let error=owner.process_batch(input(&ctx,&[Some("a"),Some("a")],&[Some(1),Some(2)],&[5,5],Some((&[true,false],&[[1;16];2],&[0,0]))),&mut ctx,&mut output).await.unwrap_err();
    assert!(error.to_string().contains("pending-output"),"{error}");
    assert_eq!(correction_inventory(owner.native_store.as_ref().unwrap()).await,before);
    assert_eq!(items(&output),vec![vec![Some(1)]]);
}
