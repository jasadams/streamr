// Common operation/result preparation exercised through the real constructor.
use super::*;

fn array_config(corrections: bool) -> (UpdatingAggregateOperator, Arc<Registry>) {
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
    (UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap(), registry)
}
async fn array_owner(corrections: bool) -> (IncrementalAggregatingFunc, OperatorContext) {
    let (config, registry) = array_config(corrections);
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

async fn terminal_array_owner() -> (IncrementalAggregatingFunc, OperatorContext) {
    let (mut config, registry) = array_config(true);
    let accepted: ArroyoSchema = config.final_schema.take().unwrap().try_into().unwrap();
    let mut fields = accepted.schema.fields().to_vec(); fields.push(arroyo_rpc::updating_meta_field());
    config.final_schema = Some(ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new_with_metadata(fields, accepted.schema.metadata().clone()))).unwrap().into());
    config.accepted_output_schema = Some(accepted.clone().into());
    config.emit_per_event = false; config.response_root_operator = Some(7);
    let config = UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
    let mut ctx = native_event_context(&config).await; ctx.out_schema = Some(Arc::new(accepted));
    let mut owner = IncrementalAggregatingConstructor::build_with_native_config(config, registry, Some(native_test_config())).unwrap();
    owner.native_store = Some(native_test_store());
    (owner, ctx)
}
fn terminal_response(owner: &IncrementalAggregatingFunc, sequence: u64, time: i64, rows: Option<RecordBatch>) -> EventResponse {
    let keys = owner.key_converter.convert_columns(&[Arc::new(StringArray::from(vec![Some("a")]))]).unwrap();
    EventResponse { native_body: None, context: EventContext { version: EVENT_RESPONSE_VERSION,
        root_operator: 7, root_subtask: 0, attempt_nonce: [9;16], sequence, trigger_nanos: time }, rows,
        touched_keys: Some(RecordBatch::try_new(EventEdgeSpec::keys_schema(), vec![Arc::new(
            arrow_array::BinaryArray::from_iter_values([keys.row(0).as_ref()]))]).unwrap()),
    }
}
#[tokio::test]
async fn native_result_terminal_array_complete_replacement_unchanged_absence_and_clock() {
    let (mut owner, mut ctx) = terminal_array_owner().await;
    assert!(matches!(owner.event_response_role(), EventResponseRole::Result(7)));
    let mut output = AggregateCollector::default();
    let rows = input(&ctx, &[Some("a")], &[Some(3)], &[10], Some((&[false], &[[1;16]], &[0])));
    let response = terminal_response(&owner, 1, 10, Some(rows));
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    // A qualified event with unchanged native relation still has an accepted
    // result; this is neither SQL NULL nor absence and restores the late E clock.
    let response = terminal_response(&owner, 2, 5, None);
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    let rows = input(&ctx,&[Some("a"),Some("a")],&[Some(3),Some(1)],&[10,5],Some((&[true,false],&[[1;16];2],&[0,0])));
    let response = terminal_response(&owner,3,5,Some(rows));
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output),vec![vec![Some(3)],vec![Some(3)],vec![Some(1)]]);
    let clocks=output.batches.iter().map(|batch|ScalarValue::try_from_array(batch.column(3),0).unwrap()).collect::<Vec<_>>();
    assert_eq!(clocks,vec![10,5,5].into_iter().map(|time|ScalarValue::TimestampNanosecond(Some(time),None)).collect::<Vec<_>>());
    let rows=input(&ctx,&[Some("a")],&[Some(1)],&[5],Some((&[true],&[[1;16]],&[0])));
    let response=terminal_response(&owner,4,7,Some(rows));
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).len(),3,"last absence never manufactures target DELETE");
    let rows=input(&ctx,&[Some("a")],&[Some(7)],&[9],Some((&[false],&[[2;16]],&[0])));
    let response=terminal_response(&owner,5,9,Some(rows));
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).last().unwrap(),&vec![Some(7)]);
    owner.handle_tick(0,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output).len(),4);
}
#[tokio::test]
async fn native_result_terminal_refusal_precedes_commit_and_checks_key_layout() {
    let (mut owner, mut ctx)=terminal_array_owner().await;
    assert!(owner.bind_event_response_key_types(&[Some(vec![DataType::Int64])]).is_err());
    owner.bind_event_response_key_types(&[Some(vec![DataType::Utf8])]).unwrap();
    let rows=input(&ctx,&[Some("a")],&[Some(3)],&[10],Some((&[false],&[[1;16]],&[0])));
    let before=correction_inventory(owner.native_store.as_ref().unwrap()).await;
    owner.native_config.as_mut().unwrap().max_pending_output_bytes=1;
    let response=terminal_response(&owner,1,10,Some(rows));
    let mut output=AggregateCollector::default();
    let error=owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap_err();
    assert!(error.to_string().contains("pending-output") || error.to_string().contains("output allowance"),"{error}");
    assert_eq!(correction_inventory(owner.native_store.as_ref().unwrap()).await,before);
    assert!(owner.response_previous.is_none()); assert!(output.batches.is_empty());
}

#[tokio::test]
async fn native_result_terminal_admits_actual_fitting_complete_replacement() {
    let (mut owner, mut ctx) = terminal_array_owner().await;
    let mut limits = owner.native_store.as_ref().unwrap().limits();
    limits.write_bytes = 256 * 1024;
    limits.overlay_bytes = 256 * 1024;
    owner.native_store = Some(native_test_store_with_limits(limits));
    owner.native_config.as_mut().unwrap().write_bytes = limits.write_bytes;
    owner.native_config.as_mut().unwrap().overlay_bytes = limits.overlay_bytes;
    // One ordinary native image is admissible. Multiplying every configured
    // maximum by both images is not the actual bounded replacement cost.
    let worst = owner.native_operations_per_image().unwrap();
    assert!(worst * (limits.key_bytes + limits.value_bytes) <= limits.overlay_bytes);
    assert!(worst * owner.native_store.as_ref().unwrap().max_encoded_entry_bytes() <= limits.write_bytes);
    assert!((2 * worst + 2) * (limits.key_bytes + limits.value_bytes) > limits.overlay_bytes);
    let mut output = AggregateCollector::default();
    let seed = input(&ctx, &[Some("a")], &[Some(3)], &[10], Some((&[false], &[[1;16]], &[0])));
    let response = terminal_response(&owner, 1, 10, Some(seed));
    owner.process_response_index(0, 1, response, &mut ctx, &mut output).await.unwrap();
    let replacement = input(&ctx, &[Some("a");2], &[Some(3),Some(1)], &[10,5],
        Some((&[true,false], &[[1;16];2], &[0,0])));
    let response = terminal_response(&owner, 2, 5, Some(replacement));
    owner.process_response_index(0, 1, response, &mut ctx, &mut output).await.unwrap();
    assert_eq!(items(&output), vec![vec![Some(3)], vec![Some(1)]]);
    assert_eq!(owner.response_previous.unwrap().sequence, 2);
    assert_eq!(ScalarValue::try_from_array(output.batches[1].column(3),0).unwrap(),
        ScalarValue::TimestampNanosecond(Some(5),None));
}

#[tokio::test]
async fn native_result_terminal_actual_scope_refusal_preserves_collection_and_sequence() {
    let (mut owner, mut ctx) = terminal_array_owner().await;
    let mut limits = owner.native_store.as_ref().unwrap().limits();
    limits.write_operations = owner.native_operations_per_image().unwrap() + 2;
    owner.native_store = Some(native_test_store_with_limits(limits));
    owner.native_config.as_mut().unwrap().write_operations = limits.write_operations;
    let mut output = AggregateCollector::default();
    let seed = input(&ctx, &[Some("a")], &[Some(3)], &[10], Some((&[false], &[[1;16]], &[0])));
    let response = terminal_response(&owner, 1, 10, Some(seed));
    owner.process_response_index(0, 1, response, &mut ctx, &mut output).await.unwrap();
    async fn inventory(store: &AggregateStore) -> Vec<(Vec<u8>, Vec<u8>)> {
        let scope = store.begin().await.unwrap();
        let mut entries = Vec::new();
        // All families reachable in this indefinite, non-calendar owner:
        // group, member/identity indexes, ARRAY totals/history, dirty/expiry/
        // cleanup markers. Store scans require a nonempty namespace prefix.
        for prefix in [b"G", b"M", b"R", b"L", b"D", b"E", b"C"] {
            scope.visit_prefix(prefix, None, |key, value| {
                entries.push((key.to_vec(), value.to_vec()));
                Ok(())
            }).await.unwrap();
        }
        entries
    }
    let before = inventory(owner.native_store.as_ref().unwrap()).await;
    assert!(before.iter().any(|(key,_)| key.starts_with(b"L")), "fixture must retain ARRAY statistics");
    let replacement = input(&ctx, &[Some("a");2], &[Some(3),Some(1)], &[10,5],
        Some((&[true,false], &[[1;16];2], &[0,0])));
    let response = terminal_response(&owner, 2, 5, Some(replacement));
    let error = owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap_err();
    assert!(error.to_string().contains("BatchLimitExceeded"), "{error}");
    assert_eq!(inventory(owner.native_store.as_ref().unwrap()).await,before);
    assert_eq!(owner.response_previous.unwrap().sequence,1);
    assert_eq!(items(&output),vec![vec![Some(3)]]);
    // A subsequent qualified unchanged result reads the retained native ARRAY,
    // proving the failed scope did not advance history or completion state.
    let response = terminal_response(&owner,2,7,None);
    owner.process_response_index(0,1,response,&mut ctx,&mut output).await.unwrap();
    assert_eq!(items(&output),vec![vec![Some(3)],vec![Some(3)]]);
}
