use super::*;

fn config(changelog:bool,max_only:bool)->(UpdatingAggregateOperator,Arc<Registry>) {
    let (mut config,registry)=if changelog {native_correction_config()} else {native_event_config()};
    let mut registry=Arc::try_unwrap(registry).ok().expect("exclusive fixture registry");
    arroyo_planner::register_functions(&mut registry);
    let registry=Arc::new(registry);
    config.emit_per_event=false;config.response_root_operator=Some(7);
    let mut fields:Vec<_>=ArroyoSchema::try_from(config.final_schema.take().unwrap()).unwrap().schema.fields().to_vec();
    if max_only {
        let mut plan=PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate))=&mut plan.physical_plan_type else {unreachable!()};
        aggregate.aggr_expr.remove(0);aggregate.aggr_expr_name.remove(0);aggregate.filter_expr.remove(0);
        config.aggregate_exec=plan.encode_to_vec();fields.remove(1);
    }
    let value_schema=Arc::new(Schema::new(fields.clone()));
    fields.push(Arc::new(Field::new(UPDATING_META_FIELD,DataType::Struct(updating_meta_fields()),false)));
    config.final_schema=Some(ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields))).unwrap().into());
    let hash:Arc<dyn PhysicalExpr>=Arc::new(datafusion::physical_expr::ScalarFunctionExpr::try_new(
        registry.udf("multi_hash").unwrap(),vec![Arc::new(Column::new("value",0))],value_schema.as_ref()).unwrap());
    let literal=|value|Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>;
    let metadata:Arc<dyn PhysicalExpr>=Arc::new(datafusion::physical_expr::ScalarFunctionExpr::try_new(
        datafusion::functions::core::named_struct(),vec![
            literal(ScalarValue::Utf8(Some("is_retract".into()))),literal(ScalarValue::Boolean(Some(false))),
            literal(ScalarValue::Utf8(Some("id".into()))),hash,
        ],value_schema.as_ref()).unwrap());
    config.metadata_expr=serialize_physical_expr(&metadata,&DefaultPhysicalExtensionCodec{}).unwrap().encode_to_vec();
    (UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap(),registry)
}
async fn owner(changelog:bool,max_only:bool)->(IncrementalAggregatingFunc,OperatorContext) {
    let (config,registry)=config(changelog,max_only);
    let mut ctx=native_event_context(&config).await;
    ctx.out_schema=Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let mut operator=IncrementalAggregatingConstructor::build_with_native_config(config,registry,Some(native_test_config())).unwrap();
    operator.native_store=Some(native_test_store());
    (operator,ctx)
}
fn response(sequence:u64,time:i64,rows:Option<RecordBatch>)->EventResponse {
    EventResponse {native_body:None,context:EventContext {version:EVENT_RESPONSE_VERSION,root_operator:7,root_subtask:0,
        attempt_nonce:[9;16],sequence,trigger_nanos:time},rows,touched_keys:None}
}
#[derive(Default)]
struct Responses {packets:Vec<EventResponse>}
#[async_trait::async_trait]
impl Collector for Responses {
    async fn collect(&mut self,_:RecordBatch)->DataflowResult<()> {Err(anyhow!("response owner fabricated ordinary Data").into())}
    async fn collect_response(&mut self,response:EventResponse)->DataflowResult<()> {self.packets.push(response);Ok(())}
    async fn broadcast_watermark(&mut self,_:Watermark)->DataflowResult<()> {Ok(())}
}
fn touched(response:&EventResponse)->usize {response.touched_keys.as_ref().map_or(0,RecordBatch::num_rows)}
fn flags(response:&EventResponse)->Vec<bool> {
    response.rows.as_ref().map_or_else(Vec::new,|batch|native_changelog_columns(batch).unwrap().0.values().iter().collect())
}
async fn send(operator:&mut IncrementalAggregatingFunc,ctx:&mut OperatorContext,collector:&mut Responses,response:EventResponse) {
    operator.process_response_index(0,1,response,ctx,collector).await.unwrap();
}
#[test]
fn branch_response_aggregate_serialized_constructor_keeps_clock_and_mode_guards() {
    for changelog in [false,true] {for max_only in [false,true] {
        let (config,registry)=config(changelog,max_only);
        let owner=IncrementalAggregatingConstructor::build_with_native_config(config,registry,Some(native_test_config())).unwrap();
        assert_eq!(owner.response_root_operator,Some(7));assert!(!owner.emit_per_event);
    }}
    for nullable in [true,false] {
        let (mut bad,registry)=config(true,false);
        let out:ArroyoSchema=bad.final_schema.take().unwrap().try_into().unwrap();
        let mut fields=out.schema.fields().to_vec();let last=fields.len()-1;
        fields[last]=Arc::new(if nullable {fields[last].as_ref().clone().with_nullable(true)} else {
            fields[last].as_ref().clone().with_metadata(HashMap::from([("forged".into(),"metadata".into())]))
        });
        bad.final_schema=Some(ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields))).unwrap().into());
        let error=IncrementalAggregatingConstructor::build_with_native_config(bad,registry,Some(native_test_config())).err().unwrap();
        assert!(error.to_string().contains("verified native changelog output"),"{error}");
    }
    let (mut bad,registry)=config(true,false);bad.emit_per_event=true;
    assert!(IncrementalAggregatingConstructor::build_with_native_config(bad,registry,Some(native_test_config())).err().unwrap().to_string().contains("branch response aggregate requires"));
    let (mut bad,registry)=config(true,false);
    let mut plan=PhysicalPlanNode::decode(bad.aggregate_exec.as_slice()).unwrap();
    let Some(PhysicalPlanType::Aggregate(aggregate))=&mut plan.physical_plan_type else {unreachable!()};
    aggregate.aggr_expr[2]=aggregate.aggr_expr[1].clone();bad.aggregate_exec=plan.encode_to_vec();
    assert!(IncrementalAggregatingConstructor::build_with_native_config(bad,registry,Some(native_test_config())).err().unwrap().to_string().contains("timestamp"));
}
#[tokio::test]
async fn branch_response_aggregate_first_empty_unchanged_and_ticks_complete_once() {
    let (mut operator,mut ctx)=owner(false,true).await;let mut collector=Responses::default();
    send(&mut operator,&mut ctx,&mut collector,response(1,5,None)).await;
    send(&mut operator,&mut ctx,&mut collector,response(2,10,Some(batch_at(&[Some("a")],&[1],&[10])))).await;
    send(&mut operator,&mut ctx,&mut collector,response(3,5,Some(batch_at(&[Some("a")],&[2],&[5])))).await;
    assert_eq!(collector.packets.len(),3);
    assert_eq!(touched(&collector.packets[0]),0);assert!(collector.packets[0].rows.is_none());
    assert_eq!(flags(&collector.packets[1]),vec![false]);assert_eq!(touched(&collector.packets[1]),1);
    assert!(collector.packets[2].rows.is_none());assert_eq!(touched(&collector.packets[2]),1);
    assert_eq!(collector.packets[2].context.trigger_nanos,5);
    assert_eq!(operator.tick_interval(),None);
    let before=correction_inventory(operator.native_store.as_ref().unwrap()).await;
    operator.handle_tick(1,&mut ctx,&mut collector).await.unwrap();
    operator.handle_checkpoint(CheckpointBarrier {epoch:1,min_epoch:0,timestamp:SystemTime::UNIX_EPOCH,then_stop:false},&mut ctx,&mut collector).await.unwrap();
    operator.on_close(&Some(SignalMessage::EndOfData),&mut ctx,&mut collector).await.unwrap();
    assert_eq!(collector.packets.len(),3);assert_eq!(correction_inventory(operator.native_store.as_ref().unwrap()).await,before);
    // No dirty emission survives the response; committed G persists its last
    // emitted native relation even though the newest E had no changed row.
    let scope=operator.native_store.as_ref().unwrap().begin().await.unwrap();
    assert!(scope.first_from(b"D",None).await.unwrap().is_none());
    let (_,bytes)=scope.first_from(b"G",None).await.unwrap().unwrap();
    let group=operator.native_group_codec().unwrap().decode(&bytes,scope.limits().value_bytes).unwrap();
    assert_eq!(group.live_rows,2);assert_eq!(group.last_emitted.unwrap().last(),Some(&ScalarValue::TimestampNanosecond(Some(10),None)));
}
#[tokio::test]
async fn branch_response_aggregate_cdc_atomic_move_delete_and_duplicate_refusal() {
    let (mut operator,mut ctx)=owner(true,false).await;let mut collector=Responses::default();
    let change=|groups:&[Option<&str>],values:&[i64],time,retracts:&[bool],ids:&[[u8;16]]| {
        correction_batch(groups,values,&vec![time;groups.len()],retracts,ids,&vec![0;groups.len()])
    };
    send(&mut operator,&mut ctx,&mut collector,response(1,10,Some(change(&[Some("a")],&[1],10,&[false],&[[1;16]])))).await;
    send(&mut operator,&mut ctx,&mut collector,response(2,5,Some(change(&[Some("a"),Some("a")],&[1,1],5,&[true,false],&[[1;16],[1;16]])))).await;
    assert!(collector.packets[1].rows.is_none());assert_eq!(touched(&collector.packets[1]),1);
    send(&mut operator,&mut ctx,&mut collector,response(3,3,Some(change(&[Some("a"),Some("b")],&[1,2],3,&[true,false],&[[1;16],[2;16]])))).await;
    assert_eq!(touched(&collector.packets[2]),2);assert_eq!(flags(&collector.packets[2]),vec![true,false]);
    let moved=collector.packets[2].rows.as_ref().unwrap();
    assert_eq!(moved.column(0).as_string::<i32>().iter().collect::<Vec<_>>(),vec![Some("a"),Some("b")]);
    assert_eq!(moved.column(1).as_primitive::<arrow_array::types::Int64Type>().values().as_ref(),&[1,1]);
    let before=correction_inventory(operator.native_store.as_ref().unwrap()).await;
    let error=operator.process_response_index(0,1,response(3,3,None),&mut ctx,&mut collector).await.unwrap_err();
    assert!(error.to_string().contains("skips or repeats"));
    assert_eq!(correction_inventory(operator.native_store.as_ref().unwrap()).await,before);
    send(&mut operator,&mut ctx,&mut collector,response(4,7,Some(change(&[Some("b")],&[2],7,&[true],&[[2;16]])))).await;
    assert_eq!(flags(&collector.packets[3]),vec![true]);assert_eq!(touched(&collector.packets[3]),1);
    assert!(correction_inventory(operator.native_store.as_ref().unwrap()).await.is_empty());
    send(&mut operator,&mut ctx,&mut collector,response(5,9,Some(change(&[Some("b")],&[3],9,&[false],&[[3;16]])))).await;
    assert_eq!(flags(&collector.packets[4]),vec![false]);
    assert_eq!(collector.packets.len(),5);
}
#[tokio::test]
async fn branch_response_aggregate_filter_false_still_touches_and_keeps_sql_null() {
    let (mut config,registry)=config(false,false);
    let mut plan=PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
    let Some(PhysicalPlanType::Aggregate(aggregate))=&mut plan.physical_plan_type else {unreachable!()};
    let include:Arc<dyn PhysicalExpr>=Arc::new(Column::new("include",2));
    let predicate=serialize_physical_expr(&include,&DefaultPhysicalExtensionCodec{}).unwrap();
    for index in [0,1] {aggregate.filter_expr[index]=MaybeFilter {expr:Some(predicate.clone())};}
    config.aggregate_exec=plan.encode_to_vec();
    let mut ctx=native_event_context(&config).await;ctx.out_schema=Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let mut operator=IncrementalAggregatingConstructor::build_with_native_config(config,registry,Some(native_test_config())).unwrap();
    operator.native_store=Some(native_test_store());let mut collector=Responses::default();
    let original=batch_at(&[Some("a")],&[1],&[10]);let mut columns=original.columns().to_vec();columns[2]=Arc::new(BooleanArray::from(vec![false]));
    let batch=RecordBatch::try_new(original.schema(),columns).unwrap();
    send(&mut operator,&mut ctx,&mut collector,response(1,10,Some(batch))).await;
    assert_eq!(touched(&collector.packets[0]),1);assert_eq!(flags(&collector.packets[0]),vec![false]);
    let output=collector.packets[0].rows.as_ref().unwrap();
    assert_eq!(ScalarValue::try_from_array(output.column(1),0).unwrap(),ScalarValue::Int64(Some(0)));
    assert_eq!(ScalarValue::try_from_array(output.column(2),0).unwrap(),ScalarValue::Utf8(None));
}
#[tokio::test]
async fn branch_response_aggregate_whole_move_output_refusal_leaves_both_keys_unchanged() {
    let (mut operator,mut ctx)=owner(true,false).await;let mut collector=Responses::default();
    operator.native_config.as_mut().unwrap().max_pending_output_rows=2;
    for (sequence,key,id,time) in [(1,"a",[1;16],10),(2,"b",[2;16],20)] {
        let batch=correction_batch(&[Some(key)],&[sequence as i64],&[time],&[false],&[id],&[0]);
        send(&mut operator,&mut ctx,&mut collector,response(sequence,time,Some(batch))).await;
    }
    let store=operator.native_store.as_ref().unwrap();
    let before=correction_inventory(store).await;
    let move_batch=correction_batch(&[Some("a"),Some("b")],&[1,1],&[5,5],&[true,false],&[[1;16],[1;16]],&[0,0]);
    let error=operator.process_response_index(0,1,response(3,5,Some(move_batch)),&mut ctx,&mut collector).await.unwrap_err();
    assert!(error.to_string().contains("pending-output row allowance"),"{error}");
    assert_eq!(correction_inventory(operator.native_store.as_ref().unwrap()).await,before);
    assert_eq!(operator.response_previous.unwrap().sequence,2);
    assert_eq!(collector.packets.len(),2);
}

fn resource_used(registry:&prometheus::Registry,resource:&str)->i64 {
    registry.gather().into_iter().find(|family|family.name()=="arroyo_live_state_resources").unwrap()
        .get_metric().iter().find(|metric| {
            metric.get_label().iter().any(|label|label.name()=="resource" && label.value()==resource)
                && metric.get_label().iter().any(|label|label.name()=="measurement" && label.value()=="used")
        }).unwrap().get_gauge().value() as i64
}
#[tokio::test]
async fn branch_response_aggregate_output_guard_lives_through_success_error_and_cancel() {
    for outcome in [0,1,2] {
        let (mut operator,mut ctx)=owner(false,false).await;
        let resources=operator.native_store.as_ref().unwrap().resources().clone();
        let registry=prometheus::Registry::new();resources.register_metrics(&registry).unwrap();
        let (entered,entered_rx)=tokio::sync::oneshot::channel();
        let (release,release_rx)=tokio::sync::oneshot::channel();
        struct Blocked {entered:Option<tokio::sync::oneshot::Sender<()>>,release:Option<tokio::sync::oneshot::Receiver<bool>>}
        #[async_trait::async_trait]
        impl Collector for Blocked {
            async fn collect(&mut self,_:RecordBatch)->DataflowResult<()> {panic!("ordinary Data")}
            async fn collect_response(&mut self,response:EventResponse)->DataflowResult<()> {
                self.entered.take().unwrap().send(()).unwrap();
                let fail=self.release.take().unwrap().await.unwrap();
                assert!(response.rows.is_some());drop(response);
                if fail {Err(arroyo_rpc::errors::DataflowError::DataError {details:"typed delivery failure".into(),count:7})} else {Ok(())}
            }
            async fn broadcast_watermark(&mut self,_:Watermark)->DataflowResult<()> {Ok(())}
        }
        let mut collector=Blocked {entered:Some(entered),release:Some(release_rx)};
        let requested=operator.native_config.unwrap().max_pending_output_bytes*3;
        let mut call=Box::pin(operator.process_response_index(0,1,response(1,10,Some(batch_at(&[Some("a")],&[1],&[10]))),&mut ctx,&mut collector));
        tokio::select! {result=&mut call=>panic!("delivery returned before release: {result:?}"),entered=entered_rx=>entered.unwrap()}
        assert_eq!(resource_used(&registry,"decoded_value_bytes"),requested as i64);
        assert_eq!(resource_used(&registry,"snapshots"),0);
        if outcome==2 {drop(call);drop(release);} else {
            release.send(outcome==1).unwrap();
            let result=call.await;
            if outcome==1 {assert!(matches!(result,Err(arroyo_rpc::errors::DataflowError::DataError {count:7,ref details}) if details=="typed delivery failure"));}
            else {result.unwrap();}
        }
        assert_eq!(resource_used(&registry,"decoded_value_bytes"),0);
        assert_eq!(resource_used(&registry,"snapshots"),0);
    }
}

#[tokio::test]
async fn branch_response_aggregate_binding_preserves_all_serialized_schema_metadata() {
    let (mut operator,ctx)=owner(false,false).await;
    let raw=Arc::new(EventEdgeSpec {
            maintenance: false,root_operator:7,root_subtask:0,schema:ctx.in_schemas[0].schema.clone(),layout:EventLayout::Raw});
    let output=operator.bind_event_response(std::slice::from_ref(&raw),&ctx).unwrap().unwrap();
    assert_eq!(output.schema,ctx.out_schema.as_ref().unwrap().schema);
    for output_side in [false,true] {for kind in [0,1,2] {
        let (mut operator,mut ctx)=owner(false,false).await;
        let original=if output_side {ctx.out_schema.as_ref().unwrap().as_ref()} else {ctx.in_schemas[0].as_ref()};
        let mut serialized:arroyo_rpc::grpc::api::ArroyoSchema=original.clone().into();
        match kind {
            0=>{serialized.has_keys = !serialized.has_keys;serialized.key_indices=vec![0];},
            1=>{serialized.has_routing_keys = !serialized.has_routing_keys;serialized.routing_key_indices=vec![0];},
            2=>{serialized.timestamp_index=(serialized.timestamp_index+1)%original.schema.fields().len() as u32;},
            _=>unreachable!(),
        }
        let forged:ArroyoSchema=serialized.try_into().unwrap();
        assert_eq!(forged.schema,original.schema,"only Arroyo key/routing/clock metadata changed");assert_ne!(&forged,original);
        let raw=Arc::new(EventEdgeSpec {
            maintenance: false,root_operator:7,root_subtask:0,schema:ctx.in_schemas[0].schema.clone(),layout:EventLayout::Raw});
        if output_side {ctx.out_schema=Some(Arc::new(forged));} else {ctx.in_schemas[0]=Arc::new(forged);}
        let error=operator.bind_event_response(std::slice::from_ref(&raw),&ctx).unwrap_err();
        assert!(error.to_string().contains("graph schema differs from actual constructed native owner"),"{error}");
    }}
}


#[tokio::test]
async fn finalized_append_branch_keeps_empty_completions_and_complete_native_replacements() {
    let (mut config, registry) = config(false, false);
    config.response_finalized_append_input = true;
    let mut ctx = native_event_context(&config).await;
    ctx.out_schema = Some(Arc::new(config.final_schema.clone().unwrap().try_into().unwrap()));
    let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
        config, registry, Some(native_test_config())).unwrap();
    operator.native_store = Some(native_test_store());
    let input = Arc::new(EventEdgeSpec {
        root_operator: 7, root_subtask: 0, schema: ctx.in_schemas[0].schema.clone(),
        layout: EventLayout::Raw, maintenance: true,
    });
    let output = operator.bind_event_response(&[input], &ctx).unwrap().unwrap();
    assert!(output.maintenance);
    let mut responses = Responses::default();
    send(&mut operator, &mut ctx, &mut responses, response(1, 100, None)).await;
    assert!(correction_inventory(operator.native_store.as_ref().unwrap()).await.is_empty());
    let mut collector = AggregateCollector::default();
    for (value, time) in [(4, 10), (2, 20)] {
        operator.process_batch(batch_at(&[Some("a")], &[value], &[time]),
            &mut ctx, &mut collector).await.unwrap();
    }
    assert_eq!(collector.batches.len(), 2);
    assert_eq!(native_changelog_columns(&collector.batches[0]).unwrap().0.values().iter().collect::<Vec<_>>(), vec![false]);
    let replacement = &collector.batches[1];
    assert_eq!(native_changelog_columns(replacement).unwrap().0.values().iter().collect::<Vec<_>>(), vec![true, false]);
    assert_eq!(replacement.column(1).as_primitive::<arrow_array::types::Int64Type>().values().as_ref(), &[1, 2]);
    assert_eq!(replacement.column(ctx.out_schema.as_ref().unwrap().timestamp_index)
        .as_primitive::<arrow_array::types::TimestampNanosecondType>().values().as_ref(), &[10, 20]);
    send(&mut operator, &mut ctx, &mut responses, response(2, 5, None)).await;
    assert_eq!(responses.packets.len(), 2);
    assert!(responses.packets.iter().all(|packet| packet.rows.is_none() && packet.touched_keys.is_none()));
    // A finalized result is maintenance, never another raw event payload.
    let before = correction_inventory(operator.native_store.as_ref().unwrap()).await;
    assert!(operator.process_response_index(0, 1,
        response(3, 20, Some(batch_at(&[Some("a")], &[9], &[20]))),
        &mut ctx, &mut responses).await.unwrap_err().to_string().contains("empty original-event"));
    assert!(operator.process_batch(batch_at(&[Some("a"), Some("a")], &[1, 2], &[30, 31]),
        &mut ctx, &mut collector).await.is_err());
    assert_eq!(correction_inventory(operator.native_store.as_ref().unwrap()).await, before);
}

#[test]
fn finalized_append_branch_rejects_changelog_and_missing_response_root() {
    for changelog in [false, true] {
        let (mut config, registry) = config(changelog, false);
        config.response_finalized_append_input = true;
        if !changelog { config.response_root_operator = None; }
        assert!(IncrementalAggregatingConstructor::build_with_native_config(
            config, registry, Some(native_test_config())).is_err());
    }
}
