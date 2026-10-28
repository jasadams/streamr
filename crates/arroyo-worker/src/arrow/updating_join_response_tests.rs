use super::*;
use arroyo_types::event_response::{EventContext,EVENT_RESPONSE_VERSION};

async fn response_config(left_outer:bool)->JoinOperator {
    let kind=if left_outer {"LEFT"} else {"INNER"};
    let sql=format!("CREATE TABLE events(k BIGINT,amount BIGINT,active BOOLEAN) WITH(connector='single_file',path='/tmp/event-join-proof.jsonl',format='json',type='source');
        WITH l AS(SELECT k,COUNT(*) AS lv,MAX(amount) AS lmax FROM events GROUP BY k),
             r AS(SELECT k,COUNT(*) AS rv,MAX(amount) AS rmax FROM events WHERE active GROUP BY k)
        SELECT l.k,l.lv,r.rv,l.lmax,r.rmax FROM l {kind} JOIN r ON l.k=r.k");
    let compiled=arroyo_planner::parse_and_get_program(&sql,arroyo_planner::ArroyoSchemaProvider::new(),
        arroyo_planner::SqlConfig {default_parallelism:1}).await.unwrap();
    let op=compiled.program.graph.node_weights().flat_map(|node|node.operator_chain.iter()).map(|(op,_)|op)
        .find(|op|op.operator_name==arroyo_datastream::logical::OperatorName::Join).unwrap();
    let mut config=JoinOperator::decode(op.operator_config.as_slice()).unwrap();
    let native:ArroyoSchema=config.output_schema.clone().unwrap().try_into().unwrap();
    let mut fields=native.schema.fields().to_vec();fields.remove(native.schema.index_of(UPDATING_META_FIELD).unwrap());
    let accepted=ArroyoSchema::from_schema_unkeyed(Arc::new(arrow_schema::Schema::new_with_metadata(fields,native.schema.metadata().clone()))).unwrap();
    config.response_root_operator=Some(7);config.accepted_output_schema=Some(accepted.into());
    JoinOperator::decode(config.encode_to_vec().as_slice()).unwrap()
}
async fn owner(left_outer:bool)->(UpdatingJoin,OperatorContext) {
    let config=response_config(left_outer).await;
    let accepted:ArroyoSchema=config.accepted_output_schema.clone().unwrap().try_into().unwrap();
    let op=operator_from_config(config);
    let (control,_rx)=tokio::sync::mpsc::channel(16);
    let ctx=OperatorContext::new(Arc::new(arroyo_types::get_test_task_info()),None,control,2,
        vec![Arc::new(op.schemas[0].clone()),Arc::new(op.schemas[1].clone())],Some(Arc::new(accepted)),HashMap::new()).await;
    (op,ctx)
}
#[derive(Clone,Copy)]
struct Image {id:u8,key:Option<i64>,count:i64,winner:i64,stamp:i64,retract:bool}
fn image(op:&UpdatingJoin,side:usize,x:Image)->RecordBatch {
    let schema=op.schemas[side].schema.clone();
    let columns=schema.fields().iter().map(|field|->Arc<dyn Array> {match field.data_type() {
        DataType::Int64=>Arc::new(Int64Array::from(vec![if field.name()=="k" || field.name().starts_with("_key_") {x.key}
            else if field.name()=="lv" || field.name()=="rv" {Some(x.count)} else {Some(x.winner)}])),
        DataType::Timestamp(_,_)=>Arc::new(TimestampNanosecondArray::from(vec![x.stamp])),
        DataType::Struct(_)=>Arc::new(StructArray::new(updating_meta_fields(),vec![Arc::new(BooleanArray::from(vec![x.retract])),
            Arc::new(FixedSizeBinaryArray::try_from_iter([&[x.id;16][..]].into_iter()).unwrap())],None)),
        other=>panic!("unexpected native COUNT/MAX field {other:?}"),
    }}).collect();
    RecordBatch::try_new(schema,columns).unwrap()
}
fn packet(op:&UpdatingJoin,side:usize,sequence:u64,trigger:i64,images:&[Image],keys:&[Option<i64>])->EventResponse {
    let rows=if images.is_empty() {None} else {Some(arrow::compute::concat_batches(&op.schemas[side].schema,
        images.iter().map(|image_spec|image(op,side,*image_spec)).collect::<Vec<_>>().iter()).unwrap())};
    assert_eq!(op.schemas[side].storage_keys().unwrap().len(),1);
    let touched=if keys.is_empty() {None} else {
        let rows=op.converters[side].convert_columns(&[Arc::new(Int64Array::from(keys.to_vec()))]).unwrap();
        let values=rows.iter().map(|row|row.as_ref().to_vec()).collect::<Vec<_>>();
        let slices=values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        Some(RecordBatch::try_new(EventEdgeSpec::keys_schema(),vec![Arc::new(arrow_array::BinaryArray::from_vec(slices))]).unwrap())
    };
    EventResponse {native_body:None,context:EventContext {version:EVENT_RESPONSE_VERSION,root_operator:7,root_subtask:0,
        attempt_nonce:[9;16],sequence,trigger_nanos:trigger},rows,touched_keys:touched}
}
async fn pair(op:&mut UpdatingJoin,ctx:&mut OperatorContext,out:&mut Capture,left:EventResponse,right:EventResponse,first:usize) {
    let packets=if first==0 {[left,right]} else {[right,left]};
    let [a,b]=packets;
    op.process_response_index(first,2,a,ctx,out).await.unwrap();
    assert!(op.event_input_blocked(first));assert!(!op.event_input_blocked(1-first));
    op.process_response_index(1-first,2,b,ctx,out).await.unwrap();
    assert!(!op.event_input_blocked(0) && !op.event_input_blocked(1));
}
fn ints(batch:&RecordBatch,name:&str)->Vec<Option<i64>> {
    batch.column(batch.schema().index_of(name).unwrap()).as_any().downcast_ref::<Int64Array>().unwrap().iter().collect()
}
fn x(id:u8,key:Option<i64>,count:i64,winner:i64,stamp:i64)->Image {Image {id,key,count,winner,stamp,retract:false}}
#[tokio::test]
async fn branch_response_join_serialized_constructor_rejects_forged_accepted_boundary() {
    let config=response_config(true).await;
    let op=operator_from_config(config.clone());assert!(op.event_pair_mode());
    let mut bad=config.clone();
    bad.output_schema.as_mut().unwrap().timestamp_index=u32::MAX;
    bad.accepted_output_schema.as_mut().unwrap().timestamp_index=u32::MAX-1;
    let bad=JoinOperator::decode(bad.encode_to_vec().as_slice()).unwrap();
    let error=match build(bad,Arc::new(arroyo_planner::physical::new_registry()),op.limits) {
        Err(error)=>error,Ok(_)=>panic!("forged serialized clock ordinal must return Err"),
    };
    assert!(error.to_string().contains("timestamp ordinal outside configured schema"),"{error}");
    let mut bad=config.clone();bad.accepted_output_schema=None;
    assert!(build(bad,Arc::new(arroyo_planner::physical::new_registry()),op.limits).err().unwrap().to_string().contains("configured together"));
    let mut bad=config.clone();bad.accepted_output_schema=bad.output_schema.clone();
    assert!(build(bad,Arc::new(arroyo_planner::physical::new_registry()),op.limits).err().unwrap().to_string().contains("remove only native metadata"));
    let mut bad=config;let native:ArroyoSchema=bad.output_schema.take().unwrap().try_into().unwrap();
    let mut fields=native.schema.fields().to_vec();let meta=native.schema.index_of(UPDATING_META_FIELD).unwrap();
    fields[meta]=Arc::new(fields[meta].as_ref().clone().with_nullable(true));
    bad.output_schema=Some(ArroyoSchema::from_schema_unkeyed(Arc::new(arrow_schema::Schema::new(fields))).unwrap().into());
    assert!(build(bad,Arc::new(arroyo_planner::physical::new_registry()),op.limits).err().unwrap().to_string().contains("canonical native output metadata"));
}
#[tokio::test]
async fn branch_response_join_first_absence_where_zero_and_qualified_unchanged_late_clock() {
    let (mut op,mut ctx)=owner(true).await;let mut out=Capture::default();
    let left=packet(&op,0,1,10,&[],&[]);let right=packet(&op,1,1,10,&[x(2,Some(1),1,20,20)],&[Some(1)]);
    pair(&mut op,&mut ctx,&mut out,left,right,1).await;assert!(out.0.is_empty());
    let left=packet(&op,0,2,20,&[x(1,Some(1),1,10,10)],&[Some(1)]);let right=packet(&op,1,2,20,&[],&[]);
    pair(&mut op,&mut ctx,&mut out,left,right,0).await;assert_eq!(out.0.len(),1);
    assert_eq!(ints(&out.0[0],"lv"),vec![Some(1)]);assert_eq!(ints(&out.0[0],"rv"),vec![Some(1)]);
    let left=packet(&op,0,3,5,&[],&[Some(1)]);let right=packet(&op,1,3,5,&[],&[Some(1)]);
    pair(&mut op,&mut ctx,&mut out,left,right,1).await;assert_eq!(out.0.len(),2);
    assert_eq!(ints(&out.0[1],"lmax"),vec![Some(10)]);assert_eq!(ints(&out.0[1],"rmax"),vec![Some(20)]);
    assert!(out.0[1].schema().index_of(UPDATING_META_FIELD).is_err());
    let clock=ctx.out_schema.as_ref().unwrap().timestamp_index;
    assert_eq!(out.0[1].column(clock).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0),5);
    let scope=op.store.as_ref().unwrap().begin().await.unwrap();
    let key=op.encoded_row_key(1,&image(&op,1,x(2,Some(1),1,20,20))).unwrap();
    let stored=op.final_native_row(&scope,1,&key).await.unwrap().unwrap();
    assert_eq!(stored.column(op.schemas[1].timestamp_index).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0),20);
    drop(scope);
    let left=packet(&op,0,4,30,&[],&[]);let right=packet(&op,1,4,30,&[],&[]);
    pair(&mut op,&mut ctx,&mut out,left,right,0).await;assert_eq!(out.0.len(),2);
}
#[tokio::test]
async fn branch_response_join_native_left_inner_null_key_semantics() {
    for outer in [false,true] {
        let (mut op,mut ctx)=owner(outer).await;let mut out=Capture::default();
        let left=packet(&op,0,1,10,&[x(1,None,1,10,10)],&[None]);
        let right=packet(&op,1,1,10,&[x(2,None,1,20,10)],&[None]);
        pair(&mut op,&mut ctx,&mut out,left,right,0).await;
        if outer {assert_eq!(out.0.len(),1);assert_eq!(ints(&out.0[0],"rv"),vec![None]);}
        else {assert!(out.0.is_empty());}
        let left=packet(&op,0,2,20,&[x(3,Some(1),1,30,20)],&[Some(1)]);
        let right=packet(&op,1,2,20,&[],&[]);
        let prior=out.0.len();pair(&mut op,&mut ctx,&mut out,left,right,1).await;
        assert_eq!(out.0.len(),prior+usize::from(outer));
        if outer {assert_eq!(ints(out.0.last().unwrap(),"rv"),vec![None]);}
    }
}
#[tokio::test]
async fn branch_response_join_native_cdc_move_and_last_absence_do_not_fabricate_delete() {
    let (mut op,mut ctx)=owner(true).await;let mut out=Capture::default();
    let left=packet(&op,0,1,10,&[x(1,Some(1),1,10,10)],&[Some(1)]);
    let right=packet(&op,1,1,10,&[x(2,Some(1),1,20,10)],&[Some(1)]);
    pair(&mut op,&mut ctx,&mut out,left,right,0).await;
    let mut before=x(1,Some(1),1,10,10);before.retract=true;
    let after=x(3,Some(2),1,5,5);
    let left=packet(&op,0,2,5,&[before,after],&[Some(1),Some(2)]);let right=packet(&op,1,2,5,&[],&[]);
    pair(&mut op,&mut ctx,&mut out,left,right,1).await;
    assert_eq!(out.0.len(),2);assert_eq!(ints(&out.0[1],"lmax"),vec![Some(5)]);assert_eq!(ints(&out.0[1],"rv"),vec![None]);
    let mut deletion=after;deletion.retract=true;
    let left=packet(&op,0,3,7,&[deletion],&[Some(2)]);let right=packet(&op,1,3,7,&[],&[]);
    pair(&mut op,&mut ctx,&mut out,left,right,0).await;
    assert_eq!(out.0.len(),2);assert!(!op.response.as_ref().unwrap().pair.pending());
}
#[tokio::test]
async fn branch_response_join_native_duplicate_full_key_refuses_final_probe() {
    let (mut op,mut ctx)=owner(true).await;
    for id in [1,2] {op.change(0,image(&op,0,x(id,Some(1),1,10,10)),&mut Discard).await.unwrap();}
    let mut out=Capture::default();let left=packet(&op,0,1,10,&[],&[Some(1)]);let right=packet(&op,1,1,10,&[],&[]);
    op.process_response_index(0,2,left,&mut ctx,&mut out).await.unwrap();
    let error=op.process_response_index(1,2,right,&mut ctx,&mut out).await.unwrap_err();
    assert!(error.to_string().contains("full key is not unique"),"{error}");assert!(out.0.is_empty());
    assert!(op.event_input_blocked(0) && op.event_input_blocked(1));
}
#[tokio::test]
async fn branch_response_join_actual_pair_hooks_allow_watermark_skew_and_compose_checkpoint() {
    let (mut op,mut ctx)=owner(true).await;let mut out=Capture::default();let mut counter=CheckpointCounter::new(2);
    let closed=HashSet::new();let watermark=SignalMessage::Watermark(arroyo_types::Watermark::Idle);
    let barrier=CheckpointBarrier {
        completed_base_epoch: None,epoch:1,min_epoch:0,timestamp:std::time::UNIX_EPOCH,then_stop:false};
    op.check_event_control_input(0,&watermark,&counter,&closed).unwrap();
    let left=packet(&op,0,1,5,&[],&[]);op.process_response_index(0,2,left,&mut ctx,&mut out).await.unwrap();
    op.check_event_control_input(1,&watermark,&counter,&closed).unwrap();
    assert!(op.check_event_control_input(1,&SignalMessage::Barrier(barrier),&counter,&closed).is_err());
    assert!(op.handle_checkpoint(barrier,&mut ctx,&mut out).await.is_err());
    let right=packet(&op,1,1,5,&[],&[]);op.check_event_response_input(1,&counter,&closed).unwrap();
    op.process_response_index(1,2,right,&mut ctx,&mut out).await.unwrap();
    op.handle_checkpoint(barrier,&mut ctx,&mut out).await.unwrap();assert!(out.0.is_empty());
    op.check_event_control_input(0,&SignalMessage::Barrier(barrier),&counter,&closed).unwrap();assert!(!counter.mark(0,&barrier));
    assert!(op.check_event_response_input(1,&counter,&closed).is_err());
    op.check_event_control_input(1,&watermark,&counter,&closed).unwrap();
    op.check_event_control_input(1,&SignalMessage::Barrier(barrier),&counter,&closed).unwrap();assert!(counter.mark(1,&barrier));
    op.check_event_response_input(0,&counter,&closed).unwrap();
    let closed=HashSet::from([0]);assert!(op.check_event_response_input(1,&counter,&closed).is_err());
    assert!(op.check_event_control_input(1,&SignalMessage::Barrier(barrier),&counter,&closed).is_err());
}
fn resource_used(registry:&prometheus::Registry,resource:&str)->i64 {
    registry.gather().into_iter().find(|family|family.name()=="arroyo_live_state_resources").unwrap().get_metric().iter()
        .find(|metric|metric.get_label().iter().any(|label|label.name()=="resource" && label.value()==resource)
            && metric.get_label().iter().any(|label|label.name()=="measurement" && label.value()=="used"))
        .unwrap().get_gauge().value() as i64
}
#[tokio::test]
async fn branch_response_join_public_block_drops_snapshots_retains_output_and_parks_both_inputs() {
    for outcome in [0,1,2] {
        let (mut op,mut ctx)=owner(true).await;
        let resources=op.store.as_ref().unwrap().resources().clone();let registry=prometheus::Registry::new();resources.register_metrics(&registry).unwrap();
        // Memory snapshots own no scan-page permit. Seed and exercise the
        // existing gauge explicitly; absence must not be mistaken for zero.
        let scan_probe=resources.try_scan_page(1).unwrap();
        assert_eq!(resource_used(&registry,"scan_page_bytes"),1);
        drop(scan_probe);assert_eq!(resource_used(&registry,"scan_page_bytes"),0);
        let passers=op.passers.clone();let mut out=Capture::default();
        let left=packet(&op,0,1,5,&[x(1,Some(1),1,10,20)],&[Some(1)]);let right=packet(&op,1,1,5,&[x(2,Some(1),1,20,20)],&[Some(1)]);
        op.process_response_index(0,2,left,&mut ctx,&mut out).await.unwrap();
        let (entered,entered_rx)=tokio::sync::oneshot::channel();let (release,release_rx)=tokio::sync::oneshot::channel();
        struct Blocked {entered:Option<tokio::sync::oneshot::Sender<()>>,release:Option<tokio::sync::oneshot::Receiver<bool>>}
        #[async_trait::async_trait]
        impl Collector for Blocked {
            async fn collect(&mut self,batch:RecordBatch)->DataflowResult<()> {
                self.entered.take().unwrap().send(()).unwrap();let fail=self.release.take().unwrap().await.unwrap();
                assert_eq!(batch.num_rows(),1);drop(batch);
                if fail {Err(arroyo_rpc::errors::DataflowError::DataError {details:"typed public delivery failure".into(),count:7})} else {Ok(())}
            }
            async fn broadcast_watermark(&mut self,_:arroyo_types::Watermark)->DataflowResult<()> {Ok(())}
        }
        let mut blocked=Blocked {entered:Some(entered),release:Some(release_rx)};let requested=op.limits.max_pending_output_bytes*3;
        let mut call=Box::pin(op.process_response_index(1,2,right,&mut ctx,&mut blocked));
        tokio::select! {result=&mut call=>panic!("public delivery returned early: {result:?}"),entered=entered_rx=>entered.unwrap()}
        assert_eq!(resource_used(&registry,"snapshots"),0);assert_eq!(resource_used(&registry,"scan_page_bytes"),0);
        assert_eq!(resource_used(&registry,"decoded_value_bytes"),requested as i64);
        assert!(passers.iter().all(|passer|passer.read().unwrap().is_none()));
        if outcome==2 {drop(call);drop(release);} else {
            release.send(outcome==1).unwrap();let result=call.await;
            if outcome==1 {assert!(matches!(result,Err(arroyo_rpc::errors::DataflowError::DataError {count:7,ref details}) if details=="typed public delivery failure"));}
            else {result.unwrap();}
        }
        assert_eq!(resource_used(&registry,"decoded_value_bytes"),0);assert_eq!(resource_used(&registry,"snapshots"),0);
        assert_eq!(op.event_input_blocked(0),outcome!=0);assert_eq!(op.event_input_blocked(1),outcome!=0);
    }
}
