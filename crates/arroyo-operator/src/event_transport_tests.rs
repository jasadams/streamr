use crate::context::{ArrowCollector, Collector, OperatorContext, batch_bounded};
use crate::operator::{ArrowOperator, ChainedOperator};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};
use arroyo_types::{ArrowMessage, ChainInfo, SignalMessage, TaskInfo};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn edge() -> Arc<EventEdgeSpec> {
    Arc::new(EventEdgeSpec {
        root_operator: 7,
        root_subtask: 0,
        schema: Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ])),
        layout: EventLayout::Raw,
    })
}
fn empty(sequence: u64) -> EventResponse {
    EventResponse {
        native_body: None,
        context: EventContext {
            version: EVENT_RESPONSE_VERSION,
            root_operator: 7,
            root_subtask: 0,
            attempt_nonce: [9; 16],
            sequence,
            trigger_nanos: 5,
        },
        rows: None,
        touched_keys: None,
    }
}
fn chain_info(id: usize) -> Arc<ChainInfo> {
    Arc::new(ChainInfo {
        job_id: format!("event-transport-{}-{id}", std::process::id()),
        task_id: 7,
        description: "transport-test".into(),
        task_index: 0,
    })
}
#[tokio::test]
async fn event_transport_empty_row_queue_blocks_and_old_queue_cannot_reach_fresh_attempt() {
    let (old_tx, mut old_rx) = batch_bounded(1);
    old_tx.send(ArrowMessage::Event(empty(1))).await.unwrap();
    assert_eq!(old_tx.capacity(), 0);
    assert_eq!(old_tx.queued_bytes(), empty(1).retained_bytes() as u64);
    let mut blocked = Box::pin(old_tx.send(ArrowMessage::Event(empty(2))));
    assert!(futures::poll!(&mut blocked).is_pending());
    assert_eq!(old_rx.recv().await, Some(ArrowMessage::Event(empty(1))));
    blocked.await.unwrap();
    assert_eq!(old_rx.recv().await, Some(ArrowMessage::Event(empty(2))));
    assert_eq!(old_tx.queued_bytes(), 0);
    drop(old_rx);
    let (fresh_tx, mut fresh_rx) = batch_bounded(1);
    let failure = old_tx
        .send(ArrowMessage::Event(empty(1)))
        .await
        .unwrap_err();
    assert_eq!(failure.0, ArrowMessage::Event(empty(1)));
    assert!(futures::poll!(Box::pin(fresh_rx.recv())).is_pending());
    fresh_tx.send(ArrowMessage::Event(empty(1))).await.unwrap();
    assert_eq!(fresh_rx.recv().await, Some(ArrowMessage::Event(empty(1))));
}
#[tokio::test]
async fn event_transport_fanout_awaits_empty_delivery_before_next_event() {
    let (left, mut left_rx) = batch_bounded(1);
    let (right, mut right_rx) = batch_bounded(1);
    right
        .send(ArrowMessage::Signal(SignalMessage::Stop))
        .await
        .unwrap();
    let spec = edge();
    let mut collector = ArrowCollector::new(
        chain_info(1),
        Some(Arc::new(ArroyoSchema::new_unkeyed(spec.schema.clone(), 1))),
        vec![vec![left], vec![right]],
    );
    collector.register_event_output(spec).unwrap();
    let mut delivery = Box::pin(collector.collect_response(empty(1)));
    assert!(futures::poll!(&mut delivery).is_pending());
    assert_eq!(left_rx.recv().await, Some(ArrowMessage::Event(empty(1))));
    assert!(futures::poll!(&mut delivery).is_pending());
    assert_eq!(
        right_rx.recv().await,
        Some(ArrowMessage::Signal(SignalMessage::Stop))
    );
    delivery.await.unwrap();
    assert_eq!(right_rx.recv().await, Some(ArrowMessage::Event(empty(1))));
    collector.collect_response(empty(2)).await.unwrap();
    assert_eq!(left_rx.recv().await, Some(ArrowMessage::Event(empty(2))));
    assert_eq!(right_rx.recv().await, Some(ArrowMessage::Event(empty(2))));
}
struct Forward(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl ArrowOperator for Forward {
    fn name(&self) -> String {
        "event-forward-test".into()
    }
    async fn process_batch(
        &mut self,
        _batch: arrow::array::RecordBatch,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Err(DataflowError::ArgumentError(
            "test expects typed response".into(),
        ))
    }
    async fn process_response_index(
        &mut self,
        index: usize,
        partitions: usize,
        response: EventResponse,
        _ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        assert_eq!((index, partitions), (0, 1));
        self.0.fetch_add(1, Ordering::Relaxed);
        collector.collect_response(response).await
    }
}
async fn context(id: usize) -> OperatorContext {
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    OperatorContext::new(
        Arc::new(TaskInfo {
            job_id: format!("event-chain-{}-{id}", std::process::id()),
            operator_idx: id as u32,
            operator_name: "event-forward-test".into(),
            operator_id: format!("test-{id}"),
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
        HashMap::new(),
    )
    .await
}
#[tokio::test]
async fn event_transport_chained_callbacks_preserve_empty_context_once() {
    let seen = Arc::new(AtomicUsize::new(0));
    let mut first = ChainedOperator::new(Box::new(Forward(seen.clone())), context(1).await);
    first.next = Some(Box::new(ChainedOperator::new(
        Box::new(Forward(seen.clone())),
        context(2).await,
    )));
    let (tx, mut rx) = batch_bounded(1);
    let spec = edge();
    let mut collector = ArrowCollector::new(
        chain_info(2),
        Some(Arc::new(ArroyoSchema::new_unkeyed(spec.schema.clone(), 1))),
        vec![vec![tx]],
    );
    collector.register_event_output(spec).unwrap();
    first
        .process_response_index(0, 1, empty(1), &mut collector)
        .await
        .unwrap();
    assert_eq!(seen.load(Ordering::Relaxed), 2);
    assert_eq!(rx.recv().await, Some(ArrowMessage::Event(empty(1))));
}

#[tokio::test]
async fn event_transport_unregistered_or_zero_capacity_routes_reject() {
    let (zero, _rx) = batch_bounded(0);
    assert!(zero.send(ArrowMessage::Event(empty(1))).await.is_err());
    let spec = edge();
    let mut collector = ArrowCollector::new(
        chain_info(3),
        Some(Arc::new(ArroyoSchema::new_unkeyed(spec.schema.clone(), 1))),
        vec![vec![zero]],
    );
    assert!(collector.register_event_output(spec).is_err());
    assert!(
        collector
            .collect_response(empty(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("unregistered response output")
    );
}
