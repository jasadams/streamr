use super::*;
use crate::event_transport::EventWireSpec;
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_operator::context::batch_bounded;
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};

fn quad() -> Quad {
    Quad {
        src_id: 1,
        src_idx: 0,
        dst_id: 2,
        dst_idx: 0,
    }
}
fn tasks() -> Vec<arroyo_rpc::grpc::rpc::TaskAssignment> {
    [(1, 100), (2, 200)]
        .into_iter()
        .map(
            |(task_id, worker_id)| arroyo_rpc::grpc::rpc::TaskAssignment {
                task_id,
                worker_id,
                subtask_idx: 0,
                worker_addr: String::new(),
                worker_rpc: String::new(),
            },
        )
        .collect()
}
fn spec(generation: u64) -> Arc<EventWireSpec> {
    Arc::new(
        EventWireSpec::registered(
            generation,
            quad(),
            [1, 2],
            &tasks(),
            Arc::new(EventEdgeSpec {
                maintenance: false,
                root_operator: 7,
                root_subtask: 0,
                schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
                layout: EventLayout::Native {
                    max_rows: 4,
                    owner_output_bytes: 4096,
                    encoded_key_bytes: 512,
                },
            }),
        )
        .unwrap(),
    )
}
fn empty() -> EventResponse {
    EventResponse {
        native_body: None,
        context: EventContext {
            version: EVENT_RESPONSE_VERSION,
            root_operator: 7,
            root_subtask: 0,
            attempt_nonce: [9; 16],
            sequence: 1,
            trigger_nanos: 5,
        },
        rows: None,
        touched_keys: None,
    }
}
#[tokio::test]
async fn event_transport_actual_network_roundtrips_empty_keys_and_null() {
    let (server_tx, mut server_rx) = batch_bounded(8);
    let mut senders = Senders::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    senders
        .add_response(quad(), spec(3), server_tx, None)
        .unwrap();
    let shutdown = Shutdown::new("event-network", SignalBehavior::None);
    let mut network = NetworkManager::new(0).await.unwrap();
    let port = network.open_listener(shutdown.guard("listener")).await;
    let (tx, rx) = batch_bounded(8);
    network
        .connect_response_reported(&format!("127.0.0.1:{port}"), quad(), rx, None, spec(3))
        .await;
    network.start(senders).await;
    let keys = RecordBatch::try_new(
        EventEdgeSpec::keys_schema(),
        vec![Arc::new(arrow::array::BinaryArray::from_vec(vec![
            b"k".as_slice(),
        ]))],
    )
    .unwrap();
    let null_row = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(vec![None]))],
    )
    .unwrap();
    for expected in [
        empty(),
        EventResponse {
            touched_keys: Some(keys.clone()),
            ..empty()
        },
        EventResponse {
            rows: Some(null_row),
            touched_keys: Some(keys),
            ..empty()
        },
    ] {
        tx.send(ArrowMessage::Event(expected.clone()))
            .await
            .unwrap();
        let actual = tokio::time::timeout(Duration::from_secs(2), server_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, ArrowMessage::Event(expected));
    }
    for signal in [
        SignalMessage::Watermark(arroyo_types::Watermark::Idle),
        SignalMessage::Barrier(arroyo_types::CheckpointBarrier {
            completed_base_epoch: None,
            epoch: 2,
            min_epoch: 1,
            timestamp: std::time::UNIX_EPOCH,
            then_stop: false,
        }),
        SignalMessage::Stop,
        SignalMessage::EndOfData,
    ] {
        tx.send(ArrowMessage::Signal(signal.clone())).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), server_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            ArrowMessage::Signal(signal)
        );
    }
    network.task_registry().abort();
}
#[tokio::test]
async fn event_transport_actual_network_rejects_stale_before_variable_frames() {
    let (tx, mut rx) = batch_bounded(1);
    let (control, mut failures) = tokio::sync::mpsc::channel(8);
    let mut senders = Senders::new();
    senders
        .add_response(
            quad(),
            spec(4),
            tx,
            Some(NetworkFailureReporter::new(control, 2, 0)),
        )
        .unwrap();
    let shutdown = Shutdown::new("event-stale-network", SignalBehavior::None);
    let mut network = NetworkManager::new(0).await.unwrap();
    let port = network.open_listener(shutdown.guard("listener")).await;
    network.start(senders).await;
    let mut stale = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    Header::from_quad(quad(), u32::MAX as usize, MessageType::Event)
        .write(&mut Pin::new(&mut stale))
        .await
        .unwrap();
    let old = spec(3);
    old.write_payload(&mut stale, &old.encode(&empty()).unwrap())
        .await
        .unwrap();
    // The peer remains open and supplies NO variable body. A large native
    // declaration cannot allocate because the old generation rejects first.
    let failure = tokio::time::timeout(Duration::from_secs(2), failures.recv())
        .await
        .unwrap()
        .unwrap();
    let ControlResp::TaskFailed {
        task_id,
        subtask_idx,
        error,
    } = failure
    else {
        panic!("expected task failure")
    };
    assert_eq!((task_id, subtask_idx), (2, 0));
    assert!(
        error
            .message
            .contains("response execution assignment mismatch")
    );
    assert!(futures::poll!(Box::pin(rx.recv())).is_pending());
    network.task_registry().abort();
}

#[tokio::test]
async fn event_transport_actual_network_rejects_stale_bound_and_unbound_signals() {
    let (tx, mut rx) = batch_bounded(1);
    let (control, mut failures) = tokio::sync::mpsc::channel(8);
    let mut senders = Senders::new();
    senders
        .add_response(
            quad(),
            spec(4),
            tx,
            Some(NetworkFailureReporter::new(control, 2, 0)),
        )
        .unwrap();
    let shutdown = Shutdown::new("event-stale-signals", SignalBehavior::None);
    let mut network = NetworkManager::new(0).await.unwrap();
    let port = network.open_listener(shutdown.guard("listener")).await;
    network.start(senders).await;
    for bound in [true, false] {
        let mut peer = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        Header::from_quad(
            quad(),
            if bound { 26 } else { 1 },
            if bound {
                MessageType::ResponseSignal
            } else {
                MessageType::Signal
            },
        )
        .write(&mut Pin::new(&mut peer))
        .await
        .unwrap();
        if bound {
            spec(3).write_identity(&mut peer).await.unwrap();
        }
        // Keep the old peer open without its signal payload. Identity/type
        // rejection must precede any attempt to allocate/read that payload.
        let failure = tokio::time::timeout(Duration::from_secs(2), failures.recv())
            .await
            .unwrap()
            .unwrap();
        let ControlResp::TaskFailed { error, .. } = failure else {
            panic!("expected task failure")
        };
        assert!(
            error.message.contains(if bound {
                "response execution assignment mismatch"
            } else {
                "unbound Data/Signal on response edge"
            }),
            "{}",
            error.message
        );
        assert!(futures::poll!(Box::pin(rx.recv())).is_pending());
    }
    network.task_registry().abort();
}
