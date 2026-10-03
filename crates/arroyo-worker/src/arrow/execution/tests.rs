//! Exercise the actual serialized-plan stateless executor, not a surrogate queue.
use super::ExecutionResources;
use crate::arrow::{ProjectionOperator, StatelessPhysicalExecutor};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{RecordBatch, UInt64Array};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_operator::operator::Registry;
use arroyo_planner::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec};
use arroyo_rpc::config::ExecutionResourceConfig;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use datafusion::common::DataFusionError;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::{ExecutionPlan, empty::EmptyExec};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::sync::Arc;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(UInt64Array::from(vec![7; 16]))]).unwrap()
}

fn resources(memory_bytes: usize, max_batch_bytes: usize) -> Arc<ExecutionResources> {
    Arc::new(
        ExecutionResources::new(ExecutionResourceConfig {
            memory_bytes,
            max_batch_bytes,
        })
        .unwrap(),
    )
}

fn executor(
    plan: Arc<dyn ExecutionPlan>,
    resources: Arc<ExecutionResources>,
) -> StatelessPhysicalExecutor {
    let proto =
        PhysicalPlanNode::try_from_physical_plan(plan, &ArroyoPhysicalExtensionCodec::default())
            .unwrap()
            .encode_to_vec();
    StatelessPhysicalExecutor::new_with_resources(&proto, &Registry::default(), Some(resources))
        .unwrap()
}

fn passthrough(resources: Arc<ExecutionResources>) -> StatelessPhysicalExecutor {
    executor(
        Arc::new(ArroyoMemExec::new("input".into(), schema())),
        resources,
    )
}

#[tokio::test]
async fn stateless_input_limit_rejects_before_queuing_or_forwarding() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes - 1);
    let mut executor = passthrough(resources.clone());
    let mut stream = executor.process_batch(batch()).await;
    assert!(executor.batch.read().unwrap().is_none());
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    assert!(error.to_string().contains("execution input batch"));
    assert!(stream.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn serialized_stateless_execution_preserves_data_and_releases_on_completion() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let mut executor = passthrough(resources.clone());
    let input = batch();
    let mut stream = executor.process_batch(input.clone()).await;
    assert_eq!(resources.runtime.memory_pool.reserved(), bytes);
    assert!(executor.batch.read().unwrap().is_none());
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(output, input);
    assert_eq!(resources.runtime.memory_pool.reserved(), 2 * bytes);
    // An actual collector consumes its batch before requesting the next one.
    tokio::task::yield_now().await;
    assert_eq!(resources.runtime.memory_pool.reserved(), 2 * bytes);
    drop(output);
    assert!(stream.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn concurrent_stateless_executors_enforce_one_shared_pool() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(3 * bytes, bytes);
    let mut first = passthrough(resources.clone());
    let mut second = passthrough(resources.clone());
    let mut first_stream = first.process_batch(batch()).await;
    let mut second_stream = second.process_batch(batch()).await;
    assert_eq!(resources.runtime.memory_pool.reserved(), 2 * bytes);
    let first_output = first_stream.next().await.unwrap().unwrap();
    assert_eq!(resources.runtime.memory_pool.reserved(), 3 * bytes);
    assert!(matches!(
        second_stream.next().await,
        Some(Err(DataFusionError::ResourcesExhausted(_)))
    ));
    assert!(second_stream.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 2 * bytes);
    drop(first_output);
    drop(first_stream);
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    // Cancellation freed capacity for a new execution rather than poisoning it.
    let mut next = second.process_batch(batch()).await;
    drop(next.next().await.unwrap().unwrap());
    assert!(next.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn overlapping_input_is_rejected_without_disturbing_active_stream() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let mut executor = passthrough(resources.clone());
    let mut first = executor.process_batch(batch()).await;
    let mut rejected = executor.process_batch(batch()).await;
    assert_eq!(resources.runtime.memory_pool.reserved(), bytes);
    assert!(matches!(
        rejected.next().await,
        Some(Err(DataFusionError::Execution(message)))
            if message.contains("previous stream to finish or be dropped")
    ));
    assert!(rejected.next().await.is_none());
    let output = first.next().await.unwrap().unwrap();
    assert_eq!(output, batch());
    drop(output);
    assert!(first.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);

    let cancelled = executor.process_batch(batch()).await;
    assert_eq!(resources.runtime.memory_pool.reserved(), bytes);
    drop(cancelled);
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    let mut resumed = executor.process_batch(batch()).await;
    drop(resumed.next().await.unwrap().unwrap());
    assert!(resumed.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn cancelling_before_first_poll_releases_input_and_pins_runtime_until_drop() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let pool = resources.runtime.memory_pool.clone();
    let runtime = Arc::downgrade(&resources.runtime);
    let mut executor = passthrough(resources.clone());
    let stream = executor.process_batch(batch()).await;
    assert_eq!(pool.reserved(), bytes);
    drop(executor);
    drop(resources);
    assert!(
        runtime.upgrade().is_some(),
        "a live stream must pin its shared runtime"
    );
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    assert!(runtime.upgrade().is_none());
}

#[tokio::test]
async fn plan_that_ignores_input_does_not_leave_unaccounted_batch_in_executor() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let mut executor = executor(Arc::new(EmptyExec::new(schema())), resources.clone());
    let mut stream = executor.process_batch(batch()).await;
    assert!(stream.next().await.is_none());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    assert!(executor.batch.read().unwrap().is_none());
}

#[test]
fn malformed_serialized_stateless_plan_returns_error() {
    let resources = resources(1024, 512);
    assert!(
        StatelessPhysicalExecutor::new_with_resources(
            &[0xff],
            &Registry::default(),
            Some(resources),
        )
        .is_err()
    );
}

#[tokio::test]
async fn process_single_propagates_limit_and_empty_output_errors() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes - 1);
    let mut limited = passthrough(resources.clone());
    assert!(matches!(
        limited.process_single(batch()).await,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    let mut empty = executor(Arc::new(EmptyExec::new(schema())), resources.clone());
    // The empty result path needs an input that fits this same resource limit.
    let small = RecordBatch::try_new(schema(), vec![Arc::new(UInt64Array::from(vec![7]))]).unwrap();
    assert!(matches!(
        empty.process_single(small).await,
        Err(DataFusionError::Execution(message)) if message.contains("received none")
    ));
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

fn projection(resources: Arc<ExecutionResources>) -> ProjectionOperator {
    ProjectionOperator {
        name: "accounted test projection".into(),
        output_schema: ArroyoSchema::new_unkeyed(schema(), 0),
        exprs: vec![Arc::new(Column::new("value", 0))],
        resources: Some(resources),
    }
}

async fn projection_context() -> OperatorContext {
    let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
    OperatorContext::new(
        Arc::new(arroyo_types::get_test_task_info()),
        None,
        control_tx,
        1,
        vec![],
        None,
        Default::default(),
    )
    .await
}

struct SlowCollector {
    started: Option<tokio::sync::oneshot::Sender<()>>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[async_trait::async_trait]
impl Collector for SlowCollector {
    async fn collect(&mut self, output: RecordBatch) -> DataflowResult<()> {
        assert_eq!(output, batch());
        self.started.take().unwrap().send(()).unwrap();
        (&mut self.release).await.unwrap();
        drop(output);
        Ok(())
    }

    async fn broadcast_watermark(&mut self, _: arroyo_types::Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn projection_reserves_input_and_output_through_slow_collector_and_cancellation() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let mut operator = projection(resources.clone());
    let mut context = projection_context().await;
    for cancel in [false, true] {
        let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut collector = SlowCollector {
            started: Some(started_tx),
            release: release_rx,
        };
        let mut processing = operator.process_batch(batch(), &mut context, &mut collector);
        assert!(futures::poll!(&mut processing).is_pending());
        started_rx.try_recv().unwrap();
        assert_eq!(resources.runtime.memory_pool.reserved(), 2 * bytes);
        if cancel {
            drop(processing);
            drop(collector);
            assert!(release_tx.send(()).is_err());
        } else {
            release_tx.send(()).unwrap();
            processing.await.unwrap();
        }
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn projection_oversize_input_does_not_reach_collector() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes - 1);
    let mut operator = projection(resources.clone());
    let mut context = projection_context().await;
    let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
    let (_release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mut collector = SlowCollector {
        started: Some(started_tx),
        release: release_rx,
    };
    let error = operator
        .process_batch(batch(), &mut context, &mut collector)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Streamr projection input batch"));
    assert!(started_rx.try_recv().is_err());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn projection_oversize_output_does_not_reach_collector() {
    let bytes = batch().get_array_memory_size();
    let resources = resources(4 * bytes, bytes);
    let mut operator = projection(resources.clone());
    operator.output_schema = ArroyoSchema::new_unkeyed(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::UInt64, false),
            Field::new("copy", DataType::UInt64, false),
        ])),
        0,
    );
    operator.exprs.push(Arc::new(Column::new("value", 0)));
    let mut context = projection_context().await;
    let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
    let (_release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mut collector = SlowCollector {
        started: Some(started_tx),
        release: release_rx,
    };
    let error = operator
        .process_batch(batch(), &mut context, &mut collector)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Streamr projection output batch")
    );
    assert!(started_rx.try_recv().is_err());
    assert_eq!(resources.runtime.memory_pool.reserved(), 0);
}
