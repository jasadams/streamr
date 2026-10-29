use crate::context::{
    ArrowCollector, BatchReceiver, BatchSender, Collector, OperatorContext, SourceCollector,
    SourceContext, send_checkpoint_event,
};
use crate::inq_reader::InQReader;
use crate::udfs::{ArroyoUdaf, UdafArg};
use crate::{CheckpointCounter, ControlOutcome, SourceFinishType};
use anyhow::{anyhow, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arroyo_datastream::logical::{DylibUdfConfig, PythonUdfConfig};
use arroyo_metrics::TaskCounters;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowError;
use arroyo_rpc::errors::{DataflowResult, TaskError};
use arroyo_rpc::grpc::rpc::{TableConfig, TaskCheckpointEventType};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_state::tables::table_manager::TableManager;
use arroyo_storage::StorageProvider;
use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::EventResponse;
use arroyo_types::{
    ArrowMessage, ChainInfo, CheckpointBarrier, SignalMessage, TaskInfo, Watermark,
};
use arroyo_udf_host::parse::inner_type;
use arroyo_udf_host::{ContainerOrLocal, LocalUdf, SyncUdfDylib, UdfDylib, UdfInterface};
use arroyo_udf_python::PythonUDF;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{
    AggregateUDF, ScalarUDF, Signature, TypeSignature, Volatility, WindowUDF, create_udaf,
};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use dlopen2::wrapper::Container;
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Barrier;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tracing::{Instrument, debug, error, info, trace, warn};

pub trait OperatorConstructor: Send {
    type ConfigT: prost::Message + Default;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator>;
}

pub struct SourceNode {
    pub operator: Box<dyn SourceOperator + Send>,
    pub context: OperatorContext,
}

pub enum ConstructedOperator {
    Source(Box<dyn SourceOperator + Send>),
    Operator(Box<dyn ArrowOperator + Send>),
}

impl ConstructedOperator {
    pub fn from_source(source: Box<dyn SourceOperator + Send>) -> Self {
        Self::Source(source)
    }

    pub fn from_operator(operator: Box<dyn ArrowOperator + Send>) -> Self {
        Self::Operator(operator)
    }

    pub fn name(&self) -> String {
        match self {
            Self::Source(s) => s.name(),
            Self::Operator(s) => s.name(),
        }
    }

    pub fn display(&self) -> DisplayableOperator<'_> {
        match self {
            Self::Source(_) => DisplayableOperator {
                name: self.name().into(),
                fields: vec![],
            },
            Self::Operator(op) => op.display(),
        }
    }
}

pub enum OperatorNode {
    Source(SourceNode),
    Chained(ChainedOperator),
}

// TODO: this is required currently because the FileSystemSink code isn't sync
unsafe impl Sync for OperatorNode {}

impl OperatorNode {
    pub fn name(&self) -> String {
        match self {
            OperatorNode::Source(s) => s.operator.name(),
            OperatorNode::Chained(s) => {
                format!(
                    "[{}]",
                    s.iter()
                        .map(|(o, _)| o.name())
                        .collect::<Vec<_>>()
                        .join(" -> ")
                )
            }
        }
    }

    pub fn task_info(&self) -> &Arc<TaskInfo> {
        match self {
            OperatorNode::Source(s) => &s.context.task_info,
            OperatorNode::Chained(s) => &s.context.task_info,
        }
    }

    pub fn operator_ids(&self) -> Vec<String> {
        match self {
            OperatorNode::Source(s) => vec![s.context.task_info.operator_id.clone()],
            OperatorNode::Chained(s) => s
                .iter()
                .map(|(_, ctx)| ctx.task_info.operator_id.clone())
                .collect(),
        }
    }

    async fn run_behavior(
        self,
        chain_info: &Arc<ChainInfo>,
        control_tx: Sender<ControlResp>,
        control_rx: Receiver<ControlMessage>,
        in_qs: &mut [BatchReceiver],
        ready: Arc<Barrier>,
        mut collector: ArrowCollector,
    ) -> Result<(), TaskError> {
        match self {
            OperatorNode::Source(mut s) => {
                let mut source_context =
                    SourceContext::from_operator(s.context, chain_info.clone(), control_rx);

                let mut collector = SourceCollector::new(
                    source_context.out_schema.clone(),
                    collector,
                    control_tx.clone(),
                    &source_context.task_info,
                );

                s.operator.on_start(&mut source_context).await?;

                ready.wait().await;
                info!(
                    "Running source {}-{}",
                    source_context.task_info.operator_name, source_context.task_info.operator_name
                );

                source_context
                    .control_tx
                    .send(ControlResp::TaskStarted {
                        task_id: source_context.task_info.operator_idx,
                        subtask_idx: source_context.task_info.task_index,
                        start_time: SystemTime::now(),
                    })
                    .await
                    .unwrap();

                let result = s
                    .operator
                    .run(&mut source_context, &mut collector)
                    .await
                    .map_err(|error| collector.delivery_error(error))?;

                s.operator
                    .on_close(&mut source_context, &mut collector)
                    .await
                    .map_err(|error| collector.delivery_error(error))?;

                collector.check_delivery_error()?;
                if let Some(final_message) = result.into() {
                    collector.broadcast_checked(final_message).await?;
                }
            }
            OperatorNode::Chained(mut o) => {
                let result = operator_run_behavior(
                    &mut o,
                    in_qs,
                    control_tx,
                    control_rx,
                    &mut collector,
                    ready,
                )
                .await?;
                if let Some(final_message) = result {
                    collector.broadcast_checked(final_message).await?;
                }
            }
        }

        Ok(())
    }

    fn node_id(&self) -> u32 {
        match self {
            OperatorNode::Source(s) => s.context.task_info.operator_idx,
            OperatorNode::Chained(s) => s.context.task_info.operator_idx,
        }
    }

    fn task_index(&self) -> u32 {
        match self {
            OperatorNode::Source(s) => s.context.task_info.task_index,
            OperatorNode::Chained(s) => s.context.task_info.task_index,
        }
    }

    pub async fn start(
        self: Box<Self>,
        control_tx: Sender<ControlResp>,
        control_rx: Receiver<ControlMessage>,
        mut in_qs: Vec<BatchReceiver>,
        out_qs: Vec<Vec<BatchSender>>,
        out_schema: Option<Arc<ArroyoSchema>>,
        ready: Arc<Barrier>,
    ) -> Vec<BatchReceiver> {
        info!(
            "Starting node {}-{} ({})",
            self.node_id(),
            self.task_index(),
            self.name()
        );

        let chain_info = Arc::new(ChainInfo {
            job_id: self.task_info().job_id.clone(),
            task_id: self.node_id(),
            description: self.name(),
            task_index: self.task_index(),
        });

        let event_spec = match self.as_ref() {
            OperatorNode::Source(_) => None,
            OperatorNode::Chained(chain) => chain
                .iter()
                .last()
                .and_then(|(operator, _)| operator.event_response_output()),
        };
        let mut collector = ArrowCollector::new(chain_info.clone(), out_schema, out_qs);
        if let Some(spec) = event_spec
            && let Err(error) = collector.register_event_output(spec)
        {
            control_tx
                .send(ControlResp::TaskFailed {
                    task_id: chain_info.task_id,
                    subtask_idx: chain_info.task_index,
                    error: error.into(),
                })
                .await
                .ok();
            return in_qs;
        }

        match self
            .run_behavior(
                &chain_info,
                control_tx.clone(),
                control_rx,
                &mut in_qs,
                ready,
                collector,
            )
            .await
        {
            Ok(()) => {
                info!(
                    task_id = chain_info.task_id,
                    subtask_index = chain_info.task_index,
                    description = chain_info.description,
                    "Task finished"
                );

                control_tx
                    .send(ControlResp::TaskFinished {
                        task_id: chain_info.task_id,
                        subtask_idx: chain_info.task_index,
                    })
                    .await
                    .expect("control response unwrap");
            }
            Err(e) => {
                error!(task_id = chain_info.task_id,
                    subtask_index = chain_info.task_index,
                    description = chain_info.description,
                    error = ?e,
                    "Task failed");

                control_tx
                    .send(ControlResp::TaskFailed {
                        task_id: chain_info.task_id,
                        subtask_idx: chain_info.task_index,
                        error: e,
                    })
                    .await
                    .expect("control response unwrap");
            }
        }

        in_qs
    }
}

async fn run_checkpoint(
    checkpoint_barrier: CheckpointBarrier,
    task_info: &TaskInfo,
    watermark: Option<SystemTime>,
    table_manager: &mut TableManager,
    control_tx: &Sender<ControlResp>,
) {
    table_manager
        .checkpoint(checkpoint_barrier, watermark)
        .await;

    send_checkpoint_event(
        control_tx,
        task_info,
        checkpoint_barrier,
        TaskCheckpointEventType::FinishedSync,
    )
    .await;
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut SourceContext) -> DataflowResult<()> {
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType>;

    #[allow(unused_variables)]
    async fn on_close(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    async fn start_checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> bool {
        send_checkpoint_event(
            &ctx.control_tx,
            &ctx.task_info,
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        run_checkpoint(
            checkpoint_barrier,
            &ctx.task_info,
            ctx.watermarks.last_present_watermark(),
            &mut ctx.table_manager,
            &ctx.control_tx,
        )
        .await;

        collector
            .broadcast(SignalMessage::Barrier(checkpoint_barrier))
            .await;

        checkpoint_barrier.then_stop
    }
}

macro_rules! call_with_collector {
    ($self:expr, $final_collector:expr, $name:ident, $arg:expr) => {
        match &mut $self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector: $final_collector,
                };

                $self
                    .operator
                    .$name($arg, &mut $self.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator($self.context.task_info.operator_id.clone()))?;
            }
            None => {
                $self
                    .operator
                    .$name($arg, &mut $self.context, $final_collector)
                    .await
                    .map_err(|e| e.with_operator($self.context.task_info.operator_id.clone()))?;
            }
        }
    };
}

pub struct ChainedCollector<'a, 'b> {
    cur: &'a mut ChainedOperator,
    final_collector: &'b mut ArrowCollector,
    index: usize,
    in_partitions: usize,
}

#[async_trait]
impl<'a, 'b> Collector for ChainedCollector<'a, 'b>
where
    'b: 'a,
    'a: 'b,
{
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        if let Some(next) = &mut self.cur.next {
            let mut collector = ChainedCollector {
                cur: next,
                final_collector: self.final_collector,
                // all chained operators (other than the first one) must have a single input
                index: 0,
                in_partitions: 1,
            };

            self.cur
                .operator
                .process_batch_index(
                    self.index,
                    self.in_partitions,
                    batch,
                    &mut self.cur.context,
                    &mut collector,
                )
                .await?;
        } else {
            self.cur
                .operator
                .process_batch_index(
                    self.index,
                    self.in_partitions,
                    batch,
                    &mut self.cur.context,
                    self.final_collector,
                )
                .await?;
        };

        Ok(())
    }

    async fn collect_response(&mut self, response: EventResponse) -> DataflowResult<()> {
        if self
            .cur
            .operator
            .event_input_count()
            .is_some_and(|count| self.in_partitions != count)
        {
            return Err(anyhow!("event pair callback bypassed indexed two-input run loop").into());
        }
        if let Some(next) = &mut self.cur.next {
            let mut collector = ChainedCollector {
                cur: next,
                final_collector: self.final_collector,
                index: 0,
                in_partitions: 1,
            };
            self.cur
                .operator
                .process_response_index(
                    self.index,
                    self.in_partitions,
                    response,
                    &mut self.cur.context,
                    &mut collector,
                )
                .await
        } else {
            self.cur
                .operator
                .process_response_index(
                    self.index,
                    self.in_partitions,
                    response,
                    &mut self.cur.context,
                    self.final_collector,
                )
                .await
        }
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.cur
            .handle_watermark(watermark, self.index, self.final_collector)
            .await?;

        Ok(())
    }
}

pub struct ChainedOperator {
    pub operator: Box<dyn ArrowOperator>,
    pub context: OperatorContext,
    pub next: Option<Box<ChainedOperator>>,
}

impl ChainedOperator {
    pub fn new(operator: Box<dyn ArrowOperator>, context: OperatorContext) -> Self {
        Self {
            operator,
            context,
            next: None,
        }
    }
}

pub struct ChainIteratorMut<'a> {
    current: Option<&'a mut ChainedOperator>,
}

impl<'a> Iterator for ChainIteratorMut<'a> {
    type Item = (&'a mut dyn ArrowOperator, &'a mut OperatorContext);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.take() {
            let next = current.next.as_deref_mut();
            self.current = next;
            Some((current.operator.as_mut(), &mut current.context))
        } else {
            None
        }
    }
}

pub struct ChainIterator<'a> {
    current: Option<&'a ChainedOperator>,
}

impl<'a> Iterator for ChainIterator<'a> {
    type Item = (&'a dyn ArrowOperator, &'a OperatorContext);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.take() {
            let next = current.next.as_deref();
            self.current = next;
            Some((current.operator.as_ref(), &current.context))
        } else {
            None
        }
    }
}

struct IndexedFuture {
    f: Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>,
    i: usize,
}

impl Future for IndexedFuture {
    type Output = (usize, Box<dyn Any + Send>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.f.as_mut().poll(cx) {
            Poll::Ready(r) => Poll::Ready((self.i, r)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ChainedOperator {
    async fn handle_controller_message(
        &mut self,
        control_message: &ControlMessage,
        shutdown_after_commit: bool,
    ) -> DataflowResult<bool> {
        match control_message {
            ControlMessage::Checkpoint(_) => {
                error!("shouldn't receive checkpoint")
            }
            ControlMessage::Stop { .. } => {
                error!("shouldn't receive stop")
            }
            ControlMessage::Commit { epoch, commit_data } => {
                assert!(
                    self.next.is_none(),
                    "can only commit sinks, which cannot be chained"
                );
                self.operator
                    .handle_commit(*epoch, commit_data, &mut self.context)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
                return Ok(shutdown_after_commit);
            }
            ControlMessage::LoadCompacted { compacted } => {
                self.iter_mut()
                    .find(|(_, ctx)| ctx.task_info.operator_id == compacted.operator_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "could not load compacted data for unknown operator '{}'",
                            compacted.operator_id
                        )
                    })
                    .1
                    .load_compacted(compacted)
                    .await;
            }
            ControlMessage::NoOp => {}
        }

        Ok(false)
    }

    pub fn iter(&self) -> ChainIterator<'_> {
        ChainIterator {
            current: Some(self),
        }
    }

    pub fn iter_mut(&mut self) -> ChainIteratorMut<'_> {
        ChainIteratorMut {
            current: Some(self),
        }
    }

    fn name(&self) -> String {
        self.iter()
            .map(|(op, _)| op.name())
            .collect::<Vec<String>>()
            .join(" -> ")
    }

    fn tick_interval(&self) -> Option<Duration> {
        self.iter().filter_map(|(op, _)| op.tick_interval()).min()
    }

    async fn on_start(&mut self) -> DataflowResult<()> {
        for (op, ctx) in self.iter_mut() {
            op.on_start(ctx)
                .await
                .map_err(|e| e.with_operator(ctx.task_info.operator_id.clone()))?;
        }

        Ok(())
    }

    async fn process_batch_index<'a, 'b>(
        &'a mut self,
        index: usize,
        in_partitions: usize,
        batch: RecordBatch,
        final_collector: &'b mut ArrowCollector,
    ) -> DataflowResult<()>
    where
        'a: 'b,
    {
        let mut collector = ChainedCollector {
            cur: self,
            index,
            in_partitions,
            final_collector,
        };

        collector
            .collect(batch)
            .await
            .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))
    }

    pub(crate) async fn process_response_index(
        &mut self,
        index: usize,
        in_partitions: usize,
        response: EventResponse,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        let mut collector = ChainedCollector {
            cur: self,
            index,
            in_partitions,
            final_collector,
        };
        collector
            .collect_response(response)
            .await
            .map_err(|error| error.with_operator(self.context.task_info.operator_id.clone()))
    }

    #[allow(clippy::type_complexity)]
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = (usize, Box<dyn Any + Send + Send>)> + Send>>> {
        let futures = self
            .iter_mut()
            .enumerate()
            .filter_map(|(i, (op, _))| {
                Some(IndexedFuture {
                    f: op.future_to_poll()?,
                    i,
                })
            })
            .collect::<Vec<_>>();

        match futures.len() {
            0 => None,
            1 => Some(Box::pin(futures.into_iter().next().unwrap())),
            _ => {
                Some(Box::pin(async move {
                    let mut futures = FuturesUnordered::from_iter(futures);
                    // we've guaranteed that the future unordered has at least one element so this
                    // will never unwrap
                    futures.next().await.unwrap()
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: &SignalMessage,
        counter: &mut CheckpointCounter,
        closed: &mut HashSet<usize>,
        in_partitions: usize,
        control_tx: &Sender<ControlResp>,
        chain_info: &ChainInfo,
        collector: &mut ArrowCollector,
    ) -> DataflowResult<ControlOutcome> {
        match message {
            SignalMessage::Barrier(t) => {
                debug!("received barrier in {}[{}]", chain_info, idx);

                if counter.all_clear() {
                    control_tx
                        .send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
                            checkpoint_epoch: t.epoch as u64,
                            operator_idx: chain_info.task_id,
                            operator_id: self.context.task_info.operator_id.clone(),
                            subtask_idx: chain_info.task_index,
                            time: SystemTime::now(),
                            event_type: TaskCheckpointEventType::StartedAlignment,
                        }))
                        .await
                        .unwrap();
                }

                if counter.mark(idx, t) {
                    debug!("Checkpointing {chain_info}");

                    self.run_checkpoint(t, control_tx, collector)
                        .await
                        .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;

                    collector
                        .broadcast_checked(SignalMessage::Barrier(*t))
                        .await?;

                    if t.then_stop {
                        // if this is a committing operator, we need to wait for the commit message
                        // before shutting down; otherwise we just stop
                        return if self.operator.is_committing() {
                            Ok(ControlOutcome::StopAfterCommit)
                        } else {
                            Ok(ControlOutcome::Stop)
                        };
                    }
                }
            }
            SignalMessage::Watermark(watermark) => {
                debug!("received watermark {:?} in {}", watermark, chain_info,);

                self.handle_watermark(*watermark, idx, collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
            }
            SignalMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return Ok(ControlOutcome::StopAndSendStop);
                }
            }
            SignalMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return Ok(ControlOutcome::Finish);
                }
            }
        }
        Ok(ControlOutcome::Continue)
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        index: usize,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        trace!(
            "handling watermark {:?} for {}",
            watermark, self.context.task_info,
        );

        let watermark = self
            .context
            .watermarks
            .set(index, watermark)
            .expect("watermark index is too big");

        let Some(watermark) = watermark else {
            return Ok(());
        };

        if let Watermark::EventTime(_t) = watermark {
            // TOOD: pass to table_manager
        }

        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };

                let watermark = self
                    .operator
                    .handle_watermark(watermark, &mut self.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;

                if let Some(watermark) = watermark {
                    Box::pin(next.handle_watermark(watermark, 0, final_collector)).await?;
                }
            }
            None => {
                let watermark = self
                    .operator
                    .handle_watermark(watermark, &mut self.context, final_collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
                if let Some(watermark) = watermark {
                    final_collector
                        .broadcast_checked(SignalMessage::Watermark(watermark))
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_future_result(
        &mut self,
        op_index: usize,
        result: Box<dyn Any + Send>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        let mut op = self;
        for _ in 0..op_index {
            op = op
                .next
                .as_mut()
                .expect("Future produced from operator index larger than chain size");
        }

        match &mut op.next {
            None => {
                op.operator
                    .handle_future_result(result, &mut op.context, final_collector)
                    .await
                    .map_err(|e| e.with_operator(op.context.task_info.operator_id.clone()))?;
            }
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    final_collector,
                    index: 0,
                    in_partitions: 1,
                };
                op.operator
                    .handle_future_result(result, &mut op.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator(op.context.task_info.operator_id.clone()))?;
            }
        }

        Ok(())
    }

    async fn run_checkpoint(
        &mut self,
        t: &CheckpointBarrier,
        control_tx: &Sender<ControlResp>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        send_checkpoint_event(
            control_tx,
            &self.context.task_info,
            *t,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        call_with_collector!(self, final_collector, handle_checkpoint, *t);

        send_checkpoint_event(
            control_tx,
            &self.context.task_info,
            *t,
            TaskCheckpointEventType::FinishedOperatorSetup,
        )
        .await;

        let last_watermark = self.context.watermarks.last_present_watermark();

        run_checkpoint(
            *t,
            &self.context.task_info,
            last_watermark,
            &mut self.context.table_manager,
            control_tx,
        )
        .await;

        if let Some(next) = &mut self.next {
            Box::pin(next.run_checkpoint(t, control_tx, final_collector)).await?;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        tick: u64,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };
                self.operator
                    .handle_tick(tick, &mut self.context, &mut collector)
                    .await?;
                Box::pin(next.handle_tick(tick, final_collector)).await?;
            }
            None => {
                self.operator
                    .handle_tick(tick, &mut self.context, final_collector)
                    .await?;
            }
        }

        Ok(())
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };

                self.operator
                    .on_close(final_message, &mut self.context, &mut collector)
                    .await?;

                Box::pin(next.on_close(final_message, final_collector)).await?;
            }
            None => {
                self.operator
                    .on_close(final_message, &mut self.context, final_collector)
                    .await?;
            }
        }

        Ok(())
    }
}

async fn operator_run_behavior(
    this: &mut ChainedOperator,
    in_qs: &mut [BatchReceiver],
    control_tx: Sender<ControlResp>,
    mut control_rx: Receiver<ControlMessage>,
    collector: &mut ArrowCollector,
    ready: Arc<Barrier>,
) -> DataflowResult<Option<SignalMessage>> {
    if this
        .iter()
        .skip(1)
        .any(|(operator, _)| operator.event_pair_mode())
    {
        return Err(anyhow!("event pair operator must be the physical chain head").into());
    }
    if this
        .operator
        .event_input_count()
        .is_some_and(|count| in_qs.len() != count)
    {
        return Err(anyhow!("event pair route requires exactly two physical inputs").into());
    }
    this.on_start().await?;

    let chain_info = &mut collector.chain_info.clone();

    ready.wait().await;

    info!("Running node {}", chain_info);

    control_tx
        .send(ControlResp::TaskStarted {
            task_id: chain_info.task_id,
            subtask_idx: chain_info.task_index,
            start_time: SystemTime::now(),
        })
        .await
        .unwrap();

    let name = this.name();
    let mut counter = CheckpointCounter::new(in_qs.len());
    let mut closed: HashSet<usize> = HashSet::new();
    let mut sel = InQReader::new();
    let in_partitions = in_qs.len();

    for (i, q) in in_qs.iter_mut().enumerate() {
        let stream = async_stream::stream! {
          while let Some(item) = q.recv().await {
            yield(i, Some(item));
          }
          // Preserve the originating input index at EOF. Ordinary operators'
          // default hook does nothing and still finish when all streams end.
          yield(i, None);
        };
        sel.push(Box::pin(stream));
    }
    let mut blocked = vec![];
    let mut final_message = None;

    let mut ticks = 0u64;
    let mut interval =
        tokio::time::interval(this.tick_interval().unwrap_or(Duration::from_secs(60)));

    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutdown_after_commit = false;
    let mut restore_commit_epoch = this
        .operator
        .is_committing()
        .then_some(this.context.restore_commit_epoch)
        .flatten();

    loop {
        let operator_future: OptionFuture<_> = if restore_commit_epoch.is_none() {
            this.future_to_poll()
        } else {
            None
        }
        .into();
        tokio::select! {
            Some(control_message) = control_rx.recv() => {
                if let (Some(expected), ControlMessage::Commit { epoch, .. }) =
                    (restore_commit_epoch, &control_message)
                    && expected != *epoch {
                    return Err(anyhow!("restore commit epoch mismatch: expected {expected}, received {epoch}").into());
                }
                if this.handle_controller_message(&control_message, shutdown_after_commit).await? {
                    break;
                }
                if matches!(control_message, ControlMessage::Commit { .. }) {
                    restore_commit_epoch = None;
                    this.context.restore_commit_epoch = None;
                }
            }

            p = sel.next(), if !shutdown_after_commit && restore_commit_epoch.is_none() => {
                match p {
                    Some(((idx, message), s)) => {
                        let Some(message) = message else {
                            this.operator.check_event_input_eof(idx, &counter, &closed)?;
                            if this.operator.event_pair_mode() { closed.insert(idx); }
                            continue;
                        };
                        let local_idx = idx;

                        trace!("[{}] Handling message {}-{}, {:?}",
                            chain_info.task_id, 0, local_idx, message);

                        match message {
                            ArrowMessage::Data(record) => {
                                if this.operator.event_pair_mode() {
                                    this.operator.check_event_maintenance_input(idx, &counter, &closed)?;
                                }
                                TaskCounters::BatchesReceived.for_task(chain_info, |c| c.inc());
                                TaskCounters::MessagesReceived.for_task(chain_info, |c| c.inc_by(record.num_rows() as u64));
                                TaskCounters::BytesReceived.for_task(chain_info, |c| c.inc_by(record.get_array_memory_size() as u64));
                                this.process_batch_index(idx, in_partitions, record, collector)
                                    .instrument(tracing::trace_span!("handle_fn",
                                        name,
                                        task_id = chain_info.task_id,
                                        subtask_idx = chain_info.task_index)
                                ).await?;
                            }
                            ArrowMessage::Event(response) => {
                                if this.operator.event_pair_mode() &&
                                    (counter.is_blocked(idx) || this.operator.event_input_blocked(idx)) {
                                    return Err(anyhow!("event response bypassed a parked input").into());
                                }
                                this.operator.check_event_response_input(idx, &counter, &closed)?;
                                this.process_response_index(idx, in_partitions, response, collector).await?;
                            }
                            ArrowMessage::Signal(signal) => {
                                this.operator.check_event_control_input(idx, &signal, &counter, &closed)?;
                                match this.handle_control_message(idx, &signal, &mut counter, &mut closed, in_partitions,
                                    &control_tx, chain_info, collector).await? {
                                    ControlOutcome::Continue => {}
                                    ControlOutcome::Stop => {
                                        // just stop; the stop will have already been broadcast for example by
                                        // a final checkpoint
                                        break;
                                    }
                                    ControlOutcome::StopAfterCommit => {
                                        shutdown_after_commit = true;
                                    }
                                    ControlOutcome::Finish => {
                                        final_message = Some(SignalMessage::EndOfData);
                                        break;
                                    }
                                    ControlOutcome::StopAndSendStop => {
                                        final_message = Some(SignalMessage::Stop);
                                        break;
                                    }
                                }
                            }
                        }

                        if counter.is_blocked(idx) || this.operator.event_input_blocked(idx) {
                            blocked.push((idx, s));
                        } else {
                            sel.push(s);
                        }
                        // A mate callback may unpark the earlier input. Retain
                        // its identity; checkpoint and event parks compose.
                        let mut still_blocked = Vec::new();
                        for (parked_idx, stream) in blocked.drain(..) {
                            if counter.is_blocked(parked_idx) || this.operator.event_input_blocked(parked_idx) {
                                still_blocked.push((parked_idx, stream));
                            } else {
                                sel.push(stream);
                            }
                        }
                        blocked = still_blocked;
                    }
                    None => {
                        info!("[{}] Stream completed", chain_info);
                        if !shutdown_after_commit {
                            break;
                        }
                    }
                }
            }
            Some(val) = operator_future, if restore_commit_epoch.is_none() => {
                this.handle_future_result(val.0, val.1, collector).await?;
            }
            _ = interval.tick(), if restore_commit_epoch.is_none() => {
                this.handle_tick(ticks, collector).await?;
                ticks += 1;
            }
            else => {
                return Err(anyhow!("operator control channel closed before restored commit").into());
            }
        }
    }

    this.on_close(&final_message, collector).await?;
    Ok(final_message)
}

pub enum AsDisplayable<'a> {
    Str(&'a str),
    String(String),
    Display(&'a dyn Display),
    Debug(&'a dyn Debug),
    Plan(&'a dyn ExecutionPlan),
    Schema(&'a Schema),
    List(Vec<String>),
}

impl<'a> From<&'a str> for AsDisplayable<'a> {
    fn from(s: &'a str) -> Self {
        AsDisplayable::Str(s)
    }
}

impl From<String> for AsDisplayable<'_> {
    fn from(s: String) -> Self {
        AsDisplayable::String(s)
    }
}

impl<'a> From<&'a dyn ExecutionPlan> for AsDisplayable<'a> {
    fn from(p: &'a dyn ExecutionPlan) -> Self {
        AsDisplayable::Plan(p)
    }
}

impl<'a> From<&'a Schema> for AsDisplayable<'a> {
    fn from(s: &'a Schema) -> Self {
        AsDisplayable::Schema(s)
    }
}

impl Display for AsDisplayable<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AsDisplayable::Str(s) => {
                write!(f, "{s}")
            }
            AsDisplayable::String(s) => {
                write!(f, "{s}")
            }
            AsDisplayable::Display(d) => {
                write!(f, "{d}")
            }
            AsDisplayable::Debug(d) => {
                write!(f, "{d:?}")
            }
            AsDisplayable::Plan(p) => {
                write!(f, "```\n{}\n```", displayable(*p).indent(false))
            }
            AsDisplayable::Schema(s) => {
                for field in s.fields() {
                    write!(f, "\n  * {}: {:?}, ", field.name(), field.data_type())?;
                }

                Ok(())
            }
            AsDisplayable::List(list) => {
                for s in list {
                    write!(f, "\n * {s}")?;
                }

                Ok(())
            }
        }
    }
}

pub struct DisplayableOperator<'a> {
    pub name: Cow<'a, str>,
    pub fields: Vec<(&'static str, AsDisplayable<'a>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventResponseRole {
    Ordinary,
    Scalar,
    Root(u32),
    Aggregate(u32),
    Finalized(u32),
    Join(u32),
    Result(u32),
    Union,
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static {
    fn name(&self) -> String;
    fn event_response_role(&self) -> EventResponseRole {
        EventResponseRole::Ordinary
    }
    fn event_response_output(&self) -> Option<Arc<EventEdgeSpec>> {
        None
    }
    /// Constructed native key encoding carried by touched-key packets. Scalars
    /// preserve it; native owners export their actual copied grouping tuple.
    fn event_response_key_types(&self) -> Option<Vec<DataType>> {
        None
    }
    fn bind_event_response_key_types(
        &self,
        _inputs: &[Option<Vec<DataType>>],
    ) -> DataflowResult<()> {
        Ok(())
    }
    fn bind_event_response(
        &mut self,
        _inputs: &[Arc<EventEdgeSpec>],
        _context: &OperatorContext,
    ) -> DataflowResult<Option<Arc<EventEdgeSpec>>> {
        Err(DataflowError::ArgumentError(
            "operator does not bind event response descriptors".into(),
        ))
    }
    fn validate_event_source_plumbing(&self) -> DataflowResult<()> {
        Err(DataflowError::ArgumentError(
            "operator is not canonical source plumbing".into(),
        ))
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    fn is_committing(&self) -> bool {
        false
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: self.name().into(),
            fields: vec![],
        }
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        Ok(())
    }

    /// Private event-pair route only. Ordinary operators retain existing polling.
    fn event_pair_mode(&self) -> bool {
        false
    }
    fn event_input_count(&self) -> Option<usize> {
        self.event_pair_mode().then_some(2)
    }
    fn check_event_maintenance_input(
        &self,
        _index: usize,
        _counter: &CheckpointCounter,
        _closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        Err(anyhow!("event pair route received ordinary Data").into())
    }
    fn event_input_blocked(&self, _index: usize) -> bool {
        false
    }
    fn check_event_response_input(
        &self,
        _index: usize,
        _counter: &CheckpointCounter,
        _closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        Ok(())
    }
    fn check_event_control_input(
        &self,
        _index: usize,
        _signal: &SignalMessage,
        _counter: &CheckpointCounter,
        _closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        Ok(())
    }
    fn check_event_input_eof(
        &self,
        _index: usize,
        _counter: &CheckpointCounter,
        _closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        Ok(())
    }

    /// Explicit response callback. Ordinary native operators reject packets;
    /// future eligible scalar/aggregate/JOIN adapters must opt in individually.
    async fn process_response_index(
        &mut self,
        _index: usize,
        _in_partitions: usize,
        _response: EventResponse,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Err(arroyo_rpc::errors::DataflowError::ArgumentError(
            "operator does not accept event responses".into(),
        ))
    }

    #[allow(unused_variables)]
    async fn process_batch_index(
        &mut self,
        index: usize,
        in_partitions: usize,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.process_batch(batch, ctx, collector).await
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()>;

    #[allow(clippy::type_complexity)]
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        None
    }

    #[allow(unused_variables)]
    async fn handle_future_result(
        &mut self,
        result: Box<dyn Any + Send>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        Ok(Some(watermark))
    }

    #[allow(unused_variables)]
    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        warn!("default handling of commit with epoch {:?}", epoch);
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_tick(
        &mut self,
        tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct Registry {
    dylibs: Arc<std::sync::Mutex<HashMap<String, Arc<UdfDylib>>>>,
    udfs: HashMap<String, Arc<ScalarUDF>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    udwfs: HashMap<String, Arc<WindowUDF>>,
}

impl Registry {
    pub async fn load_dylib(
        &mut self,
        name: &str,
        config: &DylibUdfConfig,
    ) -> anyhow::Result<Arc<UdfDylib>> {
        {
            let dylibs = self.dylibs.lock().unwrap();
            if let Some(dylib) = dylibs.get(&config.dylib_path) {
                return Ok(Arc::clone(dylib));
            }
        }

        // Download a UDF dylib from the object store
        let signature = Signature::exact(config.arg_types.clone(), Volatility::Volatile);

        let udf = StorageProvider::get_url(&config.dylib_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Unable to fetch UDF dylib from '{}': {:?}",
                    config.dylib_path, e
                )
            });

        let local_udfs_dir = "/tmp/arroyo/local_udfs";
        tokio::fs::create_dir_all(local_udfs_dir)
            .await
            .map_err(|e| anyhow!("unable to create local udfs dir: {:?}", e))?;

        let dylib_file_name = Path::new(&config.dylib_path)
            .file_name()
            .ok_or_else(|| anyhow!("Invalid dylib path: {}", config.dylib_path))?;
        let local_dylib_path = Path::new(local_udfs_dir).join(dylib_file_name);

        // write the dylib to a local file if it's not already present
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&local_dylib_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&udf).await?;
                file.sync_all().await?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                // nothing to do, UDF already written
            }
            Err(e) => {
                bail!(
                    "Failed to write UDF dylib to {}: {}",
                    local_dylib_path.to_string_lossy(),
                    e
                );
            }
        };

        let interface = if config.is_async {
            UdfInterface::Async(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        } else {
            UdfInterface::Sync(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        };

        let udf = Arc::new(UdfDylib::new(
            name.to_string(),
            signature,
            config.return_type.clone(),
            interface,
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(config.dylib_path.clone(), udf.clone());

        if !config.is_async {
            self.add_udfs(&udf, config);
        }

        Ok(udf)
    }

    pub fn add_local_udf(&mut self, local_udf: &LocalUdf) {
        let udf = Arc::new(UdfDylib::new(
            (*local_udf.config.name).to_string(),
            (*local_udf.config.signature).clone(),
            (*local_udf.config.return_type).clone(),
            local_udf.config.udf.clone(),
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(local_udf.config.name.to_string(), udf.clone());

        if !local_udf.is_async {
            self.add_udfs(
                &udf,
                &DylibUdfConfig {
                    dylib_path: local_udf.config.name.to_string(),
                    arg_types: match &local_udf.config.signature.type_signature {
                        TypeSignature::Exact(exact) => exact.clone(),
                        _ => {
                            panic!("only exact type signatures are supported for udfs")
                        }
                    },
                    return_type: (*local_udf.config.return_type).clone(),
                    aggregate: local_udf.is_aggregate,
                    is_async: local_udf.is_async,
                },
            );
        }
    }

    fn add_udfs(&mut self, dylib: &UdfDylib, config: &DylibUdfConfig) {
        let dylib: SyncUdfDylib = dylib.try_into().unwrap();
        if config.aggregate {
            let output_type = Arc::new(config.return_type.clone());

            let args: Vec<_> = config
                .arg_types
                .iter()
                .map(|arg| {
                    UdafArg::new(match arg {
                        DataType::List(f) => Arc::clone(f),
                        _ => {
                            panic!("arg type {:?} for UDAF {} is not a list", arg, dylib.name())
                        }
                    })
                })
                .collect();

            let name = dylib.name().to_string();
            let udaf = Arc::new(create_udaf(
                &name,
                config
                    .arg_types
                    .iter()
                    .map(|t| inner_type(t).expect("UDAF arg is not a vec"))
                    .collect(),
                Arc::new(config.return_type.clone()),
                Volatility::Volatile,
                Arc::new(move |_| {
                    Ok(Box::new(ArroyoUdaf::new(
                        args.clone(),
                        output_type.clone(),
                        Arc::new(dylib.clone()),
                    )))
                }),
                Arc::new(config.arg_types.clone()),
            ));
            self.udafs.insert(name, udaf);
        } else {
            self.udfs
                .insert(dylib.name().to_string(), Arc::new(ScalarUDF::from(dylib)));
        }
    }

    pub async fn add_python_udf(&mut self, udf: &PythonUdfConfig) -> anyhow::Result<()> {
        let udf = PythonUDF::parse(&*udf.definition).await?;

        self.udfs.insert((*udf.name).clone(), Arc::new(udf.into()));

        Ok(())
    }

    pub fn get_dylib(&self, path: &str) -> Option<Arc<UdfDylib>> {
        self.dylibs.lock().unwrap().get(path).cloned()
    }

    pub fn add_udf(&mut self, udf: Arc<ScalarUDF>) {
        self.udfs.insert(udf.name().to_string(), udf);
    }

    pub fn add_udaf(&mut self, udaf: Arc<AggregateUDF>) {
        self.udafs.insert(udaf.name().to_string(), udaf);
    }
}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> DFResult<Arc<ScalarUDF>> {
        self.udfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDF {name} not found")))
    }

    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        self.udafs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDAF {name} not found")))
    }

    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        self.udwfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDWF {name} not found")))
    }

    fn register_function_rewrite(
        &mut self,
        _: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> DFResult<()> {
        // no-op
        Ok(())
    }

    fn register_expr_planner(&mut self, _expr_planner: Arc<dyn ExprPlanner>) -> DFResult<()> {
        // no-op
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> DFResult<Option<Arc<ScalarUDF>>> {
        Ok(self.udfs.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> DFResult<Option<Arc<AggregateUDF>>> {
        Ok(self.udafs.insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> DFResult<Option<Arc<WindowUDF>>> {
        Ok(self.udwfs.insert(udwf.name().to_string(), udwf))
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }
}

#[cfg(test)]
mod event_pair_input_tests {
    use super::*;
    use crate::context::batch_bounded;
    use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
    use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, mpsc::UnboundedSender};

    // This is a run-loop test operator, not a native JOIN implementation. The
    // worker EventPair has separate direct tests. No SQL output is fabricated.
    struct PairProbe {
        waiting: Option<(usize, EventContext)>,
        processing: bool,
        observed: UnboundedSender<String>,
        phase: Arc<AtomicUsize>,
        delivered: Arc<Notify>,
        committing: bool,
    }
    impl PairProbe {
        fn require_ready(
            &self,
            index: usize,
            counter: &CheckpointCounter,
            closed: &HashSet<usize>,
        ) -> DataflowResult<()> {
            if index >= 2
                || !counter.all_clear()
                || !closed.is_empty()
                || self.event_input_blocked(index)
            {
                return Err(anyhow!("pair response crossed checkpoint/terminal/park gate").into());
            }
            Ok(())
        }
    }
    #[async_trait]
    impl ArrowOperator for PairProbe {
        fn name(&self) -> String {
            "pair-runloop-probe".into()
        }
        fn event_pair_mode(&self) -> bool {
            true
        }
        fn event_input_blocked(&self, side: usize) -> bool {
            self.processing || self.waiting.is_some_and(|(first, _)| first == side)
        }
        fn check_event_response_input(
            &self,
            side: usize,
            counter: &CheckpointCounter,
            closed: &HashSet<usize>,
        ) -> DataflowResult<()> {
            self.require_ready(side, counter, closed)
        }
        fn check_event_control_input(
            &self,
            side: usize,
            message: &SignalMessage,
            counter: &CheckpointCounter,
            closed: &HashSet<usize>,
        ) -> DataflowResult<()> {
            if closed.contains(&side) || counter.is_blocked(side) || self.event_input_blocked(side)
            {
                return Err(anyhow!("pair control bypassed parked/closed input").into());
            }
            if !closed.is_empty() && matches!(message, SignalMessage::Barrier(_)) {
                return Err(anyhow!("barrier follows a terminal branch input").into());
            }
            if self.waiting.is_some()
                && (!counter.all_clear() || !matches!(message, SignalMessage::Watermark(_)))
            {
                return Err(anyhow!("barrier/terminal overtook missing pair response").into());
            }
            match message {
                SignalMessage::Watermark(watermark) => {
                    self.observed
                        .send(format!("input-watermark-{side}-{watermark:?}"))
                        .unwrap();
                }
                SignalMessage::Stop | SignalMessage::EndOfData => {
                    self.observed
                        .send(format!("input-terminal-{side}"))
                        .unwrap();
                }
                SignalMessage::Barrier(_) => {}
            }
            Ok(())
        }
        fn check_event_input_eof(
            &self,
            side: usize,
            counter: &CheckpointCounter,
            _: &HashSet<usize>,
        ) -> DataflowResult<()> {
            if self.waiting.is_some() || self.processing || !counter.all_clear() {
                return Err(anyhow!("EOF with unfinished event/checkpoint").into());
            }
            self.observed.send(format!("input-eof-{side}")).unwrap();
            Ok(())
        }
        async fn process_batch(
            &mut self,
            _: RecordBatch,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<()> {
            Err(anyhow!("pair probe received Data").into())
        }
        async fn process_response_index(
            &mut self,
            side: usize,
            partitions: usize,
            response: EventResponse,
            _: &mut OperatorContext,
            collector: &mut dyn Collector,
        ) -> DataflowResult<()> {
            assert_eq!(partitions, 2);
            if let Some((first, context)) = self.waiting {
                assert_ne!(first, side);
                assert_eq!(context, response.context);
                self.processing = true;
                self.phase.store(2, Ordering::SeqCst);
                self.observed.send("processing".into()).unwrap();
                collector.collect_response(response).await?;
                // Keep Processing through the ACTUAL awaited bounded collector.
                self.processing = false;
                self.waiting = None;
                self.phase.store(0, Ordering::SeqCst);
                self.delivered.notify_one();
            } else {
                self.waiting = Some((side, response.context));
                self.phase.store(1, Ordering::SeqCst);
                self.observed
                    .send(format!("waiting-{side}-{}", response.context.sequence))
                    .unwrap();
            }
            Ok(())
        }
        async fn handle_watermark(
            &mut self,
            watermark: Watermark,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<Option<Watermark>> {
            let _ = watermark;
            Ok(None)
        }
        fn is_committing(&self) -> bool {
            self.committing
        }
        async fn handle_commit(
            &mut self,
            _: u32,
            _: &HashMap<String, HashMap<u32, Vec<u8>>>,
            _: &mut OperatorContext,
        ) -> DataflowResult<()> {
            self.observed.send("commit".into()).unwrap();
            Ok(())
        }
        async fn handle_checkpoint(
            &mut self,
            _: CheckpointBarrier,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<()> {
            self.observed.send("checkpoint".into()).unwrap();
            Ok(())
        }
    }
    fn response(sequence: u64) -> ArrowMessage {
        ArrowMessage::Event(EventResponse {
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
        })
    }
    fn barrier(epoch: u32) -> ArrowMessage {
        ArrowMessage::Signal(SignalMessage::Barrier(CheckpointBarrier {
            completed_base_epoch: None,
            epoch,
            min_epoch: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            then_stop: false,
        }))
    }
    struct Running {
        input: [crate::context::BatchSender; 2],
        output: crate::context::BatchReceiver,
        commands: Sender<ControlMessage>,
        observed: tokio::sync::mpsc::UnboundedReceiver<String>,
        events: Receiver<ControlResp>,
        phase: Arc<AtomicUsize>,
        delivered: Arc<Notify>,
        task: tokio::task::JoinHandle<DataflowResult<Option<SignalMessage>>>,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Running {
        async fn observe(&mut self) -> String {
            tokio::time::timeout(Duration::from_secs(3), self.observed.recv())
                .await
                .unwrap()
                .unwrap()
        }
        async fn alignment_started(&mut self) {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match self.events.recv().await {
                        Some(ControlResp::CheckpointEvent(event))
                            if event.event_type == TaskCheckpointEventType::StartedAlignment =>
                        {
                            break;
                        }
                        Some(_) => {}
                        None => panic!("run loop ended before actual checkpoint alignment"),
                    }
                }
            })
            .await
            .unwrap();
        }
        async fn abort_drain(&mut self) {
            self.task.abort();
            let result = tokio::time::timeout(Duration::from_secs(3), &mut self.task)
                .await
                .unwrap();
            assert!(result.is_err_and(|error| error.is_cancelled()));
        }
        async fn error(&mut self, message: &str) {
            let result = tokio::time::timeout(Duration::from_secs(3), &mut self.task)
                .await
                .unwrap()
                .unwrap();
            assert!(result.unwrap_err().to_string().contains(message));
        }
    }
    struct OrdinaryHead;
    #[async_trait]
    impl ArrowOperator for OrdinaryHead {
        fn name(&self) -> String {
            "ordinary-head-probe".into()
        }
        async fn process_batch(
            &mut self,
            _: RecordBatch,
            _: &mut OperatorContext,
            _: &mut dyn Collector,
        ) -> DataflowResult<()> {
            Ok(())
        }
    }
    async fn start(restored: bool, block_output: bool) -> Running {
        start_shape(restored, block_output, false).await
    }
    async fn start_shape(restored: bool, block_output: bool, pair_as_tail: bool) -> Running {
        let (control, events) = tokio::sync::mpsc::channel(128);
        let (commands, command_rx) = tokio::sync::mpsc::channel(8);
        let (observed, observed_rx) = tokio::sync::mpsc::unbounded_channel();
        let phase = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(Notify::new());
        let task = Arc::new(TaskInfo {
            job_id: format!(
                "pair-runloop-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ),
            operator_idx: 97,
            operator_name: "pair-probe".into(),
            operator_id: "pair-probe".into(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
            checkpoint_file_path_layout: Default::default(),
        });
        let mut context = OperatorContext::new(
            task.clone(),
            None,
            control.clone(),
            2,
            vec![],
            None,
            HashMap::new(),
        )
        .await;
        if restored {
            context.restore_commit_epoch = Some(7);
        }
        let mut operator = ChainedOperator::new(
            Box::new(PairProbe {
                waiting: None,
                processing: false,
                observed,
                phase: phase.clone(),
                delivered: delivered.clone(),
                committing: restored,
            }),
            context,
        );
        if pair_as_tail {
            let head_context = OperatorContext::new(
                task.clone(),
                None,
                control.clone(),
                2,
                vec![],
                None,
                HashMap::new(),
            )
            .await;
            let mut head = ChainedOperator::new(Box::new(OrdinaryHead), head_context);
            head.next = Some(Box::new(operator));
            operator = head;
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "_timestamp",
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        let edge = Arc::new(EventEdgeSpec {
            maintenance: false,
            root_operator: 7,
            root_subtask: 0,
            schema: schema.clone(),
            layout: EventLayout::Raw,
        });
        let info = Arc::new(ChainInfo {
            job_id: task.job_id.clone(),
            task_id: 97,
            description: "pair-runloop-probe".into(),
            task_index: 0,
        });
        let (out, output) = batch_bounded(1);
        if block_output {
            out.send(ArrowMessage::Signal(SignalMessage::Stop))
                .await
                .unwrap();
        }
        let mut collector = ArrowCollector::new(
            info,
            Some(Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(
                schema, 0,
            ))),
            vec![vec![out]],
        );
        collector.register_event_output(edge).unwrap();
        let (left, left_rx) = batch_bounded(1);
        let (right, right_rx) = batch_bounded(1);
        let task = tokio::spawn(async move {
            operator_run_behavior(
                &mut operator,
                &mut [left_rx, right_rx],
                control,
                command_rx,
                &mut collector,
                Arc::new(Barrier::new(1)),
            )
            .await
        });
        Running {
            input: [left, right],
            output,
            commands,
            observed: observed_rx,
            events,
            phase,
            delivered,
            task,
        }
    }

    #[tokio::test]
    async fn pair_runloop_parks_only_unmatched_side_and_retains_processing_through_collect() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for first in [0, 1] {
                let mut run = start(false, true).await;
                run.input[first]
                    .send(ArrowMessage::Signal(SignalMessage::Watermark(
                        Watermark::EventTime(SystemTime::UNIX_EPOCH),
                    )))
                    .await
                    .unwrap();
                assert!(run.observe().await.starts_with("input-watermark-"));
                run.input[first].send(response(1)).await.unwrap();
                assert_eq!(run.observe().await, format!("waiting-{first}-1"));
                run.input[first].send(response(2)).await.unwrap();
                let sender = run.input[first].clone();
                let mut third = Box::pin(sender.send(response(3)));
                assert!(futures::poll!(&mut third).is_pending());
                let other = 1 - first;
                for watermark in [
                    Watermark::EventTime(SystemTime::UNIX_EPOCH),
                    Watermark::Idle,
                ] {
                    run.input[other]
                        .send(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
                        .await
                        .unwrap();
                    assert!(run.observe().await.starts_with("input-watermark-"));
                    assert_eq!(run.phase.load(Ordering::SeqCst), 1);
                }
                run.input[other].send(response(1)).await.unwrap();
                assert_eq!(run.observe().await, "processing");
                assert_eq!(run.phase.load(Ordering::SeqCst), 2);
                assert!(futures::poll!(&mut third).is_pending());
                assert_eq!(
                    run.output.recv().await,
                    Some(ArrowMessage::Signal(SignalMessage::Stop))
                );
                tokio::time::timeout(Duration::from_secs(3), run.delivered.notified())
                    .await
                    .unwrap();
                assert_eq!(run.output.recv().await, Some(response(1)));
                assert_eq!(run.observe().await, format!("waiting-{first}-2"));
                tokio::time::timeout(Duration::from_secs(3), third)
                    .await
                    .unwrap()
                    .unwrap();
                run.abort_drain().await;
            }
        })
        .await
        .expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_pending_pair_rejects_barrier_and_terminal_overtake() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for control in [
                barrier(1),
                ArrowMessage::Signal(SignalMessage::Stop),
                ArrowMessage::Signal(SignalMessage::EndOfData),
            ] {
                let mut run = start(false, false).await;
                run.input[0].send(response(1)).await.unwrap();
                assert_eq!(run.observe().await, "waiting-0-1");
                run.input[1].send(control).await.unwrap();
                run.error("barrier/terminal overtook").await;
            }
        })
        .await
        .expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_unfinished_mate_eof_is_not_silent_success() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut run = start(false, false).await;
            run.input[0].send(response(1)).await.unwrap();
            assert_eq!(run.observe().await, "waiting-0-1");
            // Replacing and dropping the actual last sender closes indexed input1.
            let (replacement, _unused) = batch_bounded(1);
            drop(std::mem::replace(&mut run.input[1], replacement));
            run.error("EOF with unfinished").await;
        })
        .await
        .expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_actual_counter_blocks_cross_prefix_response_in_both_orders() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for first in [0, 1] {
                let mut run = start(false, false).await;
                run.input[first].send(barrier(1)).await.unwrap();
                run.alignment_started().await;
                // FIFO watermark proves the other side remains pollable, while
                // its response must fail the actual partially marked counter gate.
                run.input[1 - first]
                    .send(ArrowMessage::Signal(SignalMessage::Watermark(
                        Watermark::Idle,
                    )))
                    .await
                    .unwrap();
                assert!(run.observe().await.starts_with("input-watermark-"));
                run.input[1 - first].send(response(1)).await.unwrap();
                run.error("checkpoint/terminal/park").await;
            }
        })
        .await
        .expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_actual_aligned_checkpoint_releases_both_inputs() {
        tokio::time::timeout(Duration::from_secs(15), async {
        for first in [0,1] {
            let mut run = start(false,false).await;
            run.input[first].send(barrier(1)).await.unwrap();
            run.alignment_started().await;
            run.input[1-first].send(barrier(1)).await.unwrap();
            assert_eq!(run.observe().await,"checkpoint");
            let barrier = tokio::time::timeout(Duration::from_secs(3),run.output.recv()).await.unwrap().unwrap();
            assert!(matches!(barrier,ArrowMessage::Signal(SignalMessage::Barrier(checkpoint)) if checkpoint.epoch==1));
            run.input[first].send(response(1)).await.unwrap();
            let waiting=run.observe().await;
            assert_eq!(waiting,format!("waiting-{first}-1"));
            run.input[1-first].send(response(1)).await.unwrap();
            assert_eq!(run.observe().await,"processing");
            assert_eq!(tokio::time::timeout(Duration::from_secs(3),run.output.recv()).await.unwrap(),Some(response(1)));
            run.abort_drain().await;
        }

        }).await.expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_restored_commit_precedes_responses_and_rejects_wrong_epoch() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for epoch in [7, 8] {
                let mut run = start(true, false).await;
                run.input[0].send(response(1)).await.unwrap();
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(3), run.events.recv())
                        .await
                        .unwrap(),
                    Some(ControlResp::TaskStarted { .. })
                ));
                run.commands
                    .send(ControlMessage::Commit {
                        epoch,
                        commit_data: HashMap::new(),
                    })
                    .await
                    .unwrap();
                if epoch == 7 {
                    assert_eq!(run.observe().await, "commit");
                    assert_eq!(run.observe().await, "waiting-0-1");
                    run.abort_drain().await;
                } else {
                    run.error("restore commit epoch mismatch").await;
                    assert!(run.observed.try_recv().is_err());
                }
            }
        })
        .await
        .expect("finite actual run-loop scenario timed out");
    }
    #[tokio::test]
    async fn pair_runloop_terminal_side_cannot_be_bypassed_by_later_response() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for terminal in [SignalMessage::Stop, SignalMessage::EndOfData] {
                let mut run = start(false, false).await;
                run.input[0]
                    .send(ArrowMessage::Signal(terminal))
                    .await
                    .unwrap();
                assert_eq!(run.observe().await, "input-terminal-0");
                run.input[1].send(response(1)).await.unwrap();
                run.error("checkpoint/terminal/park").await;
            }
        })
        .await
        .expect("finite terminal scenario");
    }
    #[tokio::test]
    async fn pair_runloop_rejects_pair_chained_after_ordinary_head() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut run = start_shape(false, false, true).await;
            run.error("physical chain head").await;
            assert!(run.observed.try_recv().is_err());
        })
        .await
        .expect("finite physical head scenario");
    }

    #[tokio::test]
    async fn pair_runloop_barrier_after_either_terminal_cannot_finish_successfully() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for first in [0, 1] {
                for explicit in [false, true] {
                    let mut run = start(false, false).await;
                    if explicit {
                        run.input[first]
                            .send(ArrowMessage::Signal(SignalMessage::EndOfData))
                            .await
                            .unwrap();
                        assert_eq!(run.observe().await, format!("input-terminal-{first}"));
                    } else {
                        let (replacement, _unused) = batch_bounded(1);
                        drop(std::mem::replace(&mut run.input[first], replacement));
                        assert_eq!(run.observe().await, format!("input-eof-{first}"));
                    }
                    // A real trailing watermark on the live mate is allowed.
                    run.input[1 - first]
                        .send(ArrowMessage::Signal(SignalMessage::Watermark(
                            Watermark::Idle,
                        )))
                        .await
                        .unwrap();
                    assert!(run.observe().await.starts_with("input-watermark-"));
                    run.input[1 - first].send(barrier(1)).await.unwrap();
                    run.error("barrier follows a terminal branch input").await;
                    assert!(
                        run.observed.try_recv().is_err(),
                        "terminal barrier must not checkpoint or invoke event callback"
                    );
                }
            }
        })
        .await
        .expect("finite terminal-to-barrier scenario");
    }
}
