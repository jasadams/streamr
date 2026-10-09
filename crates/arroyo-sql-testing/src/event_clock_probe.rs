//! Opt-in observations of the actual reconstructed watermark operator. The
//! wrapper preserves its state namespace and forwards every callback it uses.
use arrow::array::{
    BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StructArray,
    TimestampNanosecondArray,
};
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::{ArrowOperator, OperatorNode},
};
use arroyo_rpc::{errors::DataflowResult, grpc::rpc::TableConfig};
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use arroyo_worker::engine::{Program, SubtaskOrQueueNode};
use async_trait::async_trait;
use serde_json::json;
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

type Trace = Arc<Mutex<File>>;
fn write(trace: &Trace, value: serde_json::Value) {
    let mut file = trace.lock().unwrap();
    serde_json::to_writer(&mut *file, &value).unwrap();
    file.write_all(b"\n").unwrap();
    file.flush().unwrap();
}
fn nanos(time: SystemTime) -> i128 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    }
}

pub fn install(program: &Program, output: &Path, phase: &str) {
    if std::env::var("STREAMR_CAPTURE_EVENT_CLOCK_PROBE").as_deref() != Ok("1") {
        return;
    }
    let path = output.with_extension(format!("{phase}.probe.jsonl"));
    let trace = Arc::new(Mutex::new(File::create(path).unwrap()));
    let mut graph = program.graph.write().unwrap();
    let mut installed = [0, 0];
    for node in graph.node_weights_mut() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Chained(chain) = &mut node.node
        {
            let mut current = Some(chain);
            while let Some(chain) = current {
                let role = match chain.operator.name().as_str() {
                    "expression_watermark_generator" => Some(0),
                    "SingleFileSink" => Some(1),
                    _ => None,
                };
                if let Some(role) = role {
                    let inner = std::mem::replace(&mut chain.operator, Box::new(Empty));
                    chain.operator = Box::new(Probe {
                        inner,
                        trace: trace.clone(),
                    });
                    installed[role] += 1;
                }
                current = chain.next.as_deref_mut();
            }
        }
    }
    assert_eq!(
        installed,
        [1, 1],
        "event clock probe requires one reconstructed watermark owner and sink"
    );
}
struct Empty;
#[async_trait]
impl ArrowOperator for Empty {
    fn name(&self) -> String {
        unreachable!()
    }
    async fn process_batch(
        &mut self,
        _: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        unreachable!()
    }
}
struct Probe {
    inner: Box<dyn ArrowOperator>,
    trace: Trace,
}
struct ProbeCollector<'a> {
    inner: &'a mut dyn Collector,
    trace: Trace,
}
#[async_trait]
impl Collector for ProbeCollector<'_> {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        let time = batch
            .column_by_name("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let pk = batch
            .column_by_name("session_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let meta = batch
            .column_by_name("_updating_meta")
            .map(|a| a.as_any().downcast_ref::<StructArray>().unwrap());
        for i in 0..batch.num_rows() {
            let (id, retract) = if let Some(meta) = meta {
                let id = meta
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let retract = meta
                    .column_by_name("is_retract")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                (
                    Some(
                        id.value(i)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>(),
                    ),
                    retract.value(i),
                )
            } else {
                (None, false)
            };
            write(
                &self.trace,
                json!({"kind":"row", "session_id":pk.value(i), "raw_nanos":time.value(i), "id":id, "retract":retract}),
            );
        }
        self.inner.collect(batch).await
    }
    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        if let Watermark::EventTime(time) = watermark {
            write(
                &self.trace,
                json!({"kind":"watermark", "nanos":nanos(time).to_string()}),
            );
        }
        self.inner.broadcast_watermark(watermark).await
    }
}
#[async_trait]
impl ArrowOperator for Probe {
    fn name(&self) -> String {
        self.inner.name()
    }
    fn tables(&self) -> HashMap<String, TableConfig> {
        self.inner.tables()
    }
    fn tick_interval(&self) -> Option<Duration> {
        self.inner.tick_interval()
    }
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.inner.on_start(ctx).await?;
        if self.inner.name() == "SingleFileSink" {
            write(
                &self.trace,
                json!({"kind":"start_watermark", "nanos":ctx.last_present_watermark().map(|time| nanos(time).to_string())}),
            );
        }
        Ok(())
    }
    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner
            .process_batch(
                batch,
                ctx,
                &mut ProbeCollector {
                    inner: collector,
                    trace: self.trace.clone(),
                },
            )
            .await
    }
    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.handle_checkpoint(b, ctx, collector).await
    }
    async fn handle_tick(
        &mut self,
        tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner
            .handle_tick(
                tick,
                ctx,
                &mut ProbeCollector {
                    inner: collector,
                    trace: self.trace.clone(),
                },
            )
            .await
    }
    async fn on_close(
        &mut self,
        message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner
            .on_close(
                message,
                ctx,
                &mut ProbeCollector {
                    inner: collector,
                    trace: self.trace.clone(),
                },
            )
            .await
    }
}
