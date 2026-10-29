//! External test-only adapter. No fabricated responses or SQL expectations.
//! Depends on reviewed planner + Engine event descriptor wiring; not registered.
use super::*;
use arrow::array::{Array, RecordBatch, UInt64Array};
use arroyo_datastream::logical::LogicalEdgeType;
use arroyo_operator::CheckpointCounter;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, EventResponseRole};
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::{EventContext, EventResponse};
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use petgraph::visit::EdgeRef;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Raw,
    Root,
    Left,
    Right,
    Join,
    Target,
    Sink,
}
#[derive(Clone, Debug)]
struct Boundary {
    id: String,
    stage: Stage,
}
#[derive(Clone, Debug)]
struct Observation {
    stage: Stage,
    id: String,
    kind: &'static str,
    context: Option<EventContext>,
    input: Option<usize>,
    rows: usize,
    keys: usize,
}
impl Observation {
    fn json(&self) -> Value {
        let context = self.context.map(|context| {
            serde_json::json!({
                "version":context.version,"root_operator":context.root_operator,
                "root_subtask":context.root_subtask,"attempt_nonce":context.attempt_nonce,
                "sequence":context.sequence,"trigger_nanos":context.trigger_nanos,
            })
        });
        serde_json::json!({"stage":format!("{:?}",self.stage),"operator_id":self.id,
            "kind":self.kind,"context":context,"input":self.input,"rows":self.rows,"touched_keys":self.keys})
    }
}
#[derive(Default)]
struct JournalState {
    observations: Vec<Observation>,
    raw_batches: Vec<usize>,
    image_batches: Vec<usize>,
    image_runs: Vec<Vec<usize>>,
    sink_rows: HashMap<String, Vec<Value>>,
}
struct Journal {
    state: Mutex<JournalState>,
    changed: tokio::sync::Notify,
    bound: usize,
}
impl Journal {
    fn new(raw_operations: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(JournalState::default()),
            changed: tokio::sync::Notify::new(),
            bound: raw_operations.checked_mul(64).unwrap(),
        })
    }
    fn observe(&self, observation: Observation) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.observations.len() < self.bound,
            "finite fixture observation bound exceeded"
        );
        state.observations.push(observation);
        drop(state);
        self.changed.notify_one();
    }
    async fn wait(&self, predicate: impl Fn(&JournalState) -> bool) {
        tokio::time::timeout(test_runtime_timeout(), async {
            loop {
                let changed = self.changed.notified();
                if predicate(&self.state.lock().unwrap()) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("actual runtime observation handshake timed out");
    }
    fn save(&self, path: &Path) {
        let state = self.state.lock().unwrap();
        std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({
            "raw_batches":state.raw_batches,"image_batches":state.image_batches,
            "image_runs":state.image_runs,"observations":state.observations.iter().map(Observation::json).collect::<Vec<_>>(),
            "sink_callback_rows":state.sink_rows,
        })).unwrap()).unwrap();
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Deliver,
    Fail,
}
#[derive(Clone, Copy)]
enum Selector {
    Sequence(u64),
    DataRowsBefore(usize),
}
struct Gate {
    stage: Stage,
    selector: Selector,
    outcome: Outcome,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    observation: Mutex<Option<Observation>>,
    data_rows: AtomicUsize,
}
impl Gate {
    fn new(stage: Stage, selector: Selector, outcome: Outcome) -> Arc<Self> {
        Arc::new(Self {
            stage,
            selector,
            outcome,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            observation: Mutex::new(None),
            data_rows: AtomicUsize::new(0),
        })
    }
    async fn before(&self, observation: Observation) -> DataflowResult<()> {
        if observation.stage != self.stage {
            return Ok(());
        }
        let selected = match self.selector {
            Selector::Sequence(sequence) => observation
                .context
                .is_some_and(|context| context.sequence == sequence),
            Selector::DataRowsBefore(before) => {
                self.data_rows.fetch_add(observation.rows, Ordering::SeqCst) == before
            }
        };
        if !selected {
            return Ok(());
        }
        {
            let mut stored = self.observation.lock().unwrap();
            assert!(stored.is_none(), "one real selected delivery only");
            *stored = Some(observation);
        }
        self.entered.notify_one();
        self.release.notified().await;
        if self.outcome == Outcome::Fail {
            return Err(DataflowError::InternalOperatorError {
                error: "STR64 native branch delivery refusal",
                message: "test-only refusal at real collector delivery".into(),
            });
        }
        Ok(())
    }
    async fn reached(&self) -> Observation {
        tokio::time::timeout(test_runtime_timeout(), self.entered.notified())
            .await
            .expect("selected real delivery never reached gate");
        self.observation.lock().unwrap().clone().unwrap()
    }
}
struct OutputObserver<'a> {
    inner: &'a mut dyn Collector,
    boundary: Boundary,
    journal: Arc<Journal>,
    gate: Option<Arc<Gate>>,
    context: Option<EventContext>,
    emitted: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Collector for OutputObserver<'_> {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        let observation = Observation {
            stage: self.boundary.stage,
            id: self.boundary.id.clone(),
            kind: "data_delivery",
            context: self.context,
            input: None,
            rows: batch.num_rows(),
            keys: 0,
        };
        self.journal.observe(observation.clone());
        self.emitted.fetch_add(batch.num_rows(), Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.before(observation).await?;
        }
        self.inner.collect(batch).await
    }
    async fn collect_response(&mut self, response: EventResponse) -> DataflowResult<()> {
        let observation = Observation {
            stage: self.boundary.stage,
            id: self.boundary.id.clone(),
            kind: "response_delivery",
            context: Some(response.context),
            input: None,
            rows: response.rows.as_ref().map_or(0, RecordBatch::num_rows),
            keys: response
                .touched_keys
                .as_ref()
                .map_or(0, RecordBatch::num_rows),
        };
        self.journal.observe(observation.clone());
        if let Some(gate) = &self.gate {
            gate.before(observation).await?;
        }
        self.inner.collect_response(response).await
    }
    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.inner.broadcast_watermark(watermark).await
    }
}
struct ObservedOperator {
    inner: Box<dyn ArrowOperator>,
    boundary: Boundary,
    journal: Arc<Journal>,
    gate: Option<Arc<Gate>>,
}
impl ObservedOperator {
    fn collector<'a>(
        &self,
        inner: &'a mut dyn Collector,
        context: Option<EventContext>,
        emitted: Arc<AtomicUsize>,
    ) -> OutputObserver<'a> {
        OutputObserver {
            inner,
            boundary: self.boundary.clone(),
            journal: self.journal.clone(),
            gate: self.gate.clone(),
            context,
            emitted,
        }
    }
    async fn data(
        &mut self,
        index: usize,
        parts: usize,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.boundary.stage == Stage::Raw {
            let mut state = self.journal.state.lock().unwrap();
            assert!(state.raw_batches.len() < self.journal.bound / 64);
            state.raw_batches.push(batch.num_rows());
        }
        if self.boundary.stage == Stage::Root {
            let runs =
                if let Some(markers) = batch.column_by_name(arroyo_rpc::SOURCE_ENVELOPE_FIELD) {
                    let markers = markers
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("native source operation markers must be UInt64");
                    assert_eq!(markers.null_count(), 0);
                    let mut runs = Vec::new();
                    let mut last = None;
                    for marker in markers.values().iter() {
                        if last == Some(*marker) {
                            *runs.last_mut().unwrap() += 1;
                        } else {
                            runs.push(1_usize);
                            last = Some(*marker);
                        }
                    }
                    assert!(runs.iter().all(|count| matches!(*count, 1 | 2)));
                    runs
                } else {
                    vec![1; batch.num_rows()]
                };
            let mut state = self.journal.state.lock().unwrap();
            assert!(state.image_batches.len() < self.journal.bound / 64);
            state.image_batches.push(batch.num_rows());
            state.image_runs.push(runs);
        }
        let sink_rows = (self.boundary.stage == Stage::Sink).then(|| batch_json(&batch));
        let mut output = self.collector(collector, None, Arc::new(AtomicUsize::new(0)));
        self.inner
            .process_batch_index(index, parts, batch, ctx, &mut output)
            .await?;
        if let Some(rows) = sink_rows {
            let mut state = self.journal.state.lock().unwrap();
            let stored = state.sink_rows.entry(self.boundary.id.clone()).or_default();
            assert!(stored.len().checked_add(rows.len()).unwrap() <= self.journal.bound / 32);
            stored.extend(rows);
            drop(state);
            self.journal.changed.notify_one();
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl ArrowOperator for ObservedOperator {
    fn name(&self) -> String {
        self.inner.name()
    }
    fn display(&self) -> arroyo_operator::operator::DisplayableOperator<'_> {
        self.inner.display()
    }

    fn tables(&self) -> HashMap<String, arroyo_rpc::grpc::rpc::TableConfig> {
        self.inner.tables()
    }
    fn is_committing(&self) -> bool {
        self.inner.is_committing()
    }
    fn tick_interval(&self) -> Option<Duration> {
        self.inner.tick_interval()
    }
    fn event_response_role(&self) -> EventResponseRole {
        self.inner.event_response_role()
    }
    fn event_response_output(&self) -> Option<Arc<EventEdgeSpec>> {
        self.inner.event_response_output()
    }
    fn event_response_key_types(&self) -> Option<Vec<arrow::datatypes::DataType>> {
        self.inner.event_response_key_types()
    }
    fn bind_event_response_key_types(
        &self,
        inputs: &[Option<Vec<arrow::datatypes::DataType>>],
    ) -> DataflowResult<()> {
        self.inner.bind_event_response_key_types(inputs)
    }
    fn event_input_count(&self) -> Option<usize> {
        self.inner.event_input_count()
    }
    fn check_event_maintenance_input(
        &self,
        index: usize,
        counter: &arroyo_operator::CheckpointCounter,
        closed: &std::collections::HashSet<usize>,
    ) -> DataflowResult<()> {
        self.inner
            .check_event_maintenance_input(index, counter, closed)
    }
    fn bind_event_response(
        &mut self,
        inputs: &[Arc<EventEdgeSpec>],
        context: &OperatorContext,
    ) -> DataflowResult<Option<Arc<EventEdgeSpec>>> {
        self.inner.bind_event_response(inputs, context)
    }
    fn validate_event_source_plumbing(&self) -> DataflowResult<()> {
        self.inner.validate_event_source_plumbing()
    }
    fn event_pair_mode(&self) -> bool {
        self.inner.event_pair_mode()
    }
    fn event_input_blocked(&self, index: usize) -> bool {
        self.inner.event_input_blocked(index)
    }
    fn check_event_response_input(
        &self,
        index: usize,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        self.inner
            .check_event_response_input(index, counter, closed)
    }
    fn check_event_control_input(
        &self,
        index: usize,
        signal: &SignalMessage,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        self.inner
            .check_event_control_input(index, signal, counter, closed)
    }
    fn check_event_input_eof(
        &self,
        index: usize,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        self.inner.check_event_input_eof(index, counter, closed)
    }
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        assert_eq!(ctx.task_info.operator_id, self.boundary.id);
        if self.boundary.stage == Stage::Join {
            assert!(self.inner.event_pair_mode());
        }
        self.inner.on_start(ctx).await
    }
    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.data(0, 1, batch, ctx, collector).await
    }
    async fn process_batch_index(
        &mut self,
        index: usize,
        parts: usize,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.data(index, parts, batch, ctx, collector).await
    }
    async fn process_response_index(
        &mut self,
        index: usize,
        parts: usize,
        response: EventResponse,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let context = response.context;
        let emitted = Arc::new(AtomicUsize::new(0));
        let mut output = self.collector(collector, Some(context), emitted.clone());
        self.inner
            .process_response_index(index, parts, response, ctx, &mut output)
            .await?;
        self.journal.observe(Observation {
            stage: self.boundary.stage,
            id: self.boundary.id.clone(),
            kind: "input_consumed",
            context: Some(context),
            input: Some(index),
            rows: 0,
            keys: 0,
        });
        if self.boundary.stage == Stage::Join
            && !self.inner.event_input_blocked(0)
            && !self.inner.event_input_blocked(1)
        {
            self.journal.observe(Observation {
                stage: Stage::Join,
                id: self.boundary.id.clone(),
                kind: "pair_completed",
                context: Some(context),
                input: Some(index),
                rows: emitted.load(Ordering::SeqCst),
                keys: 0,
            });
        }
        Ok(())
    }
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        self.inner.future_to_poll()
    }
    async fn handle_future_result(
        &mut self,
        result: Box<dyn Any + Send>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner
            .handle_future_result(result, ctx, collector)
            .await
    }
    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        self.inner.handle_watermark(watermark, ctx, collector).await
    }
    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.handle_checkpoint(barrier, ctx, collector).await
    }
    async fn handle_commit(
        &mut self,
        epoch: u32,
        data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        self.inner.handle_commit(epoch, data, ctx).await
    }
    async fn handle_tick(
        &mut self,
        tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.handle_tick(tick, ctx, collector).await
    }
    async fn on_close(
        &mut self,
        message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.inner.on_close(message, ctx, collector).await
    }
}
fn batch_json(batch: &RecordBatch) -> Vec<Value> {
    let indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field.name() != arroyo_rpc::TIMESTAMP_FIELD).then_some(index))
        .collect::<Vec<_>>();
    let batch = batch.project(&indices).unwrap();
    let mut writer = arrow::json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow::json::writer::LineDelimited>(Vec::new());
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    json_rows(&writer.into_inner())
}

struct Boundaries {
    source: String,
    selected: Vec<Boundary>,
    root: u32,
}
fn boundaries(logical: &LogicalProgram) -> Boundaries {
    use arroyo_rpc::grpc::api::{ExpressionWatermarkConfig, JoinOperator};
    use prost::Message;
    let graph = &logical.graph;
    let one = |kind| {
        let found = graph
            .node_indices()
            .flat_map(|index| {
                graph[index]
                    .operator_chain
                    .iter()
                    .filter_map(move |(operator, _)| {
                        (operator.operator_name == kind).then_some((
                            index,
                            operator.operator_id.clone(),
                            operator.operator_config.clone(),
                        ))
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(found.len(), 1, "fixture must identify one {kind:?}");
        found.into_iter().next().unwrap()
    };
    let (source_node, source, _) = one(OperatorName::ConnectorSource);
    let (_root_node, root_id, root_config) = one(OperatorName::ExpressionWatermark);
    let root = ExpressionWatermarkConfig::decode(root_config.as_slice())
        .unwrap()
        .response_root_operator
        .expect("actual planned response root required");
    let (join_node, join, join_config) = one(OperatorName::Join);
    assert_eq!(
        JoinOperator::decode(join_config.as_slice())
            .unwrap()
            .response_root_operator,
        Some(root)
    );
    let (_, target, _) = one(OperatorName::FusedStateTable);
    let upstream = |side| {
        let incoming = graph
            .edges_directed(join_node, petgraph::Direction::Incoming)
            .filter(|edge| edge.weight().edge_type == side)
            .map(|edge| edge.source())
            .collect::<Vec<_>>();
        assert_eq!(incoming.len(), 1);
        let mut pending = incoming;
        let mut seen = HashSet::new();
        let mut found = HashSet::new();
        while let Some(index) = pending.pop() {
            if !seen.insert(index) {
                continue;
            }
            let ids = graph[index]
                .operator_chain
                .iter()
                .filter_map(|(operator, _)| {
                    (operator.operator_name == OperatorName::UpdatingAggregate)
                        .then_some(operator.operator_id.clone())
                })
                .collect::<Vec<_>>();
            if ids.is_empty() {
                pending.extend(graph.neighbors_directed(index, petgraph::Direction::Incoming));
            } else {
                found.extend(ids);
            }
        }
        assert_eq!(found.len(), 1);
        found.into_iter().next().unwrap()
    };
    let left = upstream(LogicalEdgeType::LeftJoin);
    let right = upstream(LogicalEdgeType::RightJoin);
    assert_ne!(left, right);
    let mut selected = vec![
        Boundary {
            id: root_id,
            stage: Stage::Root,
        },
        Boundary {
            id: left,
            stage: Stage::Left,
        },
        Boundary {
            id: right,
            stage: Stage::Right,
        },
        Boundary {
            id: join,
            stage: Stage::Join,
        },
        Boundary {
            id: target,
            stage: Stage::Target,
        },
    ];
    let mut node = source_node;
    let raw = loop {
        let operators = graph[node]
            .operator_chain
            .iter()
            .map(|(operator, _)| operator)
            .collect::<Vec<_>>();
        if let Some(operator) = operators
            .into_iter()
            .find(|operator| operator.operator_name != OperatorName::ConnectorSource)
        {
            assert!(
                matches!(
                    operator.operator_name,
                    OperatorName::ArrowValue | OperatorName::Projection
                ),
                "source raw observer must precede canonical unrolling/admission"
            );
            break operator.operator_id.clone();
        }
        let next = graph
            .neighbors_directed(node, petgraph::Direction::Outgoing)
            .collect::<Vec<_>>();
        assert_eq!(next.len(), 1, "one source prelude before split");
        node = next[0];
    };
    selected.push(Boundary {
        id: raw,
        stage: Stage::Raw,
    });
    for node in graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name == OperatorName::ConnectorSink {
                selected.push(Boundary {
                    id: operator.operator_id.clone(),
                    stage: Stage::Sink,
                });
            }
        }
    }
    assert_eq!(
        selected
            .iter()
            .filter(|boundary| boundary.stage == Stage::Sink)
            .count(),
        2
    );
    assert_eq!(
        selected
            .iter()
            .map(|boundary| &boundary.id)
            .collect::<HashSet<_>>()
            .len(),
        selected.len()
    );
    Boundaries {
        source,
        selected,
        root,
    }
}
fn install(
    program: &Program,
    boundaries: &Boundaries,
    journal: &Arc<Journal>,
    gate: Option<Arc<Gate>>,
) {
    use arroyo_operator::operator::OperatorNode;
    use arroyo_worker::engine::SubtaskOrQueueNode;
    let mut matched = HashSet::new();
    for node in program.graph.write().unwrap().node_weights_mut() {
        if let SubtaskOrQueueNode::SubtaskNode(node) = node
            && let OperatorNode::Chained(chain) = &mut node.node
        {
            let mut current = Some(chain);
            while let Some(chain) = current {
                if let Some(boundary) = boundaries
                    .selected
                    .iter()
                    .find(|boundary| boundary.id == chain.context.task_info.operator_id)
                {
                    assert!(matched.insert(boundary.id.clone()));
                    let inner = std::mem::replace(&mut chain.operator, Box::new(Unreachable));
                    chain.operator = Box::new(ObservedOperator {
                        inner,
                        boundary: boundary.clone(),
                        journal: journal.clone(),
                        gate: gate.clone(),
                    });
                }
                current = chain.next.as_deref_mut();
            }
        }
    }
    assert_eq!(
        matched.len(),
        boundaries.selected.len(),
        "every actual planned operator ID must match reconstructed context exactly once"
    );
}
struct Unreachable;
#[async_trait::async_trait]
impl ArrowOperator for Unreachable {
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
struct AbortOnDrop<'a>(&'a RunningEngine);
impl Drop for AbortOnDrop<'_> {
    fn drop(&mut self) {
        self.0.abort_workers();
    }
}

struct Packet {
    manifest: Value,
    oracle: Vec<Value>,
    outputs: Vec<PathBuf>,
    timestamps: Vec<String>,
    logical: Arc<LogicalProgram>,
    ids: Boundaries,
    evidence: PathBuf,
    job: String,
    raw: usize,
    started: std::time::Instant,
}
fn path_field(value: &Value, key: &str) -> PathBuf {
    let path = PathBuf::from(value[key].as_str().unwrap());
    assert!(path.is_absolute());
    path
}
fn number(value: &Value, key: &str) -> usize {
    usize::try_from(value[key].as_u64().unwrap()).unwrap()
}
fn json_rows(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn normalize(mut rows: Vec<Value>, paths: &[String]) -> Vec<Value> {
    for row in &mut rows {
        for path in paths {
            let value = row
                .pointer_mut(path)
                .expect("declared clock field must exist, including SQL NULL");
            if value.is_null() {
                continue;
            }
            let text = value.as_str().unwrap();
            let text = text.strip_suffix('Z').unwrap_or(text);
            let whole = match text.split_once('.') {
                Some((whole, fraction)) => {
                    assert!(!fraction.is_empty() && fraction.bytes().all(|byte| byte == b'0'));
                    whole
                }
                None => text,
            };
            assert_eq!(whole.len(), 19);
            for (index, byte) in whole.bytes().enumerate() {
                assert!(match index {
                    4 | 7 => byte == b'-',
                    10 => byte == b'T',
                    13 | 16 => byte == b':',
                    _ => byte.is_ascii_digit(),
                });
            }
            *value = Value::String(whole.to_owned());
        }
    }
    rows
}
async fn packet() -> Packet {
    configure_test_worker();
    if let Ok(entries) = env::var("STREAMR_TEST_SCAN_PAGE_ENTRIES") {
        let entries: usize = entries
            .parse()
            .expect("explicit scan page entries must be an integer");
        assert!(
            matches!(entries, 1 | 64),
            "finite qualification supports explicit page1 or current64 only"
        );
        config::update(|config| {
            config
                .worker
                .aggregate_state
                .as_mut()
                .expect("native aggregate config required")
                .page_entries = entries;
            config
                .worker
                .join_state
                .as_mut()
                .expect("native JOIN config required")
                .page_entries = entries;
        });
    }
    assert!(config::config().worker.aggregate_state.is_some());
    assert!(config::config().worker.join_state.is_some());
    assert!(config::config().worker.typed_sql_state.is_some());
    assert!(matches!(config::config().pipeline.source_batch_size, 1 | 8));
    config::update(|config| {
        config.worker.queue_size = 1;
        config.pipeline.source_batch_linger = Duration::from_secs(3600).into();
    });
    let case_path = PathBuf::from(env::var("STREAMR_BRANCH_LIFECYCLE_CASE").unwrap());
    assert!(case_path.is_absolute());
    let case_bytes = tokio::fs::read(&case_path).await.unwrap();
    let manifest: Value = serde_json::from_slice(&case_bytes).unwrap();
    let packet_path = path_field(&manifest, "packet_path");
    let packet_bytes = tokio::fs::read(&packet_path).await.unwrap();
    let definition: Value = serde_json::from_slice(&packet_bytes).unwrap();
    let query_path = path_field(&definition, "query_path");
    let input_path = path_field(&definition, "input_path");
    let expected_path = path_field(&definition, "expected_path");
    let query_bytes = tokio::fs::read(&query_path).await.unwrap();
    let input_bytes = tokio::fs::read(&input_path).await.unwrap();
    let expected_bytes = tokio::fs::read(&expected_path).await.unwrap();
    let outputs = definition["output_paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|path| PathBuf::from(path.as_str().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(outputs.len(), 2);
    assert_ne!(outputs[0], outputs[1]);
    for output in &outputs {
        assert!(output.is_absolute());
        for input in [
            &case_path,
            &packet_path,
            &query_path,
            &input_path,
            &expected_path,
        ] {
            assert_ne!(output, input);
        }
        match tokio::fs::symlink_metadata(output).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            other => panic!("fresh owned output path required: {output:?}: {other:?}"),
        }
    }
    let timestamps = definition["timestamp_paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|path| {
            let path = path.as_str().unwrap().to_owned();
            assert!(path.starts_with('/'));
            path
        })
        .collect::<Vec<_>>();
    let oracle = normalize(json_rows(&expected_bytes), &timestamps);
    assert!(!oracle.is_empty());
    let raw = json_rows(&input_bytes).len();
    assert_eq!(raw, number(&manifest, "raw_operations"));
    let job = format!(
        "str64-branch-lifecycle-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let evidence = PathBuf::from(env::var("STREAMR_TEST_EVIDENCE_DIR").unwrap()).join(&job);
    assert!(evidence.is_absolute());
    tokio::fs::create_dir_all(&evidence).await.unwrap();
    for (name, bytes) in [
        ("case.json", case_bytes),
        ("packet.json", packet_bytes),
        ("query.sql", query_bytes.clone()),
        ("input.jsonl", input_bytes),
        ("expected.jsonl", expected_bytes),
    ] {
        tokio::fs::write(evidence.join(name), bytes).await.unwrap();
    }
    let started = std::time::Instant::now();
    sample_proc(&evidence, "plan", started);
    println!(
        "BRANCH_LIFECYCLE phase=plan evidence={}",
        evidence.display()
    );
    let hook = env::var("STREAMR_LIVE_SCAN_DIAGNOSTICS").unwrap_or_else(|_| "0".into());
    assert!(matches!(hook.as_str(), "0" | "1"));
    std::fs::write(evidence.join("run-manifest.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "case":manifest,"packet":definition,"hook":hook,"source_batch":config::config().pipeline.source_batch_size,
        "queue_size":config::config().worker.queue_size,"aggregate_limits":config::config().worker.aggregate_state,
        "join_limits":config::config().worker.join_state,"typed_limits":config::config().worker.typed_sql_state,
        "resources":config::config().worker.live_state_resources,"backend":format!("{:?}",config::config().worker.sql_state_backend),
        "scope":"whole test process; current limits retained; local response endpoints only",
    })).unwrap()).unwrap();
    let logical = match get_graph(String::from_utf8(query_bytes).unwrap(), &get_udfs()).await {
        Ok(logical) => Arc::new(logical),
        Err(error) => {
            tokio::fs::write(
                evidence.join("planning-error.txt"),
                format!("{error:?}\n{error}\n"),
            )
            .await
            .unwrap();
            panic!("unchanged branch packet must plan: {error:?}");
        }
    };
    let mut actual_sources = Vec::new();
    let mut actual_sinks = Vec::new();
    for node in logical.graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if matches!(
                operator.operator_name,
                OperatorName::ConnectorSource | OperatorName::ConnectorSink
            ) {
                use prost::Message;
                let connector =
                    arroyo_rpc::grpc::api::ConnectorOp::decode(operator.operator_config.as_slice())
                        .unwrap();
                assert_eq!(connector.connector, "single_file");
                let config: arroyo_rpc::OperatorConfig =
                    serde_json::from_str(&connector.config).unwrap();
                let path = PathBuf::from(config.table["path"].as_str().unwrap());
                if operator.operator_name == OperatorName::ConnectorSource {
                    actual_sources.push(path);
                } else {
                    actual_sinks.push(path);
                }
            }
        }
    }
    assert_eq!(actual_sources, vec![input_path]);
    let mut expected_outputs = outputs.clone();
    expected_outputs.sort();
    actual_sinks.sort();
    assert_eq!(actual_sinks, expected_outputs);
    let ids = boundaries(&logical);
    let nodes=logical.graph.node_weights().map(|node|serde_json::json!({"node_id":node.node_id,"parallelism":node.parallelism,"operators":node.operator_chain.iter().map(|(operator,_)|serde_json::json!({"id":operator.operator_id,"kind":format!("{:?}",operator.operator_name),"config_bytes":operator.operator_config})).collect::<Vec<_>>() })).collect::<Vec<_>>();
    std::fs::write(evidence.join("planned-graph.json"),serde_json::to_vec_pretty(&serde_json::json!({"nodes":nodes,"source_id":ids.source,"root_descriptor":ids.root,"boundaries":ids.selected.iter().map(|boundary|serde_json::json!({"stage":format!("{:?}",boundary.stage),"id":boundary.id})).collect::<Vec<_>>(),"queue_size":config::config().worker.queue_size,"source_batch_max":config::config().pipeline.source_batch_size})).unwrap()).unwrap();
    Packet {
        manifest,
        oracle,
        outputs,
        timestamps,
        logical,
        ids,
        evidence,
        job,
        raw,
        started,
    }
}
async fn compare_files(packet: &Packet, rows: usize, phase: &str) {
    for (index, path) in packet.outputs.iter().enumerate() {
        let bytes = tokio::fs::read(path).await.unwrap();
        tokio::fs::write(
            packet
                .evidence
                .join(format!("{phase}-consumer-{index}.jsonl")),
            &bytes,
        )
        .await
        .unwrap();
        assert_eq!(
            normalize(json_rows(&bytes), &packet.timestamps),
            packet.oracle[..rows],
            "real persisted consumer {index} at {phase}"
        );
    }
}
async fn compare_sink_callbacks(packet: &Packet, journal: &Journal, rows: usize) {
    journal
        .wait(|state| {
            packet
                .ids
                .selected
                .iter()
                .filter(|boundary| boundary.stage == Stage::Sink)
                .all(|sink| {
                    state
                        .sink_rows
                        .get(&sink.id)
                        .is_some_and(|values| values.len() >= rows)
                })
        })
        .await;
    let state = journal.state.lock().unwrap();
    for sink in packet
        .ids
        .selected
        .iter()
        .filter(|boundary| boundary.stage == Stage::Sink)
    {
        assert_eq!(
            normalize(state.sink_rows[&sink.id].clone(), &packet.timestamps),
            packet.oracle[packet.oracle.len() - number(&packet.manifest, "suffix_capture_rows")
                ..packet.oracle.len() - number(&packet.manifest, "suffix_capture_rows") + rows]
        );
    }
}

fn verify_observations(
    packet: &Packet,
    journal: &Journal,
    offset: usize,
    count: usize,
    phase: &str,
) -> EventContext {
    let state = journal.state.lock().unwrap();
    let contexts = packet.manifest["contexts_nanos"].as_array().unwrap();
    let images = packet.manifest["images_per_operation"].as_array().unwrap();
    let joined = packet.manifest["join_rows_per_event"].as_array().unwrap();
    assert_eq!(contexts.len(), packet.raw);
    assert_eq!(images.len(), packet.raw);
    assert_eq!(joined.len(), packet.raw);
    let output = |stage, kind| {
        state
            .observations
            .iter()
            .filter(|observation| observation.stage == stage && observation.kind == kind)
            .collect::<Vec<_>>()
    };
    let root = output(Stage::Root, "response_delivery");
    assert_eq!(root.len(), count);
    let first = root[0].context.unwrap();
    for (index, observation) in root.iter().enumerate() {
        let context = observation.context.unwrap();
        assert_eq!(context.root_operator, packet.ids.root);
        assert_eq!(context.root_subtask, 0);
        assert_eq!(
            context.version,
            arroyo_types::event_response::EVENT_RESPONSE_VERSION
        );
        assert_eq!(context.sequence, u64::try_from(index + 1).unwrap());
        assert_eq!(
            context.trigger_nanos,
            contexts[offset + index].as_i64().unwrap()
        );
        assert_eq!(context.attempt_nonce, first.attempt_nonce);
        assert_eq!(
            observation.rows,
            usize::try_from(images[offset + index].as_u64().unwrap()).unwrap()
        );
    }
    for stage in [Stage::Left, Stage::Right] {
        let rows = output(stage, "response_delivery");
        assert_eq!(rows.len(), count);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.context, root[index].context);
            assert!(row.keys <= 2);
        }
    }
    let rows = output(Stage::Join, "pair_completed");
    assert_eq!(rows.len(), count);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.context, root[index].context);
        assert_eq!(
            row.rows,
            usize::try_from(joined[offset + index].as_u64().unwrap()).unwrap()
        );
    }
    let batch = if config::config().pipeline.source_batch_size == 1 {
        "1"
    } else {
        "8"
    };
    let expected = &packet.manifest["batches"][batch][phase];
    let numbers = |name| {
        expected[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| usize::try_from(value.as_u64().unwrap()).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(state.raw_batches, numbers("raw"));
    assert_eq!(state.image_batches, numbers("images"));
    assert_eq!(
        state
            .image_runs
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>(),
        images[offset..offset + count]
            .iter()
            .map(|value| usize::try_from(value.as_u64().unwrap()).unwrap())
            .collect::<Vec<_>>()
    );
    drop(state);
    journal.save(&packet.evidence.join(format!("{phase}-observations.json")));
    first
}
fn sample_proc(directory: &Path, phase: &str, started: std::time::Instant) {
    let snapshot = serde_json::json!({"phase":phase,"monotonic_elapsed_ns":started.elapsed().as_nanos(),"pid":std::process::id(),
        "stat":std::fs::read_to_string("/proc/self/stat").unwrap(),"status":std::fs::read_to_string("/proc/self/status").unwrap(),
        "io":std::fs::read_to_string("/proc/self/io").unwrap(),"note":"Whole test process, including Engine and harness; not isolated operator CPU."});
    std::fs::write(
        directory.join(format!("proc-{phase}.json")),
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();
    println!("BRANCH_LIFECYCLE_PROC {snapshot}");
}
fn phase(packet: &Packet, name: &str) {
    sample_proc(&packet.evidence, name, packet.started);
    println!(
        "BRANCH_LIFECYCLE phase={name} evidence={}",
        packet.evidence.display()
    );
}
async fn drain(
    running: &RunningEngine,
    rx: &mut Receiver<ControlResp>,
    mut finished: HashSet<(u32, u32)>,
    forbidden: Option<(&str, u64)>,
) {
    let count = running
        .operator_controls()
        .values()
        .map(Vec::len)
        .sum::<usize>();
    running.abort_workers();
    let inspect = |message: ControlResp, finished: &mut HashSet<(u32, u32)>| match message {
        ControlResp::TaskFinished {
            task_id,
            subtask_idx,
            ..
        }
        | ControlResp::TaskFailed {
            task_id,
            subtask_idx,
            ..
        } => {
            finished.insert((task_id, subtask_idx));
        }
        ControlResp::CheckpointCompleted(checkpoint) => {
            if let Some((operator, epoch)) = forbidden
                && checkpoint.checkpoint_epoch == epoch
            {
                assert_ne!(
                    checkpoint.operator_id, operator,
                    "blocked owner completed forbidden checkpoint during failure cleanup"
                );
            }
        }
        _ => {}
    };
    tokio::time::timeout(test_runtime_timeout(), async {
        while finished.len() < count {
            match rx.recv().await {
                Some(message) => inspect(message, &mut finished),
                None => break,
            }
        }
        assert_eq!(
            finished.len(),
            count,
            "all old attempt tasks must terminate before fresh owners"
        );
    })
    .await
    .expect("whole-attempt drain timed out");
    while let Ok(message) = rx.try_recv() {
        inspect(message, &mut finished);
    }
    wait_for_capture_state_cleanup().await;
    while let Ok(message) = rx.try_recv() {
        inspect(message, &mut finished);
    }
}
async fn seed_checkpoint(packet: &Packet) -> (u64, EventContext) {
    let prefix = number(&packet.manifest, "prefix_raw");
    assert!(prefix > 0 && prefix < packet.raw);
    let (tx, mut rx) = channel(128);
    let program = local_program(&packet.job, &packet.logical.graph, &get_udfs(), None, tx).await;
    let journal = Journal::new(packet.raw);
    install(&program, &packet.ids, &journal, None);
    let running = Engine::for_local(program, "pipe-test".into(), packet.job.clone())
        .await
        .unwrap()
        .start()
        .await;
    let guard = AbortOnDrop(&running);
    phase(packet, "start");
    assert_eq!(running.source_controls().len(), 1);
    advance(&running, i32::try_from(prefix - 1).unwrap()).await;
    phase(packet, "seed-enqueued");
    let mut finished = HashSet::new();
    let epoch = 1_u64;
    let checkpoint_bytes = checkpoint_with_stop(
        &mut SmokeTestContext {
            job_id: Arc::new(packet.job.clone()),
            engine: &running,
            control_rx: &mut rx,
            program: packet.logical.clone(),
        },
        u32::try_from(epoch).unwrap(),
        true,
        &mut finished,
    )
    .await;
    std::fs::write(
        packet.evidence.join("completed-checkpoint-receipt.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "epoch":epoch,"checkpoint_bytes":checkpoint_bytes,
            "helper_result_meaning":"checkpoint bytes, not epoch",
        }))
        .unwrap(),
    )
    .unwrap();
    drain(&running, &mut rx, finished, None).await;
    drop(guard);
    phase(packet, "CP1-stopped");
    compare_files(
        packet,
        number(&packet.manifest, "prefix_captures"),
        "checkpoint1",
    )
    .await;
    let context = verify_observations(packet, &journal, 0, prefix, "prefix");
    phase(packet, "seed-verified");
    (epoch, context)
}
async fn replay(packet: &Packet, epoch: u64, old: EventContext) {
    phase(packet, "restore-start");
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &packet.job,
        &packet.logical.graph,
        &get_udfs(),
        Some(epoch),
        tx,
    )
    .await;
    let journal = Journal::new(packet.raw);
    install(&program, &packet.ids, &journal, None);
    let running = Engine::for_local(program, "pipe-test".into(), packet.job.clone())
        .await
        .unwrap()
        .start()
        .await;
    let guard = AbortOnDrop(&running);
    run_until_finished(&running, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    drop(guard);
    phase(packet, "tail-complete");
    let prefix = number(&packet.manifest, "prefix_raw");
    let fresh = verify_observations(packet, &journal, prefix, packet.raw - prefix, "suffix");
    assert_ne!(
        old.attempt_nonce, fresh.attempt_nonce,
        "fresh Engine must mint fresh root generation nonce"
    );
    compare_files(packet, packet.oracle.len(), "restored").await;
    phase(packet, "verified");
}
async fn uninterrupted() {
    let packet = packet().await;
    let (tx, mut rx) = channel(128);
    let program = local_program(&packet.job, &packet.logical.graph, &get_udfs(), None, tx).await;
    let journal = Journal::new(packet.raw);
    install(&program, &packet.ids, &journal, None);
    let running = Engine::for_local(program, "pipe-test".into(), packet.job.clone())
        .await
        .unwrap()
        .start()
        .await;
    let guard = AbortOnDrop(&running);
    phase(&packet, "start");
    run_until_finished(&running, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    drop(guard);
    verify_observations(&packet, &journal, 0, packet.raw, "full");
    compare_files(&packet, packet.oracle.len(), "uninterrupted").await;
    phase(&packet, "verified");
}
async fn skew(held: Stage) {
    assert!(matches!(held, Stage::Left | Stage::Right));
    let packet = packet().await;
    let gate = Gate::new(held, Selector::Sequence(1), Outcome::Deliver);
    let (tx, mut rx) = channel(128);
    let program = local_program(&packet.job, &packet.logical.graph, &get_udfs(), None, tx).await;
    let journal = Journal::new(packet.raw);
    install(&program, &packet.ids, &journal, Some(gate.clone()));
    let running = Engine::for_local(program, "pipe-test".into(), packet.job.clone())
        .await
        .unwrap()
        .start()
        .await;
    let guard = AbortOnDrop(&running);
    // A full source batch8 is flushed before testing its first event.
    advance(&running, i32::try_from(packet.raw - 1).unwrap()).await;
    let held_response = gate.reached().await;
    let context = held_response.context.unwrap();
    let other_index = if held == Stage::Left { 1 } else { 0 };
    journal
        .wait(|state| {
            state.observations.iter().any(|observation| {
                observation.stage == Stage::Join
                    && observation.kind == "input_consumed"
                    && observation.input == Some(other_index)
                    && observation.context == Some(context)
            })
        })
        .await;
    {
        let state = journal.state.lock().unwrap();
        assert!(
            !state
                .observations
                .iter()
                .any(|observation| observation.stage == Stage::Join
                    && matches!(observation.kind, "pair_completed" | "data_delivery")),
            "JOIN emitted before matching empty/present mate"
        );
        let right = state
            .observations
            .iter()
            .find(|observation| {
                observation.stage == Stage::Right
                    && observation.kind == "response_delivery"
                    && observation.context == Some(context)
            })
            .unwrap();
        let left = state
            .observations
            .iter()
            .find(|observation| {
                observation.stage == Stage::Left
                    && observation.kind == "response_delivery"
                    && observation.context == Some(context)
            })
            .unwrap();
        assert_eq!(
            right.rows == 0,
            packet.manifest["first_response_empty"]["Right"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            left.rows == 0,
            packet.manifest["first_response_empty"]["Left"]
                .as_bool()
                .unwrap()
        );
        assert_ne!(
            right.rows == 0,
            left.rows == 0,
            "skew fixture must declare one real empty mate and one present mate"
        );
    }
    journal.save(&packet.evidence.join("held-skew-observations.json"));
    gate.release.notify_one();
    run_until_finished(&running, &mut rx).await;
    wait_for_capture_state_cleanup().await;
    drop(guard);
    verify_observations(&packet, &journal, 0, packet.raw, "full");
    compare_files(&packet, packet.oracle.len(), "skew-restored-delivery").await;
}
async fn source_ack(
    running: &RunningEngine,
    rx: &mut Receiver<ControlResp>,
    packet: &Packet,
    blocked: &str,
    epoch: u64,
) {
    for source in running.source_controls() {
        source
            .send(ControlMessage::Checkpoint(CheckpointBarrier {
                completed_base_epoch: None,
                epoch: u32::try_from(epoch).unwrap(),
                min_epoch: 0,
                timestamp: SystemTime::now(),
                then_stop: false,
            }))
            .await
            .unwrap();
    }
    let source_task = running.operator_to_node()[&packet.ids.source];
    tokio::time::timeout(test_runtime_timeout(),async {loop {match rx.recv().await {
        Some(ControlResp::CheckpointCompleted(checkpoint)) if checkpoint.checkpoint_epoch==epoch=> {
            assert_ne!(checkpoint.operator_id,blocked,"blocked owner checkpointed ahead of actual delivery");
            if checkpoint.operator_id==packet.ids.source {use prost::Message;std::fs::write(packet.evidence.join("CP2-source-snapshot-ack.json"),serde_json::to_vec_pretty(&serde_json::json!({"epoch":epoch,"operator_id":checkpoint.operator_id,"snapshot_metadata_bytes":checkpoint.subtask_metadata.encode_to_vec(),"meaning":"source snapshot only, not downstream completion or Ready","expected_source_offset_from_fixture_and_native_control_loop":number(&packet.manifest,"source_checkpoint_offset")})).unwrap()).unwrap();break;}
        },
        Some(ControlResp::TaskFinished {task_id,..}) if task_id==source_task=>panic!("source terminated before actual CP2 snapshot ACK"),
        Some(ControlResp::TaskFailed {error,..})=>panic!("unexpected failure before injection: {error:?}"),
        Some(_)=>{},None=>panic!("workers closed before source ACK"),
    }}}).await.expect("source checkpoint snapshot ACK timed out");
}
async fn fault(stage: Stage) {
    let packet = packet().await;
    let (epoch, old) = seed_checkpoint(&packet).await;
    let selector = if stage == Stage::Target {
        Selector::DataRowsBefore(number(&packet.manifest, "fault_target_rows_before"))
    } else {
        Selector::Sequence(packet.manifest["fault_sequence"].as_u64().unwrap())
    };
    let gate = Gate::new(stage, selector, Outcome::Fail);
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &packet.job,
        &packet.logical.graph,
        &get_udfs(),
        Some(epoch),
        tx,
    )
    .await;
    let journal = Journal::new(packet.raw);
    install(&program, &packet.ids, &journal, Some(gate.clone()));
    let running = Engine::for_local(program, "pipe-test".into(), packet.job.clone())
        .await
        .unwrap()
        .start()
        .await;
    let guard = AbortOnDrop(&running);
    advance(
        &running,
        i32::try_from(number(&packet.manifest, "noops_before_fault")).unwrap(),
    )
    .await;
    let selected = gate.reached().await;
    assert_eq!(selected.stage, stage);
    if stage == Stage::Target {
        assert!(
            selected.context.is_none(),
            "public target Data has no invented carrier identity"
        );
    } else {
        let context = selected.context.unwrap();
        assert_ne!(context.attempt_nonce, old.attempt_nonce);
        assert_eq!(
            context.sequence,
            packet.manifest["fault_sequence"].as_u64().unwrap()
        );
    }
    compare_sink_callbacks(
        &packet,
        &journal,
        number(&packet.manifest, "live_capture_rows_before_fault"),
    )
    .await;
    let blocked = packet
        .ids
        .selected
        .iter()
        .find(|boundary| boundary.stage == stage)
        .unwrap()
        .id
        .clone();
    let failed_task = running.operator_to_node()[&blocked];
    let next = epoch.checked_add(1).unwrap();
    source_ack(&running, &mut rx, &packet, &blocked, next).await;
    journal.save(
        &packet
            .evidence
            .join("blocked-before-refusal-observations.json"),
    );
    gate.release.notify_one();
    let mut finished = HashSet::new();
    tokio::time::timeout(test_runtime_timeout(), async {
        loop {
            match rx.recv().await {
                Some(ControlResp::TaskFailed {
                    task_id,
                    subtask_idx,
                    error,
                    ..
                }) => {
                    assert_eq!(task_id, failed_task);
                    assert_eq!(subtask_idx, 0);
                    assert!(format!("{error:?}").contains("STR64 native branch delivery refusal"));
                    finished.insert((task_id, subtask_idx));
                    break;
                }
                Some(ControlResp::TaskFinished {
                    task_id,
                    subtask_idx,
                    ..
                }) => {
                    finished.insert((task_id, subtask_idx));
                }
                Some(ControlResp::CheckpointCompleted(checkpoint))
                    if checkpoint.checkpoint_epoch == next =>
                {
                    assert_ne!(
                        checkpoint.operator_id, blocked,
                        "blocked owner completed CP2 after failure"
                    )
                }
                Some(_) => {}
                None => panic!("no actual TaskFailed from injected collector refusal"),
            }
        }
    })
    .await
    .expect("real injected failure did not reach coordinator");
    drain(&running, &mut rx, finished, Some((&blocked, next))).await;
    drop(guard);
    // Terminal responses and native-owner cleanup do not drop this graph's
    // queue handles. Release the entire failed attempt before fresh owners.
    drop(running);
    drop(rx);
    journal.save(&packet.evidence.join("failed-attempt-observations.json"));
    replay(&packet, epoch, old).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_response_inventory() {
    uninterrupted().await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_completed_checkpoint_fresh_replay() {
    let packet = packet().await;
    let (epoch, context) = seed_checkpoint(&packet).await;
    replay(&packet, epoch, context).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_left_held_other_response_consumed() {
    skew(Stage::Left).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_right_held_other_response_consumed() {
    skew(Stage::Right).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_left_delivery_failure_fresh_replay() {
    fault(Stage::Left).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_right_delivery_failure_fresh_replay() {
    fault(Stage::Right).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_join_delivery_failure_fresh_replay() {
    fault(Stage::Join).await;
}
#[test_log(tokio::test)]
#[ignore = "explicit external native branch lifecycle packet"]
async fn native_branch_packet_target_delivery_failure_fresh_replay() {
    fault(Stage::Target).await;
}
