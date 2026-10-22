use arrow::compute::kernels;
use arrow_array::{Array, BooleanArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};

use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, EventResponseRole,
    OperatorConstructor, Registry,
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api::ExpressionWatermarkConfig;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_state::global_table_config_with_version;
use arroyo_types::event_time::from_signed_nanos;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark, from_nanos, print_time};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, info};

// Current source admission state. No finite floor exists before the first actual
// AS watermark emission; neither Idle nor terminal EOF creates one.
#[derive(Encode, Decode, Copy, Clone, Debug, PartialEq, Default)]
pub struct WatermarkGeneratorState {
    last_watermark_emitted_at: Option<i64>,
    max_watermark: Option<i64>,
    emitted_watermark: Option<i64>,
}

const WATERMARK_STATE_VERSION: u32 = 2;

pub struct WatermarkGenerator {
    interval: Duration,
    state_cache: WatermarkGeneratorState,
    idle_time: Option<Duration>,
    last_event: SystemTime,
    idle: bool,
    expression: Arc<dyn PhysicalExpr>,
    timestamp_index: usize,
    source_envelope_index: Option<usize>,
    preserve_source_envelope: bool,
    response_root: Option<ResponseRoot>,
}

impl WatermarkGenerator {
    pub fn expression(
        interval: Duration,
        idle_time: Option<Duration>,
        expression: Arc<dyn PhysicalExpr>,
        timestamp_index: usize,
        source_envelope_index: Option<usize>,
    ) -> WatermarkGenerator {
        WatermarkGenerator {
            interval,
            state_cache: WatermarkGeneratorState::default(),
            idle_time,
            last_event: SystemTime::now(),
            idle: false,
            expression,
            timestamp_index,
            source_envelope_index,
            preserve_source_envelope: false,
            response_root: None,
        }
    }
}

impl WatermarkGenerator {
    async fn admit_batch(
        &mut self,
        record: RecordBatch,
        task_index: u32,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let timestamps = record
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| admission_error("source FOR clock is not a nanosecond timestamp"))?;
        let envelopes = self
            .source_envelope_index
            .map(|index| {
                record
                    .column(index)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| admission_error("source envelope marker is not UInt64"))
            })
            .transpose()?;
        let (keep, representatives, change_timestamps) =
            admission_masks(timestamps, envelopes, self.state_cache.emitted_watermark)?;
        // User payload stays intact. The internal trigger clock belongs to the
        // current change, including its before-image's retraction of an old key.
        let record = if self.source_envelope_index.is_some() {
            let mut columns = record.columns().to_vec();
            columns[self.timestamp_index] = change_timestamps;
            RecordBatch::try_new(record.schema(), columns)?
        } else {
            record
        };
        let admitted = arrow::compute::filter_record_batch(&record, &keep)?;
        if admitted.num_rows() == 0 {
            return Ok(());
        }
        let representatives = arrow::compute::filter(&representatives, &keep)?;
        let representatives = representatives
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let timestamps = admitted
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let output = if let Some(marker) = self.source_envelope_index
            && !self.preserve_source_envelope
        {
            admitted.project(
                &(0..admitted.num_columns())
                    .filter(|i| *i != marker)
                    .collect::<Vec<_>>(),
            )?
        } else {
            admitted.clone()
        };

        // Only an accepted envelope's final image supplies its new progress;
        // update-before remains an unchanged retraction regardless of its clock.
        let watermark = self
            .expression
            .evaluate(&admitted)?
            .into_array(admitted.num_rows())?;

        let watermark = watermark
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| admission_error("source AS clock is not a nanosecond timestamp"))?;
        let watermark = arrow::compute::filter(watermark, representatives)?;
        let watermark = watermark
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let watermark = kernels::aggregate::min(watermark);
        let timestamps = arrow::compute::filter(timestamps, representatives)?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let max_timestamp = kernels::aggregate::max(timestamps);

        let progress = watermark.map(|watermark| {
            self.state_cache
                .max_watermark
                .map_or(watermark, |old| old.max(watermark))
        });
        // Validate even progress conversions before any data is forwarded.
        let finite_progress = progress.map(signed_timestamp).transpose()?;
        if let Some(root) = &mut self.response_root {
            if task_index != 0 {
                return Err(admission_error(
                    "response root requires singleton subtask zero",
                ));
            }
            let clocks = admitted
                .column(self.timestamp_index)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            root.forward_admitted(output, representatives, clocks, collector)
                .await?;
        } else {
            collector.collect(output).await?;
        }
        self.last_event = SystemTime::now();
        let (Some(progress), Some(finite_progress), Some(max_timestamp)) =
            (progress, finite_progress, max_timestamp)
        else {
            return Ok(());
        };
        self.state_cache.max_watermark = Some(progress);
        if self.idle
            || self
                .state_cache
                .last_watermark_emitted_at
                .is_none_or(|previous| {
                    i128::from(max_timestamp) - i128::from(previous)
                        > self.interval.as_nanos() as i128
                })
        {
            let watermark = finite_progress;
            debug!(
                "[{}] Emitting expression watermark {}",
                task_index,
                print_time(watermark)
            );
            collector
                .broadcast_watermark(Watermark::EventTime(watermark))
                .await?;
            self.state_cache.last_watermark_emitted_at = Some(max_timestamp);
            self.state_cache.emitted_watermark = Some(progress);
            self.idle = false;
        }
        Ok(())
    }
}

pub struct WatermarkGeneratorConstructor;

impl OperatorConstructor for WatermarkGeneratorConstructor {
    type ConfigT = ExpressionWatermarkConfig;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        if config.response_root_operator.is_some() {
            anyhow::ensure!(
                config.input_schema.is_some(),
                "response root requires planned input schema"
            );
        }
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let output_schema = arroyo_rpc::df::source_admission_output_schema(
            &input_schema,
            config.source_envelope_index.map(|index| index as usize),
            config.preserve_source_envelope,
        )?;
        if config.response_root_operator.is_some() {
            anyhow::ensure!(
                input_schema
                    .schema
                    .field(input_schema.timestamp_index)
                    .data_type()
                    == &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                "response root FOR clock must be Timestamp(Nanosecond,None)"
            );
        }
        let expression = PhysicalExprNode::decode(&mut config.expression.as_slice())?;
        let expression = parse_physical_expr(
            &expression,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let mut generator = WatermarkGenerator::expression(
            Duration::from_micros(config.period_micros),
            config.idle_time_micros.map(Duration::from_micros),
            expression,
            input_schema.timestamp_index,
            config.source_envelope_index.map(|i| i as usize),
        );
        generator.preserve_source_envelope = config.preserve_source_envelope;
        if let Some(root) = config.response_root_operator {
            generator.response_root = Some(ResponseRoot::new(
                root,
                input_schema.schema.clone(),
                output_schema.schema,
            ));
        }
        Ok(ConstructedOperator::from_operator(Box::new(generator)))
    }
}

#[async_trait]
impl ArrowOperator for WatermarkGenerator {
    fn event_response_role(&self) -> EventResponseRole {
        self.response_root
            .as_ref()
            .map_or(EventResponseRole::Ordinary, |root| {
                EventResponseRole::Root(root.spec.root_operator)
            })
    }
    fn event_response_output(&self) -> Option<Arc<EventEdgeSpec>> {
        self.response_root.as_ref().map(|root| root.spec.clone())
    }
    fn bind_event_response(
        &mut self,
        inputs: &[Arc<EventEdgeSpec>],
        context: &OperatorContext,
    ) -> DataflowResult<Option<Arc<EventEdgeSpec>>> {
        let output = context.out_schema.as_deref().ok_or_else(|| {
            arroyo_rpc::errors::DataflowError::ArgumentError(
                "response operator output schema missing".into(),
            )
        })?;
        let root = self
            .response_root
            .as_ref()
            .ok_or_else(|| admission_error("watermark has no planned response root"))?;
        root.validate_task(context)?;
        if !inputs.is_empty() || root.spec.schema != output.schema {
            return Err(admission_error(
                "response root binding schema/input mismatch",
            ));
        }
        Ok(Some(root.spec.clone()))
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config_with_version("s", "expression watermark state", WATERMARK_STATE_VERSION)
    }

    fn name(&self) -> String {
        "expression_watermark_generator".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("WatermarkGenerator"),
            fields: vec![
                ("interval", AsDisplayable::Debug(&self.interval)),
                ("idle_time", AsDisplayable::Debug(&self.idle_time)),
                ("expression", AsDisplayable::Debug(&self.expression)),
            ],
        }
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if let Some(root) = &self.response_root {
            root.validate_task(ctx)?;
        }
        let gs = ctx
            .table_manager
            .get_global_keyed_state::<u32, WatermarkGeneratorState>("s")
            .await?;
        self.last_event = SystemTime::now();

        let state = *(gs
            .get(&ctx.task_info.task_index)
            .unwrap_or(&WatermarkGeneratorState::default()));

        self.state_cache = state;
        Ok(())
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if let Some(SignalMessage::EndOfData) = final_message {
            // send final watermark on close
            collector
                .broadcast_watermark(
                    // this is in the year 2554, far enough out be close to infinity,
                    // but can still be formatted.
                    Watermark::EventTime(from_nanos(u64::MAX as u128)),
                )
                .await?;
        }
        Ok(())
    }

    async fn process_batch(
        &mut self,
        record: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.admit_batch(record, ctx.task_info.task_index, collector)
            .await
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let gs = ctx
            .table_manager
            .get_global_keyed_state::<u32, WatermarkGeneratorState>("s")
            .await?;

        gs.insert(ctx.task_info.task_index, self.state_cache).await;
        Ok(())
    }

    async fn handle_tick(
        &mut self,
        _: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if let Some(idle_time) = self.idle_time
            && self.last_event.elapsed().unwrap_or(Duration::ZERO) > idle_time
            && !self.idle
        {
            info!(
                "Setting partition {} to idle after {:?}",
                ctx.task_info.task_index, idle_time
            );
            collector.broadcast_watermark(Watermark::Idle).await?;
            self.idle = true;
        }
        Ok(())
    }
}

fn admission_error(message: &str) -> arroyo_rpc::errors::DataflowError {
    arroyo_rpc::errors::DataflowError::ArgumentError(message.into())
}

// The marker is source-local provenance, not a key: delete + create of the
// same primary key are distinct envelopes. Unrolling keeps both update images
// adjacent in one batch; the final image defines admission for the whole change.
fn admission_masks(
    timestamps: &TimestampNanosecondArray,
    envelopes: Option<&UInt64Array>,
    floor: Option<i64>,
) -> DataflowResult<(BooleanArray, BooleanArray, arrow_array::ArrayRef)> {
    let mut keep = vec![false; timestamps.len()];
    let mut representatives = vec![false; timestamps.len()];
    let mut change_indices = vec![0_u64; timestamps.len()];
    let mut start = 0;
    while start < timestamps.len() {
        let mut end = start + 1;
        if let Some(envelopes) = envelopes {
            if envelopes.is_null(start) {
                return Err(admission_error("source envelope marker is null"));
            }
            while end < timestamps.len()
                && !envelopes.is_null(end)
                && envelopes.value(end) == envelopes.value(start)
            {
                end += 1;
            }
            if end - start > 2 {
                return Err(admission_error("source envelope has more than two images"));
            }
        }
        let last = end - 1;
        if timestamps.is_null(last) {
            return Err(admission_error("source change FOR clock is NULL"));
        }
        change_indices[start..end].fill(last as u64);
        let accepted = floor.is_none_or(|floor| timestamps.value(last) >= floor);
        if accepted {
            keep[start..end].fill(true);
            representatives[last] = true;
        }
        start = end;
    }
    Ok((
        BooleanArray::from(keep),
        BooleanArray::from(representatives),
        arrow::compute::take(timestamps, &UInt64Array::from(change_indices), None)?,
    ))
}

// Arrow timestamps are signed nanoseconds; casting pre-epoch events to u128
// overflows SystemTime even when their independently declared progress is positive.
fn signed_timestamp(nanos: i64) -> DataflowResult<SystemTime> {
    from_signed_nanos(nanos).ok_or_else(|| {
        arroyo_rpc::errors::DataflowError::ArgumentError(format!(
            "timestamp {nanos}ns is outside the platform's SystemTime range"
        ))
    })
}

/// Per-attempt context only; checkpoint admission state remains unchanged.
/// The root is planner-assigned logical identity, NOT physical chained task ID.
struct ResponseRoot {
    input_schema: arrow_schema::SchemaRef,
    spec: Arc<EventEdgeSpec>,
    nonce: [u8; 16],
    next_sequence: Option<u64>,
}
impl ResponseRoot {
    fn new(
        root: u32,
        input_schema: arrow_schema::SchemaRef,
        schema: arrow_schema::SchemaRef,
    ) -> Self {
        Self {
            input_schema,
            spec: Arc::new(EventEdgeSpec {
                maintenance: false,
                root_operator: root,
                root_subtask: 0,
                schema,
                layout: EventLayout::Raw,
            }),
            nonce: rand::random(),
            next_sequence: Some(1),
        }
    }
    fn validate_task(&self, ctx: &OperatorContext) -> DataflowResult<()> {
        if ctx.task_info.parallelism != 1
            || ctx.task_info.task_index != 0
            || ctx.in_schemas.len() != 1
        {
            return Err(admission_error(
                "response root requires one singleton input/subtask",
            ));
        }
        if ctx.in_schemas[0].schema != self.input_schema {
            return Err(admission_error(
                "response root input differs from planned raw schema",
            ));
        }
        if !matches!(self.spec.layout, EventLayout::Raw)
            || self.spec.root_subtask != 0
            || ctx
                .out_schema
                .as_ref()
                .is_none_or(|output| output.schema != self.spec.schema)
        {
            return Err(admission_error(
                "response root output differs from registered raw schema",
            ));
        }
        // engine.rs supplies operator_idx=node_id to EVERY chained context;
        // comparing it with logical root identity would reject valid chaining.
        Ok(())
    }
    async fn forward_admitted(
        &mut self,
        rows: RecordBatch,
        representatives: &BooleanArray,
        clocks: &TimestampNanosecondArray,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if rows.num_rows() != representatives.len() || rows.num_rows() != clocks.len() {
            return Err(admission_error("response root admission arrays differ"));
        }
        let mut start = 0;
        for end in 1..=rows.num_rows() {
            if !representatives.value(end - 1) {
                continue;
            }
            let length = end - start;
            if length == 0 || length > 2 || clocks.is_null(end - 1) {
                return Err(admission_error(
                    "response root requires one complete source operation",
                ));
            }
            let sequence = self
                .next_sequence
                .ok_or_else(|| admission_error("response root sequence exhausted"))?;
            if sequence == 0 {
                return Err(admission_error("response root sequence must start at one"));
            }
            let response = EventResponse {
                context: EventContext {
                    version: EVENT_RESPONSE_VERSION,
                    root_operator: self.spec.root_operator,
                    root_subtask: 0,
                    attempt_nonce: self.nonce,
                    sequence,
                    trigger_nanos: clocks.value(end - 1),
                },
                rows: Some(rows.slice(start, length)),
                touched_keys: None,
                native_body: None,
            };
            self.spec
                .validate(&response)
                .map_err(|error| admission_error(&error))?;
            // Collector awaits every branch before E+1; never await a mate.
            collector.collect_response(response).await?;
            self.next_sequence = sequence.checked_add(1);
            start = end;
        }
        if start != rows.num_rows() {
            return Err(admission_error(
                "response root has an unfinished source operation",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod signed_timestamp_tests {
    use super::*;

    #[tokio::test]
    async fn direct_correction_preserves_marker_only_when_configured() {
        #[derive(Default)]
        struct Captures(Vec<RecordBatch>);
        #[async_trait]
        impl Collector for Captures {
            async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
                self.0.push(batch);
                Ok(())
            }
            async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
                Ok(())
            }
        }
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                arroyo_rpc::TIMESTAMP_FIELD,
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
            arrow_schema::Field::new(
                arroyo_rpc::SOURCE_ENVELOPE_FIELD,
                arrow_schema::DataType::UInt64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![20, 5])),
                Arc::new(UInt64Array::from(vec![0, 0])),
            ],
        )
        .unwrap();
        for preserve in [false, true] {
            let mut generator = WatermarkGenerator::expression(
                Duration::from_secs(1),
                None,
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    arroyo_rpc::TIMESTAMP_FIELD,
                    0,
                )),
                0,
                Some(1),
            );
            generator.preserve_source_envelope = preserve;
            let mut captures = Captures::default();
            generator
                .admit_batch(batch.clone(), 0, &mut captures)
                .await
                .unwrap();
            assert_eq!(captures.0.len(), 1);
            assert_eq!(captures.0[0].num_rows(), 2);
            assert_eq!(
                captures.0[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[5, 5]
            );
            assert_eq!(
                captures.0[0]
                    .column_by_name(arroyo_rpc::SOURCE_ENVELOPE_FIELD)
                    .is_some(),
                preserve
            );
            if preserve {
                assert_eq!(
                    captures.0[0]
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    &[0, 0]
                );
            }
        }
    }

    #[test]
    fn admission_preserves_update_envelopes_and_does_not_pair_equal_keys() {
        // Envelope 0 replaces an old below-floor row with an admitted after.
        // Envelope 1 is a late delete, envelope 2 is an on-time create of the
        // same opaque key. Source ordinal, never key adjacency, separates them.
        let timestamps = TimestampNanosecondArray::from(vec![1, 12, 2, 11, 12, 3]);
        let envelopes = UInt64Array::from(vec![0, 0, 1, 2, 3, 3]);
        let (keep, representatives, current_change) =
            admission_masks(&timestamps, Some(&envelopes), Some(10)).unwrap();
        assert_eq!(
            keep.values().iter().collect::<Vec<_>>(),
            vec![true, true, false, true, false, false]
        );
        let clocks = current_change
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(clocks.values().as_ref(), &[12, 12, 2, 11, 3, 3]);
        assert_eq!(
            representatives.values().iter().collect::<Vec<_>>(),
            vec![false, true, false, true, false, false]
        );
    }

    #[test]
    fn null_current_change_clock_is_rejected_precisely() {
        let error =
            admission_masks(&TimestampNanosecondArray::from(vec![None]), None, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source change FOR clock is NULL")
        );
        // A nullable historical before clock does not govern an accepted change.
        let (keep, _, clocks) = admission_masks(
            &TimestampNanosecondArray::from(vec![None, Some(12)]),
            Some(&UInt64Array::from(vec![0, 0])),
            Some(10),
        )
        .unwrap();
        assert!(keep.value(0) && keep.value(1));
        assert_eq!(clocks.null_count(), 0);
    }

    #[test]
    fn prior_floor_applies_to_whole_batch_and_equality_is_admitted() {
        let timestamps = TimestampNanosecondArray::from(vec![9, 10, 20, 11, -1]);
        let (keep, _, _) = admission_masks(&timestamps, None, Some(10)).unwrap();
        assert_eq!(
            keep.values().iter().collect::<Vec<_>>(),
            vec![false, true, true, true, false]
        );
        let (keep, _, _) = admission_masks(&timestamps, None, None).unwrap();
        assert!(keep.values().iter().all(|value| value));
        // Batch size one makes the same decision for a fixed prior floor.
        for (index, timestamp) in [9, 10, 20, 11, -1].into_iter().enumerate() {
            let (single, _, _) = admission_masks(
                &TimestampNanosecondArray::from(vec![timestamp]),
                None,
                Some(10),
            )
            .unwrap();
            assert_eq!(single.value(0), timestamp >= 10, "row {index}");
        }
    }

    #[test]
    fn current_checkpoint_state_preserves_negative_admission_floor() {
        let state = WatermarkGeneratorState {
            last_watermark_emitted_at: Some(-1),
            max_watermark: Some(-5),
            emitted_watermark: Some(-7),
        };
        let config = bincode::config::standard();
        let bytes = bincode::encode_to_vec(state, config).unwrap();
        let (restored, used) =
            bincode::decode_from_slice::<WatermarkGeneratorState, _>(&bytes, config).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(restored, state);
        let (keep, _, _) = admission_masks(
            &TimestampNanosecondArray::from(vec![-8, -7, -6]),
            None,
            restored.emitted_watermark,
        )
        .unwrap();
        assert_eq!(
            keep.values().iter().collect::<Vec<_>>(),
            vec![false, true, true]
        );
        assert_eq!(WatermarkGeneratorState::default().emitted_watermark, None);
    }

    #[test]
    fn pre_epoch_progress_can_be_formatted_for_logging() {
        assert_eq!(
            print_time(signed_timestamp(-2_000_000_000).unwrap()),
            "1969-12-31 23:59:58.000",
        );
    }

    #[test]
    fn event_times_preserve_epoch_sign_and_nanoseconds() {
        assert_eq!(
            signed_timestamp(-1).unwrap(),
            SystemTime::UNIX_EPOCH - Duration::from_nanos(1)
        );
        assert_eq!(
            signed_timestamp(1).unwrap(),
            SystemTime::UNIX_EPOCH + Duration::from_nanos(1)
        );
        assert_eq!(signed_timestamp(0).unwrap(), SystemTime::UNIX_EPOCH);
        assert_eq!(
            signed_timestamp(i64::MIN).unwrap(),
            SystemTime::UNIX_EPOCH - Duration::from_nanos(1_u64 << 63)
        );
    }
}

#[cfg(test)]
#[path = "watermark_root_response_tests.rs"]
mod root_response_tests;
