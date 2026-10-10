use super::execution::{ExecutionResources, configured_execution_resources};
use arrow::compute::kernels;
use arrow_array::{Array, BooleanArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arroyo_operator::context::{Collector, OperatorContext};

use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
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
    resources: Option<Arc<ExecutionResources>>,
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
            resources: None,
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
        let _input_reservation = self
            .resources
            .as_ref()
            .map(|resources| resources.reserve_batch("watermark admission input", &record))
            .transpose()?;
        let scratch_bytes = admission_scratch_bytes(
            record.get_array_memory_size(),
            record.num_rows(),
            record.num_columns(),
        )?;
        let mut scratch_reservation = self
            .resources
            .as_ref()
            .map(|resources| resources.reserve_bytes("watermark admission scratch", scratch_bytes))
            .transpose()?;
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
        let output = if let Some(marker) = self.source_envelope_index {
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
        if let Some(reservation) = scratch_reservation.as_mut() {
            let actual = scratch_bytes
                .checked_add(watermark.get_array_memory_size())
                .ok_or_else(|| admission_error("watermark admission progress size overflow"))?;
            reservation.try_resize(actual)?;
        }
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
        if let Some(resources) = self.resources.as_ref() {
            resources.check_batch("watermark admission output", &output)?;
            let measured = [
                record.get_array_memory_size(),
                admitted.get_array_memory_size(),
                keep.get_array_memory_size(),
                representatives.get_array_memory_size(),
                timestamps.get_array_memory_size(),
            ]
            .into_iter()
            .try_fold(0_usize, |sum, bytes| sum.checked_add(bytes))
            .ok_or_else(|| admission_error("watermark admission buffer size overflow"))?;
            let reservation = scratch_reservation.as_mut().unwrap();
            reservation.try_resize(reservation.size().max(measured))?;
        }
        let progress = watermark.map(|watermark| {
            self.state_cache
                .max_watermark
                .map_or(watermark, |old| old.max(watermark))
        });
        // Validate even progress conversions before any data is forwarded.
        let finite_progress = progress.map(signed_timestamp).transpose()?;
        collector.collect(output).await?;
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
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        anyhow::ensure!(
            input_schema.timestamp_index < input_schema.schema.fields().len(),
            "source FOR clock index is outside the input schema"
        );
        if let Some(index) = config.source_envelope_index {
            let field = input_schema
                .schema
                .fields()
                .get(index as usize)
                .ok_or_else(|| {
                    anyhow::anyhow!("source envelope marker index is outside the input schema")
                })?;
            anyhow::ensure!(
                index as usize != input_schema.timestamp_index
                    && field.name() == arroyo_rpc::SOURCE_ENVELOPE_FIELD
                    && field.data_type() == &arrow_schema::DataType::UInt64
                    && !field.is_nullable(),
                "invalid source envelope marker schema"
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
        generator.resources = configured_execution_resources()?;
        Ok(ConstructedOperator::from_operator(Box::new(generator)))
    }
}

#[async_trait]
impl ArrowOperator for WatermarkGenerator {
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

// Preflight masks, ordinal indices, timestamp normalization/filter intermediates,
// duplicated admitted payload buffers and Arrow/schema overhead before allocation.
fn admission_scratch_bytes(
    input_bytes: usize,
    rows: usize,
    columns: usize,
) -> DataflowResult<usize> {
    input_bytes
        .checked_mul(2)
        .and_then(|bytes| {
            rows.checked_mul(64)
                .and_then(|scratch| bytes.checked_add(scratch))
        })
        .and_then(|bytes| {
            columns
                .checked_add(8)
                .and_then(|columns| columns.checked_mul(256))
                .and_then(|overhead| bytes.checked_add(overhead))
        })
        .ok_or_else(|| admission_error("watermark admission scratch size overflow"))
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

#[cfg(test)]
mod signed_timestamp_tests {
    use super::*;

    struct BudgetCollector {
        resources: Arc<ExecutionResources>,
        calls: usize,
        fail_budget: bool,
    }
    #[async_trait]
    impl Collector for BudgetCollector {
        async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
            self.calls += 1;
            if self.fail_budget {
                assert!(
                    self.resources
                        .reserve_bytes(
                            "backpressure competitor",
                            self.resources.limits.memory_bytes
                        )
                        .is_err()
                );
                tokio::task::yield_now().await;
                assert!(
                    self.resources
                        .reserve_bytes(
                            "backpressure competitor",
                            self.resources.limits.memory_bytes
                        )
                        .is_err()
                );
            }
            Ok(())
        }
        async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn resource_refusal_is_before_forwarding_and_reservations_cover_collection() {
        use arroyo_rpc::config::ExecutionResourceConfig;
        use datafusion::physical_expr::expressions::Column;
        let batch = RecordBatch::try_from_iter(vec![(
            "_timestamp",
            Arc::new(TimestampNanosecondArray::from(vec![12])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        for (budget, refused) in [(1024, true), (65536, false)] {
            let resources = Arc::new(
                ExecutionResources::new(ExecutionResourceConfig {
                    memory_bytes: budget,
                    max_batch_bytes: budget.min(65536),
                })
                .unwrap(),
            );
            let mut gate = WatermarkGenerator::expression(
                Duration::from_secs(1),
                None,
                Arc::new(Column::new("_timestamp", 0)),
                0,
                None,
            );
            gate.resources = Some(resources.clone());
            let prior = gate.state_cache;
            let mut collector = BudgetCollector {
                resources: resources.clone(),
                calls: 0,
                fail_budget: !refused,
            };
            let result = gate.admit_batch(batch.clone(), 0, &mut collector).await;
            assert_eq!(result.is_err(), refused);
            assert_eq!(collector.calls, usize::from(!refused));
            if refused {
                assert_eq!(gate.state_cache, prior);
            }
            let entire_budget = resources
                .reserve_bytes("reacquire released gate budget", budget)
                .unwrap();
            drop(entire_budget);
        }
        assert!(admission_scratch_bytes(usize::MAX, 1, 1).is_err());
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
