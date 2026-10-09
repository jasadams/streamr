use arrow::compute::kernels;
use arrow_array::RecordBatch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::get_timestamp_col;
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api::ExpressionWatermarkConfig;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_state::global_table_config_with_version;
use arroyo_state::tables::MigratableState;
use arroyo_types::event_time::{from_signed_nanos, to_signed_nanos};
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

#[derive(Encode, Decode, Copy, Clone, Debug, PartialEq)]
pub struct WatermarkGeneratorState {
    last_watermark_emitted_at: SystemTime,
    max_watermark: SystemTime,
}

// Version 0 stored SystemTime directly, whose bincode encoding excludes dates
// before the epoch. Version 1 preserves the Arrow timestamp domain exactly in
// the same registered table, including an emission after an idle partition.
#[derive(Encode, Decode, Copy, Clone, Debug, PartialEq, Default)]
struct PersistedWatermarkGeneratorState {
    last_watermark_emitted_at: i64,
    max_watermark: i64,
}

impl TryFrom<WatermarkGeneratorState> for PersistedWatermarkGeneratorState {
    type Error = arroyo_rpc::errors::StateError;

    fn try_from(state: WatermarkGeneratorState) -> Result<Self, Self::Error> {
        let encode = |time| {
            to_signed_nanos(time).ok_or_else(|| arroyo_rpc::errors::StateError::Other {
                table: "s".into(),
                error: "watermark generator state exceeds signed Arrow nanosecond range".into(),
            })
        };
        Ok(Self {
            last_watermark_emitted_at: encode(state.last_watermark_emitted_at)?,
            max_watermark: encode(state.max_watermark)?,
        })
    }
}

impl TryFrom<PersistedWatermarkGeneratorState> for WatermarkGeneratorState {
    type Error = arroyo_rpc::errors::StateError;

    fn try_from(state: PersistedWatermarkGeneratorState) -> Result<Self, Self::Error> {
        let decode = |nanos| {
            from_signed_nanos(nanos).ok_or_else(|| arroyo_rpc::errors::StateError::Other {
                table: "s".into(),
                error: "watermark generator state exceeds SystemTime range".into(),
            })
        };
        Ok(Self {
            last_watermark_emitted_at: decode(state.last_watermark_emitted_at)?,
            max_watermark: decode(state.max_watermark)?,
        })
    }
}

impl MigratableState for PersistedWatermarkGeneratorState {
    const VERSION: u32 = 1;
    type PreviousVersion = WatermarkGeneratorState;

    fn migrate(previous: Self::PreviousVersion) -> Result<Self, arroyo_rpc::errors::StateError> {
        previous.try_into()
    }
}

pub struct WatermarkGenerator {
    interval: Duration,
    state_cache: WatermarkGeneratorState,
    idle_time: Option<Duration>,
    last_event: SystemTime,
    idle: bool,
    expression: Arc<dyn PhysicalExpr>,
}

impl WatermarkGenerator {
    pub fn expression(
        interval: Duration,
        idle_time: Option<Duration>,
        expression: Arc<dyn PhysicalExpr>,
    ) -> WatermarkGenerator {
        WatermarkGenerator {
            interval,
            state_cache: WatermarkGeneratorState {
                last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                max_watermark: SystemTime::UNIX_EPOCH,
            },
            idle_time,
            last_event: SystemTime::now(),
            idle: false,
            expression,
        }
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
        let expression = PhysicalExprNode::decode(&mut config.expression.as_slice())?;
        let expression = parse_physical_expr(
            &expression,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        Ok(ConstructedOperator::from_operator(Box::new(
            WatermarkGenerator::expression(
                Duration::from_micros(config.period_micros),
                config.idle_time_micros.map(Duration::from_micros),
                expression,
            ),
        )))
    }
}

#[async_trait]
impl ArrowOperator for WatermarkGenerator {
    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config_with_version(
            "s",
            "expression watermark state",
            PersistedWatermarkGeneratorState::VERSION,
        )
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
            .get_global_keyed_state_migratable::<u32, PersistedWatermarkGeneratorState>("s")
            .await?;
        self.last_event = SystemTime::now();

        let state = *(gs
            .get(&ctx.task_info.task_index)
            .unwrap_or(&PersistedWatermarkGeneratorState::default()));

        self.state_cache = state.try_into()?;
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
        collector.collect(record.clone()).await?;
        self.last_event = SystemTime::now();

        let timestamp_column = get_timestamp_col(&record, ctx);
        let Some(max_timestamp) = kernels::aggregate::max(timestamp_column) else {
            return Ok(());
        };
        let max_timestamp = signed_timestamp(max_timestamp)?;

        // calculate watermark using expression
        let watermark = self
            .expression
            .evaluate(&record)?
            .into_array(record.num_rows())?;

        let watermark = watermark
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();

        let watermark = signed_timestamp(kernels::aggregate::min(watermark).unwrap())?;

        self.state_cache.max_watermark = self.state_cache.max_watermark.max(watermark);
        if self.idle
            || max_timestamp
                .duration_since(self.state_cache.last_watermark_emitted_at)
                .unwrap_or(Duration::ZERO)
                > self.interval
        {
            debug!(
                "[{}] Emitting expression watermark {}",
                ctx.task_info.task_index,
                print_time(watermark)
            );
            collector
                .broadcast_watermark(Watermark::EventTime(watermark))
                .await?;
            self.state_cache.last_watermark_emitted_at = max_timestamp;
            self.idle = false;
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let gs = ctx
            .table_manager
            .get_global_keyed_state_migratable::<u32, PersistedWatermarkGeneratorState>("s")
            .await?;

        gs.insert(ctx.task_info.task_index, self.state_cache.try_into()?)
            .await;
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

    #[test]
    fn persisted_generator_state_migrates_legacy_and_preserves_negative_idle_emission() {
        let config = bincode::config::standard();
        // Version 0: two SystemTime values, each encoded as seconds/nanoseconds.
        let legacy_bytes = [3, 0, 2, 0];
        let (old, consumed) =
            bincode::decode_from_slice::<WatermarkGeneratorState, _>(&legacy_bytes, config)
                .unwrap();
        assert_eq!(consumed, legacy_bytes.len());
        let migrated = PersistedWatermarkGeneratorState::migrate(old).unwrap();
        assert_eq!(migrated.last_watermark_emitted_at, 3_000_000_000);
        assert_eq!(migrated.max_watermark, 2_000_000_000);
        // The runtime can record a pre-epoch FOR after the idle branch emits.
        let state = WatermarkGeneratorState {
            last_watermark_emitted_at: signed_timestamp(-2_000_000_001).unwrap(),
            max_watermark: SystemTime::UNIX_EPOCH,
        };
        let persisted = PersistedWatermarkGeneratorState::try_from(state).unwrap();
        let bytes = bincode::encode_to_vec(persisted, config).unwrap();
        let (restored, consumed) =
            bincode::decode_from_slice::<PersistedWatermarkGeneratorState, _>(&bytes, config)
                .unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(WatermarkGeneratorState::try_from(restored).unwrap(), state);
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
