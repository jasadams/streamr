//! Configured-backend SQL SESSION owner. The existing DataFusion aggregate
//! remains the final computation; only retained input and scheduling move to
//! the bounded live-state table.
use super::{
    execution::configured_execution_resources,
    session_store::{SessionMeta, SessionStore},
};
use anyhow::{Context, Result, ensure};
use arrow::compute::take;
use arrow_array::{
    Array, PrimitiveArray, RecordBatch, StructArray, TimestampNanosecondArray, UInt32Array,
    types::TimestampNanosecondType,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::Registry,
};
use arroyo_planner::{
    physical::{ArroyoPhysicalExtensionCodec, DecodingContext},
    schemas::window_arrow_struct,
};
use arroyo_rpc::{
    Converter,
    config::{WindowStateConfig, config},
    df::ArroyoSchema,
    grpc::{
        api,
        rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
    },
};
use arroyo_state::live::worker::{
    ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend,
};
use arroyo_types::to_nanos;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::{ExecutionPlan, aggregates::AggregateExec};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::{StreamExt, try_join};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc::{Receiver, channel};

const TABLE: &str = "n";

// Arrow's `take` owns one selected row while the input batch remains live. The
// source reservation covers shared dictionary/backing buffers; this separate
// allowance covers copied values, nested offsets and array construction.
fn row_working_bytes(batch: &RecordBatch) -> Result<usize> {
    batch
        .get_array_memory_size()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(batch.num_columns().checked_mul(4096)?))
        .and_then(|bytes| bytes.checked_add(4096))
        .context("native SESSION row working admission overflow")
}

fn output_allowance(
    schema: &Schema,
    output_fields: usize,
    limits: WindowStateConfig,
) -> Result<usize> {
    let variable_fields = schema
        .fields()
        .iter()
        .filter(|field| {
            !matches!(
                field.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
            )
        })
        .count();
    limits
        .partial_bytes
        .checked_mul(variable_fields)
        .and_then(|bytes| bytes.checked_add(limits.key_bytes))
        .and_then(|bytes| bytes.checked_add(output_fields.checked_mul(4096)?))
        .context("native SESSION output admission overflow")
}

pub(crate) struct NativeSession {
    gap: i64,
    input_schema: Arc<ArroyoSchema>,
    window_field: FieldRef,
    window_index: usize,
    finish: Arc<dyn ExecutionPlan>,
    finish_receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    limits: WindowStateConfig,
    identity: Vec<u8>,
    store: Option<SessionStore>,
}

impl NativeSession {
    pub fn new(
        config: &api::SessionWindowAggregateOperator,
        registry: Arc<Registry>,
        limits: WindowStateConfig,
    ) -> Result<Self> {
        limits.validate()?;
        ensure!(config.gap_micros > 0, "native SESSION gap must be positive");
        ensure!(
            arroyo_rpc::config::config()
                .worker
                .execution_resources
                .is_some(),
            "native SESSION requires worker.execution-resources"
        );
        let gap = i64::try_from(config.gap_micros)?
            .checked_mul(1_000)
            .context("native SESSION gap overflow")?;
        let input_schema: ArroyoSchema = config
            .input_schema
            .clone()
            .context("native SESSION input schema missing")?
            .try_into()?;
        let receiver = Arc::new(RwLock::new(None));
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::BoundedBatchStream(receiver.clone()),
        };
        let execution = configured_execution_resources()?
            .context("native SESSION requires execution resources")?;
        let finish = PhysicalPlanNode::decode(config.final_aggregation_plan.as_slice())?
            .try_into_physical_plan(registry.as_ref(), &execution.runtime, &codec)?;
        let aggregate = finish
            .as_any()
            .downcast_ref::<AggregateExec>()
            .context("native SESSION final plan must be a physical aggregate")?;
        ensure!(
            aggregate.group_expr().expr().is_empty(),
            "native SESSION final aggregate must consume one group at a time"
        );
        for expression in aggregate.aggr_expr() {
            let name = expression.fun().name().to_ascii_lowercase();
            let ordered_first_last = matches!(name.as_str(), "first_value" | "last_value")
                && expression
                    .order_bys()
                    .is_some_and(|order| !order.is_empty());
            let scalar = matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
                && expression.order_bys().is_none();
            ensure!(
                !expression.is_distinct() && (scalar || ordered_first_last),
                "native SESSION supports non-distinct COUNT/SUM/AVG/MIN/MAX and ordered FIRST_VALUE/LAST_VALUE; other aggregates require bounded output admission"
            );
        }
        let mut identity = Sha256::new();
        identity.update(b"streamr.native-session-raw.v1");
        identity.update(gap.to_be_bytes());
        identity.update(u64::try_from(config.final_aggregation_plan.len())?.to_be_bytes());
        identity.update(&config.final_aggregation_plan);
        identity.update(serde_json::to_vec(input_schema.schema.as_ref())?);
        identity.update(config.window_field_name.as_bytes());
        identity.update(config.window_index.to_be_bytes());
        Ok(Self {
            gap,
            input_schema: Arc::new(input_schema),
            window_field: Arc::new(Field::new(
                &config.window_field_name,
                window_arrow_struct(),
                true,
            )),
            window_index: config.window_index as usize,
            finish,
            finish_receiver: receiver,
            limits,
            identity: identity.finalize().to_vec(),
            store: None,
        })
    }

    pub fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::from([(
            TABLE.to_owned(),
            TableConfig {
                table_type: TableEnum::DiskKeyedMap.into(),
                state_version: 1,
                config: DiskKeyedTableConfig {
                    table_name: TABLE.into(),
                    encoding_version: 1,
                    schema_identity: self.identity.clone(),
                }
                .encode_to_vec(),
            },
        )])
    }

    pub async fn on_start(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        ensure!(
            ctx.task_info.parallelism == 1 && ctx.task_info.task_index == 0,
            "native SESSION requires one serial owner for its keyed state"
        );
        let worker = config();
        worker.worker.validate_sql_state()?;
        let resources = configured_worker_resources()?
            .context("native SESSION requires worker.live-state-resources")?;
        let generation = match ctx.task_info.checkpoint_file_path_layout {
            arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
            _ => 0,
        };
        let backend = construct_configured_backend(
            ConfiguredBackendOwner {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask: ctx.task_info.task_index,
                generation,
            },
            self.limits.max_resident_bytes,
            resources.clone(),
        )
        .await?;
        let table = ctx
            .table_manager
            .register_live_table(TABLE, backend.clone())
            .await?;
        self.store = Some(SessionStore::new(
            backend,
            table,
            resources,
            self.input_schema.schema.clone(),
            self.limits,
            self.gap,
        )?);
        Ok(())
    }

    fn store(&self) -> Result<&SessionStore> {
        self.store
            .as_ref()
            .context("native SESSION was not started")
    }

    pub async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        converter: &Converter,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let watermark = ctx.last_present_watermark();
        if watermark.is_some_and(|wm| to_nanos(wm) == u64::MAX as u128) {
            return Ok(());
        }
        let execution = configured_execution_resources()?
            .context("native SESSION requires execution resources")?;
        let _source = execution.reserve_batch("native SESSION source", &batch)?;
        let working_bytes = row_working_bytes(&batch)?;
        let mut _working = MemoryConsumer::new("native SESSION owned row")
            .register(&execution.runtime.memory_pool);
        _working.try_grow(working_bytes)?;
        let timestamps = batch
            .column(self.input_schema.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context("native SESSION timestamp type changed")?;
        let floor = watermark
            .map(|wm| i64::try_from(to_nanos(wm)))
            .transpose()?;
        let group_count = self.input_schema.routing_keys().map_or(0, Vec::len);
        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) && floor.is_some() {
                continue;
            }
            ensure!(
                !timestamps.is_null(index),
                "native SESSION timestamp cannot be null"
            );
            if floor.is_some_and(|floor| timestamps.value(index) < floor) {
                continue;
            }
            let indices = UInt32Array::from(vec![u32::try_from(index)?]);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None))
                .collect::<arrow::error::Result<Vec<_>>>()?;
            let row = RecordBatch::try_new(batch.schema(), columns)?;
            ensure!(
                row.get_array_memory_size() <= working_bytes,
                "native SESSION owned row exceeds admitted working bytes"
            );
            let group = if group_count == 0 {
                Vec::new()
            } else {
                converter
                    .convert_columns(&row.columns()[..group_count])?
                    .as_ref()
                    .to_vec()
            };
            self.store()?
                .insert(&group, timestamps.value(index), &row)
                .await?;
        }
        Ok(())
    }

    async fn emit(
        &mut self,
        group: &[u8],
        session: SessionMeta,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
        converter: &Converter,
    ) -> Result<()> {
        let _queue = self.store()?.reserve_final_input_queue()?;
        let snapshot = self.store()?.snapshot().await?;
        let output_fields = ctx
            .out_schema
            .as_ref()
            .context("native SESSION output schema missing")?
            .schema
            .fields()
            .len();
        let output_bytes =
            output_allowance(self.finish.schema().as_ref(), output_fields, self.limits)?;
        let execution = configured_execution_resources()?
            .context("native SESSION requires execution resources")?;
        ensure!(
            output_bytes <= execution.limits.max_batch_bytes,
            "native SESSION output allowance exceeds max-batch-bytes"
        );
        let mut _output = MemoryConsumer::new("native SESSION output allowance")
            .register(&execution.runtime.memory_pool);
        _output.try_grow(output_bytes)?;
        let (sender, receiver) = channel(1);
        *self.finish_receiver.write().unwrap() = Some(receiver);
        self.finish.reset()?;
        let mut finish = self.finish.execute(0, execution.task_context())?;
        let store = self.store()?;
        let producer = async move {
            let mut after = None;
            while let Some(row) = store
                .next_row(&snapshot, group, session, after.as_deref())
                .await?
            {
                after = Some(row.key.clone());
                sender.send(row.batch).await.map_err(|_| {
                    anyhow::anyhow!("native SESSION final aggregate stopped before consuming rows")
                })?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let consumer = async {
            let mut result = None;
            while let Some(batch) = finish.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                ensure!(
                    result.is_none() && batch.num_rows() == 1,
                    "native SESSION final aggregate returned more than one row"
                );
                result = Some(batch);
            }
            Ok::<_, anyhow::Error>(result)
        };
        let (_, result) = try_join!(producer, consumer)?;
        let result = result.context("native SESSION final aggregate returned no row")?;
        ensure!(
            result.get_array_memory_size() <= self.limits.partial_bytes,
            "native SESSION output aggregate exceeds configured value limit"
        );
        let key_columns = if let Some(parser) = converter.parser() {
            converter.convert_rows(vec![parser.parse(group)])?
        } else {
            vec![]
        };
        let start = PrimitiveArray::<TimestampNanosecondType>::from(vec![session.start]);
        let end = session
            .end
            .checked_add(self.gap)
            .context("native SESSION end overflow")?;
        let finish = PrimitiveArray::<TimestampNanosecondType>::from(vec![end]);
        let timestamp = PrimitiveArray::<TimestampNanosecondType>::from(vec![end - 1]);
        let DataType::Struct(fields) = self.window_field.data_type() else {
            anyhow::bail!("native SESSION window field is not a struct");
        };
        let window = StructArray::try_new(
            fields.clone(),
            vec![Arc::new(start), Arc::new(finish)],
            None,
        )?;
        let mut columns = key_columns;
        columns.insert(self.window_index, Arc::new(window));
        columns.extend_from_slice(result.columns());
        columns.push(Arc::new(timestamp));
        let output = RecordBatch::try_new(
            ctx.out_schema
                .as_ref()
                .context("native SESSION output schema missing")?
                .schema
                .clone(),
            columns,
        )?;
        ensure!(
            output.get_array_memory_size() <= output_bytes,
            "native SESSION output exceeds admitted allowance"
        );
        collector.collect(output).await?;
        Ok(())
    }

    pub async fn handle_watermark(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
        converter: &Converter,
    ) -> Result<()> {
        let Some(watermark) = ctx.last_present_watermark() else {
            return Ok(());
        };
        let floor = if to_nanos(watermark) == u64::MAX as u128 {
            None
        } else {
            Some(i64::try_from(to_nanos(watermark))?)
        };
        while let Some((group, session)) = self.store()?.first_due(floor).await? {
            self.emit(&group, session, ctx, collector, converter)
                .await?;
            self.store()?.retire(&group, session).await?;
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::execution::ExecutionResources;
    use arrow_array::StringArray;
    use arrow_schema::Schema;
    use arroyo_rpc::config::ExecutionResourceConfig;

    #[test]
    fn owned_row_needs_separate_admission_while_source_batch_is_live() {
        let long = "x".repeat(8192);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![long.as_str(), "y", "z"]))],
        )
        .unwrap()
        .slice(1, 2);
        let source_bytes = batch.get_array_memory_size();
        let working_bytes = row_working_bytes(&batch).unwrap();
        let resources = ExecutionResources::new(ExecutionResourceConfig {
            memory_bytes: source_bytes + working_bytes - 1,
            max_batch_bytes: source_bytes,
        })
        .unwrap();
        let _source = resources.reserve_batch("source", &batch).unwrap();
        let mut working = MemoryConsumer::new("owned row").register(&resources.runtime.memory_pool);
        assert!(working.try_grow(working_bytes).is_err());

        let resources = ExecutionResources::new(ExecutionResourceConfig {
            memory_bytes: source_bytes + working_bytes,
            max_batch_bytes: source_bytes,
        })
        .unwrap();
        let _source = resources.reserve_batch("source", &batch).unwrap();
        let mut working = MemoryConsumer::new("owned row").register(&resources.runtime.memory_pool);
        working.try_grow(working_bytes).unwrap();
        for index in 0..batch.num_rows() {
            let indices = UInt32Array::from(vec![index as u32]);
            let row = take(batch.column(0), &indices, None).unwrap();
            assert!(row.get_array_memory_size() <= working_bytes);
        }
    }

    #[test]
    fn output_allowance_counts_each_variable_aggregate() {
        let schema = Schema::new(vec![
            Field::new("first_text", DataType::Utf8, true),
            Field::new("last_text", DataType::Utf8, true),
            Field::new("count", DataType::Int64, false),
        ]);
        let limits = WindowStateConfig {
            key_bytes: 128,
            partial_bytes: 1024,
            page_bytes: 8192,
            page_entries: 8,
            write_bytes: 32768,
            write_operations: 16,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        assert_eq!(
            output_allowance(&schema, 6, limits).unwrap(),
            2 * limits.partial_bytes + limits.key_bytes + 6 * 4096
        );
    }
}
