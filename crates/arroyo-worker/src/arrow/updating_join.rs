//! Updating equijoins retain identity-indexed rows in one checkpoint namespace.
//! Fanout is streamed one pair at a time; independent input branches are not atomic.
use super::aggregate_store::{AggregateScope, AggregateStore, AggregateStoreLimits};
use super::execution::{ExecutionResources, configured_execution_resources};
use anyhow::{Context, Result};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, BooleanArray, FixedSizeBinaryArray, RecordBatch, StructArray};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, ConstructedOperator, Registry};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::config::JoinStateConfig;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{
    api::JoinOperator,
    rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
};
use arroyo_rpc::{UPDATING_META_FIELD, updating_meta_fields};
use arroyo_state::live::worker::{
    ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{Cursor, Write},
    sync::{Arc, RwLock},
};

macro_rules! join_ensure {
    ($condition:expr, $($message:tt)*) => {
        if !$condition { return Err(anyhow::anyhow!($($message)*).into()); }
    };
}

const TABLE: &str = "updating-join-v1";

pub struct UpdatingJoin {
    schemas: [ArroyoSchema; 2],
    converters: [RowConverter; 2],
    passers: [Arc<RwLock<Option<RecordBatch>>>; 2],
    plan: Arc<dyn ExecutionPlan>,
    execution: Arc<ExecutionResources>,
    limits: JoinStateConfig,
    left_outer: bool,
    schema_identity: Vec<u8>,
    store: Option<AggregateStore>,
}

fn metadata(batch: &RecordBatch) -> Result<(&BooleanArray, &FixedSizeBinaryArray)> {
    let meta = batch
        .column_by_name(UPDATING_META_FIELD)
        .context("updating join requires changelog metadata")?
        .as_any()
        .downcast_ref::<StructArray>()
        .context("updating join metadata must be a struct")?;
    let flags = meta
        .column_by_name("is_retract")
        .context("missing retract flag")?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("invalid retract flag")?;
    let ids = meta
        .column_by_name("id")
        .context("missing row identity")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("invalid row identity")?;
    join_ensure!(
        meta.null_count() == 0
            && flags.null_count() == 0
            && ids.null_count() == 0
            && ids.value_length() == 16,
        "updating join requires non-null flags and 16-byte identities"
    );
    Ok((flags, ids))
}

fn identity_key(side: usize, id: &[u8]) -> Vec<u8> {
    let mut key = vec![b'I', side as u8];
    key.extend_from_slice(id);
    key
}
fn row_prefix(side: usize, key: &[u8]) -> Result<Vec<u8>> {
    let mut prefix = vec![b'R', side as u8];
    prefix.extend_from_slice(&u32::try_from(key.len())?.to_be_bytes());
    prefix.extend_from_slice(key);
    Ok(prefix)
}
struct BoundedBuffer {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max)
        {
            return Err(std::io::Error::other(
                "updating join row exceeds value-bytes",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(batch: &RecordBatch, max: usize) -> Result<Vec<u8>> {
    let mut output = BoundedBuffer {
        bytes: Vec::new(),
        max,
    };
    {
        let mut writer = StreamWriter::try_new(&mut output, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(output.bytes)
}

fn decode(bytes: &[u8], schema: &ArroyoSchema) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let batch = reader
        .next()
        .transpose()?
        .context("missing updating join row")?;
    join_ensure!(
        batch.num_rows() == 1 && batch.schema() == schema.schema && reader.next().is_none(),
        "invalid updating join row/schema"
    );
    let index = arrow_array::UInt64Array::from(vec![0]);
    let columns = batch
        .columns()
        .iter()
        .map(|column| compact_array(arrow::compute::take(column.as_ref(), &index, None)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[derive(Clone, Copy)]
struct FanoutChange<'a> {
    side: usize,
    row: &'a RecordBatch,
    opposite_prefix: &'a [u8],
    join_key: &'a [u8],
    null_key: bool,
    retract: bool,
    own_key: &'a [u8],
}

// Arrow take retains byte-view backing buffers and dictionary values. Rebuild
// those selected values recursively, including list/struct children.
fn compact_array(array: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    use arrow_schema::DataType;
    match array.data_type() {
        DataType::Utf8View => {
            let view = array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .context("invalid UTF8 view")?;
            Ok(Arc::new(arrow_array::StringViewArray::from_iter(
                view.iter(),
            )))
        }
        DataType::BinaryView => {
            let view = array
                .as_any()
                .downcast_ref::<arrow_array::BinaryViewArray>()
                .context("invalid binary view")?;
            Ok(Arc::new(arrow_array::BinaryViewArray::from_iter(
                view.iter(),
            )))
        }
        DataType::Dictionary(_, value) => {
            let values = arrow::compute::cast(array.as_ref(), value.as_ref())?;
            let values = compact_array(values)?;
            Ok(arrow::compute::cast(values.as_ref(), array.data_type())?)
        }
        _ => {
            let data = array.to_data();
            if data.child_data().is_empty() {
                return Ok(array);
            }
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    compact_array(arrow_array::make_array(child.clone())).map(|a| a.to_data())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(arrow_array::make_array(
                data.into_builder().child_data(children).build()?,
            ))
        }
    }
}

struct Discard;
#[async_trait::async_trait]
impl Collector for Discard {
    async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: arroyo_types::Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

struct PairGuard<'a>(&'a [Arc<RwLock<Option<RecordBatch>>>; 2]);
impl Drop for PairGuard<'_> {
    fn drop(&mut self) {
        for passer in self.0 {
            passer.write().unwrap().take();
        }
    }
}

impl UpdatingJoin {
    async fn emit(
        &self,
        left: RecordBatch,
        right: Option<RecordBatch>,
        retract: bool,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let (_, left_ids) = metadata(&left)?;
        let mut hash = Sha256::new();
        hash.update(b"streamr.updating-join.pair.v1");
        hash.update(left_ids.value(0));
        if let Some(right) = &right {
            hash.update([1]);
            hash.update(metadata(right)?.1.value(0));
        } else {
            hash.update([0]);
        }
        let id = hash.finalize();
        let right = right.unwrap_or_else(|| RecordBatch::new_empty(self.schemas[1].schema.clone()));
        let left = self.schemas[0].unkeyed_batch(&left)?;
        let right = self.schemas[1].unkeyed_batch(&right)?;
        let _pair_guard = PairGuard(&self.passers);
        self.passers[0].write().unwrap().replace(left);
        self.passers[1].write().unwrap().replace(right);
        self.plan.reset()?;
        let mut stream = self.plan.execute(0, self.execution.task_context())?;
        let result: DataflowResult<()> = async {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                join_ensure!(
                    batch.num_rows() <= 1,
                    "updating join pair produced multiple rows"
                );
                if batch.num_rows() == 0 {
                    continue;
                }
                let _output = self
                    .execution
                    .reserve_batch("updating join output", &batch)?;
                join_ensure!(
                    batch.get_array_memory_size() <= self.limits.max_pending_output_bytes,
                    "updating join output exceeds max-pending-output-bytes"
                );
                let meta_index = batch.schema().index_of(UPDATING_META_FIELD)?;
                let mut columns = batch.columns().to_vec();
                columns[meta_index] = Arc::new(StructArray::new(
                    updating_meta_fields(),
                    vec![
                        Arc::new(BooleanArray::from(vec![retract])),
                        Arc::new(FixedSizeBinaryArray::try_from_iter(
                            [&id[..16]].into_iter(),
                        )?),
                    ],
                    None,
                ));
                let output = RecordBatch::try_new(batch.schema(), columns)?;
                collector.collect(output).await?;
            }
            Ok(())
        }
        .await;
        result
    }

    async fn change(
        &self,
        side: usize,
        row: RecordBatch,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let store = self
            .store
            .as_ref()
            .context("updating join was not initialized")?;
        let _workspace = self.execution.reserve_bytes(
            "updating join row workspace",
            self.limits
                .value_bytes
                .saturating_mul(12)
                .saturating_add(self.limits.key_bytes.saturating_mul(4)),
        )?;
        join_ensure!(
            row.get_array_memory_size() <= self.limits.value_bytes,
            "updating join decoded row exceeds value-bytes"
        );
        let (flags, ids) = metadata(&row)?;
        let retract = flags.value(0);
        let id = ids.value(0);
        let key_columns = self.schemas[side]
            .storage_keys()
            .context("updating join has no keys")?
            .iter()
            .map(|i| row.column(*i).clone())
            .collect::<Vec<_>>();
        let null_key = key_columns
            .iter()
            .any(|c| c.logical_nulls().is_some_and(|nulls| nulls.is_null(0)));
        let rows = self.converters[side].convert_columns(&key_columns)?;
        let join_key = rows.row(0);
        let mut own_key = row_prefix(side, join_key.as_ref())?;
        own_key.extend_from_slice(id);
        let own_identity = identity_key(side, id);
        let opposite_prefix = row_prefix(1 - side, join_key.as_ref())?;
        let mut scope = store.begin().await?;
        let existing = scope.get(&own_identity).await?;
        let count_key = [b'C'];
        let count = scope
            .get(&count_key)
            .await?
            .map(|bytes| -> Result<u64> { Ok(u64::from_be_bytes(bytes.as_slice().try_into()?)) })
            .transpose()?
            .unwrap_or(0);
        let next_count = if retract {
            count
                .checked_sub(1)
                .context("updating join retained count underflow")?
        } else {
            count
                .checked_add(1)
                .context("updating join retained count overflow")?
        };
        join_ensure!(
            next_count
                <= u64::try_from(self.limits.max_retained_rows)
                    .context("updating join retained limit overflow")?,
            "updating join exceeds max-retained-rows"
        );
        if next_count == 0 {
            scope.delete(&count_key)?;
        } else {
            scope.put(&count_key, &next_count.to_be_bytes())?;
        }
        if retract {
            join_ensure!(
                existing.as_deref() == Some(own_key.as_slice()),
                "updating join retracts missing identity or changed join key"
            );
            let stored = scope
                .get(&own_key)
                .await?
                .context("updating join missing indexed row")?;
            let old = decode(&stored, &self.schemas[side])?;
            // Retract metadata differs by definition; all user values and timestamp must match.
            for (i, field) in row.schema().fields().iter().enumerate() {
                if field.name() != UPDATING_META_FIELD {
                    join_ensure!(
                        row.column(i).as_ref() == old.column(i).as_ref(),
                        "updating join retract before-image does not match retained row"
                    );
                }
            }
            scope.delete(&own_key)?;
            scope.delete(&own_identity)?;
        } else {
            join_ensure!(
                existing.is_none(),
                "updating join appends existing identity without retraction"
            );
            let bytes = encode(&row, self.limits.value_bytes)?;
            scope.put(&own_key, &bytes)?;
            scope.put(&own_identity, &own_key)?;
        }
        // Validate all deterministic probe and output limits before publishing.
        let change = FanoutChange {
            side,
            row: &row,
            opposite_prefix: &opposite_prefix,
            join_key: join_key.as_ref(),
            null_key,
            retract,
            own_key: &own_key,
        };
        let mut discard = Discard;
        self.fanout(&change, &scope, &mut discard).await?;
        self.fanout(&change, &scope, collector).await?;
        scope.commit().await?;
        Ok(())
    }
    async fn fanout(
        &self,
        change: &FanoutChange<'_>,
        scope: &AggregateScope<'_>,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let FanoutChange {
            side,
            row,
            opposite_prefix,
            join_key,
            null_key,
            retract,
            own_key,
        } = *change;
        let mut after = None;
        let mut probes = 0usize;
        if !null_key {
            loop {
                let Some((key, bytes)) =
                    scope.first_from(opposite_prefix, after.as_deref()).await?
                else {
                    break;
                };
                probes += 1;
                join_ensure!(
                    probes <= self.limits.max_probe_rows,
                    "updating join exceeds max-probe-rows"
                );
                let opposite = decode(&bytes, &self.schemas[1 - side])?;
                join_ensure!(
                    opposite.get_array_memory_size() <= self.limits.value_bytes,
                    "updating join decoded probe row exceeds value-bytes"
                );
                if side == 0 {
                    self.emit(row.clone(), Some(opposite), retract, collector)
                        .await?;
                } else if self.left_outer {
                    let right_prefix = row_prefix(1, join_key)?;
                    let first = scope.first(&right_prefix).await?;
                    let only_current = if let Some((key, _)) = first {
                        key == own_key
                            && scope.first_from(&right_prefix, Some(&key)).await?.is_none()
                    } else {
                        false
                    };
                    if (!retract && only_current)
                        || (retract && scope.first(&right_prefix).await?.is_none())
                    {
                        if !retract {
                            self.emit(opposite.clone(), None, true, collector).await?;
                        }
                        self.emit(opposite.clone(), Some(row.clone()), retract, collector)
                            .await?;
                        if retract {
                            self.emit(opposite, None, false, collector).await?;
                        }
                    } else {
                        self.emit(opposite, Some(row.clone()), retract, collector)
                            .await?;
                    }
                } else {
                    self.emit(opposite, Some(row.clone()), retract, collector)
                        .await?;
                }
                after = Some(key);
            }
        }
        if side == 0 && self.left_outer && probes == 0 {
            self.emit(row.clone(), None, retract, collector).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ArrowOperator for UpdatingJoin {
    fn name(&self) -> String {
        "UpdatingJoin".into()
    }
    async fn process_batch(
        &mut self,
        _: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        unreachable!()
    }
    async fn process_batch_index(
        &mut self,
        index: usize,
        total_inputs: usize,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        join_ensure!(
            total_inputs >= 2 && total_inputs.is_multiple_of(2),
            "updating join requires two input sides"
        );
        let side = index / (total_inputs / 2);
        join_ensure!(side < 2, "invalid updating join input");
        let _input = self
            .execution
            .reserve_batch("updating join input", &batch)?;
        for row in 0..batch.num_rows() {
            let _compact = self.execution.reserve_bytes(
                "updating join compact input",
                batch.get_array_memory_size().saturating_mul(4),
            )?;
            let index = arrow_array::UInt64Array::from(vec![
                u64::try_from(row).context("updating join row index overflow")?,
            ]);
            let columns = batch
                .columns()
                .iter()
                .map(|column| compact_array(arrow::compute::take(column.as_ref(), &index, None)?))
                .collect::<Result<Vec<_>>>()?;
            let row = RecordBatch::try_new(batch.schema(), columns)?;
            self.change(side, row, collector).await?;
        }
        Ok(())
    }
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let resources = configured_worker_resources()
            .context("updating join worker resources")?
            .context("updating join requires worker.live-state-resources")?;
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
        .await
        .context("construct updating join live backend")?;
        let table = ctx
            .table_manager
            .register_live_table(TABLE, backend.clone())
            .await?;
        let limits = self.limits;
        self.store = Some(AggregateStore::new(
            backend,
            table,
            resources,
            AggregateStoreLimits {
                key_bytes: limits.key_bytes,
                value_bytes: limits.value_bytes,
                page_bytes: limits.page_bytes,
                page_entries: limits.page_entries,
                write_bytes: limits.write_bytes,
                write_operations: limits.write_operations,
                overlay_bytes: limits.overlay_bytes,
            },
        )?);
        Ok(())
    }
    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::from([(
            TABLE.into(),
            TableConfig {
                table_type: TableEnum::DiskKeyedMap.into(),
                state_version: 1,
                config: DiskKeyedTableConfig {
                    table_name: TABLE.into(),
                    encoding_version: 1,
                    schema_identity: self.schema_identity.clone(),
                }
                .encode_to_vec(),
            },
        )])
    }
}

pub(super) fn construct(
    config: JoinOperator,
    registry: Arc<Registry>,
) -> Result<ConstructedOperator> {
    let limits = arroyo_rpc::config::config()
        .worker
        .join_state
        .context("updating joins require worker.join-state")?;
    limits.validate()?;
    let execution = configured_execution_resources()?
        .context("updating joins require worker.execution-resources")?;
    Ok(ConstructedOperator::from_operator(Box::new(build(
        config, registry, limits, execution,
    )?)))
}

fn build(
    config: JoinOperator,
    registry: Arc<Registry>,
    limits: JoinStateConfig,
    execution: Arc<ExecutionResources>,
) -> Result<UpdatingJoin> {
    limits.validate()?;
    let passers = [Arc::new(RwLock::new(None)), Arc::new(RwLock::new(None))];
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::LockedJoinPair {
            left: passers[0].clone(),
            right: passers[1].clone(),
        },
    };
    let plan = PhysicalPlanNode::decode(config.join_plan.as_slice())?.try_into_physical_plan(
        registry.as_ref(),
        execution.runtime.as_ref(),
        &codec,
    )?;
    let schemas: [ArroyoSchema; 2] = [
        config
            .left_schema
            .clone()
            .context("missing left join schema")?
            .try_into()?,
        config
            .right_schema
            .clone()
            .context("missing right join schema")?
            .try_into()?,
    ];
    let converters = schemas
        .iter()
        .map(|schema| {
            let keys = schema
                .storage_keys()
                .context("updating join requires keyed inputs")?;
            RowConverter::new(
                keys.iter()
                    .map(|i| SortField::new(schema.schema.field(*i).data_type().clone()))
                    .collect(),
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut converters = converters.into_iter();
    let converters = [converters.next().unwrap(), converters.next().unwrap()];
    let mut hash = Sha256::new();
    hash.update(b"streamr.updating-join.v1");
    hash.update(config.encode_to_vec());
    Ok(UpdatingJoin {
        schemas,
        converters,
        passers,
        plan,
        execution,
        limits,
        left_outer: config.left_outer,
        schema_identity: hash.finalize().to_vec(),
        store: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, TimestampNanosecondArray};
    use arrow_schema::DataType;
    use arroyo_rpc::config::ExecutionResourceConfig;
    use arroyo_state::live::{
        LiveStateBackend, Ownership,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
        table::LiveTableManager,
    };

    #[derive(Default)]
    struct Capture(Vec<RecordBatch>);
    #[async_trait::async_trait]
    impl Collector for Capture {
        async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
            self.0.push(batch);
            Ok(())
        }
        async fn broadcast_watermark(&mut self, _: arroyo_types::Watermark) -> DataflowResult<()> {
            Ok(())
        }
    }

    async fn planned_config(left_outer: bool) -> JoinOperator {
        let kind = if left_outer { "LEFT" } else { "INNER" };
        let sql = format!("CREATE TABLE events (item BIGINT, k BIGINT, amount BIGINT, side BOOLEAN, attribute TEXT) WITH (connector = 'single_file', path = '/tmp/updating-join-input.jsonl', format = 'json', type = 'source');
            WITH l AS (SELECT item AS li, k, SUM(amount) AS lv,MAX(attribute) AS la FROM events WHERE side GROUP BY item,k),
                 r AS (SELECT item AS ri, k, SUM(amount) AS rv,MAX(attribute) AS ra FROM events WHERE NOT side GROUP BY item,k)
            SELECT l.li,r.ri,l.lv,r.rv,l.la,r.ra FROM l {kind} JOIN r ON l.k=r.k");
        let compiled = arroyo_planner::parse_and_get_program(
            &sql,
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let config = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|n| n.operator_chain.iter())
            .map(|(op, _)| op)
            .find(|op| op.operator_name == arroyo_datastream::logical::OperatorName::Join)
            .unwrap();
        let config = JoinOperator::decode(config.operator_config.as_slice()).unwrap();
        assert!(config.updating);
        config
    }

    async fn operator(left_outer: bool) -> UpdatingJoin {
        operator_from_config(planned_config(left_outer).await)
    }

    fn operator_from_config(config: JoinOperator) -> UpdatingJoin {
        let limits = JoinStateConfig {
            max_retained_rows: 100,
            max_probe_rows: 10,
            key_bytes: 256,
            value_bytes: 8192,
            page_bytes: 32768,
            page_entries: 1,
            write_bytes: 65536,
            write_operations: 8,
            overlay_bytes: 65536,
            max_pending_output_bytes: 65536,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        let execution = Arc::new(
            ExecutionResources::new(ExecutionResourceConfig {
                memory_bytes: 16 * 1024 * 1024,
                max_batch_bytes: 1024 * 1024,
            })
            .unwrap(),
        );
        let mut op = build(
            config,
            Arc::new(arroyo_planner::physical::new_registry()),
            limits,
            execution,
        )
        .unwrap();
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> = Arc::new(
            MemoryLiveState::bounded(resources.clone(), limits.max_resident_bytes).unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(TABLE).unwrap();
        op.store = Some(
            AggregateStore::new(
                backend,
                table,
                resources,
                AggregateStoreLimits {
                    key_bytes: limits.key_bytes,
                    value_bytes: limits.value_bytes,
                    page_bytes: limits.page_bytes,
                    page_entries: limits.page_entries,
                    write_bytes: limits.write_bytes,
                    write_operations: limits.write_operations,
                    overlay_bytes: limits.overlay_bytes,
                },
            )
            .unwrap(),
        );
        op
    }

    fn row(
        op: &UpdatingJoin,
        side: usize,
        id: u8,
        key: Option<i64>,
        value: i64,
        retract: bool,
    ) -> RecordBatch {
        let schema = op.schemas[side].schema.clone();
        let columns = schema
            .fields()
            .iter()
            .map(|field| -> Arc<dyn Array> {
                match field.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![if field.name() == "k"
                        || field.name().starts_with("_key_")
                    {
                        key
                    } else if field.name() == "li" || field.name() == "ri" {
                        Some(i64::from(id))
                    } else {
                        Some(value)
                    }])),
                    DataType::Utf8 => {
                        Arc::new(arrow_array::StringArray::from(vec!["x".repeat(2048)]))
                    }
                    DataType::Timestamp(_, _) => Arc::new(TimestampNanosecondArray::from(vec![1])),
                    DataType::Struct(_) => Arc::new(StructArray::new(
                        updating_meta_fields(),
                        vec![
                            Arc::new(BooleanArray::from(vec![retract])),
                            Arc::new(
                                FixedSizeBinaryArray::try_from_iter([&[id; 16][..]].into_iter())
                                    .unwrap(),
                            ),
                        ],
                        None,
                    )),
                    other => panic!("unexpected field {field:?}: {other:?}"),
                }
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }
    fn changes(capture: &Capture) -> Vec<(bool, Vec<u8>, Option<i64>, Option<i64>)> {
        capture
            .0
            .iter()
            .map(|batch| {
                let (flags, ids) = metadata(batch).unwrap();
                let value = |name| {
                    let a = batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (!a.is_null(0)).then(|| a.value(0))
                };
                (
                    flags.value(0),
                    ids.value(0).to_vec(),
                    value("lv"),
                    value("rv"),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn planned_left_first_last_duplicates_recreation_and_key_changes() {
        let op = operator(true).await;
        let mut capture = Capture::default();
        for (side, id, key, value, retract) in [
            (0, 1, Some(7), 10, false),
            (1, 2, Some(7), 20, false),
            (1, 3, Some(7), 20, false),
            (1, 2, Some(7), 20, true),
            (1, 3, Some(7), 20, true),
            (0, 1, Some(7), 10, true),
            (0, 1, Some(8), 11, false),
            (1, 2, Some(8), 21, false),
        ] {
            op.change(side, row(&op, side, id, key, value, retract), &mut capture)
                .await
                .unwrap();
        }
        let got = changes(&capture);
        let expected = [
            (false, Some(10), None),
            (true, Some(10), None),
            (false, Some(10), Some(20)),
            (false, Some(10), Some(20)),
            (true, Some(10), Some(20)),
            (true, Some(10), Some(20)),
            (false, Some(10), None),
            (true, Some(10), None),
            (false, Some(11), None),
            (true, Some(11), None),
            (false, Some(11), Some(21)),
        ];
        assert_eq!(
            got.iter()
                .map(|(r, _, l, v)| (*r, *l, *v))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(got[0].1, got[1].1);
        assert_eq!(got[2].1, got[4].1);
        assert_ne!(got[2].1, got[3].1);
        assert_eq!(got[0].1, got[6].1);
        assert_eq!(got[2].1, got[10].1); // Same pair identities survive deletion and changed join keys.
    }
    #[test]
    fn selected_byte_view_and_nested_view_do_not_retain_batch_buffers() {
        let values = (0..8).map(|_| "x".repeat(8192)).collect::<Vec<_>>();
        let input: Arc<dyn Array> = Arc::new(arrow_array::StringViewArray::from_iter(
            values.iter().map(|v| Some(v.as_str())),
        ));
        let index = arrow_array::UInt64Array::from(vec![0]);
        let selected = arrow::compute::take(input.as_ref(), &index, None).unwrap();
        assert!(selected.get_array_memory_size() >= input.get_array_memory_size() - 256);
        let compact = compact_array(selected.clone()).unwrap();
        assert!(compact.get_array_memory_size() < input.get_array_memory_size() / 2);
        assert_eq!(
            compact
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .unwrap()
                .value(0),
            values[0]
        );
        let nested: Arc<dyn Array> = Arc::new(StructArray::new(
            vec![Arc::new(arrow_schema::Field::new(
                "v",
                arrow_schema::DataType::Utf8View,
                true,
            ))]
            .into(),
            vec![selected],
            None,
        ));
        let nested = compact_array(nested).unwrap();
        assert!(nested.get_array_memory_size() < input.get_array_memory_size() / 2);
    }

    #[tokio::test]
    async fn dictionary_null_values_never_match_even_with_valid_keys() {
        use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
        let mut op = operator(true).await;
        let bases = [
            row(&op, 0, 1, None, 10, false),
            row(&op, 1, 2, None, 20, false),
        ];
        let data_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64));
        let mut changes_in = Vec::new();
        for (side, base) in bases.into_iter().enumerate() {
            let keys = op.schemas[side].storage_keys().unwrap().clone();
            let mut fields = op.schemas[side].schema.fields().to_vec();
            fields[keys[0]] = Arc::new(arrow_schema::Field::new(
                fields[keys[0]].name(),
                data_type.clone(),
                true,
            ));
            let schema = Arc::new(arrow_schema::Schema::new(fields));
            op.schemas[side] = ArroyoSchema::new_keyed(
                schema.clone(),
                op.schemas[side].timestamp_index,
                keys.clone(),
            );
            op.converters[side] =
                RowConverter::new(vec![SortField::new(data_type.clone())]).unwrap();
            let dictionary = DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(Int64Array::from(vec![None])),
            )
            .unwrap();
            assert!(!dictionary.is_null(0));
            assert!(dictionary.logical_nulls().unwrap().is_null(0));
            let mut columns = base.columns().to_vec();
            columns[keys[0]] = Arc::new(dictionary);
            changes_in.push(RecordBatch::try_new(schema, columns).unwrap());
        }
        let mut capture = Capture::default();
        for (side, row) in changes_in.into_iter().enumerate() {
            op.change(side, row, &mut capture).await.unwrap();
        }
        let got = changes(&capture);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].0, got[0].2, got[0].3), (false, Some(10), None));
    }

    #[tokio::test]
    async fn planned_batch_eight_compacts_rows_and_releases_cancelled_fanout() {
        use std::time::Duration;
        let mut op = operator(true).await;
        let rows = (1..=8)
            .map(|id| row(&op, 0, id, Some(i64::from(id)), 10, false))
            .collect::<Vec<_>>();
        let batch = arrow::compute::concat_batches(&op.schemas[0].schema, rows.iter()).unwrap();
        assert!(batch.get_array_memory_size() > op.limits.value_bytes);
        let (control_tx, _rx) = tokio::sync::mpsc::channel(16);
        let mut ctx = OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control_tx,
            1,
            vec![
                Arc::new(op.schemas[0].clone()),
                Arc::new(op.schemas[1].clone()),
            ],
            None,
            HashMap::new(),
        )
        .await;
        let mut capture = Capture::default();
        op.process_batch_index(0, 2, batch, &mut ctx, &mut capture)
            .await
            .unwrap();
        assert_eq!(capture.0.len(), 8);
        let oversized = row(&op, 0, 21, Some(21), 10, false);
        let mut columns = oversized.columns().to_vec();
        let text = oversized.schema().index_of("la").unwrap();
        columns[text] = Arc::new(arrow_array::StringArray::from(vec!["z".repeat(20_000)]));
        let oversized = RecordBatch::try_new(oversized.schema(), columns).unwrap();
        let before = op.execution.runtime.memory_pool.reserved();
        let error = op
            .process_batch_index(0, 2, oversized, &mut ctx, &mut capture)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("value-bytes"), "{error}");
        assert_eq!(capture.0.len(), 8);
        assert_eq!(op.execution.runtime.memory_pool.reserved(), before);

        struct Blocked;
        #[async_trait::async_trait]
        impl Collector for Blocked {
            async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
                std::future::pending().await
            }
            async fn broadcast_watermark(
                &mut self,
                _: arroyo_types::Watermark,
            ) -> DataflowResult<()> {
                Ok(())
            }
        }
        let prior = op.execution.runtime.memory_pool.reserved();
        let right = row(&op, 1, 20, Some(1), 20, false);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), op.change(1, right, &mut Blocked))
                .await
                .is_err()
        );
        assert_eq!(op.execution.runtime.memory_pool.reserved(), prior);
        for passer in &op.passers {
            assert!(passer.read().unwrap().is_none());
        }
        let scope = op.store.as_ref().unwrap().begin().await.unwrap();
        assert!(
            scope
                .get(&identity_key(1, &[20; 16]))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn planner_chained_join_and_downstream_aggregate_keep_changelog() {
        let sql = "CREATE TABLE events (item BIGINT, k BIGINT, amount BIGINT, side BOOLEAN) WITH (connector = 'single_file', path = '/tmp/updating-join-input.jsonl', format = 'json', type = 'source');
            WITH l AS (SELECT item, k, SUM(amount) AS lv FROM events WHERE side GROUP BY item,k),
                 r AS (SELECT item, k, SUM(amount) AS rv FROM events WHERE NOT side GROUP BY item,k),
                 t AS (SELECT item, k, COUNT(*) AS tv FROM events GROUP BY item,k)
            SELECT l.item,SUM(l.lv) AS total FROM l LEFT JOIN r ON l.k=r.k LEFT JOIN t ON l.k=t.k GROUP BY l.item";
        let compiled = arroyo_planner::parse_and_get_program(
            sql,
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let joins = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|n| n.operator_chain.iter())
            .map(|(op, _)| op)
            .filter(|op| op.operator_name == arroyo_datastream::logical::OperatorName::Join)
            .map(|op| JoinOperator::decode(op.operator_config.as_slice()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(joins.len(), 2);
        for join in joins {
            assert!(join.updating);
            let schema: ArroyoSchema = join.output_schema.unwrap().try_into().unwrap();
            assert_eq!(
                schema
                    .schema
                    .fields()
                    .iter()
                    .filter(|f| f.name() == UPDATING_META_FIELD)
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn inner_nulls_bad_retract_and_fanout_limits_are_precise() {
        let mut op = operator(false).await;
        let mut capture = Capture::default();
        op.change(0, row(&op, 0, 1, None, 10, false), &mut capture)
            .await
            .unwrap();
        op.change(1, row(&op, 1, 2, None, 20, false), &mut capture)
            .await
            .unwrap();
        assert!(capture.0.is_empty());
        let error = op
            .change(0, row(&op, 0, 1, None, 99, true), &mut capture)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("before-image"), "{error}");
        for id in [3, 4] {
            op.change(1, row(&op, 1, id, Some(7), 20, false), &mut capture)
                .await
                .unwrap();
        }
        op.limits.max_probe_rows = 1;
        let error = op
            .change(0, row(&op, 0, 5, Some(7), 10, false), &mut capture)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("max-probe-rows"), "{error}");
        assert!(capture.0.is_empty());
        let scope = op.store.as_ref().unwrap().begin().await.unwrap();
        assert!(
            scope
                .get(&identity_key(0, &[5; 16]))
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn full_checkpoint_nonempty_empty_fresh_backends_preserve_pair_identity() {
        use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
        let planned = planned_config(true).await;
        for rocks in [false, true] {
            let root = std::env::temp_dir()
                .join(format!("updating-join-recovery-{}", uuid::Uuid::new_v4()));
            let remote = root.join("checkpoint");
            std::fs::create_dir_all(&remote).unwrap();
            let resources = WorkerStateResources::new(ResourceConfig {
                block_cache_bytes: 1024 * 1024,
                memtable_bytes: 1024 * 1024,
                queued_write_bytes: 16 * 1024 * 1024,
                decoded_value_bytes: 32 * 1024 * 1024,
                scan_page_bytes: 16 * 1024 * 1024,
                max_blocking_operations: 2,
                max_snapshots: 2,
                max_open_databases: 3,
                disk_reserve_bytes: 0,
            })
            .unwrap();
            let mut backends: Vec<Arc<dyn LiveStateBackend>> = Vec::new();
            let mut native = Vec::new();
            for generation in 0..3 {
                if rocks {
                    let backend = Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.join("live"),
                                job_id: "updating-join".into(),
                                operator_id: "join".into(),
                                subtask: 0,
                                generation,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    );
                    backends.push(backend.clone());
                    native.push(backend);
                } else {
                    backends.push(Arc::new(
                        MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap(),
                    ));
                }
            }
            let mut operators = Vec::new();
            let mut namespace = None;
            for backend in &backends {
                let mut op = operator_from_config(planned.clone());
                let mut manager = LiveTableManager::new(
                    backend.clone(),
                    Ownership::PartitionLocal {
                        subtask: 0,
                        parallelism: 1,
                    },
                )
                .unwrap();
                let table = manager.register(TABLE).unwrap();
                namespace = Some(table.namespace().clone());
                let limits = op.limits;
                op.store = Some(
                    AggregateStore::new(
                        backend.clone(),
                        table,
                        resources.clone(),
                        AggregateStoreLimits {
                            key_bytes: limits.key_bytes,
                            value_bytes: limits.value_bytes,
                            page_bytes: limits.page_bytes,
                            page_entries: 1,
                            write_bytes: limits.write_bytes,
                            write_operations: limits.write_operations,
                            overlay_bytes: limits.overlay_bytes,
                        },
                    )
                    .unwrap(),
                );
                operators.push(op);
            }
            let namespace = namespace.unwrap();
            let config = DiskKeyedTableConfig {
                table_name: TABLE.into(),
                encoding_version: 1,
                schema_identity: operators[0].schema_identity.clone(),
            };
            assert!(
                operators
                    .iter()
                    .all(|op| op.schema_identity == config.schema_identity)
            );
            let storage =
                arroyo_state::get_storage_provider(&arroyo_state::StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{}", remote.display())),
                })
                .await
                .unwrap();
            let mut capture = Capture::default();
            for (side, id, value) in [(0, 1, 10), (1, 2, 20)] {
                operators[0]
                    .change(
                        side,
                        row(&operators[0], side, id, Some(7), value, false),
                        &mut capture,
                    )
                    .await
                    .unwrap();
            }
            let pair_id = changes(&capture)[2].1.clone();
            let snapshot = backends[0].snapshot().await.unwrap();
            let nonempty = checkpoint::export(
                &snapshot,
                &namespace,
                &config,
                &storage,
                "epoch1/join",
                1,
                0,
                0,
            )
            .await
            .unwrap();
            assert_eq!(nonempty.files.iter().map(|f| f.row_count).sum::<u64>(), 5);
            drop(snapshot);
            checkpoint::restore(
                backends[1].as_ref(),
                &namespace,
                &config,
                &nonempty,
                &storage,
            )
            .await
            .unwrap();
            let fresh = &operators[1];
            let mut continued = Capture::default();
            for (value, retract) in [(20, true), (21, false)] {
                fresh
                    .change(1, row(fresh, 1, 2, Some(7), value, retract), &mut continued)
                    .await
                    .unwrap();
            }
            let got = changes(&continued);
            assert_eq!(
                got.iter()
                    .map(|(r, _, l, v)| (*r, *l, *v))
                    .collect::<Vec<_>>(),
                [
                    (true, Some(10), Some(20)),
                    (false, Some(10), None),
                    (true, Some(10), None),
                    (false, Some(10), Some(21)),
                ]
            );
            assert_eq!(got[0].1, pair_id);
            assert_eq!(got[3].1, pair_id);
            fresh
                .change(1, row(fresh, 1, 2, Some(7), 21, true), &mut continued)
                .await
                .unwrap();
            fresh
                .change(0, row(fresh, 0, 1, Some(7), 10, true), &mut continued)
                .await
                .unwrap();
            let snapshot = backends[1].snapshot().await.unwrap();
            let empty = checkpoint::export(
                &snapshot,
                &namespace,
                &config,
                &storage,
                "epoch2/join",
                2,
                0,
                0,
            )
            .await
            .unwrap();
            assert!(empty.files.is_empty());
            assert_eq!(empty.files.iter().map(|f| f.row_count).sum::<u64>(), 0);
            drop(snapshot);
            checkpoint::restore(backends[2].as_ref(), &namespace, &config, &empty, &storage)
                .await
                .unwrap();
            let recreated = &operators[2];
            let scope = recreated.store.as_ref().unwrap().begin().await.unwrap();
            assert!(scope.get(b"C").await.unwrap().is_none());
            assert!(
                scope
                    .get(&identity_key(0, &[1; 16]))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                scope
                    .get(&identity_key(1, &[2; 16]))
                    .await
                    .unwrap()
                    .is_none()
            );
            drop(scope);
            let mut recreated_capture = Capture::default();
            for (side, id, value) in [(0, 1, 10), (1, 2, 22)] {
                recreated
                    .change(
                        side,
                        row(recreated, side, id, Some(7), value, false),
                        &mut recreated_capture,
                    )
                    .await
                    .unwrap();
            }
            let got = changes(&recreated_capture);
            assert_eq!(
                got.iter()
                    .map(|(r, _, l, v)| (*r, *l, *v))
                    .collect::<Vec<_>>(),
                [
                    (false, Some(10), None),
                    (true, Some(10), None),
                    (false, Some(10), Some(22)),
                ]
            );
            assert_eq!(got[2].1, pair_id);
            drop(operators);
            drop(backends);
            for backend in native {
                Arc::try_unwrap(backend)
                    .unwrap_or_else(|_| panic!("retained join Rocks backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
