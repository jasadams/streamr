//! Updating equijoins retain identity-indexed rows in one checkpoint namespace.
//! Fanout is streamed one pair at a time; independent input branches are not atomic.
use super::aggregate_store::scan_diagnostics::OutputProbe;
use super::aggregate_store::{AggregateScope, AggregateStore, AggregateStoreLimits};
use crate::event_pair::{EventPair, EventPairState};
use anyhow::{Context, Result};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, BooleanArray, FixedSizeBinaryArray, RecordBatch, StructArray};
use arroyo_operator::CheckpointCounter;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::EventResponseRole;
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
use arroyo_state::live::resources::ResourcePermit;
use arroyo_state::live::worker::{
    ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend,
};
use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::EventResponse;
use arroyo_types::{CheckpointBarrier, SignalMessage};
use datafusion::execution::{TaskContext, runtime_env::RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
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
    limits: JoinStateConfig,
    left_outer: bool,
    schema_identity: Vec<u8>,
    store: Option<AggregateStore>,
    response: Option<ResponseJoin>,
}

struct ResponseJoin {
    pair: EventPairState,
    root_operator: u32,
    accepted: ArroyoSchema,
}
/// Native emit executes its real physical pair plan into this bounded internal
/// collector. It never awaits an external queue while a probe scope is held.
struct FinalNativeRows {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    limit: usize,
}
#[async_trait::async_trait]
impl Collector for FinalNativeRows {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        join_ensure!(
            self.rows
                .checked_add(batch.num_rows())
                .is_some_and(|rows| rows <= 2),
            "event JOIN accepted native output exceeds two full keys"
        );
        let bytes = self
            .bytes
            .checked_add(batch.get_array_memory_size())
            .context("event JOIN output size overflow")?;
        join_ensure!(
            bytes <= self.limit,
            "event JOIN accepted native output exceeds max-pending-output-bytes"
        );
        self.rows += batch.num_rows();
        self.bytes = bytes;
        self.batches.push(batch);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: arroyo_types::Watermark) -> DataflowResult<()> {
        Ok(())
    }
}
struct PreparedAccepted {
    batch: Option<RecordBatch>,
    // Keep output ownership admitted through delivery/error/cancellation. This
    // is NOT a backend snapshot, scan page or working-scope guard.
    _output: Option<ResourcePermit>,
    // Field drop order releases output before the diagnostic release marker.
    diagnostic: OutputProbe,
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

fn compact_row(batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
    let index = arrow_array::UInt64Array::from(vec![
        u64::try_from(row).context("updating join row index overflow")?,
    ]);
    let columns = batch
        .columns()
        .iter()
        .map(|column| compact_array(arrow::compute::take(column.as_ref(), &index, None)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
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
    fn response_keys(&self, response: &EventResponse) -> Result<Vec<Vec<u8>>> {
        let Some(keys) = &response.touched_keys else {
            return Ok(Vec::new());
        };
        join_ensure!(
            keys.schema() == EventEdgeSpec::keys_schema() && (1..=2).contains(&keys.num_rows()),
            "event JOIN touched keys differ from native full-key schema/count"
        );
        let keys = keys
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .context("invalid event JOIN touched key type")?;
        join_ensure!(keys.null_count() == 0, "event JOIN touched key is NULL");
        let mut result = Vec::new();
        for key in keys.iter() {
            let key = key.context("event JOIN touched key is NULL")?;
            // Same native R key: tag + side + u32 length + key + 16-byte id.
            join_ensure!(
                key.len()
                    .checked_add(22)
                    .is_some_and(|n| n <= self.limits.key_bytes),
                "event JOIN touched key exceeds native indexed key allowance"
            );
            join_ensure!(
                !result.iter().any(|old: &Vec<u8>| old.as_slice() == key),
                "event JOIN repeats a touched full key"
            );
            result.push(key.to_vec());
        }
        Ok(result)
    }
    fn encoded_row_key(&self, side: usize, row: &RecordBatch) -> Result<Vec<u8>> {
        let columns = self.schemas[side]
            .storage_keys()
            .context("event JOIN full key missing")?
            .iter()
            .map(|index| row.column(*index).clone())
            .collect::<Vec<_>>();
        Ok(self.converters[side]
            .convert_columns(&columns)?
            .row(0)
            .as_ref()
            .to_vec())
    }
    fn validate_branch_response(&self, side: usize, response: &EventResponse) -> Result<()> {
        join_ensure!(
            self.response.is_some() && side < 2,
            "unregistered event JOIN response input"
        );
        let keys = self.response_keys(response)?;
        if let Some(rows) = &response.rows {
            join_ensure!(
                rows.schema() == self.schemas[side].schema && (1..=4).contains(&rows.num_rows()),
                "event JOIN branch rows differ from registered native schema/count"
            );
            metadata(rows)?;
            join_ensure!(
                !keys.is_empty(),
                "event JOIN native delta has no affected full key"
            );
            for row in 0..rows.num_rows() {
                // Existing native one-row compaction detaches IPC backing;
                // change() retains its exact per-row value-bytes admission.
                let row = compact_row(rows, row)?;
                join_ensure!(
                    row.get_array_memory_size() <= self.limits.value_bytes,
                    "updating join decoded row exceeds value-bytes"
                );
                let key = self.encoded_row_key(side, &row)?;
                join_ensure!(
                    keys.contains(&key),
                    "event JOIN delta lies outside branch touched full keys"
                );
            }
        }
        Ok(())
    }
    /// Existing native R index, at most two returned rows to prove uniqueness.
    /// A valid grouped relation returns <=1; the second row fails immediately.
    async fn final_native_row(
        &self,
        scope: &AggregateScope<'_>,
        side: usize,
        key: &[u8],
    ) -> Result<Option<RecordBatch>> {
        let prefix = row_prefix(side, key)?;
        let mut found = None;
        scope
            .visit_prefix(&prefix, Some(2), |stored, bytes| {
                join_ensure!(
                    found.is_none(),
                    "event JOIN full key is not unique on an input side"
                );
                let row = decode(bytes, &self.schemas[side])?;
                join_ensure!(
                    row.get_array_memory_size() <= self.limits.value_bytes,
                    "updating join decoded probe row exceeds value-bytes"
                );
                let (_, ids) = metadata(&row)?;
                join_ensure!(
                    stored.len() == prefix.len() + 16
                        && stored.starts_with(&prefix)
                        && &stored[prefix.len()..] == ids.value(0)
                        && self.encoded_row_key(side, &row)?.as_slice() == key,
                    "event JOIN native full-key index disagrees with retained row"
                );
                found = Some(row);
                Ok(())
            })
            .await?;
        Ok(found)
    }
    async fn prepare_accepted_pair(
        &self,
        pair: EventPair,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<PreparedAccepted> {
        let mode = self.response.as_ref().context("event JOIN mode missing")?;
        let mut touched = Vec::new();
        for response in &pair.branches {
            for key in self.response_keys(response)? {
                if !touched.contains(&key) {
                    touched.push(key);
                }
            }
        }
        join_ensure!(
            touched.len() <= 2,
            "event JOIN branches disagree on common original full keys"
        );
        if touched.is_empty() {
            return Ok(PreparedAccepted {
                batch: None,
                _output: None,
                diagnostic: OutputProbe::new(),
            });
        }
        let store = self
            .store
            .as_ref()
            .context("updating join was not initialized")?;
        let requested = self
            .limits
            .max_pending_output_bytes
            .checked_mul(3)
            .context("event JOIN output workspace overflow")?;
        // Declare before output so an early prepare error releases output
        // ownership before the same diagnostic terminal marker.
        let mut diagnostic = OutputProbe::new();
        let output = store
            .resources()
            .try_decoded_value(requested)
            .map_err(anyhow::Error::from)?;
        // Preserve existing per-row native scopes and failure recovery. Native
        // intermediate fanout runs into Discard, not a downstream SQL consumer.
        let mut discard = Discard;
        for (side, response) in pair.branches.iter().enumerate() {
            if let Some(rows) = &response.rows {
                for row in 0..rows.num_rows() {
                    self.change(side, compact_row(rows, row)?, &mut discard)
                        .await?;
                }
            }
        }
        let mut capture = FinalNativeRows {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            limit: self.limits.max_pending_output_bytes,
        };
        if !touched.is_empty() {
            let scope = store.begin().await?;
            let scope_id = scope.diagnostic_scope_id();
            diagnostic.bind(scope_id);
            if let Some(scope_id) = scope_id {
                eprintln!(
                    "STREAMR_SCAN_DIAGNOSTIC {}",
                    serde_json::json!({
                        "event":"join_response_context","scope_id":scope_id,
                        "operator_id":ctx.task_info.operator_id,"root_operator":pair.context.root_operator,
                        "root_subtask":pair.context.root_subtask,"attempt_nonce":pair.context.attempt_nonce,
                        "sequence":pair.context.sequence,"trigger_nanos":pair.context.trigger_nanos,"touched_full_keys":touched.len(),
                    })
                );
            }
            for key in touched {
                let left = self.final_native_row(&scope, 0, &key).await?;
                let right = self.final_native_row(&scope, 1, &key).await?;
                if let Some(left) = left {
                    // GROUP BY coalesces NULL keys, ON = must not match NULL.
                    // Reuse the same logical-null test as ordinary native change
                    // and native emit/physical join for LEFT/INNER absence.
                    let null = self.schemas[0]
                        .storage_keys()
                        .context("event JOIN full key missing")?
                        .iter()
                        .any(|index| {
                            left.column(*index)
                                .logical_nulls()
                                .is_some_and(|nulls| nulls.is_null(0))
                        });
                    self.emit(left, if null { None } else { right }, false, &mut capture)
                        .await?;
                }
            }
            drop(scope); // No final snapshot/scan/overlay guard enters public await.
        }
        join_ensure!(
            self.passers
                .iter()
                .all(|passer| passer.read().unwrap().is_none()),
            "event JOIN native physical input guard survived final emit"
        );
        let batch = if capture.batches.is_empty() {
            None
        } else {
            let native_schema = self.plan.schema();
            let native = arrow::compute::concat_batches(&native_schema, &capture.batches)?;
            let meta = native.schema().index_of(UPDATING_META_FIELD)?;
            let mut columns = native.columns().to_vec();
            columns.remove(meta);
            columns[mode.accepted.timestamp_index] =
                Arc::new(arrow_array::TimestampNanosecondArray::from(
                    vec![pair.context.trigger_nanos; native.num_rows()],
                ));
            let batch = RecordBatch::try_new(mode.accepted.schema.clone(), columns)?;
            join_ensure!(
                batch.num_rows() <= 2
                    && batch.get_array_memory_size() <= self.limits.max_pending_output_bytes,
                "event JOIN accepted output exceeds native pending-output allowance"
            );
            join_ensure!(
                ctx.out_schema
                    .as_ref()
                    .is_some_and(|out| out.schema == mode.accepted.schema),
                "event JOIN public output schema differs from registered accepted boundary"
            );
            Some(batch)
        };
        // Native stored clocks/beforeimages are untouched. Only this append
        // accepted boundary carries root E even when its hidden clock is late.
        Ok(PreparedAccepted {
            batch,
            _output: Some(output),
            diagnostic,
        })
    }

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
        let mut stream = self.plan.execute(0, Arc::new(TaskContext::default()))?;
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
        // Both passes use the same immutable snapshot and mutation overlay.
        // Cache only the scalar LEFT unmatched decision, lazily after a match.
        let mut unmatched_transition = None;
        let mut discard = Discard;
        self.fanout(&change, &scope, &mut unmatched_transition, &mut discard)
            .await?;
        self.fanout(&change, &scope, &mut unmatched_transition, collector)
            .await?;
        scope.commit().await?;
        Ok(())
    }
    async fn fanout(
        &self,
        change: &FanoutChange<'_>,
        scope: &AggregateScope<'_>,
        unmatched_transition: &mut Option<bool>,
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
                    let transition = if let Some(transition) = *unmatched_transition {
                        transition
                    } else {
                        let right_prefix = row_prefix(1, join_key)?;
                        let first = scope.first(&right_prefix).await?;
                        let transition = if retract {
                            first.is_none()
                        } else if let Some((key, _)) = first {
                            key == own_key
                                && scope.first_from(&right_prefix, Some(&key)).await?.is_none()
                        } else {
                            false
                        };
                        *unmatched_transition = Some(transition);
                        transition
                    };
                    if transition {
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
    fn event_response_role(&self) -> EventResponseRole {
        self.response
            .as_ref()
            .map_or(EventResponseRole::Ordinary, |mode| {
                EventResponseRole::Join(mode.root_operator)
            })
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
        let mode = self
            .response
            .as_ref()
            .context("JOIN has no planned response mode")?;
        join_ensure!(
            inputs.len() == 2 && output.schema == mode.accepted.schema,
            "event JOIN accepted/input descriptor mismatch"
        );
        for (side, input) in inputs.iter().enumerate() {
            join_ensure!(
                input.root_operator == mode.root_operator
                    && input.root_subtask == 0
                    && input.schema == self.schemas[side].schema
                    && matches!(
                        input.layout,
                        arroyo_types::event_edge::EventLayout::Native { .. }
                    ),
                "event JOIN requires two matching native branch descriptors"
            );
        }
        Ok(None)
    }

    fn name(&self) -> String {
        "UpdatingJoin".into()
    }
    fn event_pair_mode(&self) -> bool {
        self.response.is_some()
    }
    fn event_input_blocked(&self, index: usize) -> bool {
        self.response
            .as_ref()
            .is_some_and(|mode| mode.pair.blocked(index))
    }
    fn check_event_response_input(
        &self,
        index: usize,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        if let Some(mode) = &self.response {
            mode.pair
                .before_response_with_closed(index, counter, closed)?;
        }
        Ok(())
    }
    fn check_event_control_input(
        &self,
        index: usize,
        signal: &SignalMessage,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        if let Some(mode) = &self.response {
            mode.pair
                .check_control_with_closed(index, signal, counter, closed)?;
        }
        Ok(())
    }
    fn check_event_input_eof(
        &self,
        index: usize,
        counter: &CheckpointCounter,
        closed: &HashSet<usize>,
    ) -> DataflowResult<()> {
        if let Some(mode) = &self.response {
            mode.pair.check_eof(index, counter, closed)?;
        }
        Ok(())
    }
    async fn process_response_index(
        &mut self,
        index: usize,
        in_partitions: usize,
        response: EventResponse,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        join_ensure!(
            in_partitions == 2 && index < 2 && ctx.in_schemas.len() == 2,
            "event JOIN requires exactly two singleton inputs"
        );
        self.validate_branch_response(index, &response)?;
        let pair = self
            .response
            .as_mut()
            .context("JOIN is not configured for event responses")?
            .pair
            .receive(index, response)?;
        let Some(pair) = pair else { return Ok(()) };
        let context = pair.context;
        let accepted = self.prepare_accepted_pair(pair, ctx).await?;
        if let Some(batch) = accepted.batch {
            accepted.diagnostic.event("collector_begin");
            let delivered = collector.collect(batch).await;
            accepted.diagnostic.event(if delivered.is_ok() {
                "collector_complete"
            } else {
                "collector_error"
            });
            delivered?;
        }
        // Both inputs stay parked in Processing while output awaits. Errors or
        // cancellation never finish; the attempt must fail and restore fresh.
        self.response
            .as_mut()
            .context("event JOIN mode disappeared")?
            .pair
            .finish(context)?;
        drop(accepted._output);
        Ok(())
    }
    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        join_ensure!(
            self.response
                .as_ref()
                .is_none_or(|mode| !mode.pair.pending()),
            "event JOIN checkpoint contains an unfinished pair"
        );
        Ok(())
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
        join_ensure!(self.response.is_none(), "ordinary Data on event JOIN input");
        join_ensure!(
            total_inputs >= 2 && total_inputs.is_multiple_of(2),
            "updating join requires two input sides"
        );
        let side = index / (total_inputs / 2);
        join_ensure!(side < 2, "invalid updating join input");
        for row in 0..batch.num_rows() {
            let row = compact_row(&batch, row)?;
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
    Ok(ConstructedOperator::from_operator(Box::new(build(
        config, registry, limits,
    )?)))
}

fn build(
    config: JoinOperator,
    registry: Arc<Registry>,
    limits: JoinStateConfig,
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
        &RuntimeEnv::default(),
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
    let response = match (
        config.response_root_operator,
        config.accepted_output_schema.clone(),
    ) {
        (None, None) => None,
        (Some(root), Some(accepted)) => {
            join_ensure!(config.updating, "event JOIN requires native updating mode");
            let native: ArroyoSchema = config
                .output_schema
                .clone()
                .context("event JOIN native output schema missing")?
                .try_into()?;
            let accepted: ArroyoSchema = accepted.try_into()?;
            join_ensure!(
                native.timestamp_index < native.schema.fields().len()
                    && accepted.timestamp_index < accepted.schema.fields().len(),
                "event JOIN timestamp ordinal outside configured schema"
            );
            let meta = native.schema.index_of(UPDATING_META_FIELD)?;
            join_ensure!(
                plan.schema() == native.schema
                    && native.schema.field(meta) == arroyo_rpc::updating_meta_field().as_ref(),
                "event JOIN requires canonical native output metadata and actual physical output schema"
            );
            let mut fields = native.schema.fields().to_vec();
            fields.remove(meta);
            let public =
                arrow_schema::Schema::new_with_metadata(fields, native.schema.metadata().clone());
            join_ensure!(
                meta != native.timestamp_index
                    && accepted.schema.as_ref() == &public
                    && accepted.schema.index_of(UPDATING_META_FIELD).is_err()
                    && accepted.timestamp_index
                        == native.timestamp_index - usize::from(meta < native.timestamp_index)
                    && accepted.schema.field(accepted.timestamp_index).name()
                        == arroyo_rpc::TIMESTAMP_FIELD
                    && accepted.schema.field(accepted.timestamp_index).data_type()
                        == &arrow_schema::DataType::Timestamp(
                            arrow_schema::TimeUnit::Nanosecond,
                            None
                        ),
                "event JOIN accepted schema must remove only native metadata"
            );
            let left = schemas[0]
                .storage_keys()
                .context("event JOIN left full key missing")?;
            let right = schemas[1]
                .storage_keys()
                .context("event JOIN right full key missing")?;
            join_ensure!(
                !left.is_empty()
                    && left.len() == right.len()
                    && left.iter().zip(right).all(|(left, right)| schemas[0]
                        .schema
                        .field(*left)
                        .data_type()
                        == schemas[1].schema.field(*right).data_type()),
                "event JOIN requires equal full-key native encodings"
            );
            for schema in &schemas {
                let meta = schema.schema.index_of(UPDATING_META_FIELD)?;
                join_ensure!(
                    schema.schema.field(meta) == arroyo_rpc::updating_meta_field().as_ref(),
                    "event JOIN input metadata is not canonical"
                );
            }
            Some(ResponseJoin {
                pair: EventPairState::new(root, 0),
                root_operator: root,
                accepted,
            })
        }
        _ => anyhow::bail!("event JOIN root and accepted schema must be configured together"),
    };
    let mut hash = Sha256::new();
    hash.update(b"streamr.updating-join.v1");
    hash.update(config.encode_to_vec());
    Ok(UpdatingJoin {
        schemas,
        converters,
        passers,
        plan,
        limits,
        left_outer: config.left_outer,
        schema_identity: hash.finalize().to_vec(),
        store: None,
        response,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    mod response_tests {
        include!("updating_join_response_tests.rs");
    }
    use arrow_array::{Int64Array, TimestampNanosecondArray};
    use arrow_schema::DataType;
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
        let mut op = build(
            config,
            Arc::new(arroyo_planner::physical::new_registry()),
            limits,
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

    /// Finite operator CDC/traversal diagnostic; intentionally excluded from routine runs.
    #[tokio::test]
    #[ignore = "opt-in STR122 finite fanout growth/churn diagnostic"]
    async fn native_join_fanout_growth_churn() {
        use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
        for left_outer in [false, true] {
            let planned = planned_config(left_outer).await;
            for rocks in [false, true] {
                let root = std::env::temp_dir().join(format!("str122-{}", uuid::Uuid::new_v4()));
                let mut op = operator_from_config(planned.clone());
                // Finite fixture settings, identical for baseline/candidate; production limits are unchanged.
                op.limits.max_probe_rows = 48;
                let limits = op.store.as_ref().unwrap().limits();
                let resources = op.store.as_ref().unwrap().resources().clone();
                op.store.take();
                let native = if rocks {
                    Some(Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.clone(),
                                job_id: "fanout-pilot".into(),
                                operator_id: "join".into(),
                                subtask: 0,
                                generation: 0,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    ))
                } else {
                    None
                };
                let backend: Arc<dyn LiveStateBackend> = native
                    .as_ref()
                    .map(|b| b.clone() as Arc<dyn LiveStateBackend>)
                    .unwrap_or_else(|| {
                        Arc::new(
                            MemoryLiveState::bounded(
                                resources.clone(),
                                op.limits.max_resident_bytes,
                            )
                            .unwrap(),
                        )
                    });
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
                    AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap(),
                );
                eprintln!(
                    "STR122 settings outer={left_outer} rocks={rocks} limits={:?} resources={:?}",
                    op.limits,
                    resources.config()
                );
                // Retain historical versions while deleting right-side identities lower than the live IDs.
                for id in 64..96 {
                    op.change(1, row(&op, 1, id, Some(7), 20, false), &mut Discard)
                        .await
                        .unwrap();
                }
                let history = backend.snapshot().await.unwrap();
                for id in 64..96 {
                    op.change(1, row(&op, 1, id, Some(7), 20, true), &mut Discard)
                        .await
                        .unwrap();
                }
                let mut completed = 0usize;
                let mut outputs = 0usize;
                for (stage, end) in [16_u8, 32, 48].into_iter().enumerate() {
                    for id in (end - 15)..=end {
                        op.change(
                            0,
                            row(&op, 0, id, Some(7), i64::from(id), false),
                            &mut Discard,
                        )
                        .await
                        .unwrap();
                    }
                    eprintln!(
                        "STR122 begin outer={left_outer} rocks={rocks} stage={stage} retained={end}"
                    );
                    for field in ["stat", "status", "io"] {
                        eprintln!(
                            "STR122 process_start field={field} raw={:?}",
                            std::fs::read_to_string(format!("/proc/self/{field}")).unwrap()
                        );
                    }
                    let started = std::time::Instant::now();
                    // No matches and NULL keys must not trigger spurious opposite transitions.
                    let mut absent = Capture::default();
                    op.change(1, row(&op, 1, 210, Some(99), 77, false), &mut absent)
                        .await
                        .unwrap();
                    op.change(1, row(&op, 1, 211, None, 88, false), &mut absent)
                        .await
                        .unwrap();
                    assert!(absent.0.is_empty());
                    op.change(1, row(&op, 1, 210, Some(99), 77, true), &mut absent)
                        .await
                        .unwrap();
                    op.change(1, row(&op, 1, 211, None, 88, true), &mut absent)
                        .await
                        .unwrap();
                    for cycle in 0..4 {
                        for (id, retract, transition) in [
                            (200, false, true),
                            (201, false, false),
                            (200, true, false),
                            (201, true, true),
                        ] {
                            eprintln!(
                                "STR122 change stage={stage} cycle={cycle} id={id} retract={retract}"
                            );
                            let mut capture = Capture::default();
                            op.change(1, row(&op, 1, id, Some(7), 20, retract), &mut capture)
                                .await
                                .unwrap();
                            let got = changes(&capture);
                            let mut expected = Vec::new();
                            for left in 1..=end {
                                if left_outer && transition && !retract {
                                    expected.push((true, Some(i64::from(left)), None));
                                }
                                expected.push((retract, Some(i64::from(left)), Some(20)));
                                if left_outer && transition && retract {
                                    expected.push((false, Some(i64::from(left)), None));
                                }
                            }
                            assert_eq!(
                                got.iter()
                                    .map(|(r, _, l, v)| (*r, *l, *v))
                                    .collect::<Vec<_>>(),
                                expected
                            );
                            assert_eq!(
                                got.iter()
                                    .map(|(_, id, _, _)| id)
                                    .collect::<std::collections::HashSet<_>>()
                                    .len(),
                                got.len()
                            );
                            completed += 1;
                            outputs += got.len();
                        }
                    }
                    eprintln!(
                        "STR122 end outer={left_outer} rocks={rocks} stage={stage} completed={completed} outputs={outputs} elapsed_ns={}",
                        started.elapsed().as_nanos()
                    );
                    for field in ["stat", "status", "io"] {
                        eprintln!(
                            "STR122 process field={field} raw={:?}",
                            std::fs::read_to_string(format!("/proc/self/{field}")).unwrap()
                        );
                    }
                }
                let notify = Arc::new(tokio::sync::Notify::new());
                // The collector signal proves cancellation happens during real output,
                // after the deterministic preflight, rather than during a backend read.
                struct Slow(Arc<tokio::sync::Notify>);
                #[async_trait::async_trait]
                impl Collector for Slow {
                    async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
                        self.0.notify_one();
                        std::future::pending().await
                    }
                    async fn broadcast_watermark(
                        &mut self,
                        _: arroyo_types::Watermark,
                    ) -> DataflowResult<()> {
                        Ok(())
                    }
                }
                let mut slow = Slow(notify.clone());
                let mut pending =
                    Box::pin(op.change(1, row(&op, 1, 200, Some(7), 20, false), &mut slow));
                tokio::select! {
                    _ = notify.notified() => {},
                    result = &mut pending => panic!("slow collector returned: {result:?}"),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("collector not reached"),
                }
                drop(pending);
                for passer in &op.passers {
                    assert!(passer.read().unwrap().is_none());
                }
                let scope = op.store.as_ref().unwrap().begin().await.unwrap();
                assert!(
                    scope
                        .get(&identity_key(1, &[200; 16]))
                        .await
                        .unwrap()
                        .is_none()
                );
                drop(scope);
                // Exact probe bound succeeds above; the same change one below must publish nothing and roll back.
                op.limits.max_probe_rows = 47;
                let mut capture = Capture::default();
                let err = op
                    .change(1, row(&op, 1, 200, Some(7), 20, false), &mut capture)
                    .await
                    .unwrap_err();
                assert!(err.to_string().contains("max-probe-rows"), "{err}");
                assert!(capture.0.is_empty());
                let scope = op.store.as_ref().unwrap().begin().await.unwrap();
                assert!(
                    scope
                        .get(&identity_key(1, &[200; 16]))
                        .await
                        .unwrap()
                        .is_none()
                );
                drop(scope);
                drop(history);
                drop(op);
                drop(manager);
                drop(backend);
                if let Some(native) = native {
                    Arc::try_unwrap(native)
                        .unwrap_or_else(|_| panic!("retained backend"))
                        .close_and_remove()
                        .await
                        .unwrap();
                    std::fs::remove_dir_all(root).unwrap();
                }
            }
        }
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
        let error = op
            .process_batch_index(0, 2, oversized, &mut ctx, &mut capture)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("value-bytes"), "{error}");
        assert_eq!(capture.0.len(), 8);

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
        let right = row(&op, 1, 20, Some(1), 20, false);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), op.change(1, right, &mut Blocked))
                .await
                .is_err()
        );
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
