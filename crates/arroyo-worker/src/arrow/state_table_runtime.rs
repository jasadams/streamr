//! One serial owner for all related typed state-table accesses in an event.
//! Input batches are bounded before processing; a shared working scope commits
//! a chunk once, then its captured branches are emitted under backpressure.
use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, anyhow, ensure};
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::Schema;
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::{ArrowOperator, ConstructedOperator, OperatorConstructor, Registry},
};
use arroyo_rpc::{
    config::{TypedSqlStateConfig, config},
    errors::{DataflowError, DataflowResult},
    grpc::{
        api::{FusedStateTableOperator, StateTableDefinition},
        rpc::{TableConfig, TableEnum, TypedStateTableConfig},
    },
};
use arroyo_state::live::{
    resources::WorkerStateResources,
    typed_table::{TableDescriptor, TableLimits, TypedTable},
    worker::{ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend},
};
use arroyo_state_protocol::typed_checkpoint::transport_table_name;
use prost::Message;

use super::state_table_owner::{EventOperation, FusedEventProgram, PendingEnvelopes};

fn external(error: impl std::fmt::Display) -> DataflowError {
    DataflowError::ExternalError(error.to_string())
}

pub struct FusedStateTableConstructor;

pub struct FusedStateTable {
    config: FusedStateTableOperator,
    limits: TypedSqlStateConfig,
    program: FusedEventProgram,
    tables: HashMap<String, TypedTable>,
    resources: Option<WorkerStateResources>,
    chunk_rows: usize,
    max_input_batch_bytes: usize,
}

fn descriptor(definition: &StateTableDefinition) -> Result<TableDescriptor> {
    ensure!(
        definition.parallelism == 1
            && !definition.table_identity.is_empty()
            && !definition.schema_identity.is_empty(),
        "native state table requires singleton ownership and stable identities"
    );
    Ok(TableDescriptor {
        table_identity: definition.table_identity.as_bytes().to_vec(),
        schema_identity: definition.schema_identity.as_bytes().to_vec(),
        schema: Arc::new(serde_json::from_str::<Schema>(&definition.schema_json)?),
        primary_key: definition
            .primary_key
            .iter()
            .map(|index| usize::try_from(*index).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn limits(config: &TypedSqlStateConfig) -> TableLimits {
    TableLimits {
        key_bytes: config.key_bytes,
        row_bytes: config.row_bytes,
        decoded_bytes: config.decoded_bytes,
        scope_bytes: config.scope_bytes,
        scope_operations: config.scope_operations,
        page_bytes: config.page_bytes,
        page_entries: config.page_entries,
    }
}

fn compact_event(batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
    let index = u32::try_from(row).context("source batch row exceeds Arrow index range")?;
    let selection = UInt32Array::from(vec![index]);
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &selection, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

impl OperatorConstructor for FusedStateTableConstructor {
    type ConfigT = FusedStateTableOperator;

    fn with_config(
        &self,
        plan: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> Result<ConstructedOperator> {
        let worker = &config().worker;
        let typed = worker
            .typed_sql_state
            .as_ref()
            .context("native state table requires worker.typed-sql-state")?;
        typed.validate()?;
        let max_input_batch_bytes = worker
            .execution_resources
            .as_ref()
            .context("native state table requires worker.execution-resources.max-batch-bytes")?
            .max_batch_bytes;
        ensure!(
            worker.live_state_resources.is_some(),
            "native state table requires worker.live-state-resources"
        );
        let program = FusedEventProgram::decode(&plan, typed, registry.as_ref())?;
        let mut definitions = HashMap::new();
        for table in &plan.tables {
            ensure!(
                table
                    .primary_key
                    .iter()
                    .chain(&table.partition_key)
                    .all(|index| u32::try_from(*index).is_ok()),
                "state-table key index exceeds checkpoint wire range"
            );
            let transport = transport_table_name(table.table_identity.as_bytes())
                .map_err(|error| anyhow!(error))?;
            ensure!(
                definitions.insert(transport, descriptor(table)?).is_none(),
                "duplicate state-table descriptor in fused owner"
            );
        }
        ensure!(
            !definitions.is_empty(),
            "fused owner has no typed state tables"
        );
        for step in &program.steps {
            if let EventOperation::StateTable(access) = &step.operation {
                let transport = transport_table_name(access.table_identity.as_bytes())
                    .map_err(|error| anyhow!(error))?;
                let planned = definitions
                    .get(&transport)
                    .context("state access has no declared table")?;
                ensure!(
                    planned.table_identity == access.descriptor().table_identity
                        && planned.schema_identity == access.descriptor().schema_identity
                        && planned.schema == access.descriptor().schema
                        && planned.primary_key == access.descriptor().primary_key,
                    "state access differs from fused table descriptor"
                );
                let table = plan
                    .tables
                    .iter()
                    .find(|table| table.table_identity == access.table_identity)
                    .context("state access lacks full table definition")?;
                ensure!(
                    table.partition_key
                        == access
                            .partition_key
                            .iter()
                            .map(|index| *index as u64)
                            .collect::<Vec<_>>(),
                    "state access partition ownership differs from table registration"
                );
            }
        }
        let mutations_per_event = program
            .steps
            .iter()
            .filter(|step| matches!(&step.operation, EventOperation::StateTable(access) if access.is_mutation()))
            .count()
            .max(1);
        let worst_write_bytes = typed
            .row_bytes
            .checked_add(typed.key_bytes)
            .and_then(|bytes| bytes.checked_add(64))
            .and_then(|bytes| bytes.checked_mul(mutations_per_event))
            .context("state-table chunk bound overflow")?;
        let chunk_rows = typed
            .scope_operations
            .checked_div(mutations_per_event)
            .unwrap_or(0)
            .min(typed.scope_bytes / worst_write_bytes)
            .min(typed.max_pending_output_rows)
            .min(typed.max_pending_output_bytes / (typed.max_captured_event_bytes + 64));
        ensure!(
            chunk_rows >= 1,
            "native state-table budgets cannot admit one complete event"
        );
        Ok(ConstructedOperator::from_operator(Box::new(
            FusedStateTable {
                config: plan,
                limits: typed.clone(),
                program,
                tables: HashMap::new(),
                resources: None,
                chunk_rows,
                max_input_batch_bytes,
            },
        )))
    }
}

impl FusedStateTable {
    async fn emit_chunk(
        &mut self,
        input: Vec<RecordBatch>,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let first = self
            .tables
            .values()
            .next()
            .ok_or_else(|| anyhow::anyhow!("state-table backend is not registered"))?;
        let resources = self
            .resources
            .as_ref()
            .ok_or_else(|| anyhow!("state-table resource pool is not registered"))?;
        let mut scope = first.begin().await.map_err(external)?;
        let mut pending = PendingEnvelopes::new(
            self.limits.max_pending_output_rows,
            self.limits.max_pending_output_bytes,
        )?;
        let mut chunk_rows = 0usize;
        for batch in input {
            for row in 0..batch.num_rows() {
                if chunk_rows == self.chunk_rows
                    || !pending.can_fit_worst_case(self.limits.max_captured_event_bytes)
                {
                    scope.commit().await.map_err(external)?;
                    self.emit_pending(pending, collector).await?;
                    scope = first.begin().await.map_err(external)?;
                    pending = PendingEnvelopes::new(
                        self.limits.max_pending_output_rows,
                        self.limits.max_pending_output_bytes,
                    )?;
                    chunk_rows = 0;
                }
                // Reserve the worst-case working/capture allocations before
                // materializing an owned row. `slice` would retain the whole
                // incoming batch and make both limits and queue admission lie.
                let _working = resources
                    .try_decoded_value(self.limits.max_working_event_bytes)
                    .map_err(external)?;
                let captured = resources
                    .try_decoded_value(self.limits.max_captured_event_bytes)
                    .map_err(external)?;
                let event = compact_event(&batch, row)?;
                let envelope = self
                    .program
                    .execute_event(&event, &self.tables, &mut scope)
                    .await?;
                pending.push_admitted(envelope, captured)?;
                chunk_rows += 1;
            }
        }
        if chunk_rows > 0 {
            scope.commit().await.map_err(external)?;
            self.emit_pending(pending, collector).await?;
        }
        Ok(())
    }

    async fn emit_pending(
        &self,
        pending: PendingEnvelopes,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let resources = self
            .resources
            .as_ref()
            .ok_or_else(|| external("state-table resource pool is not registered"))?;
        // Concatenation allocates a second Arrow batch while all captured
        // envelopes (and their permits) remain live under sink backpressure.
        let _concat = resources
            .try_decoded_value(pending.bytes())
            .map_err(external)?;
        let (rows, _permits) = pending.into_admitted_rows();
        if !rows.is_empty() {
            let output = arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
            collector.collect(output).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ArrowOperator for FusedStateTable {
    fn name(&self) -> String {
        format!("fused_state_table_{}", self.config.event_scope_id)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        self.config
            .tables
            .iter()
            .map(|table| {
                let name = transport_table_name(table.table_identity.as_bytes())
                    .expect("constructor checked table identity");
                (
                    name.clone(),
                    TableConfig {
                        table_type: TableEnum::TypedStateTable.into(),
                        state_version: 1,
                        config: TypedStateTableConfig {
                            transport_name: name,
                            table_identity: table.table_identity.as_bytes().to_vec(),
                            schema_identity: table.schema_identity.as_bytes().to_vec(),
                            schema_json: table.schema_json.as_bytes().to_vec(),
                            primary_key: table
                                .primary_key
                                .iter()
                                .map(|index| *index as u32)
                                .collect(),
                            partition_key: table
                                .partition_key
                                .iter()
                                .map(|index| *index as u32)
                                .collect(),
                            encoding_version: 1,
                        }
                        .encode_to_vec(),
                    },
                )
            })
            .collect()
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if ctx.task_info.parallelism != 1 || ctx.task_info.task_index != 0 {
            return Err(external("native state tables require singleton execution"));
        }
        let resources: WorkerStateResources = configured_worker_resources()
            .map_err(external)?
            .context("native state tables require worker live-state resources")?;
        let generation = match ctx.task_info.checkpoint_file_path_layout {
            arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
            _ => 0,
        };
        let backend = construct_configured_backend(
            ConfiguredBackendOwner {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask: 0,
                generation,
            },
            self.limits.max_resident_bytes,
            resources.clone(),
        )
        .await
        .map_err(external)?;
        let definitions = self
            .config
            .tables
            .iter()
            .map(|table| {
                let name = transport_table_name(table.table_identity.as_bytes())
                    .map_err(|error| anyhow!(error))?;
                Ok((name, (descriptor(table)?, limits(&self.limits))))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let handles = ctx
            .table_manager
            .register_typed_tables(definitions, backend, resources.clone())
            .await?;
        let largest_key_workspace = handles.values().try_fold(0usize, |largest, table| {
            let bytes = self
                .limits
                .key_bytes
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(table.descriptor().table_identity.len()))
                .and_then(|bytes| {
                    bytes.checked_add(table.descriptor().primary_key.len().checked_mul(1024)?)
                })
                .and_then(|bytes| bytes.checked_add(1024))
                .context("state-table key workspace bound overflow")?;
            Ok::<_, anyhow::Error>(largest.max(bytes))
        })?;
        // A live scope, one admitted source batch, its owned working row, one captured
        // envelope and the concat copy can coexist. Probe that combination at
        // startup so an impossible resource configuration fails before events
        // are consumed. Larger batches/chunks still use normal bounded
        // admission and may be limited by available pool capacity.
        let one_event_headroom = self
            .max_input_batch_bytes
            .checked_add(self.limits.max_working_event_bytes)
            .and_then(|bytes| {
                bytes.checked_add(self.limits.max_captured_event_bytes.checked_mul(2)?)
            })
            .and_then(|bytes| bytes.checked_add(self.limits.decoded_bytes.checked_mul(3)?))
            .and_then(|bytes| bytes.checked_add(self.limits.row_bytes))
            .and_then(|bytes| bytes.checked_add(largest_key_workspace))
            .context("state-table one-event memory bound overflow")?;
        {
            let first = handles
                .values()
                .next()
                .context("state-table registration returned no handles")?;
            let _scope = first.begin().await.map_err(external)?;
            let _headroom = resources.try_decoded_value(one_event_headroom)
                .map_err(|error| external(format!("native state-table resources cannot admit one event alongside a live scope: {error}")))?;
        }
        self.tables = handles
            .into_iter()
            .map(|(transport, table)| {
                let identity = self
                    .config
                    .tables
                    .iter()
                    .find(|definition| {
                        transport_table_name(definition.table_identity.as_bytes())
                            .is_ok_and(|name| name == transport)
                    })
                    .expect("registered transport has a declared table")
                    .table_identity
                    .clone();
                (identity, table)
            })
            .collect();
        self.resources = Some(resources);
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.get_array_memory_size() > self.max_input_batch_bytes {
            return Err(external(
                "state-table source batch exceeds configured execution max-batch-bytes",
            ));
        }
        let resources = self
            .resources
            .as_ref()
            .ok_or_else(|| external("state-table resource pool is not registered"))?
            .clone();
        // Admit the shared source backing once. Each event is then compacted
        // into its own one-row arrays before the per-event working budget is
        // checked; charging `slice` here would count this entire batch per row.
        let _input = resources
            .try_decoded_value(batch.get_array_memory_size())
            .map_err(external)?;
        self.emit_chunk(vec![batch], collector).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::state_table::{extract_capture, synthetic_merge_step};
    use super::super::state_table_owner::EventStep;
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field};
    use arroyo_rpc::df::ArroyoSchema;
    use arroyo_state::live::{memory::MemoryLiveState, resources::ResourceConfig};
    use arroyo_types::{TaskInfo, Watermark};
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};

    struct RecordingCollector {
        batches: Arc<Mutex<Vec<RecordBatch>>>,
        entered: Option<Arc<Notify>>,
        release: Option<Arc<Notify>>,
    }

    #[async_trait::async_trait]
    impl Collector for RecordingCollector {
        async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
            if let Some(entered) = &self.entered {
                entered.notify_one();
            }
            if let Some(release) = &self.release {
                release.notified().await;
            }
            self.batches.lock().await.push(batch);
            Ok(())
        }
        async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
            Ok(())
        }
    }

    fn test_resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1 << 20,
            memtable_bytes: 1 << 20,
            queued_write_bytes: 2 << 20,
            decoded_value_bytes: 4 << 20,
            scan_page_bytes: 1 << 20,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }

    fn test_limits() -> TypedSqlStateConfig {
        TypedSqlStateConfig {
            key_bytes: 1024,
            row_bytes: 8192,
            decoded_bytes: 8192,
            scope_bytes: 32768,
            scope_operations: 16,
            page_bytes: 32768,
            page_entries: 2,
            max_working_event_bytes: 128 * 1024,
            max_captured_event_bytes: 64 * 1024,
            max_pending_output_rows: 4,
            max_pending_output_bytes: 256 * 1024,
            max_resident_bytes: 1 << 20,
        }
    }

    fn test_input(schema: Arc<Schema>, values: &[(Option<&str>, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    values.iter().map(|(item, _)| *item).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    values.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampNanosecondArray::from(
                    (0..values.len() as i64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    async fn test_owner(
        chunk_rows: usize,
    ) -> (
        FusedStateTable,
        OperatorContext,
        Arc<Schema>,
        Arc<Schema>,
        WorkerStateResources,
    ) {
        let (step, input, table_schema, capture) = synthetic_merge_step();
        let resources = test_resources();
        let limits = test_limits();
        let definition = StateTableDefinition {
            name: "inventory".into(),
            table_identity: "inventory".into(),
            schema_identity: "v1".into(),
            schema_json: serde_json::to_string(table_schema.as_ref()).unwrap(),
            primary_key: vec![0],
            partition_key: vec![0],
            parallelism: 1,
        };
        let program = FusedEventProgram {
            event_scope_id: "events".into(),
            steps: vec![EventStep {
                parent: None,
                operation: EventOperation::StateTable(Box::new(step)),
                captures: vec![0],
            }],
            capture_schemas: vec![capture.clone()],
            timestamp_field: input.field(2).clone().into(),
            timestamp_index: 2,
            max_captured_event_bytes: limits.max_captured_event_bytes,
            max_working_event_bytes: limits.max_working_event_bytes,
        };
        program.validate().unwrap();
        let mut operator = FusedStateTable {
            config: FusedStateTableOperator {
                event_scope_id: "events".into(),
                tables: vec![definition],
                ..Default::default()
            },
            limits: limits.clone(),
            program,
            tables: HashMap::new(),
            resources: Some(resources.clone()),
            chunk_rows,
            max_input_batch_bytes: 1 << 20,
        };
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        let mut context = OperatorContext::new(
            Arc::new(TaskInfo {
                job_id: "fused-runtime-regression".into(),
                operator_idx: 0,
                operator_name: "FusedStateTable".into(),
                operator_id: "fused-runtime".into(),
                task_index: 0,
                parallelism: 1,
                key_range: 0..=u64::MAX,
                checkpoint_file_path_layout: Default::default(),
            }),
            None,
            control_tx,
            1,
            vec![Arc::new(ArroyoSchema::new_unkeyed(input.clone(), 2))],
            None,
            operator.tables(),
        )
        .await;
        let transport = transport_table_name(b"inventory").unwrap();
        let backend = Arc::new(
            MemoryLiveState::bounded(resources.clone(), limits.max_resident_bytes).unwrap(),
        );
        let handles = context
            .table_manager
            .register_typed_tables(
                HashMap::from([(
                    transport.clone(),
                    (
                        descriptor(&operator.config.tables[0]).unwrap(),
                        super::limits(&limits),
                    ),
                )]),
                backend,
                resources.clone(),
            )
            .await
            .unwrap();
        operator
            .tables
            .insert("inventory".into(), handles.into_iter().next().unwrap().1);
        (operator, context, input, capture, resources)
    }

    #[tokio::test]
    async fn process_batch_emits_single_row_without_followup_and_splits_hot_multirow_input() {
        let (mut owner, mut context, input_schema, capture_schema, _) = test_owner(4).await;
        owner.limits.max_pending_output_rows = 1;
        let batches = Arc::new(Mutex::new(Vec::new()));
        let mut collector = RecordingCollector {
            batches: batches.clone(),
            entered: None,
            release: None,
        };
        owner.program.max_working_event_bytes = 1;
        assert!(
            owner
                .process_batch(
                    test_input(input_schema.clone(), &[(Some("a"), 1)]),
                    &mut context,
                    &mut collector,
                )
                .await
                .is_err(),
            "failed event budget must abort its uncommitted scope"
        );
        owner.program.max_working_event_bytes = owner.limits.max_working_event_bytes;
        assert!(batches.lock().await.is_empty());
        tokio::time::timeout(
            Duration::from_secs(2),
            owner.process_batch(
                test_input(input_schema.clone(), &[(Some("a"), 1)]),
                &mut context,
                &mut collector,
            ),
        )
        .await
        .expect("one source row must make progress without another input or control message")
        .unwrap();
        assert_eq!(batches.lock().await.len(), 1);

        owner
            .process_batch(
                test_input(input_schema, &[(Some("a"), 2), (None, 99), (Some("a"), 3)]),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let outputs = batches.lock().await;
        assert_eq!(
            outputs.len(),
            4,
            "one-row pending budget must split the three-row input"
        );
        let actions = outputs
            .iter()
            .map(|envelope| {
                let captured = extract_capture(envelope, 0, capture_schema.clone()).unwrap();
                captured
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(actions, ["insert", "update", "none", "update"]);
        drop(outputs);
        let table = owner.tables.get("inventory").unwrap();
        let key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("item", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let stored = table.get(&key).await.unwrap().unwrap();
        assert_eq!(
            stored
                .batch()
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    #[tokio::test]
    async fn blocked_collector_stops_next_chunk_and_cancel_releases_output_permits() {
        let (mut owner, mut context, input_schema, _, resources) = test_owner(1).await;
        let total = resources.config().decoded_value_bytes;
        resources.try_decoded_value(total).unwrap();
        let batches = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut collector = RecordingCollector {
            batches: batches.clone(),
            entered: Some(entered.clone()),
            release: Some(release.clone()),
        };
        let input = test_input(input_schema, &[(Some("a"), 1), (Some("a"), 2)]);
        let task = tokio::spawn(async move {
            owner
                .process_batch(input, &mut context, &mut collector)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(
            !task.is_finished(),
            "the owner must wait for the downstream collector"
        );
        assert!(
            batches.lock().await.is_empty(),
            "the next chunk cannot pass a blocked collector"
        );
        assert!(
            resources.try_decoded_value(total).is_err(),
            "captured output permits remain charged while blocked"
        );
        task.abort();
        let _ = task.await;
        resources.try_decoded_value(total).unwrap();
        release.notify_waiters();
    }

    #[test]
    fn compaction_does_not_retain_a_large_shared_source_buffer() {
        let mut values = vec!["x".repeat(8 * 1024); 64];
        values.push("small".into());
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                arrow_schema::DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();
        assert!(source.get_array_memory_size() > 512 * 1024);
        let event = compact_event(&source, 64).unwrap();
        assert_eq!(event.num_rows(), 1);
        assert_eq!(
            event
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "small"
        );
        assert!(event.get_array_memory_size() < 1024);
    }
}
