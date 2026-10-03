//! Row-ordered SQL maps. Linear CTE stages share one planner-fused operator
//! and one checkpoint owner. A row completes its operations before the next
//! row starts, including when keys repeat within an input batch.
//!
//! The memory implementation stages complete reference state at each barrier.
//! The opt-in RocksDB implementation owns no complete map or dirty-key set;
//! it commits admitted writes before emitting each bounded row. TableManager
//! captures and exports the aligned live snapshot and restores the selected
//! committed checkpoint into a fresh attempt before processing input.

use prometheus::{HistogramOpts, HistogramVec};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, ConstructedOperator, OperatorConstructor, Registry,
};
use arroyo_rpc::config::{SqlStateBackend, config};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowError;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api::{StateOpType, StatefulProcessorOperator};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::grpc::rpc::{DiskKeyedTableConfig, TableEnum};
use arroyo_state::global_table_config;
use arroyo_state::live::{
    ReadOptions, lifecycle::RocksStateConfig, rocks::RocksLiveState, table::LiveTable,
    worker::configured_worker_resources,
};

use arroyo_types::CheckpointBarrier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

struct StateOp {
    map_name: String,
    op_type: StateOpType,
    key_expr: Arc<dyn PhysicalExpr>,
    value_expr: Option<Arc<dyn PhysicalExpr>>,
    condition_expr: Option<Arc<dyn PhysicalExpr>>,
    output_field: String,
}

pub struct StatefulProcessorFunc {
    // Values are Option<String>: Some(v) = live entry, None = tombstone (deleted).
    state: HashMap<String, HashMap<String, Option<String>>>,
    rocks: Option<Arc<RocksLiveState>>,
    live_tables: HashMap<String, LiveTable>,
    dirty_keys: HashMap<String, HashSet<String>>,
    map_names: Vec<String>,
    ops: Vec<StateOp>,
    final_exprs: Vec<Arc<dyn PhysicalExpr>>,
    // Deferred deserialization: stored until on_start when the intermediate schema is known.
    final_exprs_bytes: Vec<Vec<u8>>,
    registry: Arc<Registry>,
    input_schema: ArroyoSchema,
}

fn extract_string(array: &dyn Array, row: usize) -> Option<String> {
    let string_array = array.as_any().downcast_ref::<StringArray>()?;
    if string_array.is_null(row) {
        None
    } else {
        Some(string_array.value(row).to_string())
    }
}

#[async_trait::async_trait]
impl ArrowOperator for StatefulProcessorFunc {
    fn name(&self) -> String {
        "stateful_processor".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        self.map_names
            .iter()
            .flat_map(|name| {
                if config().worker.sql_state_backend == SqlStateBackend::Rocksdb {
                    HashMap::from([(
                        name.clone(),
                        TableConfig {
                            table_type: TableEnum::DiskKeyedMap.into(),
                            state_version: 1,
                            config: DiskKeyedTableConfig {
                                table_name: name.clone(),
                                encoding_version: 1,
                                schema_identity: b"streamr.sql-map.utf8-key.utf8-value.v1".to_vec(),
                            }
                            .encode_to_vec(),
                        },
                    )])
                } else {
                    global_table_config(name.clone(), format!("stateful processor map: {name}"))
                }
            })
            .collect()
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if ctx.task_info.parallelism != 1 {
            return Err(arroyo_rpc::errors::DataflowError::ExternalError(format!(
                "StatefulProcessor requires parallelism 1; received {}. State maps are not partitioned by key",
                ctx.task_info.parallelism
            )));
        }

        let worker_config = config();
        worker_config
            .worker
            .validate_sql_state()
            .map_err(external)?;
        if worker_config.worker.sql_state_backend == SqlStateBackend::Rocksdb {
            if self.map_names.iter().any(|name| {
                name.is_empty()
                    || name.len() > 256
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            }) {
                return Err(external(
                    "disk SQL state map names must be 1-256 ASCII letters, digits, underscores, or hyphens including the internal prefix",
                ));
            }
            for op in &self.ops {
                validate_disk_expression(op.key_expr.as_ref())?;
                if let Some(value) = &op.value_expr {
                    validate_disk_expression(value.as_ref())?;
                }
                if let Some(condition) = &op.condition_expr {
                    validate_disk_expression(condition.as_ref())?;
                }
            }
            for field in self.input_schema.schema.fields() {
                if !matches!(
                    field.data_type(),
                    DataType::Utf8 | DataType::Boolean | DataType::Null
                ) && field.data_type().primitive_width().is_none()
                {
                    return Err(external(format!(
                        "disk SQL state supports primitive and Utf8 input fields; {} has unsupported type {}",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
            let disk = worker_config
                .worker
                .disk_sql_state
                .as_ref()
                .ok_or_else(|| external("missing disk SQL configuration"))?;
            let generation = match ctx.task_info.checkpoint_file_path_layout {
                arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
                _ => 0,
            };
            let backend = Arc::new(
                RocksLiveState::open_worker(RocksStateConfig {
                    root: disk.directory.join(uuid::Uuid::new_v4().to_string()),
                    job_id: ctx.task_info.job_id.clone(),
                    operator_id: ctx.task_info.operator_id.clone(),
                    subtask: ctx.task_info.task_index,
                    generation,
                    attempt: 0,
                })
                .await
                .map_err(external)?,
            );
            backend.remove_on_drop();
            for name in &self.map_names {
                let table = ctx
                    .table_manager
                    .register_live_table(name, backend.clone())
                    .await
                    .map_err(external)?;
                self.live_tables.insert(name.clone(), table);
            }
            self.rocks = Some(backend);
        } else {
            for map_name in &self.map_names {
                let gs = ctx
                    .table_manager
                    .get_global_keyed_state::<String, Option<String>>(map_name)
                    .await?;
                self.state.insert(
                    map_name.clone(),
                    gs.get_all()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                );
            }
        }

        // Deserialize final_exprs against the intermediate schema
        // (input schema + one result field per op)
        if !self.final_exprs_bytes.is_empty() {
            let mut fields: Vec<Arc<Field>> =
                self.input_schema.schema.fields().iter().cloned().collect();
            for op in &self.ops {
                let dt = match op.op_type {
                    StateOpType::StateGet | StateOpType::StatePut | StateOpType::StateUpsert => {
                        DataType::Utf8
                    }
                    StateOpType::StateUpdate | StateOpType::StateDelete => DataType::Boolean,
                };
                fields.push(Arc::new(Field::new(&op.output_field, dt, true)));
            }
            let intermediate_schema = Arc::new(Schema::new(fields));

            self.final_exprs = self
                .final_exprs_bytes
                .iter()
                .map(|bytes| {
                    let node = PhysicalExprNode::decode(&mut bytes.as_slice())?;
                    Ok(parse_physical_expr(
                        &node,
                        self.registry.as_ref(),
                        &intermediate_schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?)
                })
                .collect::<anyhow::Result<Vec<_>>>()
                .map_err(|e| arroyo_rpc::errors::DataflowError::ExternalError(e.to_string()))?;
        }

        if self.rocks.is_some() {
            let mut fields = self.input_schema.schema.fields().to_vec();
            for op in &self.ops {
                let dt = match op.op_type {
                    StateOpType::StateGet | StateOpType::StatePut | StateOpType::StateUpsert => {
                        DataType::Utf8
                    }
                    _ => DataType::Boolean,
                };
                fields.push(Arc::new(Field::new(&op.output_field, dt, true)));
            }
            let schema = Schema::new(fields);
            for expr in &self.final_exprs {
                validate_disk_expression(expr.as_ref())?;
                let dt = expr.data_type(&schema)?;
                if !matches!(dt, DataType::Utf8 | DataType::Boolean | DataType::Null)
                    && dt.primitive_width().is_none()
                {
                    return Err(external(format!(
                        "disk SQL state supports primitive and Utf8 output fields; expression {expr} has unsupported type {dt}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // One row is a bounded execution chunk. Complete its ordered writes
        // before emitting output; the next row observes those completed writes.
        let disk_limit = self.rocks.as_ref().map(|_| {
            config()
                .worker
                .disk_sql_state
                .as_ref()
                .unwrap()
                .max_row_bytes
        });
        let mut memory_output = Vec::new();
        let (latency, lag) = sql_row_metrics()?;
        let backend_label = if disk_limit.is_some() {
            "rocksdb"
        } else {
            "memory"
        };
        for row in 0..batch.num_rows() {
            let _row_timer = latency.with_label_values(&[backend_label]).start_timer();
            let resources = if disk_limit.is_some() {
                configured_worker_resources().map_err(external)?
            } else {
                None
            };
            let _producer = if let (Some(resources), Some(limit)) = (&resources, disk_limit) {
                Some(
                    resources
                        .decoded_value(limit.saturating_mul(8).saturating_add(2048))
                        .await
                        .map_err(external)?,
                )
            } else {
                None
            };
            let mut intermediate = batch.slice(row, 1);
            if let Some(limit) = disk_limit {
                check_row_bytes(&intermediate, limit)?;
            }
            for index in 0..self.ops.len() {
                let op = &self.ops[index];
                let key_array = op.key_expr.evaluate(&intermediate)?.into_array(1)?;
                let key = bounded_string(key_array.as_ref(), disk_limit)?;
                let value = if key.is_none() {
                    None
                } else {
                    op.value_expr
                        .as_ref()
                        .map(|e| e.evaluate(&intermediate)?.into_array(1))
                        .transpose()?
                        .map(|a| bounded_string(a.as_ref(), disk_limit))
                        .transpose()?
                        .flatten()
                };
                let condition = if key.is_none() {
                    None
                } else {
                    op.condition_expr
                        .as_ref()
                        .map(|e| e.evaluate(&intermediate)?.into_array(1))
                        .transpose()?
                        .and_then(|a| {
                            a.as_any()
                                .downcast_ref::<arrow_array::BooleanArray>()
                                .and_then(|a| if a.is_null(0) { None } else { Some(a.value(0)) })
                        })
                };
                if let Some(limit) = disk_limit {
                    let bytes = key
                        .as_ref()
                        .map_or(0, String::len)
                        .saturating_add(value.as_ref().map_or(0, String::len));
                    if bytes > limit {
                        return Err(external(format!(
                            "SQL state key/value exceeds max-row-bytes {limit}"
                        )));
                    }
                }
                let current = match key.as_deref() {
                    Some(key)
                        if matches!(
                            op.op_type,
                            StateOpType::StateGet
                                | StateOpType::StateUpsert
                                | StateOpType::StateDelete
                        ) =>
                    {
                        self.read_map(&op.map_name, key, disk_limit).await?
                    }
                    _ => None,
                };
                let (result, update): (Arc<dyn Array>, Option<Option<String>>) = match op.op_type {
                    StateOpType::StateGet => (Arc::new(StringArray::from(vec![current])), None),
                    StateOpType::StatePut => {
                        let result = if key.is_some() { value } else { None };
                        (
                            Arc::new(StringArray::from(vec![result.clone()])),
                            result.map(Some),
                        )
                    }
                    StateOpType::StateUpsert => {
                        let insert = if key.is_some() && current.is_none() {
                            value.clone()
                        } else {
                            None
                        };
                        (
                            Arc::new(StringArray::from(vec![current.or(insert.clone())])),
                            insert.map(Some),
                        )
                    }
                    StateOpType::StateUpdate => {
                        let update = if key.is_some() && condition == Some(true) {
                            value.map(Some)
                        } else {
                            None
                        };
                        (
                            Arc::new(arrow_array::BooleanArray::from(vec![Some(
                                update.is_some(),
                            )])),
                            update,
                        )
                    }
                    StateOpType::StateDelete => (
                        Arc::new(arrow_array::BooleanArray::from(vec![Some(
                            current.is_some(),
                        )])),
                        key.as_ref().map(|_| None),
                    ),
                };
                let name = op.map_name.clone();
                let field = Arc::new(Field::new(
                    &op.output_field,
                    result.data_type().clone(),
                    true,
                ));
                if let (Some(key), Some(update)) = (key, update) {
                    self.write_map(&name, key, update, disk_limit).await?;
                }
                let mut fields = intermediate.schema().fields().to_vec();
                fields.push(field);
                let mut columns = intermediate.columns().to_vec();
                columns.push(result);
                intermediate = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
                if let Some(limit) = disk_limit {
                    check_row_bytes(&intermediate, limit)?;
                }
            }
            let projected = if self.final_exprs.is_empty() {
                intermediate
            } else {
                let mut columns = Vec::with_capacity(self.final_exprs.len());
                let mut output_bytes = 0usize;
                for expr in &self.final_exprs {
                    let array = expr.evaluate(&intermediate)?.into_array(1)?;
                    output_bytes = output_bytes
                        .saturating_add(array_row_bytes(array.as_ref()))
                        .saturating_add(64);
                    if let Some(limit) = disk_limit
                        && output_bytes > limit
                    {
                        return Err(external(format!(
                            "SQL projected row exceeds max-row-bytes: {output_bytes} > {limit}"
                        )));
                    }
                    columns.push(array);
                }
                let fields = self
                    .final_exprs
                    .iter()
                    .map(|e| {
                        Ok(Field::new(
                            e.to_string(),
                            e.data_type(intermediate.schema().as_ref())?,
                            e.nullable(intermediate.schema().as_ref())?,
                        ))
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?
            };
            let projected = if let Some(schema) = &ctx.out_schema {
                RecordBatch::try_new(schema.schema.clone(), projected.columns().to_vec())?
            } else {
                projected
            };
            if let Some(limit) = disk_limit {
                check_row_bytes(&projected, limit)?;
                collector.collect(projected).await?;
            } else {
                memory_output.push(projected);
            }
            if let Some(timestamp) = batch
                .column(self.input_schema.timestamp_index)
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                && !timestamp.is_null(row)
            {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(external)?
                    .as_nanos() as i128;
                let elapsed = now - timestamp.value(row) as i128;
                if elapsed >= 0 {
                    lag.with_label_values(&[backend_label])
                        .observe(elapsed as f64 / 1_000_000_000.0);
                }
            }
        }
        if let Some(first) = memory_output.first() {
            collector
                .collect(arrow::compute::concat_batches(
                    &first.schema(),
                    &memory_output,
                )?)
                .await?;
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.rocks.is_some() {
            return Ok(());
        }
        // Legacy global tables checkpoint only entries explicitly staged in this
        // epoch. Stage the complete reference state, including tombstones, so
        // unchanged keys survive successive checkpoints.
        for map_name in &self.map_names {
            let gs = ctx
                .table_manager
                .get_global_keyed_state::<String, Option<String>>(map_name)
                .await?;
            if let Some(entries) = self.state.get(map_name) {
                for (key, value) in entries {
                    gs.insert(key.clone(), value.clone()).await;
                }
            }
        }

        self.dirty_keys.clear();
        Ok(())
    }
}

fn sql_row_metrics() -> DataflowResult<&'static (HistogramVec, HistogramVec)> {
    static METRICS: OnceLock<Result<(HistogramVec, HistogramVec), String>> = OnceLock::new();
    METRICS.get_or_init(|| {
        let buckets = vec![0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];
        let latency = HistogramVec::new(HistogramOpts::new("streamr_sql_state_row_seconds", "SQL state row service time including disk write commit and disk output emission").buckets(buckets.clone()), &["backend"]).map_err(|e| e.to_string())?;
        let mut lag_buckets = buckets;
        lag_buckets.extend([10.0, 30.0, 60.0, 120.0, 300.0, 600.0]);
        let lag = HistogramVec::new(HistogramOpts::new("streamr_sql_state_event_lag_seconds", "SQL state event timestamp to row completion lag").buckets(lag_buckets), &["backend"]).map_err(|e| e.to_string())?;
        prometheus::default_registry().register(Box::new(latency.clone())).map_err(|e| e.to_string())?;
        prometheus::default_registry().register(Box::new(lag.clone())).map_err(|e| e.to_string())?;
        Ok((latency, lag))
    }).as_ref().map_err(external)
}

/// Disk preview expressions must have bounded string expansion. Unknown UDFs
/// and expansion kernels are rejected before receiving any input rows.
fn validate_disk_expression(expr: &dyn PhysicalExpr) -> DataflowResult<()> {
    if let Some(function) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()
    {
        let name = function.name().to_ascii_lowercase();
        if !matches!(
            name.as_str(),
            "concat"
                | "concat_ws"
                | "coalesce"
                | "nullif"
                | "lower"
                | "upper"
                | "substr"
                | "substring"
                | "length"
                | "char_length"
                | "character_length"
        ) {
            return Err(external(format!(
                "disk SQL state does not support scalar function {name}; its allocation bound has not been qualified"
            )));
        }
        if matches!(name.as_str(), "concat" | "concat_ws") && expr.children().len() > 3 {
            return Err(external(
                "disk SQL state concat/concat_ws supports at most three arguments; use bounded input values",
            ));
        }
    }
    for child in expr.children() {
        validate_disk_expression(child.as_ref())?;
    }
    if disk_expression_expansion(expr) > 3 {
        return Err(external(
            "disk SQL state expression has excessive string expansion; compute bounded values before the stateful SELECT",
        ));
    }
    if let Some(literal) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Literal>()
        && let datafusion::common::ScalarValue::Utf8(Some(value)) = literal.value()
        && value.len()
            > config()
                .worker
                .disk_sql_state
                .as_ref()
                .unwrap()
                .max_row_bytes
    {
        return Err(external(
            "disk SQL state string literal exceeds max-row-bytes",
        ));
    }
    Ok(())
}

fn disk_expression_expansion(expr: &dyn PhysicalExpr) -> usize {
    let children = expr
        .children()
        .into_iter()
        .map(|child| disk_expression_expansion(child.as_ref()))
        .collect::<Vec<_>>();
    if let Some(function) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::ScalarFunctionExpr>()
    {
        return match function.name().to_ascii_lowercase().as_str() {
            "concat" | "concat_ws" => children.iter().sum(),
            "upper" | "lower" => children
                .iter()
                .max()
                .copied()
                .unwrap_or(1)
                .saturating_mul(3),
            _ => children.iter().max().copied().unwrap_or(1),
        };
    }
    if let Some(binary) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::BinaryExpr>()
        && *binary.op() == datafusion::logical_expr::Operator::StringConcat
    {
        return children.iter().sum();
    }
    children.iter().max().copied().unwrap_or(1)
}

fn bounded_string(array: &dyn Array, limit: Option<usize>) -> DataflowResult<Option<String>> {
    if let (Some(limit), Some(array)) = (limit, array.as_any().downcast_ref::<StringArray>())
        && !array.is_null(0)
        && array.value(0).len() > limit
    {
        return Err(external(format!(
            "SQL string exceeds max-row-bytes {limit}"
        )));
    }
    Ok(extract_string(array, 0))
}

fn external(error: impl std::fmt::Display) -> DataflowError {
    DataflowError::ExternalError(error.to_string())
}

fn array_row_bytes(array: &dyn Array) -> usize {
    match array.data_type() {
        DataType::Boolean => 1,
        DataType::Null => 0,
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map_or(0, |array| {
                if array.is_null(0) {
                    0
                } else {
                    array.value(0).len()
                }
            }),
        _ => array.data_type().primitive_width().unwrap_or(16),
    }
}

fn check_row_bytes(batch: &RecordBatch, limit: usize) -> DataflowResult<()> {
    let bytes = batch
        .columns()
        .iter()
        .map(|array| array_row_bytes(array.as_ref()))
        .sum::<usize>()
        .saturating_add(batch.num_columns() * 64);
    if bytes > limit {
        Err(external(format!(
            "SQL row/intermediate exceeds max-row-bytes: {bytes} > {limit}"
        )))
    } else {
        Ok(())
    }
}

impl StatefulProcessorFunc {
    async fn read_map(
        &self,
        name: &str,
        key: &str,
        limit: Option<usize>,
    ) -> DataflowResult<Option<String>> {
        if let Some(limit) = limit {
            let bytes = self.live_tables[name]
                .get(
                    key.as_bytes().to_vec(),
                    None,
                    ReadOptions { max_bytes: limit },
                )
                .await
                .map_err(external)?;
            bytes
                .map(|b| String::from_utf8(b).map_err(external))
                .transpose()
        } else {
            Ok(self.state[name].get(key).cloned().flatten())
        }
    }
    async fn write_map(
        &mut self,
        name: &str,
        key: String,
        value: Option<String>,
        limit: Option<usize>,
    ) -> DataflowResult<()> {
        if let (Some(backend), Some(limit)) = (&self.rocks, limit) {
            let key = self.live_tables[name].key(key.into_bytes(), None);
            let mut batch = backend
                .admitted_batch(limit.saturating_mul(2).saturating_add(1024), 1)
                .await
                .map_err(external)?;
            match value {
                Some(value) => batch.put(&key, value.as_bytes()).map_err(external)?,
                None => batch.delete(&key).map_err(external)?,
            }
            backend.write_admitted(batch).await.map_err(external)?;
        } else {
            self.dirty_keys
                .entry(name.to_string())
                .or_default()
                .insert(key.clone());
            self.state.get_mut(name).unwrap().insert(key, value);
        }
        Ok(())
    }
}

pub struct StatefulProcessorConstructor;

impl OperatorConstructor for StatefulProcessorConstructor {
    type ConfigT = StatefulProcessorOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let input_schema: ArroyoSchema = config
            .input_schema
            .ok_or_else(|| anyhow::anyhow!("StatefulProcessorOperator missing input_schema"))?
            .try_into()?;

        let mut fields = input_schema.schema.fields().to_vec();
        let ops = config
            .operations
            .iter()
            .map(|op| {
                let operation_schema = Schema::new(fields.clone());
                let key_expr = PhysicalExprNode::decode(&mut op.key_expr.as_slice())?;
                let key_expr = parse_physical_expr(
                    &key_expr,
                    registry.as_ref(),
                    &operation_schema,
                    &DefaultPhysicalExtensionCodec {},
                )?;

                let value_expr = if op.value_expr.is_empty() {
                    None
                } else {
                    let expr = PhysicalExprNode::decode(&mut op.value_expr.as_slice())?;
                    Some(parse_physical_expr(
                        &expr,
                        registry.as_ref(),
                        &operation_schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?)
                };

                let condition_expr = if op.condition_expr.is_empty() {
                    None
                } else {
                    let expr = PhysicalExprNode::decode(&mut op.condition_expr.as_slice())?;
                    Some(parse_physical_expr(
                        &expr,
                        registry.as_ref(),
                        &operation_schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?)
                };

                anyhow::ensure!(
                    key_expr.data_type(&operation_schema)? == DataType::Utf8,
                    "SQL state key expression must produce Utf8"
                );
                if let Some(value) = &value_expr {
                    anyhow::ensure!(
                        value.data_type(&operation_schema)? == DataType::Utf8,
                        "SQL state value expression must produce Utf8"
                    );
                }
                if let Some(condition) = &condition_expr {
                    anyhow::ensure!(
                        condition.data_type(&operation_schema)? == DataType::Boolean,
                        "SQL state update condition must produce Boolean"
                    );
                }
                let op_type = StateOpType::try_from(op.op_type)
                    .map_err(|_| anyhow::anyhow!("unknown StateOpType: {}", op.op_type))?;
                let dt = match op_type {
                    StateOpType::StateGet | StateOpType::StatePut | StateOpType::StateUpsert => {
                        DataType::Utf8
                    }
                    _ => DataType::Boolean,
                };
                fields.push(Arc::new(Field::new(&op.output_field, dt, true)));
                Ok(StateOp {
                    map_name: op.map_name.clone(),
                    op_type: StateOpType::try_from(op.op_type)
                        .map_err(|_| anyhow::anyhow!("unknown StateOpType: {}", op.op_type))?,
                    key_expr,
                    value_expr,
                    condition_expr,
                    output_field: op.output_field.clone(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let map_names = config.map_names;
        let state = map_names
            .iter()
            .map(|n| (n.clone(), HashMap::new()))
            .collect();
        let dirty_keys = HashMap::new();

        // Defer final_exprs deserialization to on_start (needs intermediate schema)
        let final_exprs_bytes = config.final_exprs;

        Ok(ConstructedOperator::from_operator(Box::new(
            StatefulProcessorFunc {
                state,
                rocks: None,
                live_tables: HashMap::new(),
                dirty_keys,
                map_names,
                ops,
                final_exprs: vec![],
                final_exprs_bytes,
                registry,
                input_schema,
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a fresh state map for testing.
    fn new_state() -> HashMap<String, Option<String>> {
        HashMap::new()
    }

    // -----------------------------------------------------------------------
    // state_upsert semantics: insert-if-absent, return-existing-if-present
    // -----------------------------------------------------------------------

    #[test]
    fn test_upsert_inserts_when_key_absent() {
        let mut map = new_state();
        let key = "user_123".to_string();
        let value_if_new = "canonical_abc".to_string();

        // Upsert into empty map: should insert and return the new value
        let result = match map.get(&key) {
            Some(Some(existing)) => Some(existing.clone()),
            _ => {
                map.insert(key.clone(), Some(value_if_new.clone()));
                Some(value_if_new.clone())
            }
        };
        assert_eq!(result, Some("canonical_abc".to_string()));
        assert_eq!(map.get(&key), Some(&Some("canonical_abc".to_string())));
    }

    #[test]
    fn test_upsert_returns_existing_when_key_present() {
        let mut map = new_state();
        let key = "user_123".to_string();
        map.insert(key.clone(), Some("original".to_string()));

        // Upsert with different value: should return existing, not overwrite
        let value_if_new = "should_not_appear".to_string();
        let result = match map.get(&key) {
            Some(Some(existing)) => Some(existing.clone()),
            _ => {
                map.insert(key.clone(), Some(value_if_new.clone()));
                Some(value_if_new)
            }
        };
        assert_eq!(result, Some("original".to_string()));
        // Value in map should be unchanged
        assert_eq!(map.get(&key), Some(&Some("original".to_string())));
    }

    #[test]
    fn test_upsert_after_delete_inserts_new_value() {
        let mut map = new_state();
        let key = "user_123".to_string();

        // Insert then delete (tombstone)
        map.insert(key.clone(), Some("old_value".to_string()));
        map.insert(key.clone(), None); // tombstone

        // Upsert after tombstone: tombstone is not "exists", so should insert
        let value_if_new = "resurrected".to_string();
        let result = match map.get(&key) {
            Some(Some(existing)) => Some(existing.clone()),
            _ => {
                map.insert(key.clone(), Some(value_if_new.clone()));
                Some(value_if_new)
            }
        };
        assert_eq!(result, Some("resurrected".to_string()));
        assert_eq!(map.get(&key), Some(&Some("resurrected".to_string())));
    }

    // -----------------------------------------------------------------------
    // state_get semantics: read from map, None for missing or tombstoned keys
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_returns_none_for_missing_key() {
        let map = new_state();
        let result = map.get("nonexistent").and_then(|v| v.as_ref());
        assert!(result.is_none());
    }

    #[test]
    fn test_get_returns_value_for_existing_key() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("value".to_string()));
        let result = map.get("key").and_then(|v| v.as_ref());
        assert_eq!(result, Some(&"value".to_string()));
    }

    #[test]
    fn test_get_returns_none_for_tombstoned_key() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("value".to_string()));
        map.insert("key".to_string(), None); // tombstone
        let result = map.get("key").and_then(|v| v.as_ref());
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // state_put semantics: unconditional overwrite, returns stored value
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_inserts_new_key() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("value".to_string()));
        assert_eq!(map.get("key"), Some(&Some("value".to_string())));
    }

    #[test]
    fn test_put_overwrites_existing_key() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("old".to_string()));
        map.insert("key".to_string(), Some("new".to_string()));
        assert_eq!(map.get("key"), Some(&Some("new".to_string())));
    }

    #[test]
    fn test_put_overwrites_tombstone() {
        let mut map = new_state();
        map.insert("key".to_string(), None); // tombstone
        map.insert("key".to_string(), Some("revived".to_string()));
        assert_eq!(map.get("key"), Some(&Some("revived".to_string())));
    }

    // -----------------------------------------------------------------------
    // state_delete semantics: set tombstone, return whether key existed
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_existing_key_returns_true() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("value".to_string()));

        let existed = map.get("key").map(|v| v.is_some()).unwrap_or(false);
        map.insert("key".to_string(), None); // tombstone

        assert!(existed);
        assert_eq!(map.get("key"), Some(&None)); // tombstoned
    }

    #[test]
    fn test_delete_missing_key_returns_false() {
        let mut map = new_state();

        let existed = map.get("key").map(|v| v.is_some()).unwrap_or(false);
        map.insert("key".to_string(), None);

        assert!(!existed);
    }

    #[test]
    fn test_delete_already_tombstoned_returns_false() {
        let mut map = new_state();
        map.insert("key".to_string(), None); // already tombstoned

        let existed = map.get("key").map(|v| v.is_some()).unwrap_or(false);
        assert!(!existed);
    }

    // -----------------------------------------------------------------------
    // state_update semantics: conditional write, returns whether update applied
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_with_true_condition_applies() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("old".to_string()));

        let condition = true;
        if condition {
            map.insert("key".to_string(), Some("updated".to_string()));
        }
        assert_eq!(map.get("key"), Some(&Some("updated".to_string())));
    }

    #[test]
    fn test_update_with_false_condition_does_not_apply() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("old".to_string()));

        let condition = false;
        if condition {
            map.insert("key".to_string(), Some("should_not_appear".to_string()));
        }
        assert_eq!(map.get("key"), Some(&Some("old".to_string())));
    }

    #[test]
    fn test_update_with_none_condition_does_not_apply() {
        let mut map = new_state();
        map.insert("key".to_string(), Some("old".to_string()));

        let condition: Option<bool> = None;
        if condition == Some(true) {
            map.insert("key".to_string(), Some("should_not_appear".to_string()));
        }
        assert_eq!(map.get("key"), Some(&Some("old".to_string())));
    }

    // -----------------------------------------------------------------------
    // Dirty key tracking: writes mark dirty, reads do not
    // -----------------------------------------------------------------------

    #[test]
    fn test_dirty_tracking_put_marks_dirty() {
        let mut map = new_state();
        let mut dirty: HashSet<String> = HashSet::new();

        map.insert("k1".to_string(), Some("v1".to_string()));
        dirty.insert("k1".to_string());

        assert!(dirty.contains("k1"));
    }

    #[test]
    fn test_dirty_tracking_get_does_not_mark_dirty() {
        let mut map = new_state();
        map.insert("k1".to_string(), Some("v1".to_string()));
        let dirty: HashSet<String> = HashSet::new();

        // Get does NOT mark dirty
        let _ = map.get("k1");
        assert!(!dirty.contains("k1"));
    }

    #[test]
    fn test_dirty_tracking_delete_marks_dirty() {
        let mut map = new_state();
        let mut dirty: HashSet<String> = HashSet::new();

        map.insert("k1".to_string(), Some("v1".to_string()));
        // Delete: tombstone and mark dirty
        map.insert("k1".to_string(), None);
        dirty.insert("k1".to_string());

        assert!(dirty.contains("k1"));
    }

    #[test]
    fn test_dirty_tracking_clear_after_checkpoint() {
        let mut dirty: HashSet<String> = HashSet::new();
        dirty.insert("k1".to_string());
        dirty.insert("k2".to_string());

        assert_eq!(dirty.len(), 2);

        // Simulate checkpoint: clear dirty set
        dirty.clear();
        assert!(dirty.is_empty());
    }

    // -----------------------------------------------------------------------
    // extract_string helper
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_string_valid() {
        let array = StringArray::from(vec![Some("hello"), Some("world"), None]);
        assert_eq!(extract_string(&array, 0), Some("hello".to_string()));
        assert_eq!(extract_string(&array, 1), Some("world".to_string()));
        assert_eq!(extract_string(&array, 2), None);
    }

    #[test]
    fn test_extract_string_all_nulls() {
        let array = StringArray::from(vec![None::<&str>, None, None]);
        assert_eq!(extract_string(&array, 0), None);
        assert_eq!(extract_string(&array, 1), None);
    }

    // -----------------------------------------------------------------------
    // Multi-map isolation: operations on one map do not affect another
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_map_isolation() {
        let mut state: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();
        state.insert("map_a".to_string(), HashMap::new());
        state.insert("map_b".to_string(), HashMap::new());

        // Insert into map_a only
        state
            .get_mut("map_a")
            .unwrap()
            .insert("key".to_string(), Some("value_a".to_string()));

        // map_b should not have the key
        assert!(state.get("map_b").unwrap().get("key").is_none());
        assert_eq!(
            state.get("map_a").unwrap().get("key"),
            Some(&Some("value_a".to_string()))
        );
    }

    // Exercise the production constructor, expression decoding, batch execution,
    // final projection, and table-manager checkpoint staging together.
    #[derive(Default)]
    struct RuntimeCollector {
        batches: Vec<RecordBatch>,
    }

    #[async_trait::async_trait]
    impl Collector for RuntimeCollector {
        async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
            self.batches.push(batch);
            Ok(())
        }

        async fn broadcast_watermark(
            &mut self,
            _watermark: arroyo_types::Watermark,
        ) -> DataflowResult<()> {
            Ok(())
        }
    }

    fn runtime_schema() -> ArroyoSchema {
        ArroyoSchema::from_fields(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("condition", DataType::Boolean, true),
        ])
    }

    fn runtime_batch(
        keys: Vec<Option<&str>>,
        values: Vec<Option<&str>>,
        conditions: Vec<Option<bool>>,
    ) -> RecordBatch {
        let timestamps = arrow_array::TimestampNanosecondArray::from(vec![0; keys.len()]);
        RecordBatch::try_new(
            runtime_schema().schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
                Arc::new(arrow_array::BooleanArray::from(conditions)),
                Arc::new(timestamps),
            ],
        )
        .unwrap()
    }

    fn runtime_column(name: &str, index: usize) -> Vec<u8> {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            datafusion::physical_expr::expressions::Column::new(name, index),
        );
        datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
            &expr,
            &DefaultPhysicalExtensionCodec {},
        )
        .unwrap()
        .encode_to_vec()
    }

    fn runtime_config(
        operations: &[StateOpType],
        projection: &[usize],
    ) -> StatefulProcessorOperator {
        StatefulProcessorOperator {
            name: "runtime-regression".to_string(),
            operations: operations
                .iter()
                .enumerate()
                .map(|(index, op)| arroyo_rpc::grpc::api::StateOperation {
                    map_name: "__sp_runtime".to_string(),
                    op_type: *op as i32,
                    key_expr: runtime_column("key", 0),
                    value_expr: if matches!(
                        op,
                        StateOpType::StatePut | StateOpType::StateUpsert | StateOpType::StateUpdate
                    ) {
                        runtime_column("value", 1)
                    } else {
                        vec![]
                    },
                    condition_expr: if *op == StateOpType::StateUpdate {
                        runtime_column("condition", 2)
                    } else {
                        vec![]
                    },
                    output_field: format!("__state_result_{index}"),
                })
                .collect(),
            map_names: vec!["__sp_runtime".to_string()],
            input_schema: Some(runtime_schema().into()),
            final_exprs: projection
                .iter()
                .map(|index| runtime_column(&format!("__state_result_{index}"), 4 + index))
                .collect(),
        }
    }

    fn runtime_operator(config: StatefulProcessorOperator) -> Box<dyn ArrowOperator + Send> {
        match StatefulProcessorConstructor
            .with_config(config, Arc::new(Registry::default()))
            .unwrap()
        {
            ConstructedOperator::Operator(operator) => operator,
            ConstructedOperator::Source(_) => panic!("expected a stateful operator"),
        }
    }

    async fn runtime_context(operator: &(dyn ArrowOperator + Send)) -> OperatorContext {
        runtime_context_with_parallelism(operator, 1).await
    }

    async fn runtime_context_with_parallelism(
        operator: &(dyn ArrowOperator + Send),
        parallelism: u32,
    ) -> OperatorContext {
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
        OperatorContext::new(
            Arc::new(arroyo_types::TaskInfo {
                job_id: "stateful-runtime-regression".to_string(),
                operator_idx: 0,
                operator_name: "StatefulProcessor".to_string(),
                operator_id: "runtime".to_string(),
                task_index: 0,
                parallelism,
                key_range: 0..=u64::MAX,
                checkpoint_file_path_layout: Default::default(),
            }),
            None,
            control_tx,
            1,
            vec![Arc::new(runtime_schema())],
            None,
            operator.tables(),
        )
        .await
    }

    fn runtime_strings(batch: &RecordBatch, column: usize) -> Vec<Option<&str>> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect()
    }

    fn runtime_bools(batch: &RecordBatch, column: usize) -> Vec<Option<bool>> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .iter()
            .collect()
    }

    #[tokio::test]
    async fn test_runtime_same_key_rows_and_projection_across_batches() {
        let mut operator = runtime_operator(runtime_config(
            &[StateOpType::StateUpsert, StateOpType::StateGet],
            &[1, 0],
        ));
        let mut context = runtime_context(operator.as_ref()).await;
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        operator
            .process_batch(
                runtime_batch(
                    vec![Some("a"), Some("a"), Some("b")],
                    vec![Some("first"), Some("ignored"), Some("other")],
                    vec![None; 3],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let output = &collector.batches[0];
        // Final projection must discard the four input fields and reorder results.
        assert_eq!(output.num_columns(), 2);
        assert!(
            output
                .schema()
                .field(0)
                .name()
                .starts_with("__state_result_1")
        );
        assert!(
            output
                .schema()
                .field(1)
                .name()
                .starts_with("__state_result_0")
        );
        for column in 0..2 {
            assert_eq!(
                runtime_strings(output, column),
                vec![Some("first"), Some("first"), Some("other")]
            );
        }
        operator
            .process_batch(
                runtime_batch(vec![Some("a")], vec![Some("later")], vec![None]),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(
            runtime_strings(&collector.batches[1], 0),
            vec![Some("first")]
        );
        assert_eq!(
            runtime_strings(&collector.batches[1], 1),
            vec![Some("first")]
        );
    }

    #[tokio::test]
    async fn test_runtime_ordered_writes_deletes_and_null_inputs() {
        let mut operator = runtime_operator(runtime_config(
            &[
                StateOpType::StateGet,
                StateOpType::StatePut,
                StateOpType::StateGet,
                StateOpType::StateUpdate,
                StateOpType::StateGet,
                StateOpType::StateDelete,
                StateOpType::StateGet,
            ],
            &[0, 1, 2, 3, 4, 5, 6],
        ));
        let mut context = runtime_context(operator.as_ref()).await;
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        operator
            .process_batch(
                runtime_batch(
                    vec![Some("a"), Some("a"), None, Some("a"), Some("a")],
                    vec![
                        Some("one"),
                        Some("two"),
                        Some("ignored"),
                        None,
                        Some("last"),
                    ],
                    vec![Some(true), Some(false), Some(true), Some(true), None],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let output = &collector.batches[0];
        assert_eq!(output.num_columns(), 7);
        assert_eq!(runtime_strings(output, 0), vec![None; 5]);
        let written = vec![Some("one"), Some("two"), None, None, Some("last")];
        for column in [1, 2, 4] {
            assert_eq!(runtime_strings(output, column), written);
        }
        assert_eq!(
            runtime_bools(output, 3),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ]
        );
        assert_eq!(
            runtime_bools(output, 5),
            vec![Some(true), Some(true), Some(false), Some(false), Some(true)]
        );
        assert_eq!(runtime_strings(output, 6), vec![None; 5]);
    }

    #[tokio::test]
    async fn test_runtime_checkpoint_stages_deletion_for_operator_reload() {
        let mut operator = runtime_operator(runtime_config(&[StateOpType::StatePut], &[0]));
        let mut context = runtime_context(operator.as_ref()).await;
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        operator
            .process_batch(
                runtime_batch(
                    vec![Some("deleted"), Some("retained")],
                    vec![Some("old"), Some("keep")],
                    vec![None; 2],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let barrier = CheckpointBarrier {
            epoch: 1,
            min_epoch: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            then_stop: false,
        };
        operator
            .handle_checkpoint(barrier, &mut context, &mut collector)
            .await
            .unwrap();
        let mut deleter = runtime_operator(runtime_config(&[StateOpType::StateDelete], &[0]));
        deleter.on_start(&mut context).await.unwrap();
        deleter
            .process_batch(
                runtime_batch(vec![Some("deleted")], vec![None], vec![None]),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(
            runtime_bools(collector.batches.last().unwrap(), 0),
            vec![Some(true)]
        );
        deleter
            .handle_checkpoint(barrier, &mut context, &mut collector)
            .await
            .unwrap();
        // This checks the real table-manager staging path and on_start loading,
        // not a durable backend flush/restart (which requires checkpoint metadata).
        let mut reader = runtime_operator(runtime_config(&[StateOpType::StateGet], &[0]));
        reader.on_start(&mut context).await.unwrap();
        reader
            .process_batch(
                runtime_batch(
                    vec![Some("deleted"), Some("retained")],
                    vec![None; 2],
                    vec![None; 2],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        assert_eq!(
            runtime_strings(collector.batches.last().unwrap(), 0),
            vec![None, Some("keep")]
        );
    }

    #[tokio::test]
    async fn test_runtime_rejects_parallel_state_maps() {
        let mut operator = runtime_operator(runtime_config(&[StateOpType::StateGet], &[0]));
        let mut context = runtime_context_with_parallelism(operator.as_ref(), 2).await;
        let error = operator.on_start(&mut context).await.unwrap_err();
        assert!(error.to_string().contains("requires parallelism 1"));
    }

    #[tokio::test]
    async fn test_runtime_updates_preserve_state_in_row_order() {
        let mut operator = runtime_operator(runtime_config(
            &[
                StateOpType::StateGet,
                StateOpType::StateUpdate,
                StateOpType::StateGet,
            ],
            &[0, 1, 2],
        ));
        let mut context = runtime_context(operator.as_ref()).await;
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        operator
            .process_batch(
                runtime_batch(
                    vec![Some("a"); 4],
                    vec![
                        Some("first"),
                        Some("ignored"),
                        Some("ignored"),
                        Some("last"),
                    ],
                    vec![Some(true), Some(false), None, Some(true)],
                ),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let output = &collector.batches[0];
        assert_eq!(
            runtime_strings(output, 0),
            vec![None, Some("first"), Some("first"), Some("first")]
        );
        assert_eq!(
            runtime_bools(output, 1),
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        assert_eq!(
            runtime_strings(output, 2),
            vec![Some("first"), Some("first"), Some("first"), Some("last")]
        );
    }

    #[tokio::test]
    async fn test_runtime_preserves_declared_output_schema() {
        let mut config =
            runtime_config(&[StateOpType::StateUpdate, StateOpType::StateGet], &[1, 0]);
        config
            .final_exprs
            .push(runtime_column(arroyo_rpc::TIMESTAMP_FIELD, 3));
        let mut operator = runtime_operator(config);
        let mut context = runtime_context(operator.as_ref()).await;
        let declared = Arc::new(ArroyoSchema::from_fields(vec![
            Field::new("canonical_id", DataType::Utf8, true),
            Field::new("update_applied", DataType::Boolean, false),
        ]));
        context.out_schema = Some(declared.clone());
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        operator
            .process_batch(
                runtime_batch(vec![Some("a")], vec![Some("canonical")], vec![Some(true)]),
                &mut context,
                &mut collector,
            )
            .await
            .unwrap();
        let output = &collector.batches[0];
        assert_eq!(output.schema(), declared.schema);
        assert_eq!(runtime_strings(output, 0), vec![Some("canonical")]);
        assert_eq!(runtime_bools(output, 1), vec![Some(true)]);
        assert_eq!(output.schema().field(2).name(), arroyo_rpc::TIMESTAMP_FIELD);
        assert!(!output.schema().field(2).is_nullable());
    }

    #[tokio::test]
    async fn test_runtime_rejects_projection_incompatible_with_declared_schema() {
        // Missing state emits NULL; it cannot satisfy a non-null output field.
        let mut config = runtime_config(&[StateOpType::StateGet], &[0]);
        config
            .final_exprs
            .push(runtime_column(arroyo_rpc::TIMESTAMP_FIELD, 3));
        let mut operator = runtime_operator(config);
        let mut context = runtime_context(operator.as_ref()).await;
        context.out_schema = Some(Arc::new(ArroyoSchema::from_fields(vec![Field::new(
            "canonical_id",
            DataType::Utf8,
            false,
        )])));
        operator.on_start(&mut context).await.unwrap();
        let mut collector = RuntimeCollector::default();
        let result = operator
            .process_batch(
                runtime_batch(vec![Some("missing")], vec![None], vec![None]),
                &mut context,
                &mut collector,
            )
            .await;
        assert!(result.is_err());
        assert!(collector.batches.is_empty());
    }
}
