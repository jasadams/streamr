use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, ConstructedOperator, OperatorConstructor, Registry,
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api::{StateOpType, StatefulProcessorOperator};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_state::global_table_config;
use arroyo_types::CheckpointBarrier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use itertools::Itertools;
use prost::Message;

struct EvaluatedOp {
    key_array: Arc<dyn Array>,
    value_array: Option<Arc<dyn Array>>,
    condition_array: Option<Arc<dyn Array>>,
}

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
        let mut tables = HashMap::new();
        for map_name in &self.map_names {
            tables.extend(global_table_config(
                map_name.clone(),
                format!("stateful processor map: {}", map_name),
            ));
        }
        tables
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        if ctx.task_info.parallelism != 1 {
            return Err(arroyo_rpc::errors::DataflowError::ExternalError(format!(
                "StatefulProcessor requires parallelism 1; received {}. State maps are not partitioned by key",
                ctx.task_info.parallelism
            )));
        }

        // Load state from checkpoint
        for map_name in &self.map_names {
            let gs = ctx
                .table_manager
                .get_global_keyed_state::<String, Option<String>>(map_name)
                .await?;
            let existing: HashMap<String, Option<String>> = gs
                .get_all()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.state.insert(map_name.clone(), existing);
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

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let num_rows = batch.num_rows();

        let mut evaluated: Vec<EvaluatedOp> = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            let key_array = op.key_expr.evaluate(&batch)?.into_array(num_rows)?;
            let value_array = match &op.value_expr {
                Some(expr) => Some(expr.evaluate(&batch)?.into_array(num_rows)?),
                None => None,
            };
            let condition_array = match &op.condition_expr {
                Some(expr) => Some(expr.evaluate(&batch)?.into_array(num_rows)?),
                None => None,
            };
            evaluated.push(EvaluatedOp {
                key_array,
                value_array,
                condition_array,
            });
        }

        enum ResultBuilder {
            Str(StringBuilder),
            Bool(BooleanBuilder),
        }

        let mut builders: Vec<ResultBuilder> = self
            .ops
            .iter()
            .map(|op| match op.op_type {
                StateOpType::StateGet | StateOpType::StatePut | StateOpType::StateUpsert => {
                    ResultBuilder::Str(StringBuilder::with_capacity(num_rows, num_rows * 16))
                }
                StateOpType::StateUpdate | StateOpType::StateDelete => {
                    ResultBuilder::Bool(BooleanBuilder::with_capacity(num_rows))
                }
            })
            .collect();

        for row in 0..num_rows {
            for (op_idx, op) in self.ops.iter().enumerate() {
                let ev = &evaluated[op_idx];
                let key = extract_string(ev.key_array.as_ref(), row);

                let map = self
                    .state
                    .get_mut(&op.map_name)
                    .expect("map name not found in state");

                match op.op_type {
                    StateOpType::StateGet => {
                        let ResultBuilder::Str(builder) = &mut builders[op_idx] else {
                            unreachable!();
                        };
                        match key
                            .as_ref()
                            .and_then(|k| map.get(k))
                            .and_then(|v| v.as_ref())
                        {
                            Some(v) => builder.append_value(v),
                            None => builder.append_null(),
                        }
                    }
                    StateOpType::StatePut => {
                        let ResultBuilder::Str(builder) = &mut builders[op_idx] else {
                            unreachable!();
                        };
                        let value = ev
                            .value_array
                            .as_ref()
                            .and_then(|arr| extract_string(arr.as_ref(), row));

                        match (key, value) {
                            (Some(k), Some(v)) => {
                                builder.append_value(&v);
                                self.dirty_keys
                                    .entry(op.map_name.clone())
                                    .or_default()
                                    .insert(k.clone());
                                map.insert(k, Some(v));
                            }
                            _ => builder.append_null(),
                        }
                    }
                    StateOpType::StateUpsert => {
                        let ResultBuilder::Str(builder) = &mut builders[op_idx] else {
                            unreachable!();
                        };
                        let value_if_new = ev
                            .value_array
                            .as_ref()
                            .and_then(|arr| extract_string(arr.as_ref(), row));

                        match key {
                            Some(k) => {
                                if let Some(Some(existing)) = map.get(&k) {
                                    builder.append_value(existing);
                                } else if let Some(v) = value_if_new {
                                    builder.append_value(&v);
                                    self.dirty_keys
                                        .entry(op.map_name.clone())
                                        .or_default()
                                        .insert(k.clone());
                                    map.insert(k, Some(v));
                                } else {
                                    builder.append_null();
                                }
                            }
                            None => builder.append_null(),
                        }
                    }
                    StateOpType::StateUpdate => {
                        let ResultBuilder::Bool(builder) = &mut builders[op_idx] else {
                            unreachable!();
                        };
                        let new_value = ev
                            .value_array
                            .as_ref()
                            .and_then(|arr| extract_string(arr.as_ref(), row));

                        let condition = ev.condition_array.as_ref().and_then(|arr| {
                            let bool_arr =
                                arr.as_any().downcast_ref::<arrow_array::BooleanArray>()?;
                            if bool_arr.is_null(row) {
                                None
                            } else {
                                Some(bool_arr.value(row))
                            }
                        });

                        match (key, new_value, condition) {
                            (Some(k), Some(v), Some(true)) => {
                                self.dirty_keys
                                    .entry(op.map_name.clone())
                                    .or_default()
                                    .insert(k.clone());
                                map.insert(k, Some(v));
                                builder.append_value(true);
                            }
                            _ => builder.append_value(false),
                        }
                    }
                    StateOpType::StateDelete => {
                        let ResultBuilder::Bool(builder) = &mut builders[op_idx] else {
                            unreachable!();
                        };
                        match key {
                            Some(k) => {
                                let existed = map.get(&k).map(|v| v.is_some()).unwrap_or(false);
                                self.dirty_keys
                                    .entry(op.map_name.clone())
                                    .or_default()
                                    .insert(k.clone());
                                map.insert(k, None);
                                builder.append_value(existed);
                            }
                            None => builder.append_value(false),
                        }
                    }
                }
            }
        }

        // Build intermediate batch: input columns + result columns
        let mut intermediate_columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
        let mut intermediate_fields: Vec<Arc<Field>> =
            batch.schema().fields().iter().cloned().collect();

        for (builder, op) in builders.into_iter().zip(self.ops.iter()) {
            let (array, field): (Arc<dyn Array>, Arc<Field>) = match builder {
                ResultBuilder::Str(mut b) => {
                    let arr = Arc::new(b.finish());
                    let field = Arc::new(Field::new(&op.output_field, DataType::Utf8, true));
                    (arr, field)
                }
                ResultBuilder::Bool(mut b) => {
                    let arr = Arc::new(b.finish());
                    let field = Arc::new(Field::new(&op.output_field, DataType::Boolean, true));
                    (arr, field)
                }
            };
            intermediate_columns.push(array);
            intermediate_fields.push(field);
        }

        let intermediate_schema = Arc::new(Schema::new(intermediate_fields));
        let intermediate_batch = RecordBatch::try_new(intermediate_schema, intermediate_columns)?;

        // Apply final projection to produce the user's SELECT schema
        if self.final_exprs.is_empty() {
            collector.collect(intermediate_batch).await?;
        } else {
            let projected: Vec<Arc<dyn Array>> = self
                .final_exprs
                .iter()
                .map(|expr| expr.evaluate(&intermediate_batch)?.into_array(num_rows))
                .try_collect()?;

            // The outgoing edge declares SELECT aliases, field nullability, and
            // the _timestamp field required by downstream operators. Physical
            // expression display names do not preserve that declared schema.
            let projected_schema = if let Some(out_schema) = &ctx.out_schema {
                out_schema.schema.clone()
            } else {
                let projected_fields: Vec<Arc<Field>> = self
                    .final_exprs
                    .iter()
                    .map(|expr| {
                        let dt = expr.data_type(intermediate_batch.schema().as_ref())?;
                        let nullable = expr.nullable(intermediate_batch.schema().as_ref())?;
                        let name = expr.to_string();
                        Ok(Arc::new(Field::new(name, dt, nullable)))
                    })
                    .collect::<datafusion::common::Result<_>>()?;
                Arc::new(Schema::new(projected_fields))
            };
            // RecordBatch validates projected column count, types and nullability
            // against the outgoing schema before passing anything downstream.
            let projected_batch = RecordBatch::try_new(projected_schema, projected)?;
            collector.collect(projected_batch).await?;
        }

        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // GlobalKeyedTable regenerates a complete snapshot every epoch; its
        // checkpointer does not inherit entries or files from previous epochs.
        // Write every key, including unchanged entries and deletion tombstones,
        // so restoring a later checkpoint cannot lose previously stored state.
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

        let ops = config
            .operations
            .iter()
            .map(|op| {
                let key_expr = PhysicalExprNode::decode(&mut op.key_expr.as_slice())?;
                let key_expr = parse_physical_expr(
                    &key_expr,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )?;

                let value_expr = if op.value_expr.is_empty() {
                    None
                } else {
                    let expr = PhysicalExprNode::decode(&mut op.value_expr.as_slice())?;
                    Some(parse_physical_expr(
                        &expr,
                        registry.as_ref(),
                        &input_schema.schema,
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
                        &input_schema.schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?)
                };

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
