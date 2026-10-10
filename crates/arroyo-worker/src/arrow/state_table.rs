//! One row of a planned state-table access. The fused event owner supplies a
//! shared working scope and decides when a bounded chunk is committed/emitted.
//! This module has no backend selection or checkpoint policy.
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use arroyo_operator::operator::Registry;
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::{ArrowOperator, ConstructedOperator, OperatorConstructor},
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_rpc::grpc::api::{
    StateTableCaptureOperator as CaptureConfig, StateTableMutationClause, StateTableOperator,
};
use arroyo_state::live::typed_table::{TableDescriptor, TypedTable, WorkingScope};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

use super::state_table_concat::{ConcatAllowance, guard_expression};

struct MutationClause {
    matched: bool,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    action: MutationAction,
    values: Vec<(usize, Arc<dyn PhysicalExpr>)>,
}

#[derive(Clone, Copy)]
enum MutationAction {
    Insert,
    Update,
    Delete,
}

pub(crate) struct StateTableStep {
    pub(crate) table_identity: String,
    pub(crate) schema_identity: String,
    pub(crate) partition_key: Vec<usize>,
    pub(crate) event_scope_id: String,
    input_schema: ArroyoSchema,
    output_schema: ArroyoSchema,
    table_schema: SchemaRef,
    expression_schema: SchemaRef,
    primary_key: Vec<usize>,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    clauses: Vec<MutationClause>,
    lookup: Option<LookupKind>,
}

#[cfg(test)]
pub(crate) fn synthetic_merge_step() -> (StateTableStep, SchemaRef, SchemaRef, SchemaRef) {
    use arrow_schema::{Field, TimeUnit};
    let input = Arc::new(Schema::new(vec![
        Field::new("item", DataType::Utf8, true),
        Field::new("quantity", DataType::Int64, false),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let table = Arc::new(Schema::new(vec![
        Field::new("item", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
    ]));
    let output = Arc::new(Schema::new(vec![
        Field::new(
            "source",
            DataType::Struct(vec![input.field(0).clone(), input.field(1).clone()].into()),
            false,
        ),
        Field::new("old", DataType::Struct(table.fields().clone()), true),
        Field::new("new", DataType::Struct(table.fields().clone()), true),
        Field::new("action", DataType::Utf8, false),
        input.field(2).clone(),
    ]));
    let expression = Arc::new(Schema::new(vec![
        input.field(0).clone(),
        input.field(1).clone(),
        input.field(2).clone(),
        Field::new("item", DataType::Utf8, true),
        Field::new("quantity", DataType::Int64, true),
    ]));
    let item: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new("item", 0),
    );
    let quantity: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new("quantity", 1),
    );
    let step = StateTableStep {
        table_identity: "inventory".into(),
        schema_identity: "v1".into(),
        partition_key: vec![0],
        event_scope_id: "events".into(),
        input_schema: ArroyoSchema::new_unkeyed(input.clone(), 2),
        output_schema: ArroyoSchema::new_unkeyed(output.clone(), 4),
        table_schema: table.clone(),
        expression_schema: expression,
        primary_key: vec![0],
        keys: vec![item.clone()],
        clauses: vec![
            MutationClause {
                matched: true,
                predicate: None,
                action: MutationAction::Update,
                values: vec![(1, quantity.clone())],
            },
            MutationClause {
                matched: false,
                predicate: None,
                action: MutationAction::Insert,
                values: vec![(0, item), (1, quantity)],
            },
        ],
        lookup: None,
    };
    (step, input, table, output)
}

#[derive(Clone, Copy)]
enum LookupKind {
    Inner,
    Left,
}

fn decode_expr(
    bytes: &[u8],
    schema: &Schema,
    registry: &Registry,
    allowance: &Arc<ConcatAllowance>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let node = PhysicalExprNode::decode(bytes)?;
    let expression =
        parse_physical_expr(&node, registry, schema, &DefaultPhysicalExtensionCodec {})?;
    guard_expression(expression, allowance).map_err(Into::into)
}

fn decode_clause(
    clause: StateTableMutationClause,
    schema: &Schema,
    registry: &Registry,
    field_count: usize,
    primary_key: &[usize],
    allowance: &Arc<ConcatAllowance>,
) -> Result<MutationClause> {
    let action = match clause.action.as_str() {
        "insert" if !clause.matched => MutationAction::Insert,
        "update" if clause.matched => MutationAction::Update,
        "delete" if clause.matched => MutationAction::Delete,
        _ => bail!("invalid planned state-table mutation action"),
    };
    let predicate = clause
        .predicate
        .map(|bytes| decode_expr(&bytes, schema, registry, allowance))
        .transpose()?;
    let mut values = Vec::with_capacity(clause.values.len());
    for value in clause.values {
        let index = usize::try_from(value.field_index)?;
        ensure!(index < field_count, "state-table field index out of range");
        ensure!(
            !values.iter().any(|(prior, _)| *prior == index),
            "duplicate state-table field assignment"
        );
        values.push((
            index,
            decode_expr(&value.expression, schema, registry, allowance)?,
        ));
    }
    match action {
        MutationAction::Insert => ensure!(
            values.len() == field_count,
            "state-table insert must assign every field"
        ),
        MutationAction::Delete => ensure!(values.is_empty(), "delete cannot assign fields"),
        MutationAction::Update => ensure!(
            values.iter().all(|(index, _)| !primary_key.contains(index)),
            "state-table update cannot change primary-key fields"
        ),
    }
    Ok(MutationClause {
        matched: clause.matched,
        predicate,
        action,
        values,
    })
}

impl StateTableStep {
    pub(crate) fn descriptor(&self) -> TableDescriptor {
        TableDescriptor {
            table_identity: self.table_identity.as_bytes().to_vec(),
            schema_identity: self.schema_identity.as_bytes().to_vec(),
            schema: self.table_schema.clone(),
            primary_key: self.primary_key.clone(),
        }
    }

    pub(crate) fn is_mutation(&self) -> bool {
        self.lookup.is_none()
    }

    pub(super) fn decode(
        config: StateTableOperator,
        registry: &Registry,
        allowance: &Arc<ConcatAllowance>,
    ) -> Result<Self> {
        ensure!(
            config.requires_fused_serial_owner,
            "state-table access requires a fused serial event owner"
        );
        let table = config.table.context("missing state-table descriptor")?;
        ensure!(
            table.parallelism == 1,
            "state tables currently require parallelism 1"
        );
        let table_schema = Arc::new(serde_json::from_str::<Schema>(&table.schema_json)?);
        let input_schema: ArroyoSchema = config
            .input_schema
            .context("missing state-table input schema")?
            .try_into()?;
        let output_schema: ArroyoSchema = config
            .output_schema
            .context("missing state-table output schema")?
            .try_into()?;
        let expression_schema = Arc::new(serde_json::from_str::<Schema>(
            &config.expression_schema_json,
        )?);
        let primary_key = table
            .primary_key
            .iter()
            .map(|index| usize::try_from(*index))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let partition_key = table
            .partition_key
            .iter()
            .map(|index| usize::try_from(*index))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            !primary_key.is_empty() && primary_key.len() == config.key_expressions.len(),
            "state-table access requires one expression per primary-key field"
        );
        ensure!(
            primary_key.iter().all(|&i| i < table_schema.fields().len()),
            "state-table primary-key index out of range"
        );
        ensure!(
            !partition_key.is_empty()
                && partition_key
                    .iter()
                    .all(|index| primary_key.contains(index)),
            "state-table partition key must be a nonempty primary-key subset"
        );
        ensure!(
            expression_schema.fields().len()
                == input_schema.schema.fields().len() + table_schema.fields().len(),
            "state-table expression schema must contain input and old target fields"
        );
        let keys = config
            .key_expressions
            .iter()
            .map(|bytes| decode_expr(bytes, &input_schema.schema, registry, allowance))
            .collect::<Result<Vec<_>>>()?;
        let clauses = config
            .clauses
            .into_iter()
            .map(|clause| {
                decode_clause(
                    clause,
                    &expression_schema,
                    registry,
                    table_schema.fields().len(),
                    &primary_key,
                    allowance,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let lookup = config
            .lookup_join_type
            .map(|kind| match kind.to_ascii_lowercase().as_str() {
                "inner" => Ok(LookupKind::Inner),
                "left" => Ok(LookupKind::Left),
                _ => bail!("unsupported state-table lookup join type"),
            })
            .transpose()?;
        ensure!(
            lookup.is_some() == clauses.is_empty(),
            "state-table access must be exactly one lookup or mutation"
        );
        Ok(Self {
            table_identity: table.table_identity,
            schema_identity: table.schema_identity,
            partition_key,
            event_scope_id: config.event_scope_id,
            input_schema,
            output_schema,
            table_schema,
            expression_schema,
            primary_key,
            keys,
            clauses,
            lookup,
        })
    }

    fn key(&self, input: &RecordBatch) -> Result<Option<RecordBatch>> {
        let mut arrays = Vec::with_capacity(self.keys.len());
        for expression in &self.keys {
            let array = expression.evaluate(input)?.into_array(1)?;
            if array.is_null(0) {
                return Ok(None);
            }
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(
            self.primary_key
                .iter()
                .map(|&index| self.table_schema.field(index).clone())
                .collect::<Vec<_>>(),
        ));
        Ok(Some(RecordBatch::try_new(schema, arrays)?))
    }

    fn expression_input(
        &self,
        input: &RecordBatch,
        old: Option<&RecordBatch>,
    ) -> Result<RecordBatch> {
        let mut columns = input.columns().to_vec();
        columns.extend(match old {
            Some(old) => old.columns().to_vec(),
            None => self
                .table_schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_null_array(field.data_type(), 1))
                .collect(),
        });
        Ok(RecordBatch::try_new(
            self.expression_schema.clone(),
            columns,
        )?)
    }

    fn predicate_is_true(predicate: &dyn PhysicalExpr, input: &RecordBatch) -> Result<bool> {
        let values = predicate.evaluate(input)?.into_array(1)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("state-table WHEN predicate did not return BOOLEAN")?;
        Ok(!values.is_null(0) && values.value(0))
    }

    fn apply_values(
        &self,
        clause: &MutationClause,
        expression_input: &RecordBatch,
        old: Option<&RecordBatch>,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<Option<ArrayRef>> = match old {
            Some(old) => old.columns().iter().map(|c| Some(c.clone())).collect(),
            None => vec![None; self.table_schema.fields().len()],
        };
        for (index, expr) in &clause.values {
            columns[*index] = Some(expr.evaluate(expression_input)?.into_array(1)?);
        }
        let columns = columns
            .into_iter()
            .map(|column| column.context("state-table mutation omitted a target field"))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.table_schema.clone(), columns)?)
    }

    fn row_struct(fields: &arrow_schema::Fields, row: Option<&RecordBatch>) -> Result<ArrayRef> {
        match row {
            Some(row) => Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                row.columns().to_vec(),
                None,
            )?)),
            None => Ok(arrow_array::new_null_array(
                &DataType::Struct(fields.clone()),
                1,
            )),
        }
    }

    fn merge_output(
        &self,
        source: &RecordBatch,
        old: Option<&RecordBatch>,
        new: Option<&RecordBatch>,
        action: &str,
    ) -> Result<RecordBatch> {
        let output = &self.output_schema.schema;
        ensure!(output.fields().len() == 5, "invalid MERGE result schema");
        let DataType::Struct(source_fields) = output.field(0).data_type() else {
            bail!("MERGE source context must be a struct")
        };
        let mut source_columns = Vec::with_capacity(source_fields.len());
        for field in source_fields {
            let index = source.schema().index_of(field.name())?;
            source_columns.push(source.column(index).clone());
        }
        let source_struct = Arc::new(StructArray::try_new(
            source_fields.clone(),
            source_columns,
            None,
        )?);
        let timestamp = source.column(self.input_schema.timestamp_index).clone();
        let columns: Vec<ArrayRef> = vec![
            source_struct,
            Self::row_struct(self.table_schema.fields(), old)?,
            Self::row_struct(self.table_schema.fields(), new)?,
            Arc::new(StringArray::from(vec![action])),
            timestamp,
        ];
        Ok(RecordBatch::try_new(output.clone(), columns)?)
    }

    fn lookup_output(&self, input: &RecordBatch, old: Option<&RecordBatch>) -> Result<RecordBatch> {
        // The logical JOIN can carry the event timestamp in the left schema or
        // append it afterward. The Arrow event always has one timestamp; place
        // it at the position declared by the planned output schema.
        let timestamp = input.column(self.input_schema.timestamp_index).clone();
        let mut columns = input
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.input_schema.timestamp_index)
            .map(|(_, column)| column.clone())
            .collect::<Vec<_>>();
        columns.extend(match old {
            Some(old) => old.columns().to_vec(),
            None => self
                .table_schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_null_array(field.data_type(), 1))
                .collect(),
        });
        let timestamp_index = self.output_schema.timestamp_index;
        ensure!(
            timestamp_index <= columns.len(),
            "planned lookup timestamp index exceeds output columns"
        );
        columns.insert(timestamp_index, timestamp);
        let planned = self.output_schema.schema.fields();
        let input_fields = input.schema();
        let mut expected_fields = input_fields
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.input_schema.timestamp_index)
            .map(|(_, field)| field.as_ref().clone())
            .collect::<Vec<_>>();
        let left_join = matches!(self.lookup, Some(LookupKind::Left));
        expected_fields.extend(self.table_schema.fields().iter().map(|field| {
            field
                .as_ref()
                .clone()
                .with_nullable(left_join || field.is_nullable())
        }));
        ensure!(
            planned.len() == columns.len()
                && planned[timestamp_index].as_ref()
                    == input_fields.field(self.input_schema.timestamp_index)
                && planned
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != timestamp_index)
                    .map(|(_, field)| field.as_ref())
                    .eq(expected_fields.iter()),
            "state-table lookup output differs from planned event/target/timestamp schema"
        );
        Ok(RecordBatch::try_new(
            self.output_schema.schema.clone(),
            columns,
        )?)
    }

    /// Execute exactly one input row. `None` means an unmatched INNER lookup;
    /// MERGE always captures source/old/new/action, including no-action events.
    pub(crate) async fn execute(
        &self,
        input: &RecordBatch,
        table: &TypedTable,
        scope: &mut WorkingScope<'_>,
    ) -> Result<Option<RecordBatch>> {
        ensure!(
            input.num_rows() == 1,
            "state-table step requires one event row"
        );
        ensure!(
            input.schema().as_ref() == self.input_schema.schema.as_ref(),
            "state-table input schema changed after planning"
        );
        ensure!(
            table.descriptor().table_identity == self.table_identity.as_bytes()
                && table.descriptor().schema_identity == self.schema_identity.as_bytes()
                && table.descriptor().schema.as_ref() == self.table_schema.as_ref()
                && table.descriptor().primary_key == self.primary_key,
            "state-table descriptor differs from planned access"
        );
        let key = self.key(input)?;
        let old = match &key {
            Some(key) => scope.get_from(table, key).await?,
            None => None,
        };
        let old_batch = old.as_ref().map(|row| row.batch());
        if let Some(lookup) = self.lookup {
            return match (lookup, old_batch) {
                (LookupKind::Inner, None) => Ok(None),
                (_, old) => Ok(Some(self.lookup_output(input, old)?)),
            };
        }

        let expression_input = self.expression_input(input, old_batch)?;
        let mut action = "none";
        let mut new = old_batch.cloned();
        if let Some(key) = &key {
            for clause in &self.clauses {
                if clause.matched != old_batch.is_some() {
                    continue;
                }
                if let Some(predicate) = &clause.predicate
                    && !Self::predicate_is_true(predicate.as_ref(), &expression_input)?
                {
                    continue;
                }
                match clause.action {
                    MutationAction::Insert | MutationAction::Update => {
                        let value = self.apply_values(clause, &expression_input, old_batch)?;
                        ensure!(
                            &value.project(&self.primary_key)? == key,
                            "state-table mutation changed its matched primary key"
                        );
                        scope.put_into(table, &value)?;
                        action = if matches!(clause.action, MutationAction::Insert) {
                            "insert"
                        } else {
                            "update"
                        };
                        new = Some(value);
                    }
                    MutationAction::Delete => {
                        scope.delete_from(table, key)?;
                        action = "delete";
                        new = None;
                    }
                }
                break;
            }
        }
        Ok(Some(self.merge_output(
            input,
            old_batch,
            new.as_ref(),
            action,
        )?))
    }
}

/// Materialize one event's exit branches as nullable typed structs. Presence is
/// the parent struct validity bit, so a present row whose fields are all NULL
/// remains different from an absent/filtered branch.
pub(crate) fn capture_envelope(
    captures: &[Option<RecordBatch>],
    capture_schemas: &[SchemaRef],
    timestamp_field: Arc<arrow_schema::Field>,
    timestamp: ArrayRef,
) -> Result<RecordBatch> {
    ensure!(
        captures.len() == capture_schemas.len() && timestamp.len() == 1,
        "invalid state-table capture envelope"
    );
    let mut fields = Vec::with_capacity(captures.len() + 1);
    let mut columns = Vec::with_capacity(captures.len() + 1);
    for (index, (capture, schema)) in captures.iter().zip(capture_schemas).enumerate() {
        let data_type = DataType::Struct(schema.fields().clone());
        fields.push(Arc::new(arrow_schema::Field::new(
            format!("capture_{index}"),
            data_type.clone(),
            true,
        )));
        columns.push(match capture {
            Some(batch) => {
                ensure!(
                    batch.num_rows() == 1 && batch.schema().as_ref() == schema.as_ref(),
                    "captured state-table branch changed schema or cardinality"
                );
                Arc::new(StructArray::try_new(
                    schema.fields().clone(),
                    batch.columns().to_vec(),
                    None,
                )?) as ArrayRef
            }
            None => arrow_array::new_null_array(&data_type, 1),
        });
    }
    fields.push(timestamp_field);
    columns.push(timestamp);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Stateless branch fanout. It never re-runs state access or evaluates the
/// source query: it only selects a previously captured, typed per-event value.
pub(crate) fn extract_capture(
    envelope: &RecordBatch,
    index: usize,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let capture = envelope
        .columns()
        .get(index)
        .context("capture branch index out of range")?
        .as_any()
        .downcast_ref::<StructArray>()
        .context("capture branch is not a typed struct")?;
    ensure!(
        capture.data_type() == &DataType::Struct(output_schema.fields().clone()),
        "capture branch schema changed"
    );
    let selected = BooleanArray::from(
        (0..capture.len())
            .map(|row| capture.is_valid(row))
            .collect::<Vec<_>>(),
    );
    let columns = capture
        .columns()
        .iter()
        .map(|column| arrow::compute::filter(column.as_ref(), &selected))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(output_schema, columns)?)
}

pub struct StateTableCaptureConstructor;

struct StateTableCapture {
    index: usize,
    input: ArroyoSchema,
    output: ArroyoSchema,
}

impl OperatorConstructor for StateTableCaptureConstructor {
    type ConfigT = CaptureConfig;

    fn with_config(&self, config: Self::ConfigT, _: Arc<Registry>) -> Result<ConstructedOperator> {
        let input: ArroyoSchema = config
            .input_schema
            .context("capture extractor lacks input schema")?
            .try_into()?;
        let output: ArroyoSchema = config
            .output_schema
            .context("capture extractor lacks output schema")?
            .try_into()?;
        let index = usize::try_from(config.capture_index)?;
        let field = input
            .schema
            .fields()
            .get(index)
            .context("capture index exceeds envelope schema")?;
        ensure!(
            field.name() == &format!("capture_{index}")
                && field.is_nullable()
                && field.data_type() == &DataType::Struct(output.schema.fields().clone())
                && input.timestamp_index == input.schema.fields().len() - 1,
            "capture extractor has incompatible branch or timestamp schema"
        );
        Ok(ConstructedOperator::from_operator(Box::new(
            StateTableCapture {
                index,
                input,
                output,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for StateTableCapture {
    fn name(&self) -> String {
        format!("state_table_capture_{}", self.index)
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if batch.schema().as_ref() != self.input.schema.as_ref() {
            return Err(DataflowError::ExternalError(
                "capture extractor input schema changed".into(),
            ));
        }
        let selected = extract_capture(&batch, self.index, self.output.schema.clone())?;
        if selected.num_rows() > 0 {
            collector.collect(selected).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::state_table_owner::{
        EventOperation, EventStep, FusedEventProgram, PendingEnvelopes,
    };
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_schema::{Field, TimeUnit};
    use arroyo_state::live::{
        Ownership, StateNamespace,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
    };
    use std::collections::HashMap;

    #[test]
    fn capture_presence_does_not_depend_on_child_null_values() {
        let branch_schema = Arc::new(Schema::new(vec![Field::new(
            "nullable_value",
            DataType::Int64,
            true,
        )]));
        let present_all_null = RecordBatch::try_new(
            branch_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None]))],
        )
        .unwrap();
        let envelope = capture_envelope(
            &[Some(present_all_null), None],
            &[branch_schema.clone(), branch_schema.clone()],
            Arc::new(Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(TimestampNanosecondArray::from(vec![1])),
        )
        .unwrap();
        let present = extract_capture(&envelope, 0, branch_schema.clone()).unwrap();
        let absent = extract_capture(&envelope, 1, branch_schema).unwrap();
        assert_eq!(present.num_rows(), 1);
        assert!(present.column(0).is_null(0));
        assert_eq!(absent.num_rows(), 0);
    }

    #[test]
    fn lookup_result_matches_planned_left_then_right_schema() {
        let timestamp_field = Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("item", DataType::Utf8, false),
            timestamp_field.clone(),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("item", DataType::Utf8, false),
            timestamp_field.clone(),
            Field::new("quantity", DataType::Int64, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "quantity",
            DataType::Int64,
            false,
        )]));
        let input = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(TimestampNanosecondArray::from(vec![99])),
            ],
        )
        .unwrap();
        let target = RecordBatch::try_new(
            target_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap();
        let mut step = StateTableStep {
            table_identity: "table".into(),
            schema_identity: "schema".into(),
            partition_key: vec![0],
            event_scope_id: "source".into(),
            input_schema: ArroyoSchema::new_unkeyed(input_schema, 1),
            output_schema: ArroyoSchema::new_unkeyed(output_schema, 1),
            table_schema: target_schema,
            expression_schema: Arc::new(Schema::empty()),
            primary_key: vec![0],
            keys: vec![],
            clauses: vec![],
            lookup: Some(LookupKind::Left),
        };
        let hit = step.lookup_output(&input, Some(&target)).unwrap();
        let miss = step.lookup_output(&input, None).unwrap();
        assert_eq!(
            hit.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            hit.column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            99
        );
        assert!(miss.column(2).is_null(0));
        assert_eq!(miss.num_rows(), 1);
        step.output_schema = ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new("item", DataType::Utf8, false),
                Field::new("quantity", DataType::Int64, true),
                timestamp_field,
            ])),
            2,
        );
        let appended = step.lookup_output(&input, Some(&target)).unwrap();
        assert_eq!(
            appended
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            appended
                .column(2)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            99
        );
    }

    #[tokio::test]
    async fn serial_owner_reads_its_pending_writes_for_one_and_many_row_inputs() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("item", DataType::Utf8, true),
            Field::new("quantity", DataType::Int64, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("item", DataType::Utf8, false),
            Field::new("quantity", DataType::Int64, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new(
                "source",
                DataType::Struct(
                    vec![input_schema.field(0).clone(), input_schema.field(1).clone()].into(),
                ),
                false,
            ),
            Field::new("old", DataType::Struct(table_schema.fields().clone()), true),
            Field::new("new", DataType::Struct(table_schema.fields().clone()), true),
            Field::new("action", DataType::Utf8, false),
            input_schema.field(2).clone(),
        ]));
        let expression_schema = Arc::new(Schema::new(vec![
            input_schema.field(0).clone(),
            input_schema.field(1).clone(),
            input_schema.field(2).clone(),
            Field::new("item", DataType::Utf8, true),
            Field::new("quantity", DataType::Int64, true),
        ]));
        let source_item: Arc<dyn PhysicalExpr> = Arc::new(
            datafusion::physical_expr::expressions::Column::new("item", 0),
        );
        let source_quantity: Arc<dyn PhysicalExpr> = Arc::new(
            datafusion::physical_expr::expressions::Column::new("quantity", 1),
        );
        let step = StateTableStep {
            table_identity: "inventory".into(),
            schema_identity: "v1".into(),
            partition_key: vec![0],
            event_scope_id: "events".into(),
            input_schema: ArroyoSchema::new_unkeyed(input_schema.clone(), 2),
            output_schema: ArroyoSchema::new_unkeyed(output_schema.clone(), 4),
            table_schema: table_schema.clone(),
            expression_schema,
            primary_key: vec![0],
            keys: vec![source_item.clone()],
            clauses: vec![
                MutationClause {
                    matched: true,
                    predicate: None,
                    action: MutationAction::Update,
                    values: vec![(1, source_quantity.clone())],
                },
                MutationClause {
                    matched: false,
                    predicate: None,
                    action: MutationAction::Insert,
                    values: vec![(0, source_item), (1, source_quantity)],
                },
            ],
            lookup: None,
        };
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1 << 20,
            memtable_bytes: 1 << 20,
            queued_write_bytes: 1 << 20,
            decoded_value_bytes: 16 << 20,
            scan_page_bytes: 1 << 20,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let table = TypedTable::new(
            Arc::new(MemoryLiveState::bounded(resources.clone(), 1 << 20).unwrap()),
            StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: b"inventory".to_vec(),
            },
            TableDescriptor {
                table_identity: b"inventory".to_vec(),
                schema_identity: b"v1".to_vec(),
                schema: table_schema,
                primary_key: vec![0],
            },
            arroyo_state::live::typed_table::TableLimits {
                key_bytes: 1024,
                row_bytes: 8192,
                decoded_bytes: 8192,
                scope_bytes: 32768,
                scope_operations: 16,
                page_bytes: 32768,
                page_entries: 2,
            },
            resources,
        )
        .unwrap();
        let mut tables = HashMap::new();
        tables.insert("inventory".to_string(), table);
        let table = tables.get("inventory").unwrap();
        let mut program = FusedEventProgram {
            event_scope_id: "events".into(),
            steps: vec![EventStep {
                parent: None,
                operation: EventOperation::StateTable(Box::new(step)),
                captures: vec![0],
            }],
            capture_schemas: vec![output_schema],
            timestamp_field: input_schema.field(2).clone().into(),
            timestamp_index: 2,
            max_captured_event_bytes: 64 * 1024,
            max_working_event_bytes: 128 * 1024,
            concat_allowance: Arc::new(super::super::state_table_concat::ConcatAllowance::default()),
        };
        program.validate().unwrap();
        for values in [
            vec![(Some("a"), 1_i64)],
            vec![(Some("a"), 2), (None, 9), (Some("a"), 3)],
        ] {
            let input = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        values.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        values.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                    )),
                    Arc::new(TimestampNanosecondArray::from(
                        (0..values.len() as i64).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let mut scope = table.begin().await.unwrap();
            let mut actions = Vec::new();
            for row in 0..input.num_rows() {
                let envelope = program
                    .execute_event(&input.slice(row, 1), &tables, &mut scope)
                    .await
                    .unwrap();
                let extracted =
                    extract_capture(&envelope, 0, program.capture_schemas[0].clone()).unwrap();
                actions.push(
                    extracted
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0)
                        .to_owned(),
                );
            }
            scope.commit().await.unwrap();
            if values.len() == 1 {
                assert_eq!(actions, ["insert"]);
            } else {
                assert_eq!(actions, ["update", "none", "update"]);
            }
        }
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

    /// Parse SQL and decode exactly the fused worker plan emitted by the
    /// planner. These tests exercise MERGE, rather than invoking JSON UDFs.
    async fn planned_json_merge(expression: &str) -> (FusedEventProgram, SchemaRef) {
        use arroyo_rpc::grpc::api::FusedStateTableOperator;
        use prost::Message;
        let sql = format!(
            r#"
CREATE TABLE json_events (k TEXT NOT NULL, payload TEXT NOT NULL)
WITH (connector='single_file',path='/tmp/json-atomic-source.jsonl',format='json',type='source');
CREATE STATE TABLE json_rows (k TEXT PRIMARY KEY, payload TEXT) PARTITION BY k;
CREATE VIEW applied AS MERGE INTO json_rows AS target USING json_events AS source
ON target.k=source.k
WHEN MATCHED THEN UPDATE SET payload={expression}
WHEN NOT MATCHED THEN INSERT (k,payload) VALUES (source.k,{expression})
RETURNING source AS source,old AS old,new AS new,action AS action;
SELECT * FROM applied;
"#
        );
        let compiled = arroyo_planner::parse_and_get_program(
            &sql,
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let operator = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .map(|(operator, _)| operator)
            .find(|operator| {
                operator.operator_name == arroyo_datastream::logical::OperatorName::FusedStateTable
            })
            .expect("SQL MERGE must produce a fused worker owner");
        let config = FusedStateTableOperator::decode(operator.operator_config.as_slice()).unwrap();
        let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
        let limits = arroyo_rpc::config::TypedSqlStateConfig {
            key_bytes: 1024,
            row_bytes: 64 * 1024,
            decoded_bytes: 64 * 1024,
            scope_bytes: 256 * 1024,
            scope_operations: 16,
            page_bytes: 256 * 1024,
            page_entries: 2,
            max_working_event_bytes: 4 * 1024 * 1024,
            max_captured_event_bytes: 256 * 1024,
            max_pending_output_rows: 8,
            max_pending_output_bytes: 1024 * 1024,
            max_resident_bytes: 1024 * 1024,
        };
        (
            FusedEventProgram::decode(&config, &limits, &arroyo_planner::physical::new_registry())
                .unwrap(),
            input.schema,
        )
    }

    fn json_event(schema: &SchemaRef, payload: &str) -> RecordBatch {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "k" => Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                "payload" => Arc::new(StringArray::from(vec![payload])) as ArrayRef,
                "_timestamp" => Arc::new(TimestampNanosecondArray::from(vec![1])) as ArrayRef,
                other => panic!("unexpected planned source field {other}"),
            })
            .collect();
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    struct JsonBackendCleanup {
        root: std::path::PathBuf,
        backend: Arc<arroyo_state::live::rocks::RocksLiveState>,
    }

    async fn json_tables(
        program: &FusedEventProgram,
        rocks: bool,
    ) -> (
        Arc<HashMap<String, TypedTable>>,
        WorkerStateResources,
        Option<JsonBackendCleanup>,
    ) {
        use arroyo_state::live::{
            LiveStateBackend, lifecycle::RocksStateConfig, rocks::RocksLiveState,
        };
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1 << 20,
            memtable_bytes: 1 << 20,
            // Cover the table's configured worst-case headroom: queued writes
            // reserve 5 * scope_bytes plus operation overhead; scans reserve
            // 6 * page_bytes plus namespace/key/entry overhead.
            queued_write_bytes: 2 << 20,
            decoded_value_bytes: 16 << 20,
            scan_page_bytes: 2 << 20,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let root = rocks.then(|| {
            std::env::temp_dir().join(format!(
                "streamr-json-atomic-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
        });
        let rocks_backend = if let Some(root) = &root {
            Some(Arc::new(
                RocksLiveState::open(
                    RocksStateConfig {
                        root: root.clone(),
                        job_id: "json-atomic".into(),
                        operator_id: "owner".into(),
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
        let backend: Arc<dyn LiveStateBackend> = match &rocks_backend {
            Some(backend) => backend.clone(),
            None => Arc::new(MemoryLiveState::bounded(resources.clone(), 1 << 20).unwrap()),
        };
        let mut tables = HashMap::new();
        for step in &program.steps {
            if let EventOperation::StateTable(access) = &step.operation {
                tables
                    .entry(access.table_identity.clone())
                    .or_insert_with(|| {
                        TypedTable::new(
                            backend.clone(),
                            StateNamespace {
                                ownership: Ownership::PartitionLocal {
                                    subtask: 0,
                                    parallelism: 1,
                                },
                                table: access.table_identity.as_bytes().to_vec(),
                            },
                            access.descriptor(),
                            arroyo_state::live::typed_table::TableLimits {
                                key_bytes: 1024,
                                row_bytes: 64 * 1024,
                                decoded_bytes: 64 * 1024,
                                scope_bytes: 256 * 1024,
                                scope_operations: 16,
                                page_bytes: 256 * 1024,
                                page_entries: 2,
                            },
                            resources.clone(),
                        )
                        .unwrap()
                    });
            }
        }
        assert_eq!(tables.len(), 1);
        (
            Arc::new(tables),
            resources,
            root.zip(rocks_backend)
                .map(|(root, backend)| JsonBackendCleanup { root, backend }),
        )
    }

    fn assert_json_scope_permits_released(resources: &WorkerStateResources) {
        // Acquire the full configured pools: no event/scope permit can remain.
        let _decoded = resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .unwrap();
        let _queued = resources
            .try_queued_write(resources.config().queued_write_bytes)
            .unwrap();
    }

    async fn stored_json(table: &TypedTable) -> String {
        let key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let stored = table.get(&key).await.unwrap().unwrap();
        stored
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    }

    #[tokio::test]
    async fn planned_sql_json_merge_errors_discard_pending_writes_and_release_reservations() {
        let expansion = format!(
            "{{\"items\":[{}]}}",
            vec!["{}"; arroyo_planner::sql_json::path::MAX_PATH_ITEMS + 1].join(",")
        );
        let large = format!("{{\"value\":\"{}\"}}", "x".repeat(32 * 1024));
        let nested = format!("{{\"value\":\"{}\"}}", "x".repeat(128));
        for rocks in [false, true] {
            for (
                expression,
                initial,
                pending_value,
                bad,
                limit,
                diagnostic,
                expected_invocations,
            ) in [
                (
                    "JSON_QUERY(source.payload, '$')",
                    "{}",
                    "{\"pending\":true}",
                    large.as_str(),
                    128 * 1024,
                    "backing-byte allowance",
                    0,
                ),
                (
                    "JSON_QUERY(source.payload, '$.items[*]')",
                    "{\"items\":[{}]}",
                    "{\"items\":[{\"pending\":true}]}",
                    expansion.as_str(),
                    4 * 1024 * 1024,
                    "maximum",
                    1,
                ),
                (
                    "JSON_OBJECT('v' VALUE source.payload FORMAT JSON)",
                    "{}",
                    "{\"pending\":true}",
                    "invalid",
                    4 * 1024 * 1024,
                    "complete JSON value",
                    1,
                ),
                (
                    "JSON_OBJECT('v' VALUE source.payload)",
                    "seed",
                    "pending",
                    large.as_str(),
                    128 * 1024,
                    "backing-byte allowance",
                    0,
                ),
                (
                    "JSON_OBJECT('x' VALUE JSON_QUERY(source.payload, '$') FORMAT JSON, 'y' VALUE JSON_QUERY(source.payload, '$') FORMAT JSON)",
                    "{}",
                    "{\"pending\":true}",
                    nested.as_str(),
                    100 * 1024,
                    "backing-byte allowance",
                    1,
                ),
            ] {
                // 128 KiB admits the 32 KiB source and its planned projection,
                // while the conservative JSON charge still greatly exceeds it.
                // The nested case admits the first extraction but rejects the
                // second, proving nested calls consume one shared allowance.
                let (mut program, schema) = planned_json_merge(expression).await;
                program.max_working_event_bytes = limit;
                let (tables, resources, root) = json_tables(&program, rocks).await;
                let table = tables.values().next().unwrap();
                let initial = json_event(&schema, initial);
                let mut scope = table.begin().await.unwrap();
                program
                    .execute_event(&initial, &tables, &mut scope)
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                let before = stored_json(table).await;
                let mut scope = table.begin().await.unwrap();
                // A prior event already staged a write/output in this chunk.
                let envelope = program
                    .execute_event(&json_event(&schema, pending_value), &tables, &mut scope)
                    .await
                    .unwrap();
                let permit = resources
                    .try_decoded_value(envelope.get_array_memory_size() + 64)
                    .unwrap();
                let mut pending = PendingEnvelopes::new(8, 1024 * 1024).unwrap();
                pending.push_admitted(envelope, permit).unwrap();
                let invocations_before = program.concat_allowance.invocation_count();
                let error = program
                    .execute_event(&json_event(&schema, bad), &tables, &mut scope)
                    .await
                    .unwrap_err();
                assert!(
                    format!("{error:#}").contains(diagnostic),
                    "{expression}: {error:#}"
                );
                let invocations_after = program.concat_allowance.invocation_count();
                assert_eq!(
                    invocations_after - invocations_before,
                    expected_invocations,
                    "decoded MERGE must reserve before kernel execution; nested calls share one cumulative allowance"
                );
                drop(pending); // No pending envelope is emitted on failure.
                drop(scope);
                assert_eq!(stored_json(table).await, before);
                assert_json_scope_permits_released(&resources);
                // Retry through the same guarded physical plan proves that the
                // per-operation allowance was disarmed as well.
                let mut scope = table.begin().await.unwrap();
                program
                    .execute_event(&initial, &tables, &mut scope)
                    .await
                    .unwrap();
                scope.commit().await.unwrap();
                drop(tables);
                if let Some(JsonBackendCleanup { root, backend }) = root {
                    Arc::try_unwrap(backend)
                        .unwrap_or_else(|_| panic!("test retained live backend"))
                        .close_and_remove()
                        .await
                        .unwrap();
                    std::fs::remove_dir_all(root).unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn planned_sql_json_merge_cancellation_discards_uncommitted_output_and_state() {
        for rocks in [false, true] {
            let (mut program, schema) =
                planned_json_merge("JSON_OBJECT('v' VALUE source.payload FORMAT JSON)").await;
            let (tables, resources, root) = json_tables(&program, rocks).await;
            let table = tables.values().next().unwrap();
            let mut scope = table.begin().await.unwrap();
            program
                .execute_event(&json_event(&schema, "{}"), &tables, &mut scope)
                .await
                .unwrap();
            scope.commit().await.unwrap();
            let before = stored_json(table).await;
            let worker_tables = tables.clone();
            let worker_resources = resources.clone();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let table = worker_tables.values().next().unwrap();
                let mut scope = table.begin().await.unwrap();
                let envelope = program
                    .execute_event(
                        &json_event(&schema, "{\"changed\":true}"),
                        &worker_tables,
                        &mut scope,
                    )
                    .await
                    .unwrap();
                let permit = worker_resources
                    .try_decoded_value(envelope.get_array_memory_size() + 64)
                    .unwrap();
                let mut _pending = PendingEnvelopes::new(8, 1024 * 1024).unwrap();
                _pending.push_admitted(envelope, permit).unwrap();
                entered_tx.send(()).unwrap();
                // SQL kernels are synchronous: cancellation is observed at an
                // await boundary while the worker owns uncommitted envelopes.
                futures::future::pending::<()>().await;
            });
            entered_rx.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(stored_json(table).await, before);
            assert_json_scope_permits_released(&resources);
            drop(tables);
            if let Some(JsonBackendCleanup { root, backend }) = root {
                Arc::try_unwrap(backend)
                    .unwrap_or_else(|_| panic!("test retained live backend"))
                    .close_and_remove()
                    .await
                    .unwrap();
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}
