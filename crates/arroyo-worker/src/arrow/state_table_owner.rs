//! Per-input-event execution of a planned acyclic state access sequence.
//! Graph lowering must supply topological steps from one source scope and
//! captured exit branches. It must reject unsupported graph lineages before
//! constructing this program.
use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, Schema, SchemaRef};
use arroyo_operator::operator::Registry;
use arroyo_rpc::config::TypedSqlStateConfig;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api::{FusedStateTableOperator, ProjectionOperator, ValuePlanOperator};
use arroyo_state::live::resources::ResourcePermit;
use arroyo_state::live::typed_table::{TypedTable, WorkingScope};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use futures::StreamExt;
use prost::Message;

use super::{
    StatelessPhysicalExecutor,
    state_table::{StateTableStep, capture_envelope},
    state_table_concat::{ConcatAllowance, guard_expression, guard_plan},
};

// Physical expressions may prove an outer field non-null while the logical
// graph conservatively declares it nullable. Only that widening is admissible;
// nested types/fields, names, order and metadata retain the existing Arrow
// equality contract (including its treatment of dictionary attributes).
fn scalar_schema_compatible(physical: &Schema, declared: &Schema) -> bool {
    physical.metadata() == declared.metadata()
        && physical.fields().len() == declared.fields().len()
        && physical
            .fields()
            .iter()
            .zip(declared.fields())
            .all(|(actual, expected)| {
                if actual.is_nullable() == expected.is_nullable() {
                    return actual == expected;
                }
                if actual.is_nullable() {
                    return false;
                }
                let normalized = actual
                    .as_ref()
                    .clone()
                    .with_nullable(expected.is_nullable());
                normalized == **expected
            })
}

fn normalize_scalar_batch(batch: RecordBatch, declared: SchemaRef) -> Result<RecordBatch> {
    ensure!(
        scalar_schema_compatible(batch.schema().as_ref(), declared.as_ref()),
        "state-table scalar transform changed schema"
    );
    // Preserve the formerly accepted batch/schema (including dictionary
    // attributes ignored by Arrow equality) without reconstructing it.
    if batch.schema().as_ref() == declared.as_ref() {
        return Ok(batch);
    }
    // Reuse the owned column references and preserve explicit row count even
    // for zero-column batches. Do not use with_schema: its contains check
    // would add dictionary-attribute restrictions absent from Arrow equality.
    let (_, columns, row_count) = batch.into_parts();
    Ok(RecordBatch::try_new_with_options(
        declared,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )?)
}

pub(crate) enum EventOperation {
    Projection {
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    },
    /// Existing ArrowValue physical plans materialize intervening scalar CTE
    /// projections and filters. The graph lowerer must prove these are pure and
    /// row-preserving or row-dropping before admitting them to the event scope.
    Value {
        executor: StatelessPhysicalExecutor,
        output_schema: SchemaRef,
    },
    StateTable(Box<StateTableStep>),
}

/// Input is the original event when `parent` is None, otherwise the named
/// output of a preceding step. A missing parent row skips the dependent step.
pub(crate) struct EventStep {
    pub(crate) parent: Option<usize>,
    pub(crate) operation: EventOperation,
    pub(crate) captures: Vec<usize>,
}

pub(crate) struct FusedEventProgram {
    pub(crate) event_scope_id: String,
    pub(crate) steps: Vec<EventStep>,
    pub(crate) capture_schemas: Vec<SchemaRef>,
    pub(crate) timestamp_field: Arc<Field>,
    pub(crate) timestamp_index: usize,
    pub(crate) max_captured_event_bytes: usize,
    pub(crate) max_working_event_bytes: usize,
    pub(super) concat_allowance: Arc<ConcatAllowance>,
}

/// Buffers captured output while the shared working scope has pending writes.
/// Reads and no-action events still count, so a slow sink cannot make an
/// unbounded queue even when the storage overlay is empty.
pub(crate) struct PendingEnvelopes {
    rows: Vec<RecordBatch>,
    permits: Vec<ResourcePermit>,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
}

impl PendingEnvelopes {
    pub(crate) fn new(max_rows: usize, max_bytes: usize) -> Result<Self> {
        ensure!(max_rows > 0 && max_bytes > 0, "invalid capture queue limit");
        Ok(Self {
            rows: Vec::new(),
            permits: Vec::new(),
            bytes: 0,
            max_rows,
            max_bytes,
        })
    }

    pub(crate) fn push(&mut self, envelope: RecordBatch) -> Result<()> {
        ensure!(
            envelope.num_rows() == 1,
            "capture queue requires one complete event envelope"
        );
        let bytes = self
            .bytes
            .checked_add(envelope.get_array_memory_size())
            .and_then(|size| size.checked_add(64))
            .context("capture queue byte counter overflow")?;
        ensure!(
            self.rows.len() < self.max_rows && bytes <= self.max_bytes,
            "pending state-table output exceeds configured row/byte limit"
        );
        self.bytes = bytes;
        self.rows.push(envelope);
        Ok(())
    }

    pub(crate) fn push_admitted(
        &mut self,
        envelope: RecordBatch,
        permit: ResourcePermit,
    ) -> Result<()> {
        self.push(envelope)?;
        self.permits.push(permit);
        Ok(())
    }

    /// A caller uses the configured maximum envelope size to close a chunk at
    /// an event boundary, before any mutation for the next event is attempted.
    pub(crate) fn can_fit_worst_case(&self, max_event_bytes: usize) -> bool {
        self.rows.len() < self.max_rows
            && self
                .bytes
                .checked_add(max_event_bytes)
                .and_then(|bytes| bytes.checked_add(64))
                .is_some_and(|bytes| bytes <= self.max_bytes)
    }

    #[cfg(test)]
    pub(crate) fn into_rows(self) -> Vec<RecordBatch> {
        self.rows
    }

    pub(crate) fn into_admitted_rows(self) -> (Vec<RecordBatch>, Vec<ResourcePermit>) {
        (self.rows, self.permits)
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl FusedEventProgram {
    pub(crate) fn decode(
        config: &FusedStateTableOperator,
        limits: &TypedSqlStateConfig,
        registry: &Registry,
    ) -> Result<Self> {
        let input: ArroyoSchema = config
            .input_schema
            .clone()
            .context("fused state owner lacks input schema")?
            .try_into()?;
        let output: ArroyoSchema = config
            .output_schema
            .clone()
            .context("fused state owner lacks output schema")?
            .try_into()?;
        let captures = config
            .capture_schemas
            .iter()
            .cloned()
            .map(|proto| -> Result<SchemaRef> {
                let schema: ArroyoSchema = proto.try_into()?;
                Ok(schema.schema)
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            output.schema.fields().len() == captures.len() + 1
                && output.timestamp_index == captures.len()
                && input.timestamp_index < input.schema.fields().len(),
            "fused owner capture/timestamp schema mismatch"
        );
        for (index, capture) in captures.iter().enumerate() {
            let field = output.schema.field(index);
            ensure!(
                field.name() == &format!("capture_{index}")
                    && field.is_nullable()
                    && field.data_type()
                        == &arrow_schema::DataType::Struct(capture.fields().clone()),
                "fused owner capture branch schema mismatch"
            );
        }
        let timestamp_field = output.schema.fields()[output.timestamp_index].clone();
        ensure!(
            timestamp_field.as_ref() == input.schema.field(input.timestamp_index),
            "fused owner changed event timestamp field"
        );
        let concat_allowance = Arc::new(ConcatAllowance::default());
        let steps = config
            .steps
            .iter()
            .map(|step| {
                let output: ArroyoSchema = step
                    .output_schema
                    .clone()
                    .context("fused step lacks output schema")?
                    .try_into()?;
                let operation = match step.kind.as_str() {
                    "state_access" => EventOperation::StateTable(Box::new(StateTableStep::decode(
                        arroyo_rpc::grpc::api::StateTableOperator::decode(
                            step.operator_config.as_slice(),
                        )?,
                        registry,
                    )?)),
                    "projection" => {
                        let projection =
                            ProjectionOperator::decode(step.operator_config.as_slice())?;
                        let input: ArroyoSchema = projection
                            .input_schema
                            .context("fused projection lacks input schema")?
                            .try_into()?;
                        let expressions = projection
                            .exprs
                            .iter()
                            .map(|bytes| {
                                let proto = PhysicalExprNode::decode(bytes.as_slice())?;
                                let expression = parse_physical_expr(
                                    &proto,
                                    registry,
                                    input.schema.as_ref(),
                                    &DefaultPhysicalExtensionCodec {},
                                )?;
                                Ok(guard_expression(expression, &concat_allowance)?)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        ensure!(
                            expressions.len() == output.schema.fields().len(),
                            "fused projection expression count mismatch"
                        );
                        EventOperation::Projection {
                            expressions,
                            input_schema: input.schema,
                            output_schema: output.schema,
                        }
                    }
                    "value" => {
                        let value = ValuePlanOperator::decode(step.operator_config.as_slice())?;
                        let mut executor =
                            StatelessPhysicalExecutor::new(&value.physical_plan, registry)?;
                        executor.plan = guard_plan(executor.plan, &concat_allowance)?;
                        EventOperation::Value {
                            executor,
                            output_schema: output.schema,
                        }
                    }
                    _ => anyhow::bail!("unsupported fused state-table step kind"),
                };
                Ok(EventStep {
                    parent: step.parent_step.map(|index| index as usize),
                    operation,
                    captures: step.captures.iter().map(|index| *index as usize).collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let program = Self {
            event_scope_id: config.event_scope_id.clone(),
            steps,
            capture_schemas: captures,
            timestamp_field,
            timestamp_index: input.timestamp_index,
            max_captured_event_bytes: limits.max_captured_event_bytes,
            max_working_event_bytes: limits.max_working_event_bytes,
            concat_allowance,
        };
        program.validate()?;
        Ok(program)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.event_scope_id.is_empty()
                && !self.steps.is_empty()
                && self.max_captured_event_bytes > 0
                && self.max_working_event_bytes > 0,
            "state-table event program requires steps and positive byte limits"
        );
        let mut captured = vec![false; self.capture_schemas.len()];
        let mut state_accesses = 0usize;
        for (index, step) in self.steps.iter().enumerate() {
            ensure!(
                step.parent.is_none_or(|parent| parent < index),
                "state-table event program is not topologically ordered"
            );
            for &capture in &step.captures {
                let slot = captured
                    .get_mut(capture)
                    .context("state-table capture index out of range")?;
                ensure!(!*slot, "state-table capture is produced more than once");
                *slot = true;
            }
            if let EventOperation::StateTable(access) = &step.operation {
                state_accesses += 1;
                ensure!(
                    access.event_scope_id == self.event_scope_id,
                    "state-table access belongs to a different input event scope"
                );
            }
        }
        ensure!(
            state_accesses > 0 && captured.into_iter().all(|capture| capture),
            "state-table capture has no producer"
        );
        Ok(())
    }

    /// Execute every related operation for event N before event N+1. Pending
    /// writes stay in `scope` for read-after-write and may be committed as one
    /// backend batch for a bounded chunk of serial events. The caller retains
    /// the envelope until that chunk has committed, then emits it once.
    pub(crate) async fn execute_event(
        &mut self,
        event: &RecordBatch,
        tables: &HashMap<String, TypedTable>,
        scope: &mut WorkingScope<'_>,
    ) -> Result<RecordBatch> {
        ensure!(
            event.num_rows() == 1,
            "state-table owner requires one event row"
        );
        let timestamp = event
            .columns()
            .get(self.timestamp_index)
            .context("state-table event timestamp index out of range")?
            .clone();
        let mut results: Vec<Option<RecordBatch>> = Vec::with_capacity(self.steps.len());
        let mut captures = vec![None; self.capture_schemas.len()];
        let mut capture_bytes = 0usize;
        let mut working_bytes = event.get_array_memory_size();
        ensure!(
            working_bytes <= self.max_working_event_bytes,
            "state-table source event exceeds configured working byte limit"
        );
        for step in &mut self.steps {
            let input = match step.parent {
                Some(parent) => results[parent].as_ref(),
                None => Some(event),
            };
            // Nested CONCAT calls share this remaining backing-allocation
            // allowance. Hold it until the scalar stream/output is complete;
            // an error or cancelled future disarms it without retaining data.
            let _concat_scope = self
                .concat_allowance
                .begin(self.max_working_event_bytes - working_bytes)?;
            let mut output_charged = false;
            let output = match input {
                None => None,
                Some(input) => match &mut step.operation {
                    EventOperation::Projection {
                        expressions,
                        input_schema,
                        output_schema,
                    } => {
                        ensure!(
                            input.schema().as_ref() == input_schema.as_ref(),
                            "fused projection input schema changed"
                        );
                        let columns = expressions
                            .iter()
                            .map(|expression| expression.evaluate(input)?.into_array(1))
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        Some(RecordBatch::try_new(output_schema.clone(), columns)?)
                    }
                    EventOperation::Value {
                        executor,
                        output_schema,
                    } => {
                        let mut stream = executor.process_batch(input.clone()).await;
                        let mut one = None;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            working_bytes = working_bytes
                                .checked_add(batch.get_array_memory_size())
                                .context("state-table scalar stream size overflow")?;
                            ensure!(
                                working_bytes <= self.max_working_event_bytes,
                                "state-table scalar stream exceeds configured working byte limit"
                            );
                            ensure!(
                                batch.num_rows() <= 1,
                                "state-table scalar transform changed schema or expanded one event"
                            );
                            let batch = normalize_scalar_batch(batch, output_schema.clone())?;
                            if batch.num_rows() == 1 {
                                ensure!(
                                    one.is_none(),
                                    "state-table scalar transform expanded one event"
                                );
                                output_charged = true;
                                one = Some(batch);
                            }
                        }
                        one
                    }
                    EventOperation::StateTable(access) => {
                        let table = tables
                            .get(&access.table_identity)
                            .context("state-table owner has no registered target table")?;
                        access.execute(input, table, scope).await?
                    }
                },
            };
            if let Some(batch) = &output
                && !output_charged
            {
                working_bytes = working_bytes
                    .checked_add(batch.get_array_memory_size())
                    .context("state-table intermediate size overflow")?;
                ensure!(
                    working_bytes <= self.max_working_event_bytes,
                    "state-table event intermediates exceed configured working byte limit"
                );
            }
            for &index in &step.captures {
                if let Some(batch) = &output {
                    ensure!(
                        batch.schema().as_ref() == self.capture_schemas[index].as_ref(),
                        "state-table captured branch changed its planned schema"
                    );
                    capture_bytes = capture_bytes
                        .checked_add(batch.get_array_memory_size())
                        .context("state-table capture size overflow")?;
                    ensure!(
                        capture_bytes <= self.max_captured_event_bytes,
                        "state-table captured event exceeds configured byte limit"
                    );
                    captures[index] = Some(batch.clone());
                }
            }
            results.push(output);
        }
        let envelope = capture_envelope(
            &captures,
            &self.capture_schemas,
            self.timestamp_field.clone(),
            timestamp,
        )?;
        ensure!(
            envelope.get_array_memory_size() <= self.max_captured_event_bytes,
            "state-table event envelope exceeds configured captured byte limit"
        );
        Ok(envelope)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn scalar_schema_allows_only_outer_nullability_widening() {
        let child = Arc::new(Field::new("field", DataType::Utf8, true));
        let physical = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("changed_fields", DataType::List(child.clone()), false),
        ]);
        let declared = Schema::new(vec![
            physical.field(0).clone(),
            physical.field(1).clone().with_nullable(true),
        ]);
        assert!(scalar_schema_compatible(&physical, &physical));
        assert!(scalar_schema_compatible(&physical, &declared));
        assert!(!scalar_schema_compatible(&declared, &physical));
        let invalid_fields = [
            Field::new("changed_fields", DataType::Utf8, true),
            Field::new("renamed", DataType::List(child.clone()), true),
            Field::new(
                "changed_fields",
                DataType::List(Arc::new(Field::new("field", DataType::Utf8, false))),
                true,
            ),
            Field::new(
                "changed_fields",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            declared
                .field(1)
                .clone()
                .with_metadata(HashMap::from([("origin".into(), "different".into())])),
        ];
        for field in invalid_fields {
            let other = Schema::new(vec![physical.field(0).clone(), field]);
            assert!(!scalar_schema_compatible(&physical, &other), "{other:?}");
        }
        assert!(!scalar_schema_compatible(
            &physical,
            &Schema::new(vec![declared.field(1).clone(), declared.field(0).clone()])
        ));
        assert!(!scalar_schema_compatible(
            &physical,
            &Schema::new(vec![declared.field(0).clone()])
        ));
        assert!(!scalar_schema_compatible(
            &physical,
            &declared
                .clone()
                .with_metadata(HashMap::from([("origin".into(), "different".into())]))
        ));
        let dictionary = Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        );
        let ordered_dictionary = dictionary.clone().with_dict_is_ordered(true);
        let physical_dictionary = Schema::new(vec![dictionary]);
        let declared_dictionary = Schema::new(vec![ordered_dictionary]);
        assert_eq!(physical_dictionary, declared_dictionary);
        // Field equality deliberately ignores dictionary ordering attributes.
        // Preserve that established acceptance while widening outer nullability.
        assert!(scalar_schema_compatible(
            &physical_dictionary,
            &declared_dictionary
        ));
    }

    use super::*;
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::{DataType, TimeUnit};

    #[test]
    fn scalar_batch_relabels_populated_empty_and_zero_row_lists_without_copying() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        for values in [vec!["n", "value"], vec![]] {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
            let array: arrow_array::ArrayRef = Arc::new(builder.finish());
            let physical = Arc::new(Schema::new(vec![Field::new(
                "changed_fields",
                array.data_type().clone(),
                false,
            )]));
            let declared = Arc::new(Schema::new(vec![
                physical.field(0).clone().with_nullable(true),
            ]));
            let batch = RecordBatch::try_new(physical.clone(), vec![array.clone()]).unwrap();
            let normalized = normalize_scalar_batch(batch.clone(), declared.clone()).unwrap();
            assert_eq!(normalized.schema(), declared);
            assert_eq!(normalized.num_rows(), 1);
            assert!(Arc::ptr_eq(normalized.column(0), &array));
            assert_eq!(normalized.column(0).to_data(), batch.column(0).to_data());
            assert!(normalize_scalar_batch(normalized, physical.clone()).is_err());
            let empty = batch.slice(0, 0);
            let empty_column = empty.column(0).clone();
            let normalized = normalize_scalar_batch(empty, declared).unwrap();
            assert_eq!(normalized.num_rows(), 0);
            assert!(Arc::ptr_eq(normalized.column(0), &empty_column));
            let wrong_type = Arc::new(Schema::new(vec![Field::new(
                "changed_fields",
                DataType::Utf8,
                true,
            )]));
            assert!(normalize_scalar_batch(batch, wrong_type).is_err());
        }
    }

    #[test]
    fn scalar_batch_relabel_preserves_existing_dictionary_attribute_equality() {
        use arrow_array::{builder::StringDictionaryBuilder, types::Int32Type};
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("value").unwrap();
        let array: arrow_array::ArrayRef = Arc::new(builder.finish());
        let physical = Arc::new(Schema::new(vec![Field::new(
            "dictionary",
            array.data_type().clone(),
            false,
        )]));
        let declared = Arc::new(Schema::new(vec![
            physical.field(0).clone().with_dict_is_ordered(true),
        ]));
        assert_eq!(physical, declared);
        let batch = RecordBatch::try_new(physical, vec![array.clone()]).unwrap();
        let normalized = normalize_scalar_batch(batch, declared.clone()).unwrap();
        assert_eq!(normalized.schema().field(0).dict_is_ordered(), Some(false));
        assert!(Arc::ptr_eq(normalized.column(0), &array));
        assert_eq!(normalized.num_rows(), 1);
    }

    #[test]
    fn scalar_batch_relabel_preserves_zero_column_row_count() {
        for rows in [0, 1] {
            let schema = Arc::new(Schema::empty());
            let batch = RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            let normalized = normalize_scalar_batch(batch, schema.clone()).unwrap();
            assert_eq!(normalized.schema(), schema);
            assert_eq!(normalized.num_rows(), rows);
            assert_eq!(normalized.num_columns(), 0);
        }
    }

    #[test]
    fn no_action_events_still_fill_the_pending_capture_queue() {
        let branch = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let envelope = capture_envelope(
            &[None],
            &[branch],
            Arc::new(Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(TimestampNanosecondArray::from(vec![1])),
        )
        .unwrap();
        let mut pending = PendingEnvelopes::new(1, 4096).unwrap();
        pending.push(envelope.clone()).unwrap();
        assert!(!pending.can_fit_worst_case(1));
        assert!(pending.push(envelope).is_err());
        assert_eq!(pending.into_rows().len(), 1);
    }
}
