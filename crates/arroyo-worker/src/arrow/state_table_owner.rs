//! Per-input-event execution of a planned acyclic state access sequence.
//! Graph lowering must supply topological steps from one source scope and
//! captured exit branches. It must reject unsupported graph lineages before
//! constructing this program.
use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use arrow_array::RecordBatch;
use arrow_schema::{Field, SchemaRef};
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
};

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
                                Ok(parse_physical_expr(
                                    &proto,
                                    registry,
                                    input.schema.as_ref(),
                                    &DefaultPhysicalExtensionCodec {},
                                )?)
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
                        EventOperation::Value {
                            executor: StatelessPhysicalExecutor::new(
                                &value.physical_plan,
                                registry,
                            )?,
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
                                batch.schema().as_ref() == output_schema.as_ref()
                                    && batch.num_rows() <= 1,
                                "state-table scalar transform changed schema or expanded one event"
                            );
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
    use super::*;
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::{DataType, Schema, TimeUnit};

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
