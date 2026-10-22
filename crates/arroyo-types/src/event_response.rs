//! Context carried outside SQL rows for the private, one-response event route.
//! This module does not register a message variant or enable route dispatch.
use crate::event_edge::{EventEdgeSpec, EventLayout, native_body_bound, native_schema_shape};
use arrow::buffer::Buffer;
use arrow_array::RecordBatch;
use std::sync::Arc;

pub const EVENT_RESPONSE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventContext {
    pub version: u32,
    pub root_operator: u32,
    pub root_subtask: u32,
    pub attempt_nonce: [u8; 16],
    pub sequence: u64,
    pub trigger_nanos: i64,
}

#[derive(Debug, Clone)]
pub struct EventResponse {
    pub context: EventContext,
    /// Native changes after aggregation; original images before aggregation.
    pub rows: Option<RecordBatch>,
    /// Native encoded full grouping keys, after WHERE and before FILTER.
    pub touched_keys: Option<RecordBatch>,
    /// Opaque bounded ownership proof; copying a handle does not authorize new
    /// rows/context/schema. The proof is rechecked on every forwarding use.
    pub native_body: Option<Arc<NativeBodyProof>>,
}
impl PartialEq for EventResponse {
    fn eq(&self, other: &Self) -> bool {
        self.context == other.context
            && self.rows == other.rows
            && self.touched_keys == other.touched_keys
    }
}
pub struct NativeBodyProof {
    context: EventContext,
    spec: EventEdgeSpec,
    rows: RecordBatch,
    body: Buffer,
}
impl std::fmt::Debug for NativeBodyProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBodyProof")
            .field("context", &self.context)
            .field("rows", &self.rows.num_rows())
            .field("body_bytes", &self.body.len())
            .finish_non_exhaustive()
    }
}
impl NativeBodyProof {
    fn matches(&self, spec: &EventEdgeSpec, response: &EventResponse) -> bool {
        self.context == response.context
            && self.spec == *spec
            && response.rows.as_ref().is_some_and(|rows| {
                rows.schema() == self.rows.schema()
                    && rows.num_rows() == self.rows.num_rows()
                    && rows.num_columns() == self.rows.num_columns()
                    && rows
                        .columns()
                        .iter()
                        .zip(self.rows.columns())
                        .all(|(a, b)| Arc::ptr_eq(a, b))
            })
    }
}

impl EventResponse {
    /// Checked decoded-body representation, not an assertion that a newly
    /// allocated native output passed original sender ownership admission.
    /// All buffers must actually share this exact admitted body allocation.
    pub fn with_native_body(mut self, spec: &EventEdgeSpec, body: Buffer) -> Result<Self, String> {
        self.native_body = None;
        spec.validate_shape(&self)?;
        let EventLayout::Native {
            owner_output_bytes,
            max_rows,
            ..
        } = spec.layout
        else {
            return Err("native body proof requires native layout".into());
        };
        let rows = self
            .rows
            .as_ref()
            .ok_or("native body proof requires rows")?;
        let (nodes, buffers) = native_schema_shape(&spec.schema)?;
        let bound = native_body_bound(owner_output_bytes, max_rows, nodes, buffers)?;
        if body.ptr_offset() != 0 || body.capacity() != body.len() || body.len() > bound {
            return Err("native body allocation exceeds registered proof".into());
        }
        fn check(data: &arrow::array::ArrayData, body: &Buffer) -> Result<(), String> {
            data.validate_full().map_err(|e| e.to_string())?;
            let check_buffer = |buffer: &Buffer| -> Result<(), String> {
                if buffer.data_ptr() != body.data_ptr()
                    || buffer
                        .ptr_offset()
                        .checked_add(buffer.len())
                        .is_none_or(|end| end > body.len())
                {
                    return Err("native row buffer is not owned by proved body".into());
                }
                Ok(())
            };
            for buffer in data.buffers() {
                check_buffer(buffer)?;
            }
            if let Some(nulls) = data.nulls() {
                check_buffer(nulls.buffer())?;
            }
            for child in data.child_data() {
                check(child, body)?;
            }
            Ok(())
        }
        for column in rows.columns() {
            check(&column.to_data(), &body)?;
        }
        self.native_body = Some(Arc::new(NativeBodyProof {
            context: self.context,
            spec: spec.clone(),
            rows: rows.clone(),
            body,
        }));
        Ok(self)
    }
    pub fn is_native_forward(&self, spec: &EventEdgeSpec) -> bool {
        self.native_body
            .as_ref()
            .is_some_and(|proof| proof.matches(spec, self))
    }
    /// Check a physical column forwarding/subset/reorder/contiguous-slice map.
    /// Values are not compared: exact immutable Arrow layouts must match a
    /// slice of one original column, including every child/null/offset buffer.
    pub fn proves_native_projection(&self, next: &EventEdgeSpec, rows: &RecordBatch) -> bool {
        let Some(proof) = &self.native_body else {
            return false;
        };
        if !proof.matches(&proof.spec, self)
            || next.root_operator != self.context.root_operator
            || next.root_subtask != self.context.root_subtask
            || rows.schema() != next.schema
        {
            return false;
        }
        fn same_buffer(a: &Buffer, b: &Buffer) -> bool {
            a.data_ptr() == b.data_ptr() && a.ptr_offset() == b.ptr_offset() && a.len() == b.len()
        }
        fn same_data(a: &arrow::array::ArrayData, b: &arrow::array::ArrayData) -> bool {
            a.data_type() == b.data_type()
                && a.len() == b.len()
                && a.offset() == b.offset()
                && a.buffers().len() == b.buffers().len()
                && a.buffers()
                    .iter()
                    .zip(b.buffers())
                    .all(|(a, b)| same_buffer(a, b))
                && (match (a.nulls(), b.nulls()) {
                    (None, None) => true,
                    (Some(a), Some(b)) => {
                        a.inner().offset() == b.inner().offset()
                            && a.len() == b.len()
                            && same_buffer(a.buffer(), b.buffer())
                    }
                    _ => false,
                })
                && a.child_data().len() == b.child_data().len()
                && a.child_data()
                    .iter()
                    .zip(b.child_data())
                    .all(|(a, b)| same_data(a, b))
        }
        rows.columns().iter().all(|column| {
            proof.rows.columns().iter().any(|original| {
                if column.len() > original.len() || column.data_type() != original.data_type() {
                    return false;
                }
                (0..=original.len() - column.len()).any(|offset| {
                    same_data(
                        &column.to_data(),
                        &original.slice(offset, column.len()).to_data(),
                    )
                })
            })
        })
    }
    /// First try ordinary owner admission; otherwise reuse only a checked
    /// immutable projection whose ACTUAL retained body fits the new stage.
    pub fn try_rebind_native_rows(
        &self,
        next: &EventEdgeSpec,
        rows: Option<RecordBatch>,
    ) -> Result<Self, String> {
        let result = Self {
            context: self.context,
            rows,
            touched_keys: self.touched_keys.clone(),
            native_body: None,
        };
        if next.validate(&result).is_ok() {
            return Ok(result);
        }
        next.validate_shape(&result)?;
        let rows = result
            .rows
            .as_ref()
            .ok_or("native row rebind requires rows")?;
        if !self.proves_native_projection(next, rows) {
            return Err("native row change requires original owner admission".into());
        }
        let body = self
            .native_body
            .as_ref()
            .ok_or("missing native body proof")?
            .body
            .clone();
        result.with_native_body(next, body)
    }
    pub fn discard_invalid_body_proof(&mut self, spec: &EventEdgeSpec) {
        if !self.is_native_forward(spec) {
            self.native_body = None;
        }
    }
    /// Retained row count before the existing queue-capacity saturation.
    pub fn row_cost(&self) -> usize {
        let rows = self.rows.as_ref().map_or(0, RecordBatch::num_rows);
        let keys = self.touched_keys.as_ref().map_or(0, RecordBatch::num_rows);
        rows.saturating_add(keys).max(1)
    }

    /// Retained Arrow bytes for the existing diagnostic gauge, not admission.
    pub fn retained_bytes(&self) -> usize {
        self.native_body
            .as_ref()
            .filter(|proof| proof.matches(&proof.spec, self))
            .map_or_else(
                || {
                    self.rows
                        .as_ref()
                        .map_or(0, RecordBatch::get_array_memory_size)
                },
                |proof| proof.body.len(),
            )
            .saturating_add(
                self.touched_keys
                    .as_ref()
                    .map_or(0, RecordBatch::get_array_memory_size),
            )
            .saturating_add(std::mem::size_of::<EventContext>())
    }

    /// Same saturated row admission as the existing queue, charging emptiness.
    /// Key rows count as retained rows too; this adds no byte-admission pool.
    pub fn queue_cost(&self, queue_capacity: u32) -> u32 {
        self.row_cost().min(queue_capacity as usize) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, UInt64Array};
    use std::sync::Arc;

    fn response(rows: usize, keys: usize) -> EventResponse {
        let batch = |len| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)])),
                vec![Arc::new(UInt64Array::from_iter_values(0..len as u64)) as ArrayRef],
            )
            .unwrap()
        };
        EventResponse {
            native_body: None,
            context: EventContext {
                version: EVENT_RESPONSE_VERSION,
                root_operator: 1,
                root_subtask: 0,
                attempt_nonce: [7; 16],
                sequence: 1,
                trigger_nanos: -5,
            },
            rows: (rows != 0).then(|| batch(rows)),
            touched_keys: (keys != 0).then(|| batch(keys)),
        }
    }
    #[test]
    fn event_response_queue_cost_charges_empty_and_all_retained_rows() {
        assert_eq!(response(0, 0).row_cost(), 1);
        assert_eq!(response(0, 0).queue_cost(8), 1);
        assert_eq!(response(4, 2).queue_cost(8), 6);
        assert_eq!(response(4, 2).queue_cost(1), 1);
        assert_eq!(response(0, 2).queue_cost(8), 2);
        assert_eq!(response(0, 0).queue_cost(0), 0);
    }
}
