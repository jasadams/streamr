//! Context carried outside SQL rows for the private, one-response event route.
//! This module does not register a message variant or enable route dispatch.
use arrow_array::RecordBatch;

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

#[derive(Debug, Clone, PartialEq)]
pub struct EventResponse {
    pub context: EventContext,
    /// Native changes after aggregation; original images before aggregation.
    pub rows: Option<RecordBatch>,
    /// Native encoded full grouping keys, after WHERE and before FILTER.
    pub touched_keys: Option<RecordBatch>,
}

impl EventResponse {
    /// Retained row count before the existing queue-capacity saturation.
    pub fn row_cost(&self) -> usize {
        let rows = self.rows.as_ref().map_or(0, RecordBatch::num_rows);
        let keys = self.touched_keys.as_ref().map_or(0, RecordBatch::num_rows);
        rows.saturating_add(keys).max(1)
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
