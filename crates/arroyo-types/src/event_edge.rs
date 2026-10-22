//! Private response-route row/layout contract. No byte pool or scheduler.
use crate::event_response::{EVENT_RESPONSE_VERSION, EventResponse};
use arrow::array::{Array, BinaryArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventLayout {
    Raw,
    Native {
        max_rows: usize,
        owner_output_bytes: usize,
        encoded_key_bytes: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEdgeSpec {
    pub root_operator: u32,
    pub root_subtask: u32,
    pub schema: SchemaRef,
    pub layout: EventLayout,
}
impl EventEdgeSpec {
    pub fn keys_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "encoded_group_key",
            DataType::Binary,
            false,
        )]))
    }
    pub fn validate(&self, response: &EventResponse) -> Result<(), String> {
        self.validate_shape(response)?;
        if let (
            Some(rows),
            EventLayout::Native {
                owner_output_bytes, ..
            },
        ) = (&response.rows, &self.layout)
            && !response.is_native_forward(self)
            && rows.get_array_memory_size() > *owner_output_bytes
        {
            return Err("native sender exceeds its existing output allowance".into());
        }
        Ok(())
    }
    pub fn validate_shape(&self, response: &EventResponse) -> Result<(), String> {
        if response.context.version != EVENT_RESPONSE_VERSION
            || response.context.root_operator != self.root_operator
            || response.context.root_subtask != self.root_subtask
        {
            return Err("response version/root mismatch".into());
        }
        let max_rows = match self.layout {
            EventLayout::Raw => 2,
            EventLayout::Native { max_rows, .. } if (1..=4).contains(&max_rows) => max_rows,
            _ => return Err("invalid native response row count".into()),
        };
        if let Some(rows) = &response.rows
            && (rows.schema() != self.schema || rows.num_rows() == 0 || rows.num_rows() > max_rows)
        {
            return Err("response rows differ from registered schema/count".into());
        }
        if let Some(keys) = &response.touched_keys {
            let EventLayout::Native {
                encoded_key_bytes, ..
            } = self.layout
            else {
                return Err("raw response carries native keys".into());
            };
            if keys.schema() != Self::keys_schema() || keys.num_rows() == 0 || keys.num_rows() > 2 {
                return Err("response keys differ from registered schema/count".into());
            }
            let keys = keys
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "invalid touched key array".to_owned())?;
            if keys.null_count() != 0
                || !keys
                    .iter()
                    .all(|key| key.is_some_and(|key| key.len() <= encoded_key_bytes))
            {
                return Err("touched key exceeds existing key allowance".into());
            }
        }
        Ok(())
    }
}

/// The same canonical native body bound used by the codec and body provenance.
/// Owner admission stays unchanged; these terms cover generated validity/padding.
pub fn native_body_bound(
    owner: usize,
    rows: usize,
    nodes: usize,
    buffers: usize,
) -> Result<usize, String> {
    let bitmap = rows.checked_add(7).ok_or("native body size overflow")? / 8;
    owner
        .checked_add(
            nodes
                .checked_mul(bitmap)
                .ok_or("native body size overflow")?,
        )
        .and_then(|n| {
            buffers
                .checked_mul(NATIVE_IPC_ALIGNMENT - 1)
                .and_then(|padding| n.checked_add(padding))
        })
        .ok_or_else(|| "native body size overflow".into())
}
pub fn native_schema_shape(schema: &Schema) -> Result<(usize, usize), String> {
    fn count(ty: &DataType) -> Result<(usize, usize), String> {
        match ty {
            DataType::Boolean
            | DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _) => Ok((1, 2)),
            DataType::FixedSizeBinary(width) if *width > 0 => Ok((1, 2)),
            DataType::Utf8 | DataType::Binary => Ok((1, 3)),
            DataType::Struct(fields) => {
                let mut shape = (1usize, 1usize);
                for field in fields {
                    let (nodes, buffers) = count(field.data_type())?;
                    shape.0 = shape
                        .0
                        .checked_add(nodes)
                        .ok_or("native body size overflow")?;
                    shape.1 = shape
                        .1
                        .checked_add(buffers)
                        .ok_or("native body size overflow")?;
                }
                Ok(shape)
            }
            _ => Err("unproved native body layout".into()),
        }
    }
    let mut shape = (0usize, 0usize);
    for field in schema.fields() {
        let (nodes, buffers) = count(field.data_type())?;
        shape.0 = shape
            .0
            .checked_add(nodes)
            .ok_or("native body size overflow")?;
        shape.1 = shape
            .1
            .checked_add(buffers)
            .ok_or("native body size overflow")?;
    }
    Ok(shape)
}

pub const NATIVE_IPC_ALIGNMENT: usize = 8;
/// Exact canonical writer body length for the proved fixed/Utf8/Binary/Struct
/// layouts. Borrow values/offsets only; no payload allocation before admission.
pub fn native_canonical_body_len(batch: &arrow_array::RecordBatch) -> Result<usize, String> {
    use arrow::array::{Array, BinaryArray, StringArray, StructArray};
    fn push(total: &mut usize, bytes: usize) -> Result<(), String> {
        *total = total
            .checked_add(bytes)
            .and_then(|n| n.checked_add(NATIVE_IPC_ALIGNMENT - 1))
            .map(|n| n & !(NATIVE_IPC_ALIGNMENT - 1))
            .ok_or("native body size overflow")?;
        Ok(())
    }
    fn array(total: &mut usize, value: &dyn Array) -> Result<(), String> {
        push(
            total,
            value
                .len()
                .checked_add(7)
                .ok_or("native body size overflow")?
                / 8,
        )?;
        match value.data_type() {
            DataType::Boolean => push(
                total,
                value
                    .len()
                    .checked_add(7)
                    .ok_or("native body size overflow")?
                    / 8,
            )?,
            DataType::Int8 | DataType::UInt8 => push(total, value.len())?,
            DataType::Int16 | DataType::UInt16 => push(
                total,
                value
                    .len()
                    .checked_mul(2)
                    .ok_or("native body size overflow")?,
            )?,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => push(
                total,
                value
                    .len()
                    .checked_mul(4)
                    .ok_or("native body size overflow")?,
            )?,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => {
                push(
                    total,
                    value
                        .len()
                        .checked_mul(8)
                        .ok_or("native body size overflow")?,
                )?
            }
            DataType::FixedSizeBinary(width) if *width > 0 => push(
                total,
                value
                    .len()
                    .checked_mul(usize::try_from(*width).map_err(|e| e.to_string())?)
                    .ok_or("native body size overflow")?,
            )?,
            DataType::Utf8 | DataType::Binary => {
                let offsets = if let Some(strings) = value.as_any().downcast_ref::<StringArray>() {
                    strings.value_offsets()
                } else {
                    value
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or("invalid native bytes")?
                        .value_offsets()
                };
                push(
                    total,
                    value
                        .len()
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(4))
                        .ok_or("native body size overflow")?,
                )?;
                push(
                    total,
                    usize::try_from(
                        offsets[value.len()]
                            .checked_sub(offsets[0])
                            .ok_or("invalid native offsets")?,
                    )
                    .map_err(|e| e.to_string())?,
                )?;
            }
            DataType::Struct(_) => {
                let values = value
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or("invalid native struct")?;
                for child in values.columns() {
                    array(total, child.as_ref())?;
                }
            }
            _ => return Err("unproved native body layout".into()),
        }
        Ok(())
    }
    let mut total = 0;
    for column in batch.columns() {
        array(&mut total, column.as_ref())?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_response::EventContext;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow_array::RecordBatch;
    fn empty() -> EventResponse {
        EventResponse {
            native_body: None,
            context: EventContext {
                version: EVENT_RESPONSE_VERSION,
                root_operator: 7,
                root_subtask: 0,
                attempt_nonce: [9; 16],
                sequence: 1,
                trigger_nanos: 5,
            },
            rows: None,
            touched_keys: None,
        }
    }
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }
    #[test]
    fn event_transport_local_descriptor_rejects_wrong_root_schema_and_counts() {
        let spec = EventEdgeSpec {
            root_operator: 7,
            root_subtask: 0,
            schema: schema(),
            layout: EventLayout::Raw,
        };
        spec.validate(&empty()).unwrap();
        let mut bad = empty();
        bad.context.root_operator = 8;
        assert_eq!(
            spec.validate(&bad).unwrap_err(),
            "response version/root mismatch"
        );
        for rows in [0, 3] {
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef],
            )
            .unwrap();
            assert_eq!(
                spec.validate(&EventResponse {
                    rows: Some(batch),
                    ..empty()
                })
                .unwrap_err(),
                "response rows differ from registered schema/count"
            );
        }
        let wrong = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(wrong, vec![Arc::new(Int64Array::from(vec![None]))]).unwrap();
        assert!(
            spec.validate(&EventResponse {
                rows: Some(batch),
                ..empty()
            })
            .is_err()
        );
    }
    #[test]
    fn event_transport_native_descriptor_preserves_null_and_existing_owner_key_limits() {
        let row =
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![None]))]).unwrap();
        let key = |bytes: &[u8]| {
            RecordBatch::try_new(
                EventEdgeSpec::keys_schema(),
                vec![Arc::new(BinaryArray::from_vec(vec![bytes])) as ArrayRef],
            )
            .unwrap()
        };
        let bytes = row.get_array_memory_size();
        let mut spec = EventEdgeSpec {
            root_operator: 7,
            root_subtask: 0,
            schema: schema(),
            layout: EventLayout::Native {
                max_rows: 4,
                owner_output_bytes: bytes,
                encoded_key_bytes: 1,
            },
        };
        let mut response = EventResponse {
            rows: Some(row),
            touched_keys: Some(key(b"k")),
            ..empty()
        };
        spec.validate(&response).unwrap();
        response.touched_keys = Some(key(b"xx"));
        assert_eq!(
            spec.validate(&response).unwrap_err(),
            "touched key exceeds existing key allowance"
        );
        response.touched_keys = None;
        spec.layout = EventLayout::Native {
            max_rows: 4,
            owner_output_bytes: bytes - 1,
            encoded_key_bytes: 1,
        };
        assert_eq!(
            spec.validate(&response).unwrap_err(),
            "native sender exceeds its existing output allowance"
        );
    }
}
