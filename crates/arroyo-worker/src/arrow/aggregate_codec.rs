//! Versioned native aggregate state codec. Arrow IPC retains SQL scalar types;
//! the persisted value contains accumulator state and the last emitted row,
//! never application-defined profile or session state.
use anyhow::{Context, Result, bail, ensure};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema};
use datafusion::common::ScalarValue;
use std::{io::Cursor, sync::Arc};

const MAGIC: &[u8; 8] = b"STRAGG03";
const HEADER_BYTES: usize = 8 + 8 + 8 + 8 + 8 + 4 + 4;

#[derive(Debug)]
pub(crate) struct EncodedGroup {
    pub last_update_nanos: i64,
    pub generation: u64,
    pub next_ordinal: u64,
    /// Event-time deadline of the retained current result; unused when the
    /// operator has no event-time expiry.
    pub validity_deadline_nanos: i64,
    pub accumulator_state: Vec<ScalarValue>,
    pub last_emitted: Option<Vec<ScalarValue>>,
}

pub(crate) fn encode_group(group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
    ensure!(
        max_bytes >= HEADER_BYTES,
        "aggregate value limit is too small"
    );
    let state_len = u32::try_from(group.accumulator_state.len())?;
    let emitted_len = u32::try_from(group.last_emitted.as_ref().map_or(0, Vec::len))?;
    let values = group
        .accumulator_state
        .iter()
        .chain(group.last_emitted.iter().flatten());
    let mut columns = values
        .map(ScalarValue::to_array)
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    columns.insert(0, Arc::new(BooleanArray::from(vec![true])));
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Arc::new(Field::new(
                    format!("v{index}"),
                    column.data_type().clone(),
                    true,
                ))
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut body = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut body, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let total = HEADER_BYTES
        .checked_add(body.len())
        .context("aggregate value size overflow")?;
    ensure!(
        total <= max_bytes,
        "aggregate value exceeds configured limit"
    );
    let mut output = Vec::with_capacity(total);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&group.last_update_nanos.to_be_bytes());
    output.extend_from_slice(&group.generation.to_be_bytes());
    output.extend_from_slice(&group.next_ordinal.to_be_bytes());
    output.extend_from_slice(&group.validity_deadline_nanos.to_be_bytes());
    output.extend_from_slice(&state_len.to_be_bytes());
    output.extend_from_slice(&emitted_len.to_be_bytes());
    output.extend_from_slice(&body);
    Ok(output)
}

pub(crate) fn decode_group(
    bytes: &[u8],
    expected_state_types: &[arrow_schema::DataType],
    expected_output_types: &[arrow_schema::DataType],
    max_bytes: usize,
) -> Result<EncodedGroup> {
    ensure!(
        bytes.len() <= max_bytes,
        "aggregate value exceeds configured limit"
    );
    ensure!(
        bytes.len() >= HEADER_BYTES && &bytes[..8] == MAGIC,
        "invalid aggregate state codec"
    );
    let last_update_nanos = i64::from_be_bytes(bytes[8..16].try_into()?);
    let generation = u64::from_be_bytes(bytes[16..24].try_into()?);
    let next_ordinal = u64::from_be_bytes(bytes[24..32].try_into()?);
    let validity_deadline_nanos = i64::from_be_bytes(bytes[32..40].try_into()?);
    let state_len = u32::from_be_bytes(bytes[40..44].try_into()?) as usize;
    let emitted_len = u32::from_be_bytes(bytes[44..48].try_into()?) as usize;
    ensure!(
        state_len == expected_state_types.len(),
        "aggregate accumulator schema changed"
    );
    ensure!(
        emitted_len == 0 || emitted_len == expected_output_types.len(),
        "aggregate emitted-row schema changed"
    );
    let mut reader = StreamReader::try_new(Cursor::new(&bytes[HEADER_BYTES..]), None)?;
    let batch = reader
        .next()
        .transpose()?
        .context("aggregate state is missing an Arrow row")?;
    ensure!(
        batch.num_rows() == 1,
        "aggregate state must contain one Arrow row"
    );
    ensure!(
        batch.num_columns() == 1 + state_len + emitted_len,
        "aggregate state column count changed"
    );
    let types = expected_state_types
        .iter()
        .chain(expected_output_types.iter());
    ensure!(
        batch.column(0).data_type() == &arrow_schema::DataType::Boolean,
        "aggregate state sentinel changed"
    );
    for (column, expected) in batch.columns().iter().skip(1).zip(types) {
        ensure!(
            column.data_type() == expected,
            "aggregate state type changed"
        );
    }
    if reader.next().transpose()?.is_some() {
        bail!("aggregate state contains extra Arrow rows");
    }
    let values = batch
        .columns()
        .iter()
        .skip(1)
        .map(|column| ScalarValue::try_from_array(column, 0))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let (accumulator_state, emitted) = values.split_at(state_len);
    Ok(EncodedGroup {
        last_update_nanos,
        generation,
        next_ordinal,
        validity_deadline_nanos,
        accumulator_state: accumulator_state.to_vec(),
        last_emitted: (emitted_len > 0).then(|| emitted.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    #[test]
    fn typed_state_and_last_emitted_survive_a_fresh_decode() {
        let original = EncodedGroup {
            last_update_nanos: 1234,
            generation: 7,
            next_ordinal: 29,
            validity_deadline_nanos: 42_000,
            accumulator_state: vec![ScalarValue::Int64(Some(12))],
            last_emitted: Some(vec![
                ScalarValue::Utf8(Some("α".into())),
                ScalarValue::TimestampNanosecond(Some(42), None),
            ]),
        };
        let encoded = encode_group(&original, 8192).unwrap();
        let restored = decode_group(
            &encoded,
            &[DataType::Int64],
            &[
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            8192,
        )
        .unwrap();
        assert_eq!(restored.accumulator_state, original.accumulator_state);
        assert_eq!(restored.last_emitted, original.last_emitted);
        assert_eq!(restored.last_update_nanos, original.last_update_nanos);
        assert_eq!(restored.generation, original.generation);
        assert_eq!(restored.next_ordinal, original.next_ordinal);
        assert_eq!(
            restored.validity_deadline_nanos,
            original.validity_deadline_nanos
        );
        assert!(decode_group(&encoded, &[DataType::Utf8], &[], 8192).is_err());
    }

    #[test]
    fn empty_accumulator_state_keeps_one_arrow_row() {
        let state = EncodedGroup {
            last_update_nanos: 0,
            generation: 0,
            next_ordinal: 0,
            validity_deadline_nanos: 0,
            accumulator_state: vec![],
            last_emitted: None,
        };
        let bytes = encode_group(&state, 4096).unwrap();
        let decoded = decode_group(&bytes, &[], &[], 4096).unwrap();
        assert!(decoded.accumulator_state.is_empty());
        assert!(decoded.last_emitted.is_none());
    }
}
