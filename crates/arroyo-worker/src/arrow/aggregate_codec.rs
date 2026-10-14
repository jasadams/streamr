//! Versioned native aggregate state codec. Arrow IPC retains SQL scalar types;
//! the persisted value contains accumulator state and the last emitted row,
//! never application-defined profile or session state.
use anyhow::{Context, Result, bail, ensure};
use arrow::buffer::Buffer;
use arrow::ipc::{
    reader::{read_dictionary, read_record_batch},
    writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"STRAGG03";
const HEADER_BYTES: usize = 8 + 8 + 8 + 8 + 4 + 4 + 32 + 4;

#[derive(Debug)]
pub(crate) struct EncodedGroup {
    pub last_update_nanos: i64,
    pub generation: u64,
    pub next_ordinal: u64,
    pub accumulator_state: Vec<ScalarValue>,
    pub last_emitted: Option<Vec<ScalarValue>>,
}

/// Schemas are operator-owned; each stored value contains only their identity
/// and Arrow dictionary/record-batch messages, not another schema IPC stream.
pub(crate) struct GroupCodec {
    state_len: usize,
    output_len: usize,
    without_emitted: GroupSchema,
    with_emitted: GroupSchema,
}

struct GroupSchema {
    schema: Arc<Schema>,
    identity: [u8; 32],
    dictionary_count: usize,
}

impl GroupSchema {
    fn new(types: impl Iterator<Item = DataType>) -> Result<Self> {
        let fields = std::iter::once(DataType::Boolean)
            .chain(types)
            .enumerate()
            .map(|(index, data_type)| Field::new(format!("v{index}"), data_type, true))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields);
        let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        );
        let identity = Sha256::digest(&encoded.ipc_message).into();
        // Use the assigned dictionary IDs from Arrow's canonical schema, both
        // for independent decoding and for matching nested dictionary layouts.
        let message = arrow::ipc::root_as_message(&encoded.ipc_message)
            .map_err(|error| anyhow::anyhow!("invalid generated aggregate schema: {error}"))?;
        let schema = Arc::new(arrow::ipc::convert::fb_to_schema(
            message
                .header_as_schema()
                .context("missing aggregate schema")?,
        ));
        let dictionary_count = schema
            .flattened_fields()
            .iter()
            .filter(|field| matches!(field.data_type(), DataType::Dictionary(_, _)))
            .count();
        Ok(Self {
            schema,
            identity,
            dictionary_count,
        })
    }
}

impl GroupCodec {
    pub(crate) fn new(state_types: &[DataType], output_types: &[DataType]) -> Result<Self> {
        Ok(Self {
            state_len: state_types.len(),
            output_len: output_types.len(),
            without_emitted: GroupSchema::new(state_types.iter().cloned())?,
            with_emitted: GroupSchema::new(state_types.iter().chain(output_types).cloned())?,
        })
    }

    pub(crate) fn encode(&self, group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
        ensure!(
            max_bytes >= HEADER_BYTES,
            "aggregate value limit is too small"
        );
        ensure!(
            group.accumulator_state.len() == self.state_len,
            "aggregate accumulator schema changed"
        );
        let emitted_len = group.last_emitted.as_ref().map_or(0, Vec::len);
        ensure!(
            emitted_len == 0 || emitted_len == self.output_len,
            "aggregate emitted-row schema changed"
        );
        let layout = if emitted_len == 0 {
            &self.without_emitted
        } else {
            &self.with_emitted
        };
        let mut columns = group
            .accumulator_state
            .iter()
            .chain(group.last_emitted.iter().flatten())
            .map(ScalarValue::to_array)
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        columns.insert(0, Arc::new(BooleanArray::from(vec![true])));
        let batch = RecordBatch::try_new(layout.schema.clone(), columns)?;
        let generator = IpcDataGenerator::default();
        let options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);
        if layout.dictionary_count > 0 {
            generator.schema_to_bytes_with_dictionary_tracker(
                &layout.schema,
                &mut tracker,
                &options,
            );
        }
        let (dictionaries, batch) = generator.encoded_batch(&batch, &mut tracker, &options)?;
        let messages = dictionaries
            .into_iter()
            .chain(std::iter::once(batch))
            .collect::<Vec<_>>();
        let total = messages.iter().try_fold(HEADER_BYTES, |total, message| {
            total
                .checked_add(8)
                .and_then(|n| n.checked_add(message.ipc_message.len()))
                .and_then(|n| n.checked_add(message.arrow_data.len()))
                .context("aggregate value size overflow")
        })?;
        ensure!(
            total <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        let mut output = Vec::with_capacity(total);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&group.last_update_nanos.to_be_bytes());
        output.extend_from_slice(&group.generation.to_be_bytes());
        output.extend_from_slice(&group.next_ordinal.to_be_bytes());
        output.extend_from_slice(&u32::try_from(self.state_len)?.to_be_bytes());
        output.extend_from_slice(&u32::try_from(emitted_len)?.to_be_bytes());
        output.extend_from_slice(&layout.identity);
        output.extend_from_slice(&u32::try_from(messages.len())?.to_be_bytes());
        for message in messages {
            output.extend_from_slice(&u32::try_from(message.ipc_message.len())?.to_be_bytes());
            output.extend_from_slice(&u32::try_from(message.arrow_data.len())?.to_be_bytes());
            output.extend_from_slice(&message.ipc_message);
            output.extend_from_slice(&message.arrow_data);
        }
        Ok(output)
    }

    pub(crate) fn decode(&self, bytes: &[u8], max_bytes: usize) -> Result<EncodedGroup> {
        ensure!(
            bytes.len() <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        ensure!(
            bytes.len() >= HEADER_BYTES && &bytes[..8] == MAGIC,
            "invalid aggregate state codec"
        );
        let state_len = u32::from_be_bytes(bytes[32..36].try_into()?) as usize;
        let emitted_len = u32::from_be_bytes(bytes[36..40].try_into()?) as usize;
        ensure!(
            state_len == self.state_len,
            "aggregate accumulator schema changed"
        );
        ensure!(
            emitted_len == 0 || emitted_len == self.output_len,
            "aggregate emitted-row schema changed"
        );
        let layout = if emitted_len == 0 {
            &self.without_emitted
        } else {
            &self.with_emitted
        };
        ensure!(
            bytes[40..72] == layout.identity,
            "aggregate state type changed"
        );
        let count = u32::from_be_bytes(bytes[72..76].try_into()?) as usize;
        ensure!(
            count > 0 && count <= layout.dictionary_count + 1,
            "invalid aggregate Arrow message count"
        );
        let mut remaining = &bytes[HEADER_BYTES..];
        let mut dictionaries = HashMap::new();
        let mut batch = None;
        for index in 0..count {
            ensure!(remaining.len() >= 8, "truncated aggregate Arrow message");
            let metadata_len = u32::from_be_bytes(remaining[..4].try_into()?) as usize;
            let body_len = u32::from_be_bytes(remaining[4..8].try_into()?) as usize;
            remaining = &remaining[8..];
            ensure!(
                metadata_len <= remaining.len(),
                "truncated aggregate Arrow metadata"
            );
            let (metadata, rest) = remaining.split_at(metadata_len);
            ensure!(body_len <= rest.len(), "truncated aggregate Arrow body");
            let (body, rest) = rest.split_at(body_len);
            remaining = rest;
            let message = arrow::ipc::root_as_message(metadata)
                .map_err(|error| anyhow::anyhow!("invalid aggregate Arrow message: {error}"))?;
            ensure!(
                usize::try_from(message.bodyLength())? == body_len,
                "aggregate Arrow body length changed"
            );
            let body = Buffer::from(body);
            match message.header_type() {
                arrow::ipc::MessageHeader::DictionaryBatch if index + 1 < count => {
                    read_dictionary(
                        &body,
                        message
                            .header_as_dictionary_batch()
                            .context("missing aggregate dictionary")?,
                        &layout.schema,
                        &mut dictionaries,
                        &message.version(),
                    )?;
                }
                arrow::ipc::MessageHeader::RecordBatch if index + 1 == count => {
                    batch = Some(read_record_batch(
                        &body,
                        message
                            .header_as_record_batch()
                            .context("missing aggregate record batch")?,
                        layout.schema.clone(),
                        &dictionaries,
                        None,
                        &message.version(),
                    )?);
                }
                _ => bail!("invalid aggregate Arrow message order"),
            }
        }
        ensure!(
            remaining.is_empty(),
            "aggregate state contains trailing data"
        );
        let batch = batch.context("aggregate state is missing an Arrow row")?;
        ensure!(
            batch.num_rows() == 1,
            "aggregate state must contain one Arrow row"
        );
        ensure!(
            batch.num_columns() == 1 + state_len + emitted_len,
            "aggregate state column count changed"
        );
        let values = batch
            .columns()
            .iter()
            .skip(1)
            .map(|column| ScalarValue::try_from_array(column, 0))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let (state, emitted) = values.split_at(state_len);
        Ok(EncodedGroup {
            last_update_nanos: i64::from_be_bytes(bytes[8..16].try_into()?),
            generation: u64::from_be_bytes(bytes[16..24].try_into()?),
            next_ordinal: u64::from_be_bytes(bytes[24..32].try_into()?),
            accumulator_state: state.to_vec(),
            last_emitted: (emitted_len > 0).then(|| emitted.to_vec()),
        })
    }
}

pub(crate) fn encode_group(group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
    let state_types = group
        .accumulator_state
        .iter()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    let output_types = group
        .last_emitted
        .iter()
        .flatten()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    GroupCodec::new(&state_types, &output_types)?.encode(group, max_bytes)
}

pub(crate) fn decode_group(
    bytes: &[u8],
    state_types: &[DataType],
    output_types: &[DataType],
    max_bytes: usize,
) -> Result<EncodedGroup> {
    GroupCodec::new(state_types, output_types)?.decode(bytes, max_bytes)
}

/// Calendar pruning progress belongs to the same checkpoint as buckets and
/// original correction metadata. The boundary advances only with real progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CalendarProgress {
    pub watermark_nanos: i64,
    pub first_retained_day: i32,
}

impl CalendarProgress {
    pub fn encode(self) -> [u8; 13] {
        let mut bytes = [0; 13];
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&self.watermark_nanos.to_be_bytes());
        bytes[9..13].copy_from_slice(&self.first_retained_day.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == 13 && bytes[0] == 1,
            "invalid calendar progress codec"
        );
        Ok(Self {
            watermark_nanos: i64::from_be_bytes(bytes[1..9].try_into()?),
            first_retained_day: i32::from_be_bytes(bytes[9..13].try_into()?),
        })
    }
}

/// One resumable bucket-cleanup frontier. `after` advances atomically with its
/// deletion page; completion suppresses rescans until the watermark day changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CalendarCleanup {
    pub watermark_day: i32,
    pub after: Option<Vec<u8>>,
    pub complete: bool,
}

impl CalendarCleanup {
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>> {
        ensure!(
            !self.complete || self.after.is_none(),
            "completed calendar cleanup retains cursor"
        );
        let mut bytes = vec![1];
        bytes.extend_from_slice(&self.watermark_day.to_be_bytes());
        bytes.push(if self.complete {
            2
        } else if self.after.is_some() {
            1
        } else {
            0
        });
        if let Some(after) = &self.after {
            bytes.extend_from_slice(after);
        }
        ensure!(
            bytes.len() <= max_bytes,
            "calendar cleanup cursor exceeds configured limit"
        );
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 6 && bytes[0] == 1,
            "invalid calendar cleanup codec"
        );
        let (after, complete) = match bytes[5] {
            0 if bytes.len() == 6 => (None, false),
            1 if bytes.len() > 6 && bytes[6] == b'B' => (Some(bytes[6..].to_vec()), false),
            2 if bytes.len() == 6 => (None, true),
            _ => bail!("invalid calendar cleanup cursor"),
        };
        Ok(Self {
            watermark_day: i32::from_be_bytes(bytes[1..5].try_into()?),
            after,
            complete,
        })
    }
}

/// Typed correction metadata is independent of recent membership. The ordinary
/// state codec validates column counts and types before values can be retracted.
pub(crate) fn encode_calendar_contribution(
    selected: Option<&[arrow_array::ArrayRef]>,
    day: Option<i32>,
    argument_types: &[arrow_schema::DataType],
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut state = vec![
        ScalarValue::Boolean(Some(selected.is_some())),
        ScalarValue::Date32(day),
    ];
    for (index, data_type) in argument_types.iter().enumerate() {
        state.push(if let Some(values) = selected {
            ensure!(
                values.len() == argument_types.len(),
                "calendar contribution argument width changed"
            );
            ScalarValue::try_from_array(&values[index], 0)?
        } else {
            ScalarValue::try_from(data_type)?
        });
    }
    encode_group(
        &EncodedGroup {
            last_update_nanos: 0,
            generation: 1,
            next_ordinal: 0,
            accumulator_state: state,
            last_emitted: None,
        },
        max_bytes,
    )
}

pub(crate) fn decode_calendar_contribution(
    bytes: &[u8],
    argument_types: &[arrow_schema::DataType],
    max_bytes: usize,
) -> Result<(Option<Vec<arrow_array::ArrayRef>>, Option<i32>)> {
    let mut types = vec![
        arrow_schema::DataType::Boolean,
        arrow_schema::DataType::Date32,
    ];
    types.extend_from_slice(argument_types);
    let group = decode_group(bytes, &types, &[], max_bytes)?;
    ensure!(
        group.generation == 1 && group.next_ordinal == 0 && group.last_update_nanos == 0,
        "invalid calendar contribution codec version"
    );
    let selected = match group.accumulator_state[0] {
        ScalarValue::Boolean(Some(value)) => value,
        _ => bail!("invalid calendar contribution eligibility"),
    };
    let ScalarValue::Date32(day) = group.accumulator_state[1] else {
        bail!("invalid calendar contribution day");
    };
    let values = selected
        .then(|| {
            group.accumulator_state[2..]
                .iter()
                .map(ScalarValue::to_array)
                .collect::<datafusion::common::Result<Vec<_>>>()
        })
        .transpose()?;
    Ok((values, day))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    #[test]
    fn calendar_cleanup_round_trips_cursor_completion_and_limits() {
        for cleanup in [
            CalendarCleanup {
                watermark_day: -1,
                after: None,
                complete: false,
            },
            CalendarCleanup {
                watermark_day: 200,
                after: Some(b"B-cursor".to_vec()),
                complete: false,
            },
            CalendarCleanup {
                watermark_day: 201,
                after: None,
                complete: true,
            },
        ] {
            let bytes = cleanup.encode(256).unwrap();
            assert_eq!(CalendarCleanup::decode(&bytes).unwrap(), cleanup);
            assert!(cleanup.encode(5).is_err());
            assert!(CalendarCleanup::decode(&bytes[..5]).is_err());
        }
        assert!(CalendarCleanup::decode(&[1, 0, 0, 0, 0, 3]).is_err());
    }

    #[test]
    fn calendar_progress_round_trips_and_rejects_invalid_codec() {
        let progress = CalendarProgress {
            watermark_nanos: -1,
            first_retained_day: -7,
        };
        let bytes = progress.encode();
        assert_eq!(CalendarProgress::decode(&bytes).unwrap(), progress);
        assert!(CalendarProgress::decode(&bytes[..12]).is_err());
        let mut wrong_version = bytes;
        wrong_version[0] = 2;
        assert!(CalendarProgress::decode(&wrong_version).is_err());
    }

    #[test]
    fn calendar_contribution_retains_null_and_gate_and_rejects_changed_types() {
        let types = [DataType::Int64];
        let values = vec![ScalarValue::Int64(None).to_array().unwrap()];
        let bytes =
            encode_calendar_contribution(Some(&values), Some(-7), &types, 16 * 1024).unwrap();
        let (decoded, day) = decode_calendar_contribution(&bytes, &types, 16 * 1024).unwrap();
        assert_eq!(day, Some(-7));
        assert!(decoded.unwrap()[0].is_null(0));
        assert!(decode_calendar_contribution(&bytes, &[DataType::Float64], 16 * 1024).is_err());
        assert!(
            decode_calendar_contribution(&bytes[..bytes.len() / 2], &types, 16 * 1024).is_err()
        );
        let bytes = encode_calendar_contribution(None, Some(12), &types, 16 * 1024).unwrap();
        let (decoded, day) = decode_calendar_contribution(&bytes, &types, 16 * 1024).unwrap();
        assert!(decoded.is_none());
        assert_eq!(day, Some(12));
    }

    #[test]
    fn typed_state_and_last_emitted_survive_a_fresh_decode() {
        let original = EncodedGroup {
            last_update_nanos: 1234,
            generation: 7,
            next_ordinal: 29,
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
        assert!(decode_group(&encoded, &[DataType::Utf8], &[], 8192).is_err());
    }

    #[test]
    fn cached_codec_preserves_nested_nulls_and_exact_float_bits() {
        let nested = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Utf8(Some("α".into())), ScalarValue::Utf8(None)],
            &DataType::Utf8,
            true,
        ));
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let group = EncodedGroup {
            last_update_nanos: -123456789,
            generation: u64::MAX,
            next_ordinal: 17,
            accumulator_state: vec![
                nested.clone(),
                ScalarValue::Float64(Some(nan)),
                ScalarValue::Float64(Some(-0.0)),
            ],
            last_emitted: Some(vec![ScalarValue::TimestampNanosecond(
                Some(-1),
                Some("UTC".into()),
            )]),
        };
        let state_types = group
            .accumulator_state
            .iter()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>();
        let output_types = [DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )];
        let codec = GroupCodec::new(&state_types, &output_types).unwrap();
        for emitted in [true, false] {
            let group = EncodedGroup {
                last_emitted: emitted.then(|| group.last_emitted.clone().unwrap()),
                accumulator_state: group.accumulator_state.clone(),
                ..group
            };
            let encoded = codec.encode(&group, 8192).unwrap();
            let decoded = codec.decode(&encoded, 8192).unwrap();
            assert_eq!(decoded.accumulator_state[0], nested);
            for (index, expected) in [(1, nan.to_bits()), (2, (-0.0_f64).to_bits())] {
                let ScalarValue::Float64(Some(value)) = decoded.accumulator_state[index] else {
                    panic!("float type changed")
                };
                assert_eq!(value.to_bits(), expected);
            }
            assert_eq!(decoded.last_emitted, group.last_emitted);
            assert_eq!(decoded.last_update_nanos, group.last_update_nanos);
            assert_eq!(decoded.generation, group.generation);
            assert_eq!(decoded.next_ordinal, group.next_ordinal);
            assert!(codec.encode(&group, encoded.len() - 1).is_err());
            assert!(codec.decode(&encoded, encoded.len() - 1).is_err());
            for length in 0..encoded.len() {
                assert!(
                    codec.decode(&encoded[..length], 8192).is_err(),
                    "accepted prefix {length}"
                );
            }
            let mut trailing = encoded.clone();
            trailing.push(0);
            assert!(codec.decode(&trailing, 8192).is_err());
            for offset in [0, 32, 36, 40, 72, 76, 80, 84] {
                let mut corrupted = encoded.clone();
                corrupted[offset] ^= 0xff;
                assert!(
                    codec.decode(&corrupted, 8192).is_err(),
                    "accepted corruption at {offset}"
                );
            }
        }
        let wrong_types = [DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )];
        let wrong_codec = GroupCodec::new(&state_types, &wrong_types).unwrap();
        assert!(
            wrong_codec
                .decode(&codec.encode(&group, 8192).unwrap(), 8192)
                .is_err()
        );
    }

    #[test]
    fn dictionary_values_round_trip_without_repeating_schema() {
        let value = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("same".into()))),
        );
        let group = EncodedGroup {
            last_update_nanos: 0,
            generation: 1,
            next_ordinal: 2,
            accumulator_state: vec![value.clone()],
            last_emitted: Some(vec![value]),
        };
        let encoded = encode_group(&group, 8192).unwrap();
        let types = [group.accumulator_state[0].data_type()];
        let decoded = decode_group(&encoded, &types, &types, 8192).unwrap();
        assert_eq!(decoded.accumulator_state, group.accumulator_state);
        assert_eq!(decoded.last_emitted, group.last_emitted);
    }

    #[test]
    fn empty_accumulator_state_keeps_one_arrow_row() {
        let state = EncodedGroup {
            last_update_nanos: 0,
            generation: 0,
            next_ordinal: 0,
            accumulator_state: vec![],
            last_emitted: None,
        };
        let bytes = encode_group(&state, 4096).unwrap();
        let decoded = decode_group(&bytes, &[], &[], 4096).unwrap();
        assert!(decoded.accumulator_state.is_empty());
        assert!(decoded.last_emitted.is_none());
    }
}
