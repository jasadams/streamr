//! Schema-bound native aggregate state codec. Arrow record-batch messages retain
//! every SQL scalar type without repeating a standalone IPC stream's schema.
use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::buffer::Buffer;
use arrow::ipc::{
    self,
    reader::{read_dictionary, read_record_batch},
    writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

const MAGIC: &[u8; 8] = b"STRAGG03";
const HEADER_BYTES: usize = 8 + 8 + 8 + 8 + 4 + 4 + 32;

#[derive(Debug)]
pub(crate) struct EncodedGroup {
    pub last_update_nanos: i64,
    pub generation: u64,
    pub next_ordinal: u64,
    pub accumulator_state: Vec<ScalarValue>,
    pub last_emitted: Option<Vec<ScalarValue>>,
}

#[derive(Debug)]
struct GroupSchema {
    schema: Arc<Schema>,
    identity: [u8; 32],
    dictionary_count: usize,
}

impl GroupSchema {
    fn new(types: impl Iterator<Item = DataType>, options: &IpcWriteOptions) -> Result<Self> {
        let schema = Schema::new(
            types
                .enumerate()
                .map(|(i, ty)| Field::new(format!("v{i}"), ty, true))
                .collect::<Vec<_>>(),
        );
        let mut tracker = DictionaryTracker::new(false);
        let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut tracker,
            options,
        );
        // IPC assigns IDs while traversing nested dictionary fields. Keep that
        // exact schema and seed subsequent per-value trackers with the same IDs.
        let message = ipc::root_as_message(&encoded.ipc_message)
            .map_err(|error| anyhow!("invalid aggregate schema message: {error:?}"))?;
        let schema = Arc::new(ipc::convert::fb_to_schema(
            message
                .header_as_schema()
                .context("aggregate IPC schema missing")?,
        ));
        Ok(Self {
            schema,
            identity: Sha256::digest(&encoded.ipc_message).into(),
            dictionary_count: tracker.dict_id().len(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct AggregateCodec {
    state: GroupSchema,
    emitted: GroupSchema,
    state_len: usize,
    output_len: usize,
    options: IpcWriteOptions,
}

impl AggregateCodec {
    pub fn new(state_types: &[DataType], output_types: &[DataType]) -> Result<Self> {
        let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5)?;
        Ok(Self {
            state: GroupSchema::new(state_types.iter().cloned(), &options)?,
            emitted: GroupSchema::new(state_types.iter().chain(output_types).cloned(), &options)?,
            state_len: state_types.len(),
            output_len: output_types.len(),
            options,
        })
    }

    pub fn encode(&self, group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
        ensure!(
            max_bytes >= HEADER_BYTES,
            "aggregate value limit is too small"
        );
        ensure!(
            group.accumulator_state.len() == self.state_len,
            "aggregate accumulator schema changed"
        );
        let has_emitted = group.last_emitted.is_some();
        ensure!(
            group
                .last_emitted
                .as_ref()
                .is_none_or(|v| v.len() == self.output_len),
            "aggregate emitted-row schema changed"
        );
        let prepared = if has_emitted {
            &self.emitted
        } else {
            &self.state
        };
        let values = group
            .accumulator_state
            .iter()
            .chain(group.last_emitted.iter().flatten());
        let columns = values
            .zip(prepared.schema.fields())
            .map(|(value, field)| {
                ensure!(
                    value.data_type() == *field.data_type(),
                    "aggregate state type changed"
                );
                Ok(value.to_array()?)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            prepared.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        let mut tracker = DictionaryTracker::new(false);
        for _ in 0..prepared.dictionary_count {
            #[allow(deprecated)] // Arrow 55 names its sequential ID allocator set_dict_id.
            tracker.set_dict_id(prepared.schema.field(0));
        }
        let (dictionaries, batch) =
            IpcDataGenerator::default().encoded_batch(&batch, &mut tracker, &self.options)?;
        let mut output = Vec::with_capacity(HEADER_BYTES);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&group.last_update_nanos.to_be_bytes());
        output.extend_from_slice(&group.generation.to_be_bytes());
        output.extend_from_slice(&group.next_ordinal.to_be_bytes());
        output.extend_from_slice(&u32::try_from(self.state_len)?.to_be_bytes());
        // A presence flag preserves Some([]), independently of column count.
        output.extend_from_slice(&u32::from(has_emitted).to_be_bytes());
        output.extend_from_slice(&prepared.identity);
        for message in dictionaries.iter().chain(std::iter::once(&batch)) {
            let total = output
                .len()
                .checked_add(8)
                .and_then(|n| n.checked_add(message.ipc_message.len()))
                .and_then(|n| n.checked_add(message.arrow_data.len()))
                .context("aggregate value size overflow")?;
            ensure!(
                total <= max_bytes,
                "aggregate value exceeds configured limit"
            );
            output.extend_from_slice(&u32::try_from(message.ipc_message.len())?.to_be_bytes());
            output.extend_from_slice(&u32::try_from(message.arrow_data.len())?.to_be_bytes());
            output.extend_from_slice(&message.ipc_message);
            output.extend_from_slice(&message.arrow_data);
        }
        Ok(output)
    }

    pub fn decode(&self, bytes: &[u8], max_bytes: usize) -> Result<EncodedGroup> {
        ensure!(
            bytes.len() <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        ensure!(
            bytes.len() >= HEADER_BYTES && &bytes[..8] == MAGIC,
            "invalid aggregate state codec"
        );
        ensure!(
            u32::from_be_bytes(bytes[32..36].try_into()?) as usize == self.state_len,
            "aggregate accumulator schema changed"
        );
        let has_emitted = match u32::from_be_bytes(bytes[36..40].try_into()?) {
            0 => false,
            1 => true,
            _ => bail!("invalid aggregate emitted-row presence"),
        };
        let prepared = if has_emitted {
            &self.emitted
        } else {
            &self.state
        };
        ensure!(
            bytes[40..HEADER_BYTES] == prepared.identity,
            "aggregate state type changed"
        );
        let mut cursor = HEADER_BYTES;
        let mut dictionaries = HashMap::new();
        let mut batch = None;
        while cursor < bytes.len() {
            ensure!(
                batch.is_none(),
                "aggregate state contains extra Arrow messages"
            );
            let lengths = bytes
                .get(
                    cursor
                        ..cursor
                            .checked_add(8)
                            .context("aggregate message size overflow")?,
                )
                .context("truncated aggregate message lengths")?;
            let metadata_len = u32::from_be_bytes(lengths[..4].try_into()?) as usize;
            let body_len = u32::from_be_bytes(lengths[4..].try_into()?) as usize;
            cursor += 8;
            let metadata_end = cursor
                .checked_add(metadata_len)
                .context("aggregate message size overflow")?;
            let body_end = metadata_end
                .checked_add(body_len)
                .context("aggregate message size overflow")?;
            let metadata = bytes
                .get(cursor..metadata_end)
                .context("truncated aggregate message")?;
            let body = bytes
                .get(metadata_end..body_end)
                .context("truncated aggregate message body")?;
            cursor = body_end;
            let message = ipc::root_as_message(metadata)
                .map_err(|error| anyhow!("invalid aggregate Arrow message: {error:?}"))?;
            ensure!(
                message.version() == ipc::MetadataVersion::V5
                    && message.bodyLength() == i64::try_from(body_len)?,
                "invalid aggregate Arrow message framing"
            );
            let body = Buffer::from(body);
            if let Some(dictionary) = message.header_as_dictionary_batch() {
                ensure!(
                    !dictionary.isDelta() && !dictionaries.contains_key(&dictionary.id()),
                    "invalid aggregate dictionary replacement"
                );
                validate_batch(
                    dictionary
                        .data()
                        .context("aggregate dictionary data missing")?,
                    body.len(),
                )?;
                decode_arrow(|| {
                    read_dictionary(
                        &body,
                        dictionary,
                        &prepared.schema,
                        &mut dictionaries,
                        &message.version(),
                    )
                })?;
            } else if let Some(record) = message.header_as_record_batch() {
                ensure!(
                    record.length() == 1,
                    "aggregate state must contain one Arrow row"
                );
                validate_batch(record, body.len())?;
                batch = Some(decode_arrow(|| {
                    read_record_batch(
                        &body,
                        record,
                        prepared.schema.clone(),
                        &dictionaries,
                        None,
                        &message.version(),
                    )
                })?);
            } else {
                bail!("invalid aggregate Arrow message type");
            }
        }
        let batch = batch.context("aggregate state is missing an Arrow row")?;
        let values = batch
            .columns()
            .iter()
            .map(|column| ScalarValue::try_from_array(column, 0))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let (state, output) = values.split_at(self.state_len);
        Ok(EncodedGroup {
            last_update_nanos: i64::from_be_bytes(bytes[8..16].try_into()?),
            generation: u64::from_be_bytes(bytes[16..24].try_into()?),
            next_ordinal: u64::from_be_bytes(bytes[24..32].try_into()?),
            accumulator_state: state.to_vec(),
            last_emitted: has_emitted.then(|| output.to_vec()),
        })
    }
}

// Arrow 55's validated decoder still uses unchecked buffer iterators and
// assertions for inconsistent IPC metadata. Corrupt persisted bytes must return
// an error rather than unwind the worker. No validation is skipped by this guard.
fn decode_arrow<T>(
    decode: impl FnOnce() -> std::result::Result<T, arrow_schema::ArrowError>,
) -> Result<T> {
    catch_unwind(AssertUnwindSafe(decode))
        .map_err(|_| anyhow!("malformed aggregate Arrow data"))?
        .map_err(Into::into)
}

fn validate_batch(batch: ipc::RecordBatch<'_>, body_len: usize) -> Result<()> {
    ensure!(
        batch.length() >= 0 && batch.compression().is_none(),
        "invalid aggregate Arrow batch"
    );
    for node in batch.nodes().context("aggregate Arrow nodes missing")? {
        ensure!(
            node.length() >= 0 && node.null_count() >= 0 && node.null_count() <= node.length(),
            "invalid aggregate Arrow node"
        );
    }
    for buffer in batch.buffers().context("aggregate Arrow buffers missing")? {
        let offset =
            usize::try_from(buffer.offset()).context("negative aggregate Arrow buffer offset")?;
        let length =
            usize::try_from(buffer.length()).context("negative aggregate Arrow buffer length")?;
        ensure!(
            offset
                .checked_add(length)
                .is_some_and(|end| end <= body_len),
            "aggregate Arrow buffer exceeds message body"
        );
    }
    if let Some(counts) = batch.variadicBufferCounts() {
        ensure!(
            counts
                .iter()
                .all(|count| count >= 0 && usize::try_from(count).is_ok_and(|n| n <= body_len)),
            "invalid aggregate Arrow variadic buffers"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn encode_group(group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
    AggregateCodec::new(
        &group
            .accumulator_state
            .iter()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>(),
        &group
            .last_emitted
            .iter()
            .flatten()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>(),
    )?
    .encode(group, max_bytes)
}
#[cfg(test)]
pub(crate) fn decode_group(
    bytes: &[u8],
    state: &[DataType],
    output: &[DataType],
    max_bytes: usize,
) -> Result<EncodedGroup> {
    AggregateCodec::new(state, output)?.decode(bytes, max_bytes)
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
    codec: &AggregateCodec,
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
    codec.encode(
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
    codec: &AggregateCodec,
    max_bytes: usize,
) -> Result<(Option<Vec<arrow_array::ArrayRef>>, Option<i32>)> {
    let mut types = vec![
        arrow_schema::DataType::Boolean,
        arrow_schema::DataType::Date32,
    ];
    types.extend_from_slice(argument_types);
    let group = codec.decode(bytes, max_bytes)?;
    ensure!(
        codec
            .state
            .schema
            .fields()
            .iter()
            .map(|field| field.data_type())
            .eq(types.iter()),
        "calendar contribution schema changed"
    );
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

    fn group(values: Vec<ScalarValue>, emitted: Option<Vec<ScalarValue>>) -> EncodedGroup {
        EncodedGroup {
            last_update_nanos: -123_456_789_012_345_678,
            generation: u64::MAX,
            next_ordinal: u64::MAX - 1,
            accumulator_state: values,
            last_emitted: emitted,
        }
    }

    fn codec(group: &EncodedGroup) -> AggregateCodec {
        AggregateCodec::new(
            &group
                .accumulator_state
                .iter()
                .map(ScalarValue::data_type)
                .collect::<Vec<_>>(),
            &group
                .last_emitted
                .iter()
                .flatten()
                .map(ScalarValue::data_type)
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn assert_group(original: &EncodedGroup, restored: &EncodedGroup) {
        assert_eq!(restored.accumulator_state, original.accumulator_state);
        assert_eq!(restored.last_emitted, original.last_emitted);
        assert_eq!(restored.last_update_nanos, original.last_update_nanos);
        assert_eq!(restored.generation, original.generation);
        assert_eq!(restored.next_ordinal, original.next_ordinal);
    }

    fn scalar_samples() -> Vec<ScalarValue> {
        use arrow::array::ArrayData;
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Array, MapArray, StructArray, make_array};
        use arrow_schema::{IntervalUnit, UnionFields, UnionMode};
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let list = ScalarValue::new_list(
            &[ScalarValue::Int64(None), ScalarValue::Int64(Some(-9))],
            &DataType::Int64,
            true,
        );
        let entry_fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ];
        let entries = StructArray::new(
            entry_fields.clone().into(),
            vec![
                ScalarValue::Utf8(Some("α".into())).to_array().unwrap(),
                ScalarValue::Int64(None).to_array().unwrap(),
            ],
            None,
        );
        let map = MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
            entries,
            None,
            false,
        );
        let structure = StructArray::new(
            vec![
                Arc::new(Field::new("nested", list.data_type().clone(), true)),
                Arc::new(Field::new("map", map.data_type().clone(), true)),
            ]
            .into(),
            vec![list.clone(), Arc::new(map.clone())],
            None,
        );
        let union_fields = UnionFields::new(
            [3, 7],
            [
                Field::new("number", DataType::Int64, true),
                Field::new("text", DataType::Utf8, true),
            ],
        );
        let half = make_array(
            ArrayData::builder(DataType::Float16)
                .len(1)
                .add_buffer(Buffer::from_slice_ref([0x7c01u16]))
                .build()
                .unwrap(),
        );
        let mut values = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(Some(false)),
            ScalarValue::try_from_array(&half, 0).unwrap(),
            ScalarValue::Float32(Some(f32::from_bits(0x7f80_0001))),
            ScalarValue::Float32(Some(-0.0)),
            ScalarValue::Float64(Some(f64::from_bits(0x7ff0_0000_0000_0001))),
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Decimal128(Some(-123), 20, 4),
            ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_i128(-456)), 40, -2),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::Utf8(Some("α\0z".into())),
            ScalarValue::Utf8View(Some("".into())),
            ScalarValue::LargeUtf8(Some("".into())),
            ScalarValue::Binary(Some(vec![])),
            ScalarValue::BinaryView(Some(vec![0, 255])),
            ScalarValue::LargeBinary(Some(vec![0, 255])),
            ScalarValue::FixedSizeBinary(2, Some(vec![0, 255])),
            ScalarValue::List(list),
            ScalarValue::LargeList(Arc::new(arrow_array::LargeListArray::new(
                item.clone(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 0])),
                ScalarValue::Int64(Some(0)).to_array().unwrap().slice(0, 0),
                None,
            ))),
            ScalarValue::FixedSizeList(Arc::new(arrow_array::FixedSizeListArray::new(
                item,
                1,
                ScalarValue::Int64(Some(7)).to_array().unwrap(),
                None,
            ))),
            ScalarValue::Struct(Arc::new(structure)),
            ScalarValue::Map(Arc::new(map)),
            ScalarValue::Date32(Some(-1)),
            ScalarValue::Date64(Some(-86_400_001)),
            ScalarValue::Time32Second(Some(1)),
            ScalarValue::Time32Millisecond(Some(1234)),
            ScalarValue::Time64Microsecond(Some(1234567)),
            ScalarValue::Time64Nanosecond(Some(1234567891)),
            ScalarValue::TimestampSecond(Some(-1), Some("UTC".into())),
            ScalarValue::TimestampMillisecond(Some(-1234), None),
            ScalarValue::TimestampMicrosecond(Some(-1234567), None),
            ScalarValue::TimestampNanosecond(Some(-1234567891), Some("+09:30".into())),
            ScalarValue::IntervalYearMonth(Some(-17)),
            ScalarValue::IntervalDayTime(Some(arrow::datatypes::IntervalDayTime::new(-7, 123))),
            ScalarValue::IntervalMonthDayNano(Some(arrow::datatypes::IntervalMonthDayNano::new(
                -1, 2, -123456789,
            ))),
            ScalarValue::DurationSecond(Some(-1)),
            ScalarValue::DurationMillisecond(Some(-1234)),
            ScalarValue::DurationMicrosecond(Some(-1234567)),
            ScalarValue::DurationNanosecond(Some(-1234567891)),
            ScalarValue::Union(
                Some((7, Box::new(ScalarValue::Utf8(Some("union".into()))))),
                union_fields.clone(),
                UnionMode::Dense,
            ),
            ScalarValue::Union(
                Some((3, Box::new(ScalarValue::Int64(None)))),
                union_fields,
                UnionMode::Sparse,
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(ScalarValue::Utf8(Some("dictionary".into()))),
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Int64(None)),
            ),
        ];
        // Every scalar variant above also exercises its typed NULL. Arrow's union
        // scalar has no generic typed-null constructor; its selected child is NULL.
        let types = values
            .iter()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>();
        for ty in types {
            if !matches!(ty, DataType::Union(_, _)) {
                values.push(ScalarValue::try_from(&ty).unwrap());
            }
        }
        for unit in [
            IntervalUnit::YearMonth,
            IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano,
        ] {
            assert!(
                values
                    .iter()
                    .any(|value| value.data_type() == DataType::Interval(unit))
            );
        }
        values
    }

    #[test]
    fn all_scalar_variants_and_typed_nulls_keep_exact_values() {
        let values = scalar_samples();
        for value in values {
            // Separate fields exercise distinct per-column dictionary IDs, and
            // keep state/emitted schema variants independently recoverable.
            let original = group(vec![value.clone()], Some(vec![value]));
            let codec = codec(&original);
            let bytes = codec.encode(&original, 1 << 20).unwrap();
            assert_group(&original, &codec.decode(&bytes, 1 << 20).unwrap());
            assert_group(
                &original,
                &AggregateCodec::new(
                    &original
                        .accumulator_state
                        .iter()
                        .map(ScalarValue::data_type)
                        .collect::<Vec<_>>(),
                    &original
                        .last_emitted
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(ScalarValue::data_type)
                        .collect::<Vec<_>>(),
                )
                .unwrap()
                .decode(&bytes, 1 << 20)
                .unwrap(),
            );
        }
    }

    #[test]
    fn schema_identity_is_stable_for_nested_metadata_and_dictionary_ids() {
        let entries = [
            ("z".to_string(), "last".to_string()),
            ("a".to_string(), "first".to_string()),
        ];
        let forward = entries.clone().into_iter().collect();
        let reverse = entries.into_iter().rev().collect();
        let nested = |metadata| {
            DataType::Struct(
                vec![Arc::new(
                    Field::new(
                        "dictionary",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        true,
                    )
                    .with_metadata(metadata),
                )]
                .into(),
            )
        };
        let first = AggregateCodec::new(&[nested(forward)], &[DataType::Int64]).unwrap();
        let second = AggregateCodec::new(&[nested(reverse)], &[DataType::Int64]).unwrap();
        assert_eq!(first.state.identity, second.state.identity);
        assert_eq!(first.emitted.identity, second.emitted.identity);
        let state = ScalarValue::try_from(first.state.schema.field(0).data_type()).unwrap();
        let original = group(vec![state], Some(vec![ScalarValue::Int64(Some(7))]));
        let bytes = first.encode(&original, 8192).unwrap();
        assert_group(&original, &second.decode(&bytes, 8192).unwrap());
        let dictionary = group(
            vec![ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("value".into()))),
            )],
            None,
        );
        let bytes = codec(&dictionary).encode(&dictionary, 8192).unwrap();
        assert!(
            AggregateCodec::new(
                &[DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8)
                )],
                &[]
            )
            .unwrap()
            .decode(&bytes, 8192)
            .is_err()
        );
    }

    #[test]
    fn limits_schema_presence_and_malformed_messages_are_rejected() {
        let original = group(
            vec![ScalarValue::Int64(Some(42))],
            Some(vec![ScalarValue::Utf8(Some("".into()))]),
        );
        let codec = codec(&original);
        let bytes = codec.encode(&original, 8192).unwrap();
        assert!(codec.encode(&original, bytes.len() - 1).is_err());
        assert!(codec.decode(&bytes, bytes.len() - 1).is_err());
        assert_group(
            &original,
            &codec
                .decode(&codec.encode(&original, bytes.len()).unwrap(), bytes.len())
                .unwrap(),
        );
        for cut in 0..bytes.len() {
            assert!(
                codec.decode(&bytes[..cut], 8192).is_err(),
                "accepted cut {cut}"
            );
        }
        for index in [
            0,
            32,
            36,
            40,
            HEADER_BYTES,
            HEADER_BYTES + 4,
            HEADER_BYTES + 8,
        ] {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 0xff;
            assert!(
                codec.decode(&corrupt, 8192).is_err(),
                "accepted corrupt offset {index}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(codec.decode(&trailing, 8192).is_err());
        let mut extra = bytes.clone();
        extra.extend_from_slice(&bytes[HEADER_BYTES..]);
        assert!(codec.decode(&extra, 8192).is_err());
        assert!(
            AggregateCodec::new(&[DataType::Float64], &[DataType::Utf8])
                .unwrap()
                .decode(&bytes, 8192)
                .is_err()
        );
        let wrong_state = group(
            vec![ScalarValue::Float64(Some(42.0))],
            original.last_emitted.clone(),
        );
        assert!(codec.encode(&wrong_state, 8192).is_err());
        let wrong_width = group(vec![], original.last_emitted.clone());
        assert!(codec.encode(&wrong_width, 8192).is_err());
        let empty_codec = AggregateCodec::new(&[], &[]).unwrap();
        for emitted in [None, Some(vec![])] {
            let original = group(vec![], emitted);
            assert_group(
                &original,
                &empty_codec
                    .decode(&empty_codec.encode(&original, 8192).unwrap(), 8192)
                    .unwrap(),
            );
        }
    }

    #[test]
    fn malformed_arrow_nodes_buffers_and_dictionary_ids_return_errors() {
        let original = group(vec![ScalarValue::Int64(Some(42))], None);
        let codec = codec(&original);
        let bytes = codec.encode(&original, 8192).unwrap();
        let start = HEADER_BYTES + 8;
        let metadata_len =
            u32::from_be_bytes(bytes[HEADER_BYTES..HEADER_BYTES + 4].try_into().unwrap()) as usize;
        let message = ipc::root_as_message(&bytes[start..start + metadata_len]).unwrap();
        let batch = message.header_as_record_batch().unwrap();
        let buffers = batch.buffers().unwrap();
        let buffer_position =
            buffers.get(0) as *const ipc::Buffer as usize - bytes.as_ptr() as usize;
        let vector_position = buffers.bytes().as_ptr() as usize - bytes.as_ptr() as usize - 4;
        let node_position = batch.nodes().unwrap().get(0) as *const ipc::FieldNode as usize
            - bytes.as_ptr() as usize;
        let mut corrupt_offset = bytes.clone();
        corrupt_offset[buffer_position..buffer_position + 8]
            .copy_from_slice(&i64::MAX.to_le_bytes());
        assert!(codec.decode(&corrupt_offset, 8192).is_err());
        let mut corrupt_nulls = bytes.clone();
        corrupt_nulls[node_position + 8..node_position + 16].copy_from_slice(&2i64.to_le_bytes());
        assert!(codec.decode(&corrupt_nulls, 8192).is_err());
        // An empty buffer vector is a valid FlatBuffer but invalid for an Int64
        // array. Arrow 55 panics in next_buffer; the persistence boundary rejects it.
        let mut missing_buffers = bytes.clone();
        missing_buffers[vector_position..vector_position + 4].copy_from_slice(&0u32.to_le_bytes());
        assert!(codec.decode(&missing_buffers, 8192).is_err());
        let dictionary = group(
            vec![ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("value".into()))),
            )],
            None,
        );
        let dictionary_codec =
            AggregateCodec::new(&[dictionary.accumulator_state[0].data_type()], &[]).unwrap();
        let bytes = dictionary_codec.encode(&dictionary, 8192).unwrap();
        let metadata_len =
            u32::from_be_bytes(bytes[HEADER_BYTES..HEADER_BYTES + 4].try_into().unwrap()) as usize;
        let body_len = u32::from_be_bytes(
            bytes[HEADER_BYTES + 4..HEADER_BYTES + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let dictionary_end = HEADER_BYTES + 8 + metadata_len + body_len;
        // Duplicate dictionary messages must never replace previously decoded state.
        let mut duplicate = bytes[..dictionary_end].to_vec();
        duplicate.extend_from_slice(&bytes[HEADER_BYTES..]);
        assert!(dictionary_codec.decode(&duplicate, 8192).is_err());
        // A dictionary record without its values must also be rejected.
        let mut missing = bytes[..HEADER_BYTES].to_vec();
        missing.extend_from_slice(&bytes[dictionary_end..]);
        assert!(dictionary_codec.decode(&missing, 8192).is_err());
    }

    /// Mechanical same-binary comparison against the previous standalone Arrow
    /// stream representation. External reference files contain generic aggregate
    /// rows, not application schemas or an engine recovery path. Dev-profile
    /// timings are micro measurements, never native throughput qualification.
    #[test]
    #[ignore = "manual codec measurement with optional externally supplied reference rows"]
    fn measure_schema_bound_codec_against_arrow_stream() {
        use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
        use arrow_array::BooleanArray;
        use std::{hint::black_box, io::Cursor, time::Instant};
        fn reference_encode(group: &EncodedGroup) -> Vec<u8> {
            let mut columns = group
                .accumulator_state
                .iter()
                .chain(group.last_emitted.iter().flatten())
                .map(|value| value.to_array().unwrap())
                .collect::<Vec<_>>();
            columns.insert(0, Arc::new(BooleanArray::from(vec![true])));
            let schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        Field::new(format!("v{index}"), value.data_type().clone(), true)
                    })
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let mut output = b"STRAGG02".to_vec();
            output.extend_from_slice(&group.last_update_nanos.to_be_bytes());
            output.extend_from_slice(&group.generation.to_be_bytes());
            output.extend_from_slice(&group.next_ordinal.to_be_bytes());
            output.extend_from_slice(&(group.accumulator_state.len() as u32).to_be_bytes());
            output.extend_from_slice(
                &(group.last_emitted.as_ref().map_or(0, Vec::len) as u32).to_be_bytes(),
            );
            let mut writer = StreamWriter::try_new(&mut output, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            output
        }
        fn reference_decode(bytes: &[u8]) -> EncodedGroup {
            let state_len = u32::from_be_bytes(bytes[32..36].try_into().unwrap()) as usize;
            let emitted_len = u32::from_be_bytes(bytes[36..40].try_into().unwrap()) as usize;
            let mut reader = StreamReader::try_new(Cursor::new(&bytes[40..]), None).unwrap();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1 + state_len + emitted_len);
            assert!(reader.next().is_none());
            let values = batch
                .columns()
                .iter()
                .skip(1)
                .map(|column| ScalarValue::try_from_array(column, 0).unwrap())
                .collect::<Vec<_>>();
            let (state, emitted) = values.split_at(state_len);
            EncodedGroup {
                last_update_nanos: i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
                generation: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
                next_ordinal: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
                accumulator_state: state.to_vec(),
                last_emitted: (emitted_len > 0).then(|| emitted.to_vec()),
            }
        }
        let mut fixtures = vec![
            (
                "fixed-width".to_string(),
                group(
                    (0..16).map(|n| ScalarValue::Int64(Some(n))).collect(),
                    Some(
                        (0..11)
                            .map(|n| ScalarValue::Float64(Some(n as f64 + 0.5)))
                            .collect(),
                    ),
                ),
            ),
            (
                "variable-nested".to_string(),
                group(
                    scalar_samples()
                        .into_iter()
                        .filter(|value| {
                            matches!(
                                value,
                                ScalarValue::Utf8(_)
                                    | ScalarValue::Binary(_)
                                    | ScalarValue::List(_)
                                    | ScalarValue::Struct(_)
                                    | ScalarValue::Map(_)
                                    | ScalarValue::Dictionary(_, _)
                            )
                        })
                        .collect(),
                    Some(vec![ScalarValue::Utf8(Some("α".repeat(256)))]),
                ),
            ),
        ];
        let reference_dir = std::env::var_os("STREAMR_CODEC_REFERENCE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../docs/review-logs/codec-reference")
            });
        if reference_dir.exists() {
            let mut paths = std::fs::read_dir(&reference_dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "bin"))
                .collect::<Vec<_>>();
            paths.sort();
            for path in paths {
                let bytes = std::fs::read(&path).unwrap();
                let original = reference_decode(&bytes);
                assert_eq!(
                    reference_encode(&original),
                    bytes,
                    "reference differs from original fixture"
                );
                fixtures.push((
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    original,
                ));
            }
        }
        const ITERATIONS: usize = 2000;
        for (name, original) in fixtures {
            let codec = codec(&original);
            let reference = reference_encode(&original);
            let encoded = codec.encode(&original, 1 << 20).unwrap();
            assert_group(&original, &reference_decode(&reference));
            assert_group(&original, &codec.decode(&encoded, 1 << 20).unwrap());
            assert!(encoded.len() < reference.len());
            for sample in 0..5 {
                // Warm both paths before each alternating matched sample.
                for _ in 0..100 {
                    black_box(reference_encode(&original));
                    black_box(codec.encode(&original, 1 << 20).unwrap());
                    black_box(reference_decode(&reference));
                    black_box(codec.decode(&encoded, 1 << 20).unwrap());
                }
                let reference_encode_time = || {
                    let start = Instant::now();
                    for _ in 0..ITERATIONS {
                        black_box(reference_encode(black_box(&original)));
                    }
                    start.elapsed().as_nanos() as f64 / ITERATIONS as f64
                };
                let encode_time = || {
                    let start = Instant::now();
                    for _ in 0..ITERATIONS {
                        black_box(codec.encode(black_box(&original), 1 << 20).unwrap());
                    }
                    start.elapsed().as_nanos() as f64 / ITERATIONS as f64
                };
                let reference_decode_time = || {
                    let start = Instant::now();
                    for _ in 0..ITERATIONS {
                        black_box(reference_decode(black_box(&reference)));
                    }
                    start.elapsed().as_nanos() as f64 / ITERATIONS as f64
                };
                let decode_time = || {
                    let start = Instant::now();
                    for _ in 0..ITERATIONS {
                        black_box(codec.decode(black_box(&encoded), 1 << 20).unwrap());
                    }
                    start.elapsed().as_nanos() as f64 / ITERATIONS as f64
                };
                let (old_encode, new_encode, old_decode, new_decode) = if sample % 2 == 0 {
                    (
                        reference_encode_time(),
                        encode_time(),
                        reference_decode_time(),
                        decode_time(),
                    )
                } else {
                    let new_encode = encode_time();
                    let old_encode = reference_encode_time();
                    let new_decode = decode_time();
                    let old_decode = reference_decode_time();
                    (old_encode, new_encode, old_decode, new_decode)
                };
                println!(
                    "CODEC_MEASUREMENT {}",
                    serde_json::json!({"fixture": name, "sample": sample, "iterations": ITERATIONS, "profile": "test-unoptimized", "state_columns": original.accumulator_state.len(), "emitted_columns": original.last_emitted.as_ref().map_or(0, Vec::len), "reference_bytes": reference.len(), "schema_bound_bytes": encoded.len(), "reference_encode_ns": old_encode, "schema_bound_encode_ns": new_encode, "reference_decode_ns": old_decode, "schema_bound_decode_ns": new_decode})
                );
            }
        }
    }

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
        let codec =
            AggregateCodec::new(&[DataType::Boolean, DataType::Date32, DataType::Int64], &[])
                .unwrap();
        let values = vec![ScalarValue::Int64(None).to_array().unwrap()];
        let bytes =
            encode_calendar_contribution(Some(&values), Some(-7), &types, &codec, 16 * 1024)
                .unwrap();
        let (decoded, day) =
            decode_calendar_contribution(&bytes, &types, &codec, 16 * 1024).unwrap();
        assert_eq!(day, Some(-7));
        assert!(decoded.unwrap()[0].is_null(0));
        assert!(
            decode_calendar_contribution(
                &bytes,
                &[DataType::Float64],
                &AggregateCodec::new(
                    &[DataType::Boolean, DataType::Date32, DataType::Float64],
                    &[]
                )
                .unwrap(),
                16 * 1024
            )
            .is_err()
        );
        assert!(
            decode_calendar_contribution(&bytes[..bytes.len() / 2], &types, &codec, 16 * 1024)
                .is_err()
        );
        let bytes =
            encode_calendar_contribution(None, Some(12), &types, &codec, 16 * 1024).unwrap();
        let (decoded, day) =
            decode_calendar_contribution(&bytes, &types, &codec, 16 * 1024).unwrap();
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
