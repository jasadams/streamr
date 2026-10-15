//! Schema-bound compact native aggregate values. SQL execution remains Arrow-based.
use anyhow::{Context, Result, bail, ensure};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use sha2::{Digest, Sha256};
#[path = "aggregate_codec_complex.rs"]
mod complex;

const MAGIC: &[u8; 8] = b"STRAGG04";
const HEADER_BYTES: usize = 65;

#[derive(Debug)]
pub(crate) struct EncodedGroup {
    pub last_update_nanos: i64,
    pub generation: u64,
    pub next_ordinal: u64,
    pub accumulator_state: Vec<ScalarValue>,
    pub last_emitted: Option<Vec<ScalarValue>>,
}

pub(crate) struct GroupCodec {
    state_types: Vec<DataType>,
    output_types: Vec<DataType>,
    identity: [u8; 32],
    without_emitted: complex::ComplexLayout,
    with_emitted: complex::ComplexLayout,
}

fn validate_type(t: &DataType) -> Result<()> {
    validate_nested_type(t)?;
    match t {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _) => Ok(()),
        // No array-valued or nested allocation occurs for ordinary null Scalars.
        _ => {
            ScalarValue::try_from(t)?;
            Ok(())
        }
    }
}
fn validate_nested_type(t: &DataType) -> Result<()> {
    match t {
        DataType::FixedSizeBinary(n) => ensure!(*n >= 0, "negative aggregate binary width"),
        DataType::FixedSizeList(child, n) => {
            ensure!(*n >= 0, "negative aggregate list width");
            validate_nested_type(child.data_type())?;
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
            validate_nested_type(child.data_type())?
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_nested_type(field.data_type())?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_nested_type(field.data_type())?;
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_nested_type(run_ends.data_type())?;
            validate_nested_type(values.data_type())?;
        }
        DataType::Dictionary(key, value) => {
            ensure!(
                matches!(
                    key.as_ref(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                ),
                "invalid aggregate dictionary key"
            );
            validate_nested_type(value)?;
        }
        DataType::Decimal128(p, s) => arrow_array::types::validate_decimal_precision_and_scale::<
            arrow_array::types::Decimal128Type,
        >(*p, *s)?,
        DataType::Decimal256(p, s) => arrow_array::types::validate_decimal_precision_and_scale::<
            arrow_array::types::Decimal256Type,
        >(*p, *s)?,
        _ => {}
    }
    Ok(())
}

fn is_complex(t: &DataType) -> bool {
    matches!(
        t,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
    )
}

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(n <= self.bytes.len(), "truncated aggregate value");
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into()?)
    }
    fn u32(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.fixed()?) as usize)
    }
}

fn bitmap_len(n: usize) -> usize {
    n.div_ceil(8)
}
fn checked_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).context("aggregate value size overflow")
}
fn workspace(bytes: usize, max: usize) -> Result<()> {
    ensure!(
        bytes
            <= max
                .checked_mul(3)
                .context("aggregate workspace limit overflow")?,
        "aggregate decoded workspace exceeds configured limit"
    );
    Ok(())
}

impl GroupCodec {
    pub(crate) fn new(state_types: &[DataType], output_types: &[DataType]) -> Result<Self> {
        let mut hash = Sha256::new();
        hash.update(b"Streamr compact aggregate v4");
        for types in [state_types, output_types] {
            // Validate that every ordinary type has a ScalarValue representation.
            for t in types {
                validate_type(t)?;
            }
            let schema = Schema::new(
                types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| Field::new(format!("v{i}"), t.clone(), true))
                    .collect::<Vec<_>>(),
            );
            let bytes = complex::schema_bytes(&schema);
            hash.update(u64::try_from(bytes.len())?.to_le_bytes());
            hash.update(bytes);
        }
        Ok(Self {
            state_types: state_types.to_vec(),
            output_types: output_types.to_vec(),
            identity: hash.finalize().into(),
            without_emitted: complex::ComplexLayout::new(state_types.iter())?,
            with_emitted: complex::ComplexLayout::new(state_types.iter().chain(output_types))?,
        })
    }
    pub(crate) fn encode(&self, group: &EncodedGroup, max_bytes: usize) -> Result<Vec<u8>> {
        ensure!(
            group.accumulator_state.len() == self.state_types.len(),
            "aggregate accumulator schema changed"
        );
        if let Some(values) = &group.last_emitted {
            ensure!(
                values.len() == self.output_types.len(),
                "aggregate emitted-row schema changed"
            );
        }
        let present = group.last_emitted.is_some();
        let types = self
            .state_types
            .iter()
            .chain(self.output_types.iter().take(if present {
                self.output_types.len()
            } else {
                0
            }));
        let values = group
            .accumulator_state
            .iter()
            .chain(group.last_emitted.iter().flatten());
        let bitmap_bytes = checked_add(
            bitmap_len(self.state_types.len()),
            if present {
                bitmap_len(self.output_types.len())
            } else {
                0
            },
        )?;
        let mut total = checked_add(HEADER_BYTES, bitmap_bytes)?;
        for (t, v) in types.clone().zip(values.clone()) {
            ensure!(&v.data_type() == t, "aggregate state type changed");
            if !is_complex(t) {
                total = checked_add(total, direct_size(v)?)?;
            }
        }
        ensure!(
            total <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        let layout = if present {
            &self.with_emitted
        } else {
            &self.without_emitted
        };
        let messages = layout.encode(values.clone(), max_bytes)?;
        total = checked_add(total, messages.encoded_len())?;
        ensure!(
            total <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        let mut output = Vec::new();
        output.try_reserve_exact(total)?;
        output.extend_from_slice(MAGIC);
        output.push(u8::from(present));
        output.extend_from_slice(&group.last_update_nanos.to_le_bytes());
        output.extend_from_slice(&group.generation.to_le_bytes());
        output.extend_from_slice(&group.next_ordinal.to_le_bytes());
        output.extend_from_slice(&self.identity);
        let bitmap_start = output.len();
        output.resize(checked_add(bitmap_start, bitmap_bytes)?, 0);
        for (i, (t, v)) in self
            .state_types
            .iter()
            .zip(&group.accumulator_state)
            .enumerate()
        {
            if is_complex(t) || !v.is_null() {
                output[bitmap_start + i / 8] |= 1 << (i % 8);
            }
            if !is_complex(t) {
                encode_direct(v, &mut output)?;
            }
        }
        if let Some(values) = &group.last_emitted {
            let start = bitmap_start + bitmap_len(self.state_types.len());
            for (i, (t, v)) in self.output_types.iter().zip(values).enumerate() {
                if is_complex(t) || !v.is_null() {
                    output[start + i / 8] |= 1 << (i % 8);
                }
                if !is_complex(t) {
                    encode_direct(v, &mut output)?;
                }
            }
        }
        messages.append_to(&mut output)?;
        ensure!(output.len() == total, "aggregate encoded size changed");
        Ok(output)
    }
    pub(crate) fn decode(&self, bytes: &[u8], max_bytes: usize) -> Result<EncodedGroup> {
        ensure!(
            bytes.len() <= max_bytes,
            "aggregate value exceeds configured limit"
        );
        let mut input = Cursor { bytes };
        ensure!(input.take(8)? == MAGIC, "invalid aggregate state codec");
        let flags = input.take(1)?[0];
        ensure!(flags <= 1, "invalid aggregate flags");
        let present = flags == 1;
        let last_update_nanos = i64::from_le_bytes(input.fixed()?);
        let generation = u64::from_le_bytes(input.fixed()?);
        let next_ordinal = u64::from_le_bytes(input.fixed()?);
        ensure!(
            input.take(32)? == self.identity,
            "aggregate state type changed"
        );
        let state_bits = input.take(bitmap_len(self.state_types.len()))?;
        let output_bits = input.take(if present {
            bitmap_len(self.output_types.len())
        } else {
            0
        })?;
        let value_count = checked_add(
            self.state_types.len(),
            if present { self.output_types.len() } else { 0 },
        )?;
        let containers = value_count
            .checked_mul(std::mem::size_of::<ScalarValue>())
            .context("aggregate vector size overflow")?;
        workspace(checked_add(bytes.len(), containers)?, max_bytes)?;
        // First pass validates all direct lengths and counts owned payloads before allocation.
        let mut probe = Cursor { bytes: input.bytes };
        let mut direct_owned = 0;
        for (types, bits) in [
            (&self.state_types[..], state_bits),
            (
                if present { &self.output_types[..] } else { &[] },
                output_bits,
            ),
        ] {
            validate_bitmap(types, bits)?;
            for (i, t) in types.iter().enumerate() {
                if !is_complex(t) {
                    direct_owned = checked_add(
                        direct_owned,
                        scan_direct(t, bits[i / 8] & (1 << (i % 8)) != 0, &mut probe)?,
                    )?;
                }
            }
        }
        let layout = if present {
            &self.with_emitted
        } else {
            &self.without_emitted
        };
        let base = checked_add(checked_add(bytes.len(), containers)?, direct_owned)?;
        workspace(base, max_bytes)?;
        let complex = layout.decode(&mut probe)?;
        ensure!(
            probe.bytes.is_empty(),
            "aggregate state contains trailing data"
        );
        let mut complex_index = 0;
        let mut decode_values = |types: &[DataType], bits: &[u8]| -> Result<Vec<ScalarValue>> {
            let mut values = Vec::new();
            values.try_reserve_exact(types.len())?;
            for (i, t) in types.iter().enumerate() {
                values.push(if is_complex(t) {
                    let batch = complex
                        .as_ref()
                        .context("missing aggregate complex value")?;
                    let value = ScalarValue::try_from_array(batch.column(complex_index), 0)?;
                    complex_index += 1;
                    value
                } else {
                    decode_direct(t, bits[i / 8] & (1 << (i % 8)) != 0, &mut input)?
                });
            }
            Ok(values)
        };
        let accumulator_state = decode_values(&self.state_types, state_bits)?;
        let last_emitted = if present {
            Some(decode_values(&self.output_types, output_bits)?)
        } else {
            None
        };
        Ok(EncodedGroup {
            last_update_nanos,
            generation,
            next_ordinal,
            accumulator_state,
            last_emitted,
        })
    }
}
fn validate_bitmap(types: &[DataType], bits: &[u8]) -> Result<()> {
    if !types.len().is_multiple_of(8) {
        ensure!(
            bits.last().copied().unwrap_or(0) >> (types.len() % 8) == 0,
            "invalid aggregate bitmap padding"
        );
    }
    for (i, t) in types.iter().enumerate() {
        let present = bits[i / 8] & (1 << (i % 8)) != 0;
        if is_complex(t) {
            ensure!(present, "missing aggregate complex field");
        }
        if *t == DataType::Null {
            ensure!(!present, "nonnull aggregate Null field");
        }
    }
    Ok(())
}
fn variable_size(len: usize) -> Result<usize> {
    u32::try_from(len).context("aggregate variable payload exceeds u32 framing")?;
    checked_add(4, len)
}

fn direct_size(value: &ScalarValue) -> Result<usize> {
    if value.is_null() {
        return Ok(0);
    }
    Ok(match value {
        ScalarValue::Int8(_) | ScalarValue::UInt8(_) => 1,
        ScalarValue::Int16(_) | ScalarValue::UInt16(_) => 2,
        ScalarValue::Int32(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::Date32(_)
        | ScalarValue::Time32Second(_)
        | ScalarValue::Time32Millisecond(_)
        | ScalarValue::IntervalYearMonth(_) => 4,
        ScalarValue::Int64(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Date64(_)
        | ScalarValue::Time64Microsecond(_)
        | ScalarValue::Time64Nanosecond(_)
        | ScalarValue::DurationSecond(_)
        | ScalarValue::DurationMillisecond(_)
        | ScalarValue::DurationMicrosecond(_)
        | ScalarValue::DurationNanosecond(_) => 8,
        ScalarValue::Boolean(_) => 1,
        ScalarValue::Float16(_) => 2,
        ScalarValue::Float32(_) => 4,
        ScalarValue::Float64(_) => 8,
        ScalarValue::Decimal128(_, _, _) => 16,
        ScalarValue::Decimal256(_, _, _) => 32,
        ScalarValue::TimestampSecond(_, _)
        | ScalarValue::TimestampMillisecond(_, _)
        | ScalarValue::TimestampMicrosecond(_, _)
        | ScalarValue::TimestampNanosecond(_, _) => 8,
        ScalarValue::IntervalDayTime(_) => 8,
        ScalarValue::IntervalMonthDayNano(_) => 16,
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => variable_size(v.len())?,
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => variable_size(v.len())?,
        ScalarValue::FixedSizeBinary(n, Some(v)) => {
            ensure!(
                usize::try_from(*n)? == v.len(),
                "aggregate fixed binary width changed"
            );
            v.len()
        }
        ScalarValue::Null
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::Dictionary(_, _) => bail!("invalid direct aggregate scalar"),
        ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::FixedSizeBinary(_, None) => 0,
    })
}
fn encode_direct(value: &ScalarValue, out: &mut Vec<u8>) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    match value {
        ScalarValue::Int8(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int16(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt16(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date32(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time32Second(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time32Millisecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::IntervalYearMonth(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date64(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time64Microsecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time64Nanosecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::DurationSecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::DurationMillisecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::DurationMicrosecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::DurationNanosecond(Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Boolean(Some(v)) => out.push(u8::from(*v)),
        ScalarValue::Float16(Some(v)) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Float32(Some(v)) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Float64(Some(v)) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Decimal128(Some(v), _, _) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Decimal256(Some(v), _, _) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _) => out.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::IntervalDayTime(Some(v)) => {
            out.extend_from_slice(&v.days.to_le_bytes());
            out.extend_from_slice(&v.milliseconds.to_le_bytes());
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            out.extend_from_slice(&v.months.to_le_bytes());
            out.extend_from_slice(&v.days.to_le_bytes());
            out.extend_from_slice(&v.nanoseconds.to_le_bytes());
        }
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => {
            out.extend_from_slice(&u32::try_from(v.len())?.to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => {
            out.extend_from_slice(&u32::try_from(v.len())?.to_le_bytes());
            out.extend_from_slice(v);
        }
        ScalarValue::FixedSizeBinary(_, Some(v)) => out.extend_from_slice(v),
        ScalarValue::Null
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::Dictionary(_, _) => bail!("invalid direct aggregate scalar"),
        ScalarValue::Boolean(None)
        | ScalarValue::Float16(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::Time32Second(None)
        | ScalarValue::Time32Millisecond(None)
        | ScalarValue::Time64Microsecond(None)
        | ScalarValue::Time64Nanosecond(None)
        | ScalarValue::IntervalYearMonth(None)
        | ScalarValue::IntervalDayTime(None)
        | ScalarValue::IntervalMonthDayNano(None)
        | ScalarValue::DurationSecond(None)
        | ScalarValue::DurationMillisecond(None)
        | ScalarValue::DurationMicrosecond(None)
        | ScalarValue::DurationNanosecond(None)
        | ScalarValue::FixedSizeBinary(_, None)
        | ScalarValue::Decimal128(None, _, _)
        | ScalarValue::Decimal256(None, _, _)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _) => {}
    }
    Ok(())
}
fn fixed_width(t: &DataType) -> Result<usize> {
    Ok(match t {
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => 8,
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::FixedSizeBinary(n) => usize::try_from(*n)?,
        _ => bail!("invalid direct aggregate type"),
    })
}
fn scan_direct(t: &DataType, present: bool, input: &mut Cursor<'_>) -> Result<usize> {
    if !present {
        return Ok(0);
    }
    match t {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            let n = input.u32()?;
            std::str::from_utf8(input.take(n)?)?;
            Ok(n)
        }
        DataType::Binary | DataType::BinaryView | DataType::LargeBinary => {
            let n = input.u32()?;
            input.take(n)?;
            Ok(n)
        }
        DataType::FixedSizeBinary(n) => {
            let n = usize::try_from(*n)?;
            input.take(n)?;
            Ok(n)
        }
        DataType::Boolean => {
            ensure!(input.take(1)?[0] <= 1, "invalid aggregate Boolean");
            Ok(0)
        }
        _ => {
            input.take(fixed_width(t)?)?;
            Ok(0)
        }
    }
}
fn decode_direct(t: &DataType, present: bool, input: &mut Cursor<'_>) -> Result<ScalarValue> {
    if !present {
        return Ok(ScalarValue::try_from(t)?);
    }
    Ok(match t {
        DataType::Int8=>ScalarValue::Int8(Some(i8::from_le_bytes(input.fixed()?))),
        DataType::UInt8=>ScalarValue::UInt8(Some(u8::from_le_bytes(input.fixed()?))),
        DataType::Int16=>ScalarValue::Int16(Some(i16::from_le_bytes(input.fixed()?))),
        DataType::UInt16=>ScalarValue::UInt16(Some(u16::from_le_bytes(input.fixed()?))),
        DataType::Int32=>ScalarValue::Int32(Some(i32::from_le_bytes(input.fixed()?))),
        DataType::UInt32=>ScalarValue::UInt32(Some(u32::from_le_bytes(input.fixed()?))),
        DataType::Date32=>ScalarValue::Date32(Some(i32::from_le_bytes(input.fixed()?))),
        DataType::Time32(TimeUnit::Second)=>ScalarValue::Time32Second(Some(i32::from_le_bytes(input.fixed()?))),
        DataType::Time32(TimeUnit::Millisecond)=>ScalarValue::Time32Millisecond(Some(i32::from_le_bytes(input.fixed()?))),
        DataType::Interval(IntervalUnit::YearMonth)=>ScalarValue::IntervalYearMonth(Some(i32::from_le_bytes(input.fixed()?))),
        DataType::Int64=>ScalarValue::Int64(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::UInt64=>ScalarValue::UInt64(Some(u64::from_le_bytes(input.fixed()?))),
        DataType::Date64=>ScalarValue::Date64(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Time64(TimeUnit::Microsecond)=>ScalarValue::Time64Microsecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Time64(TimeUnit::Nanosecond)=>ScalarValue::Time64Nanosecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Duration(TimeUnit::Second)=>ScalarValue::DurationSecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Duration(TimeUnit::Millisecond)=>ScalarValue::DurationMillisecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Duration(TimeUnit::Microsecond)=>ScalarValue::DurationMicrosecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Duration(TimeUnit::Nanosecond)=>ScalarValue::DurationNanosecond(Some(i64::from_le_bytes(input.fixed()?))),
        DataType::Boolean=>ScalarValue::Boolean(Some(input.take(1)?[0]==1)),
        DataType::Float16=>ScalarValue::Float16(Some(<arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native::from_bits(u16::from_le_bytes(input.fixed()?)))),
        DataType::Float32=>ScalarValue::Float32(Some(f32::from_bits(u32::from_le_bytes(input.fixed()?)))),
        DataType::Float64=>ScalarValue::Float64(Some(f64::from_bits(u64::from_le_bytes(input.fixed()?)))),
        DataType::Decimal128(p,s)=>ScalarValue::Decimal128(Some(i128::from_le_bytes(input.fixed()?)),*p,*s),
        DataType::Decimal256(p,s)=>ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_le_bytes(input.fixed()?)),*p,*s),
        DataType::Timestamp(unit,tz)=>{let v=Some(i64::from_le_bytes(input.fixed()?));match unit {TimeUnit::Second=>ScalarValue::TimestampSecond(v,tz.clone()),TimeUnit::Millisecond=>ScalarValue::TimestampMillisecond(v,tz.clone()),TimeUnit::Microsecond=>ScalarValue::TimestampMicrosecond(v,tz.clone()),TimeUnit::Nanosecond=>ScalarValue::TimestampNanosecond(v,tz.clone())}},
        DataType::Interval(IntervalUnit::DayTime)=>ScalarValue::IntervalDayTime(Some(arrow::datatypes::IntervalDayTime::new(i32::from_le_bytes(input.fixed()?),i32::from_le_bytes(input.fixed()?)))),
        DataType::Interval(IntervalUnit::MonthDayNano)=>ScalarValue::IntervalMonthDayNano(Some(arrow::datatypes::IntervalMonthDayNano::new(i32::from_le_bytes(input.fixed()?),i32::from_le_bytes(input.fixed()?),i64::from_le_bytes(input.fixed()?)))),
        DataType::Utf8|DataType::Utf8View|DataType::LargeUtf8=>{let n=input.u32()?;let v=Some(std::str::from_utf8(input.take(n)?)?.to_owned());match t {DataType::Utf8=>ScalarValue::Utf8(v),DataType::Utf8View=>ScalarValue::Utf8View(v),_=>ScalarValue::LargeUtf8(v)}},
        DataType::Binary|DataType::BinaryView|DataType::LargeBinary=>{let n=input.u32()?;let v=Some(input.take(n)?.to_vec());match t {DataType::Binary=>ScalarValue::Binary(v),DataType::BinaryView=>ScalarValue::BinaryView(v),_=>ScalarValue::LargeBinary(v)}},
        DataType::FixedSizeBinary(n)=>ScalarValue::FixedSizeBinary(*n,Some(input.take(usize::try_from(*n)?)?.to_vec())),
        _=>bail!("invalid direct aggregate type"),
    })
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
            for offset in [0, 8, 33, 64, 65] {
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
    fn empty_accumulator_state_round_trips_without_arrow() {
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

#[cfg(test)]
#[path = "aggregate_codec_tests.rs"]
mod coverage;
