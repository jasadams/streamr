use super::*;
use arrow_array::builder::{MapBuilder, StringBuilder};
use arrow_array::{FixedSizeListArray, Int32Array, StructArray};
use arrow_schema::{UnionFields, UnionMode};
use std::collections::HashMap;
use std::sync::Arc;

fn roundtrip(values: Vec<ScalarValue>) {
    let types = values
        .iter()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: i64::MIN,
        generation: u64::MAX,
        next_ordinal: u64::MAX,
        live_rows: 0,
        accumulator_state: values.clone(),
        last_emitted: Some(values),
    };
    let codec = GroupCodec::new(&types, &types).unwrap();
    let bytes = codec.encode(&group, 1024 * 1024).unwrap();
    let decoded = GroupCodec::new(&types, &types)
        .unwrap()
        .decode(&bytes, 1024 * 1024)
        .unwrap();
    for (actual, expected) in decoded
        .accumulator_state
        .iter()
        .zip(&group.accumulator_state)
    {
        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual, expected);
    }
    assert_eq!(decoded.last_emitted, group.last_emitted);
    assert_eq!(decoded.last_update_nanos, i64::MIN);
    assert_eq!(decoded.generation, u64::MAX);
    assert_eq!(decoded.next_ordinal, u64::MAX);
    let nulls = types
        .iter()
        .map(|t| ScalarValue::try_from(t).unwrap())
        .collect::<Vec<_>>();
    let group = EncodedGroup {
        accumulator_state: nulls.clone(),
        last_emitted: Some(nulls),
        ..group
    };
    let bytes = codec.encode(&group, 1024 * 1024).unwrap();
    let decoded = codec.decode(&bytes, 1024 * 1024).unwrap();
    assert_eq!(decoded.accumulator_state, group.accumulator_state);
    assert_eq!(decoded.last_emitted, group.last_emitted);
}

#[test]
fn all_direct_scalar_variants_retain_type_bits_and_nulls() {
    roundtrip(vec![
        ScalarValue::Null,ScalarValue::Boolean(Some(true)),
        ScalarValue::Int8(Some(i8::MIN)),ScalarValue::Int16(Some(i16::MIN)),ScalarValue::Int32(Some(i32::MIN)),ScalarValue::Int64(Some(i64::MIN)),
        ScalarValue::UInt8(Some(u8::MAX)),ScalarValue::UInt16(Some(u16::MAX)),ScalarValue::UInt32(Some(u32::MAX)),ScalarValue::UInt64(Some(u64::MAX)),
        ScalarValue::Float16(Some(<arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native::from_bits(0xfe01))),
        ScalarValue::Float32(Some(f32::from_bits(0xffc00123))),ScalarValue::Float64(Some(f64::from_bits(0xfff8000000001234))),
        ScalarValue::Float32(Some(-0.0)),ScalarValue::Float64(Some(-0.0)),ScalarValue::Float32(Some(f32::INFINITY)),ScalarValue::Float64(Some(f64::NEG_INFINITY)),
        ScalarValue::Decimal128(Some(-1234567),38,-3),ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_i128(-1234567)),76,7),
        ScalarValue::Utf8(Some("α\0🙂".into())),ScalarValue::Utf8View(Some("".into())),ScalarValue::LargeUtf8(Some("long".repeat(1024))),
        ScalarValue::Binary(Some(vec![0,255,1])),ScalarValue::BinaryView(Some(vec![])),ScalarValue::LargeBinary(Some(vec![255;4096])),ScalarValue::FixedSizeBinary(3,Some(vec![255,0,2])),
        ScalarValue::Date32(Some(i32::MIN)),ScalarValue::Date64(Some(i64::MIN)),
        ScalarValue::Time32Second(Some(-1)),ScalarValue::Time32Millisecond(Some(-2)),ScalarValue::Time64Microsecond(Some(-3)),ScalarValue::Time64Nanosecond(Some(-4)),
        ScalarValue::TimestampSecond(Some(-1),Some("Australia/Sydney".into())),ScalarValue::TimestampMillisecond(Some(-2),None),ScalarValue::TimestampMicrosecond(Some(-3),Some("UTC".into())),ScalarValue::TimestampNanosecond(Some(i64::MIN),None),
        ScalarValue::IntervalYearMonth(Some(i32::MIN)),ScalarValue::IntervalDayTime(Some(arrow::datatypes::IntervalDayTime::new(-1,i32::MIN))),ScalarValue::IntervalMonthDayNano(Some(arrow::datatypes::IntervalMonthDayNano::new(-1,2,i64::MIN))),
        ScalarValue::DurationSecond(Some(i64::MIN)),ScalarValue::DurationMillisecond(Some(-2)),ScalarValue::DurationMicrosecond(Some(-3)),ScalarValue::DurationNanosecond(Some(-4)),
    ]);
}

#[test]
fn complex_scalar_families_and_declared_metadata_round_trip() {
    let child = Arc::new(Field::new("item", DataType::Int32, true));
    let fixed = ScalarValue::FixedSizeList(Arc::new(
        FixedSizeListArray::try_new(
            child,
            2,
            Arc::new(Int32Array::from(vec![Some(1), None])),
            None,
        )
        .unwrap(),
    ));
    let field = Arc::new(
        Field::new("named", DataType::Int32, true)
            .with_metadata(HashMap::from([("business-free".into(), "metadata".into())])),
    );
    let structure = ScalarValue::Struct(Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7)]))],
        None,
    )));
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    map.keys().append_value("k");
    map.values().append_null();
    map.append(true).unwrap();
    let map = ScalarValue::Map(Arc::new(map.finish()));
    roundtrip(vec![
        ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Utf8(Some("x".into())), ScalarValue::Utf8(None)],
            &DataType::Utf8,
            true,
        )),
        ScalarValue::LargeList(ScalarValue::new_large_list(
            &[ScalarValue::Int32(None), ScalarValue::Int32(Some(5))],
            &DataType::Int32,
        )),
        fixed,
        structure,
        map,
    ]);
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        let fields = UnionFields::new(
            vec![3, 7],
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("s", DataType::Utf8, true),
            ],
        );
        // None union itself is not assumed equivalent to a selected null child.
        for selected in [
            ScalarValue::Utf8(Some("union".into())),
            ScalarValue::Utf8(None),
        ] {
            let value = ScalarValue::Union(Some((7, Box::new(selected))), fields.clone(), mode);
            let types = [value.data_type()];
            let codec = GroupCodec::new(&types, &[]).unwrap();
            let group = EncodedGroup {
                calendar_references: CalendarReferences::default(),
                last_update_nanos: 0,
                generation: 0,
                next_ordinal: 0,
                live_rows: 0,
                accumulator_state: vec![value.clone()],
                last_emitted: None,
            };
            let bytes = codec.encode(&group, 1024 * 1024).unwrap();
            let decoded = codec.decode(&bytes, 1024 * 1024).unwrap();
            assert_eq!(decoded.accumulator_state[0].data_type(), value.data_type());
            assert_eq!(decoded.accumulator_state[0], value);
        }
    }
    for key in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        roundtrip(vec![ScalarValue::Dictionary(
            Box::new(key),
            Box::new(ScalarValue::Utf8(Some("dictionary".into()))),
        )]);
    }
    let nested = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("nested".into()))),
        )],
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    ));
    roundtrip(vec![nested]);
}

#[test]
fn declared_output_identity_and_empty_presence_are_explicit() {
    let types = [DataType::Int64];
    let outputs = [DataType::Utf8];
    let codec = GroupCodec::new(&types, &outputs).unwrap();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![ScalarValue::Int64(Some(1))],
        last_emitted: None,
    };
    let bytes = codec.encode(&group, 8192).unwrap();
    assert!(
        GroupCodec::new(&types, &[])
            .unwrap()
            .decode(&bytes, 8192)
            .is_err()
    );
    assert!(
        GroupCodec::new(&types, &outputs)
            .unwrap()
            .decode(&bytes, 8192)
            .unwrap()
            .last_emitted
            .is_none()
    );
    let empty = GroupCodec::new(&types, &[]).unwrap();
    let group = EncodedGroup {
        last_emitted: Some(vec![]),
        ..group
    };
    let bytes = empty.encode(&group, 8192).unwrap();
    assert_eq!(
        empty.decode(&bytes, 8192).unwrap().last_emitted,
        Some(vec![])
    );
}

#[test]
fn primitive_layout_size_and_corruption_have_bounded_failure() {
    let codec =
        GroupCodec::new(&[DataType::Int64, DataType::Utf8, DataType::Boolean], &[]).unwrap();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Utf8(Some("ok".into())),
            ScalarValue::Boolean(Some(true)),
        ],
        last_emitted: None,
    };
    let bytes = codec.encode(&group, 8192).unwrap();
    assert_eq!(bytes.len(), HEADER_BYTES + 1 + 8 + 4 + 2 + 1);
    for n in 0..bytes.len() {
        assert!(codec.decode(&bytes[..n], 8192).is_err());
    }
    for (offset, value) in [(8, 2), (73, 0x80), (82, 255), (86, 255), (88, 2)] {
        let mut invalid = bytes.clone();
        invalid[offset] = value;
        assert!(
            std::panic::catch_unwind(|| codec.decode(&invalid, 8192))
                .unwrap()
                .is_err()
        );
    }
    assert!(codec.encode(&group, bytes.len() - 1).is_err());
    assert!(codec.decode(&bytes, bytes.len() - 1).is_err());
    // Encoded size is not decoded admission: fixed ScalarValue ownership still
    // uses the unchanged configured limit, even for compact numeric/null rows.
    let tiny = GroupCodec::new(&vec![DataType::Int64; 32], &[]).unwrap();
    let group = EncodedGroup {
        accumulator_state: vec![ScalarValue::Int64(None); 32],
        ..group
    };
    assert!(tiny.encode(&group, 128).is_ok());
    let bytes = tiny.encode(&group, 8192).unwrap();
    assert!(tiny.decode(&bytes, 128).is_err());
    assert_eq!(
        tiny.decode(&bytes, 8192).unwrap().accumulator_state.len(),
        32
    );
}

#[test]
fn schema_validation_does_not_materialize_fixed_width_children() {
    for t in [
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeList(Arc::new(Field::new("child", DataType::Int64, true)), -1),
    ] {
        assert!(
            std::panic::catch_unwind(|| GroupCodec::new(&[t], &[]))
                .unwrap()
                .is_err()
        );
    }
    // This schema is small even though materializing a null value would need
    // billions of integer slots. Construction must inspect metadata only.
    let wide = DataType::FixedSizeList(
        Arc::new(Field::new("child", DataType::Int64, true)),
        i32::MAX,
    );
    assert!(GroupCodec::new(&[wide], &[]).is_ok());
    let child = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", DataType::Utf8, true)),
    );
    assert!(
        GroupCodec::new(
            &[DataType::List(Arc::new(Field::new("item", child, true)))],
            &[]
        )
        .is_ok()
    );
}

#[test]
fn caller_owned_capacity_does_not_become_a_new_admission_limit() {
    let mut text = String::with_capacity(1024 * 1024);
    text.push('x');
    let codec = GroupCodec::new(&[DataType::Utf8], &[]).unwrap();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![ScalarValue::Utf8(Some(text))],
        last_emitted: None,
    };
    let bytes = codec.encode(&group, 1024).unwrap();
    assert_eq!(
        codec.decode(&bytes, 1024).unwrap().accumulator_state,
        group.accumulator_state
    );
    let values = Arc::new(Int32Array::from_iter_values(0..32768)) as arrow_array::ArrayRef;
    let list = arrow_array::ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        arrow::buffer::OffsetBuffer::new(vec![0, 1].into()),
        values,
        None,
    );
    let group = EncodedGroup {
        accumulator_state: vec![ScalarValue::List(Arc::new(list))],
        ..group
    };
    let codec = GroupCodec::new(&[group.accumulator_state[0].data_type()], &[]).unwrap();
    let bytes = codec.encode(&group, 4096).unwrap();
    let decoded = codec.decode(&bytes, 4096).unwrap();
    assert_eq!(decoded.accumulator_state, group.accumulator_state);
}

#[test]
fn variable_framing_size_rejects_unrepresentable_lengths_before_output_allocation() {
    assert_eq!(variable_size(0).unwrap(), 4);
    if let Ok(too_large) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert!(variable_size(too_large).is_err());
        assert!(complex::message_size(too_large, 0).is_err());
        assert!(complex::message_size(0, too_large).is_err());
    }
}

// These edits start from valid writer-produced metadata. Offsets refer to its
// fixed structs/vector headers; no foreign FlatBuffer builder/parser is needed.
struct ComplexMetadata {
    nodes: usize,
    buffers: usize,
    variadic: Option<usize>,
    dictionary_data_slot: Option<usize>,
}

fn complex_frames(bytes: &[u8], section: usize) -> Vec<std::ops::Range<usize>> {
    let count = u32::from_le_bytes(bytes[section..section + 4].try_into().unwrap()) as usize;
    let mut start = section + 4;
    (0..count)
        .map(|_| {
            let metadata = u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap()) as usize;
            let body = u32::from_le_bytes(bytes[start + 4..start + 8].try_into().unwrap()) as usize;
            let frame = start..start + 8 + metadata + body;
            start = frame.end;
            frame
        })
        .collect()
}

fn complex_metadata(bytes: &[u8], frame: &std::ops::Range<usize>) -> ComplexMetadata {
    let start = frame.start + 8;
    let len = u32::from_le_bytes(bytes[frame.start..frame.start + 4].try_into().unwrap()) as usize;
    let metadata = &bytes[start..start + len];
    let message = arrow::ipc::root_as_message(metadata).unwrap();
    let dictionary = message.header_as_dictionary_batch();
    let batch = dictionary.map_or_else(
        || message.header_as_record_batch().unwrap(),
        |d| d.data().unwrap(),
    );
    let offset = |ptr: *const u8| start + (ptr as usize - metadata.as_ptr() as usize);
    let dictionary_data_slot = dictionary.map(|d| {
        let table = start + d._tab.loc();
        let distance = i32::from_le_bytes(bytes[table..table + 4].try_into().unwrap());
        let vtable = table.checked_add_signed(-(distance as isize)).unwrap();
        vtable + usize::from(arrow::ipc::DictionaryBatch::VT_DATA)
    });
    ComplexMetadata {
        nodes: offset(batch.nodes().unwrap().bytes().as_ptr()),
        buffers: offset(batch.buffers().unwrap().bytes().as_ptr()),
        variadic: batch
            .variadicBufferCounts()
            .map(|v| offset(v.bytes().as_ptr())),
        dictionary_data_slot,
    }
}

fn malformed_complex(codec: &GroupCodec, bytes: &[u8], expected: &str) {
    let error = std::panic::catch_unwind(|| codec.decode(bytes, 1024 * 1024))
        .expect("malformed complex record panicked")
        .unwrap_err();
    assert!(
        error.to_string().contains(expected),
        "expected {expected:?}, got {error:#}"
    );
}

fn complex_fixture(values: Vec<ScalarValue>) -> (GroupCodec, Vec<u8>, usize) {
    let types = values
        .iter()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    let codec = GroupCodec::new(&types, &[]).unwrap();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: values,
        last_emitted: None,
    };
    let direct = group
        .accumulator_state
        .iter()
        .filter(|v| !is_complex(&v.data_type()))
        .map(|v| direct_size(v).unwrap())
        .sum::<usize>();
    let section = HEADER_BYTES + bitmap_len(types.len()) + direct;
    let bytes = codec.encode(&group, 1024 * 1024).unwrap();
    assert!(codec.decode(&bytes, 1024 * 1024).is_ok());
    (codec, bytes, section)
}

#[test]
fn complex_preflight_rejects_missing_nodes_buffers_and_out_of_body_ranges() {
    let structure = ScalarValue::Struct(Arc::new(StructArray::new(
        vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7)]))],
        None,
    )));
    let (codec, bytes, section) = complex_fixture(vec![ScalarValue::Int64(Some(3)), structure]);
    let frame = complex_frames(&bytes, section).remove(0);
    let positions = complex_metadata(&bytes, &frame);
    let mut malformed = bytes.clone();
    malformed[positions.nodes - 4..positions.nodes].copy_from_slice(&0u32.to_le_bytes());
    malformed_complex(&codec, &malformed, "missing aggregate Arrow node");
    let mut malformed = bytes.clone();
    malformed[positions.buffers - 4..positions.buffers].copy_from_slice(&0u32.to_le_bytes());
    malformed_complex(&codec, &malformed, "missing aggregate Arrow buffer");
    let mut malformed = bytes.clone();
    malformed[positions.buffers..positions.buffers + 8].copy_from_slice(&i64::MAX.to_le_bytes());
    malformed_complex(&codec, &malformed, "aggregate Arrow buffer exceeds body");
    let mut malformed = bytes.clone();
    // Struct parent claims one null but its first validity descriptor is empty.
    malformed[positions.nodes + 8..positions.nodes + 16].copy_from_slice(&1i64.to_le_bytes());
    malformed[positions.buffers + 8..positions.buffers + 16].copy_from_slice(&0i64.to_le_bytes());
    malformed_complex(&codec, &malformed, "short aggregate validity buffer");
    let mut malformed = bytes.clone();
    // Envelope framing remains complete; only the IPC FlatBuffer root is bad.
    malformed[frame.start + 8..frame.start + 12].copy_from_slice(&u32::MAX.to_le_bytes());
    malformed_complex(&codec, &malformed, "invalid aggregate Arrow message");
    for end in section..bytes.len() {
        assert!(
            std::panic::catch_unwind(|| codec.decode(&bytes[..end], 1024 * 1024))
                .unwrap()
                .is_err()
        );
    }
}

#[test]
fn complex_preflight_rejects_short_union_and_invalid_variadic_buffers() {
    let fields = UnionFields::new(vec![3], vec![Field::new("integer", DataType::Int32, true)]);
    let union = ScalarValue::Union(
        Some((3, Box::new(ScalarValue::Int32(Some(7))))),
        fields,
        UnionMode::Dense,
    );
    let (codec, bytes, section) = complex_fixture(vec![union]);
    let frame = complex_frames(&bytes, section).remove(0);
    let positions = complex_metadata(&bytes, &frame);
    for (buffer, expected) in [
        (0, "short aggregate union IDs"),
        (1, "short aggregate union offsets"),
    ] {
        let mut malformed = bytes.clone();
        let length = positions.buffers + buffer * 16 + 8;
        malformed[length..length + 8].copy_from_slice(&0i64.to_le_bytes());
        malformed_complex(&codec, &malformed, expected);
    }
    let view = ScalarValue::Utf8View(Some("long enough for a view backing buffer".into()))
        .to_array()
        .unwrap();
    let structure = ScalarValue::Struct(Arc::new(StructArray::new(
        vec![Arc::new(Field::new("view", DataType::Utf8View, true))].into(),
        vec![view],
        None,
    )));
    let (codec, bytes, section) = complex_fixture(vec![structure]);
    let frame = complex_frames(&bytes, section).remove(0);
    let positions = complex_metadata(&bytes, &frame);
    let counts = positions.variadic.unwrap();
    let mut malformed = bytes.clone();
    malformed[counts - 4..counts].copy_from_slice(&0u32.to_le_bytes());
    malformed_complex(&codec, &malformed, "missing aggregate variadic count");
    let mut malformed = bytes.clone();
    malformed[counts..counts + 8].copy_from_slice(&i64::MAX.to_le_bytes());
    malformed_complex(&codec, &malformed, "invalid aggregate variadic count");
}

#[test]
fn complex_preflight_rejects_dictionary_data_order_and_truncated_messages() {
    let dictionary = ScalarValue::Dictionary(
        Box::new(DataType::Int32),
        Box::new(ScalarValue::Utf8(Some("dictionary".into()))),
    );
    let (codec, bytes, section) =
        complex_fixture(vec![ScalarValue::Boolean(Some(true)), dictionary]);
    let frames = complex_frames(&bytes, section);
    assert_eq!(frames.len(), 2);
    let positions = complex_metadata(&bytes, &frames[0]);
    let mut malformed = bytes.clone();
    let slot = positions.dictionary_data_slot.unwrap();
    malformed[slot..slot + 2].copy_from_slice(&0u16.to_le_bytes());
    malformed_complex(&codec, &malformed, "missing aggregate dictionary data");
    let mut malformed = bytes[..section + 4].to_vec();
    malformed.extend_from_slice(&bytes[frames[1].clone()]);
    malformed.extend_from_slice(&bytes[frames[0].clone()]);
    malformed_complex(&codec, &malformed, "invalid aggregate Arrow message order");
    let mut malformed = bytes.clone();
    malformed[section..section + 4].copy_from_slice(&0u32.to_le_bytes());
    malformed_complex(&codec, &malformed, "invalid aggregate Arrow message count");
    for end in section..bytes.len() {
        assert!(
            std::panic::catch_unwind(|| codec.decode(&bytes[..end], 1024 * 1024))
                .unwrap()
                .is_err()
        );
    }
}

#[test]
fn calendar_reference_codec_is_schema_bound_initialized_and_fixed_size() {
    let layout: Vec<_> = (0..8_u8).map(|index| vec![index, 7]).collect();
    let codec = GroupCodec::with_calendar_layout(&[DataType::Int64], &[], &layout).unwrap();
    let plain = GroupCodec::new(&[DataType::Int64], &[]).unwrap();
    let mut group = EncodedGroup {
        calendar_references: CalendarReferences::new(8, 32768).unwrap(),
        last_update_nanos: -1,
        generation: 4,
        next_ordinal: 9,
        live_rows: 0,
        accumulator_state: vec![ScalarValue::Int64(Some(3))],
        last_emitted: None,
    };
    group.calendar_references.set(0, i32::MIN).unwrap();
    group.calendar_references.set(7, i32::MAX).unwrap();
    let encoded = codec.encode(&group, 32768).unwrap();
    let decoded = codec.decode(&encoded, 32768).unwrap();
    assert_eq!(decoded.calendar_references.get(0).unwrap(), Some(i32::MIN));
    assert_eq!(decoded.calendar_references.get(1).unwrap(), None);
    assert_eq!(decoded.calendar_references.get(7).unwrap(), Some(i32::MAX));
    assert_eq!(decoded.generation, 4);
    assert_eq!(decoded.next_ordinal, 9);
    group.calendar_references = CalendarReferences::default();
    assert_eq!(
        encoded.len(),
        plain.encode(&group, 32768).unwrap().len() + 33
    );
    assert!(plain.decode(&encoded, 32768).is_err());
    let mut reordered = layout.clone();
    reordered.swap(0, 1);
    assert!(
        GroupCodec::with_calendar_layout(&[DataType::Int64], &[], &reordered)
            .unwrap()
            .decode(&encoded, 32768)
            .is_err()
    );
    assert!(codec.encode(&group, 32768).is_err());
    let mut cleared = decoded.calendar_references;
    cleared.clear();
    assert!((0..8).all(|index| cleared.get(index).unwrap().is_none()));
    assert!(cleared.get(8).is_err());
    assert!(CalendarReferences::new(usize::MAX, 32768).is_err());
}

#[test]
fn calendar_reference_codec_rejects_malformed_and_honors_tighter_value_boundary() {
    let layout = vec![vec![1]; 9];
    let codec = GroupCodec::with_calendar_layout(&[], &[], &layout).unwrap();
    let group = EncodedGroup {
        calendar_references: CalendarReferences::new(9, 4096).unwrap(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![],
        last_emitted: None,
    };
    let encoded = codec.encode(&group, 4096).unwrap();
    let mut invalid = encoded.clone();
    invalid[HEADER_BYTES + 1] = 0x80;
    assert!(codec.decode(&invalid, 4096).is_err());
    let mut invalid = encoded.clone();
    invalid[HEADER_BYTES + 2] = 1; // Noncanonical data in an uninitialized slot.
    assert!(codec.decode(&invalid, 4096).is_err());
    for length in 0..encoded.len() {
        assert!(codec.decode(&encoded[..length], 4096).is_err());
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert!(codec.decode(&trailing, 4096).is_err());

    let layout = vec![vec![1]; 8];
    let codec = GroupCodec::with_calendar_layout(&[DataType::Binary], &[], &layout).unwrap();
    let plain = GroupCodec::new(&[DataType::Binary], &[]).unwrap();
    let mut group = EncodedGroup {
        calendar_references: CalendarReferences::new(8, 32768).unwrap(),
        last_update_nanos: 0,
        generation: 0,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![ScalarValue::Binary(Some(vec![7; 32657]))],
        last_emitted: None,
    };
    assert_eq!(codec.encode(&group, 32768).unwrap().len(), 32768);
    group.accumulator_state = vec![ScalarValue::Binary(Some(vec![7; 32658]))];
    assert!(codec.encode(&group, 32768).is_err());
    group.calendar_references = CalendarReferences::default();
    assert_eq!(plain.encode(&group, 32768).unwrap().len(), 32736);
}

#[test]
fn calendar_bucket_and_contribution_membership_header_obeys_caps() {
    let bucket = EncodedGroup {
        calendar_references: CalendarReferences::default(),
        last_update_nanos: 0,
        generation: 1,
        next_ordinal: 0,
        live_rows: 0,
        accumulator_state: vec![ScalarValue::Int64(Some(7))],
        last_emitted: None,
    };
    assert_eq!(HEADER_BYTES, 73); // Previous fixed65-byte header plus membership8.
    let encoded = encode_group(&bucket, 4096).unwrap();
    assert_eq!(encoded.len(), 65 + 8 + 1 + 8);
    assert!(encode_group(&bucket, encoded.len() - 1).is_err());
    assert_eq!(
        decode_group(&encoded, &[DataType::Int64], &[], 4096)
            .unwrap()
            .live_rows,
        0
    );
    let contribution = encode_calendar_contribution(None, None, &[], 4096).unwrap();
    assert!(encode_calendar_contribution(None, None, &[], contribution.len() - 1).is_err());
    let mut invalid = contribution;
    invalid[33..41].copy_from_slice(&1_u64.to_le_bytes());
    assert!(decode_calendar_contribution(&invalid, &[], 4096).is_err());
    let mut obsolete = encoded;
    obsolete[7] = b'5';
    assert!(decode_group(&obsolete, &[DataType::Int64], &[], 4096).is_err());
}
