//! Shared grouped FIRST/LAST candidate semantics. Window lifecycle and backend
//! ownership stay with the callers; analytic OVER does not use this module.
use arrow::row::{RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, SortOptions};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

pub const ORDINAL_FIELD: &str = "__streamr_selector_ordinal_v1";
pub const ORDINAL_MARKER: &str = "__streamr_grouped_write_ordinal_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorKind {
    First,
    Last,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorTie {
    FirstWrite,
    LastWrite,
}

/// Existing lifecycle limits adapted to the shared candidate codec. These are
/// internal allowances, not an additional resource pool or public setting.
#[derive(Debug, Clone, Copy)]
pub struct SelectorBudget {
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub decoded_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SelectorCandidate {
    pub key: Vec<u8>,
    pub args: Vec<u8>,
    pub ordinal: u64,
}

#[derive(Debug)]
pub struct SelectorSpec {
    value_type: DataType,
    argument_types: Vec<DataType>,
    order_types: Vec<DataType>,
    order_options: Vec<SortOptions>,
    kind: SelectorKind,
    tie: SelectorTie,
    ignore_nulls: bool,
    order_codec: RowConverter,
    schema: SchemaRef,
    ipc_overhead: usize,
    ipc_prefix: Vec<u8>,
    identity: [u8; 16],
}

const MAGIC: &[u8; 8] = b"STRSEL02";
fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| error("grouped selector size overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| error("grouped selector size overflow"))
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(error(message)) }
}

/// One array node's fixed decode shape, shared with collection admission.
/// Pinned Arrow 55 row decoding constructs Struct children even for null parents.
pub fn row_node_decode_bytes(node: &DataType) -> Result<usize> {
    let wrapper = [
        std::mem::size_of::<StructArray>(),
        std::mem::size_of::<arrow_array::StringArray>(),
        std::mem::size_of::<arrow_array::LargeStringArray>(),
        std::mem::size_of::<arrow_array::BinaryArray>(),
        std::mem::size_of::<arrow_array::LargeBinaryArray>(),
        std::mem::size_of::<arrow_array::FixedSizeBinaryArray>(),
        std::mem::size_of::<arrow_array::NullArray>(),
        std::mem::size_of::<arrow_array::BooleanArray>(),
        std::mem::size_of::<arrow_array::Int64Array>(),
        std::mem::size_of::<arrow_array::Decimal256Array>(),
    ]
    .into_iter()
    .max()
    .unwrap();
    let mut bytes = node.size();
    for part in [
        2 * std::mem::size_of::<arrow::array::ArrayData>(),
        std::mem::size_of::<arrow::array::ArrayDataBuilder>(),
        wrapper,
        4 * std::mem::size_of::<arrow::buffer::Buffer>(),
        std::mem::size_of::<ScalarValue>(),
        2 * std::mem::size_of::<ArrayRef>(),
        std::mem::size_of::<usize>() * (2 + 3 * 8),
        3 * 64,
    ] {
        bytes = add(bytes, part)?;
    }
    if let DataType::FixedSizeBinary(width) = node {
        bytes = add(
            bytes,
            usize::try_from(*width).map_err(|_| error("negative fixed binary width"))?,
        )?;
    }
    Ok(bytes)
}

// Lower bound for one child's row encoding. Variable/list NULL has one byte;
// fixed scalar/struct children retain their fixed slots even under nullness.
fn minimum_row_bytes(kind: &DataType) -> Result<usize> {
    Ok(match kind {
        DataType::Struct(fields) => fields
            .iter()
            .try_fold(1, |n, f| add(n, minimum_row_bytes(f.data_type())?))?,
        DataType::Dictionary(_, value) => minimum_row_bytes(value)?,
        DataType::FixedSizeBinary(width) => add(
            1,
            usize::try_from(*width).map_err(|_| error("negative fixed binary width"))?,
        )?,
        DataType::Null => 0,
        DataType::Boolean => 2,
        other => other.primitive_width().map_or(1, |n| n + 1),
    })
}

fn row_shape(kind: &DataType, rows: usize, encoded: usize) -> Result<usize> {
    // ArrayData/builder/Arc/buffer headers are per array. Offsets, row slices,
    // temporary decoded offsets and fixed payload grow with child cardinality.
    let width = match kind {
        DataType::FixedSizeBinary(width) => {
            usize::try_from(*width).map_err(|_| error("negative fixed binary width"))?
        }
        _ => kind.primitive_width().unwrap_or(16),
    };
    let mut bytes = add(
        row_node_decode_bytes(kind)?,
        mul(
            rows.saturating_sub(1),
            add(width, 4 * std::mem::size_of::<usize>())?,
        )?,
    )?;
    match kind {
        DataType::Struct(fields) => {
            for field in fields {
                bytes = add(bytes, row_shape(field.data_type(), rows, encoded)?)?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            let child_rows = encoded / minimum_row_bytes(field.data_type())?.max(1);
            bytes = add(bytes, row_shape(field.data_type(), child_rows, encoded)?)?;
            // list::decode retains copied encoded child bytes plus offsets and
            // borrowed child-row slices until its recursive decode completes.
            bytes = add(bytes, mul(encoded, 2)?)?;
        }
        DataType::FixedSizeList(field, width) => {
            let width = usize::try_from(*width).map_err(|_| error("negative fixed list width"))?;
            bytes = add(
                bytes,
                row_shape(field.data_type(), mul(rows, width)?, encoded)?,
            )?;
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                bytes = add(bytes, row_shape(field.data_type(), rows, encoded)?)?;
            }
        }
        DataType::Dictionary(_, value) => bytes = add(bytes, row_shape(value, rows, encoded)?)?,
        DataType::RunEndEncoded(ends, values) => {
            bytes = add(bytes, row_shape(ends.data_type(), rows, encoded)?)?;
            bytes = add(bytes, row_shape(values.data_type(), rows, encoded)?)?;
            bytes = add(bytes, mul(encoded, 2)?)?;
        }
        // RowConverter rejects other unsupported nested types itself. Admission
        // does not replace its type support or introduce a selector-only list.
        _ => {}
    }
    Ok(bytes)
}

pub fn decoded_row_bound(types: &[DataType], encoded_bytes: usize) -> Result<usize> {
    let shape = types.iter().try_fold(0, |n, kind| {
        add(n, add(kind.size(), row_shape(kind, 1, encoded_bytes)?)?)
    })?;
    mul(add(shape, encoded_bytes)?, 3)
}

// IPC writes are capped independently of estimates; an oversized logical value
// is refused by inspect_selected before any compaction, row conversion or IPC.
struct CappedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "grouped selector IPC exceeds configured encoded-state allowance",
            ));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn schema_nodes(types: &[DataType]) -> Result<usize> {
    types.iter().try_fold(0, |n, kind| {
        let children = match kind {
            DataType::Struct(fields) => schema_nodes(
                &fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => schema_nodes(&[f.data_type().clone()])?,
            DataType::Dictionary(_, value) => schema_nodes(&[value.as_ref().clone()])?,
            DataType::RunEndEncoded(ends, values) => {
                schema_nodes(&[ends.data_type().clone(), values.data_type().clone()])?
            }
            DataType::Union(fields, _) => schema_nodes(
                &fields
                    .iter()
                    .map(|(_, f)| f.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            _ => 0,
        };
        add(n, add(1, children)?)
    })
}
fn ipc_decoded_bound(types: &[DataType], bytes: &[u8]) -> Result<usize> {
    let mut bound = decoded_row_bound(types, 0)?;
    bound = add(bound, mul(bytes.len(), 3)?)?;
    let mut offset = 0usize;
    let mut messages = 0usize;
    let mut candidates = 0usize;
    let mut terminated = false;
    while offset < bytes.len() {
        require(
            bytes.len() - offset >= 4,
            "truncated grouped selector IPC framing",
        )?;
        let mut len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if len == u32::MAX as usize {
            require(
                bytes.len() - offset >= 4,
                "truncated grouped selector IPC continuation",
            )?;
            len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
        }
        if len == 0 {
            require(offset == bytes.len(), "trailing grouped selector IPC bytes")?;
            terminated = true;
            break;
        }
        let end = add(offset, len)?;
        require(
            end <= bytes.len(),
            "truncated grouped selector IPC metadata",
        )?;
        let message = arrow::ipc::root_as_message(&bytes[offset..end])
            .map_err(|e| error(format!("invalid grouped selector IPC metadata: {e}")))?;
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| error("negative grouped selector IPC body"))?;
        require(
            add(end, body_len)? <= bytes.len(),
            "truncated grouped selector IPC body",
        )?;
        if message.header_as_schema().is_some() {
            require(
                messages == 0 && body_len == 0,
                "grouped selector IPC schema must be the first message",
            )?;
        } else {
            require(
                messages > 0
                    && (message.header_as_record_batch().is_some()
                        || message.header_as_dictionary_batch().is_some()),
                "unsupported grouped selector IPC message",
            )?;
        }
        if let Some(batch) = message.header_as_record_batch() {
            candidates += 1;
            require(
                candidates == 1 && batch.length() == 1,
                "grouped selector IPC requires exactly one candidate row",
            )?;
        }
        messages += 1;
        let batch = message.header_as_record_batch().or_else(|| {
            message
                .header_as_dictionary_batch()
                .and_then(|dictionary| dictionary.data())
        });
        if let Some(batch) = batch {
            // The canonical codec never compresses. Refuse foreign compression
            // before Arrow can allocate from a claimed decompressed length.
            require(
                batch.compression().is_none(),
                "compressed grouped selector IPC is not canonical",
            )?;
            if let Some(buffers) = batch.buffers() {
                for buffer in buffers {
                    let start = usize::try_from(buffer.offset())
                        .map_err(|_| error("negative grouped selector IPC buffer offset"))?;
                    let length = usize::try_from(buffer.length())
                        .map_err(|_| error("negative grouped selector IPC buffer length"))?;
                    require(
                        add(start, length)? <= body_len,
                        "grouped selector IPC buffer exceeds bounded body",
                    )?;
                }
            }
            if let Some(nodes) = batch.nodes() {
                for node in nodes {
                    bound = add(
                        bound,
                        mul(
                            usize::try_from(node.length())
                                .map_err(|_| error("negative grouped selector IPC cardinality"))?,
                            64,
                        )?,
                    )?;
                }
            }
        }
        offset = add(
            end,
            usize::try_from(message.bodyLength())
                .map_err(|_| error("negative grouped selector IPC body"))?,
        )?;
        require(offset <= bytes.len(), "truncated grouped selector IPC body")?;
    }
    require(
        terminated && candidates == 1,
        "grouped selector IPC candidate framing incomplete",
    )?;
    Ok(bound)
}

/// Traversal and compaction use logical indices, never retained backing sizes.
struct LogicalSize {
    bytes: usize,
    visited: usize,
    byte_limit: usize,
    decoded_limit: usize,
    fixed_bytes: usize,
}
impl LogicalSize {
    fn new(byte_limit: usize, decoded_limit: usize, fixed_bytes: usize) -> Self {
        Self {
            bytes: 0,
            visited: 0,
            byte_limit,
            decoded_limit,
            fixed_bytes,
        }
    }
    // IPC metadata for variadic buffers can grow with selected nodes. Include
    // that growth in the encoded upper bound as well as logical payload bytes.
    fn payload_bound(&self) -> Result<usize> {
        add(self.bytes, mul(self.visited, 16)?)
    }
    fn workspace_bound(&self) -> Result<usize> {
        add(
            self.fixed_bytes,
            add(mul(self.visited, 64)?, mul(self.payload_bound()?, 3)?)?,
        )
    }
    fn ensure_extra(&self, extra: usize) -> Result<()> {
        require(
            add(self.workspace_bound()?, extra)? <= self.decoded_limit,
            "grouped selector logical value exceeds configured decoded-state allowance before materialization",
        )
    }
    fn charge(&mut self, bytes: usize, rows: usize) -> Result<()> {
        self.bytes = add(self.bytes, bytes)?;
        self.visited = add(self.visited, rows)?;
        require(
            self.payload_bound()? <= self.byte_limit,
            "grouped selector logical value exceeds configured encoded-state allowance before materialization",
        )?;
        self.ensure_extra(0)
    }
}
fn dictionary_value(array: &dyn Array, row: usize) -> Result<(ArrayRef, Option<usize>)> {
    macro_rules! dictionary {
        ($kind:ty) => {{
            let a = array.as_dictionary::<$kind>();
            (a.values().clone(), a.key(row))
        }};
    }
    Ok(match array.data_type() {
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary!(arrow_array::types::Int8Type),
            DataType::Int16 => dictionary!(arrow_array::types::Int16Type),
            DataType::Int32 => dictionary!(arrow_array::types::Int32Type),
            DataType::Int64 => dictionary!(arrow_array::types::Int64Type),
            DataType::UInt8 => dictionary!(arrow_array::types::UInt8Type),
            DataType::UInt16 => dictionary!(arrow_array::types::UInt16Type),
            DataType::UInt32 => dictionary!(arrow_array::types::UInt32Type),
            DataType::UInt64 => dictionary!(arrow_array::types::UInt64Type),
            _ => return Err(error("invalid grouped selector dictionary key")),
        },
        _ => return Err(error("expected grouped selector dictionary")),
    })
}
fn run_value(array: &dyn Array, row: usize) -> Result<(ArrayRef, usize)> {
    macro_rules! run {
        ($ty:ty) => {{
            let a = array.as_run::<$ty>();
            (a.values().clone(), a.get_physical_index(row))
        }};
    }
    Ok(match array.data_type() {
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => run!(arrow_array::types::Int16Type),
            DataType::Int32 => run!(arrow_array::types::Int32Type),
            DataType::Int64 => run!(arrow_array::types::Int64Type),
            _ => return Err(error("invalid selector run end type")),
        },
        _ => return Err(error("expected selector run array")),
    })
}

fn logical_null(array: &dyn Array, row: usize) -> Result<bool> {
    if array.is_null(row) {
        return Ok(true);
    }
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let (values, key) = dictionary_value(array, row)?;
            key.map_or(Ok(true), |key| logical_null(values.as_ref(), key))
        }
        DataType::RunEndEncoded(_, _) => {
            let (values, index) = run_value(array, row)?;
            logical_null(values.as_ref(), index)
        }
        DataType::Union(_, _) => {
            let array = array.as_union();
            logical_null(
                array.child(array.type_id(row)).as_ref(),
                array.value_offset(row),
            )
        }
        // Struct and fixed-list child nullness never makes a valid parent SQL
        // NULL. Other scalar nullness is represented by the selected slot.
        _ => Ok(false),
    }
}
fn inspect_selected(array: &dyn Array, row: Option<usize>, size: &mut LogicalSize) -> Result<()> {
    let kind = array.data_type();
    size.charge(1, 1)?;
    let row = row.filter(|row| !array.is_null(*row));
    match kind {
        DataType::Dictionary(key, _) => {
            size.charge(key.primitive_width().unwrap_or(8), 0)?;
            if let Some(row) = row {
                let (values, key) = dictionary_value(array, row)?;
                inspect_selected(values.as_ref(), key, size)?;
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            size.charge(ends.data_type().primitive_width().unwrap_or(8), 0)?;
            if let Some(row) = row {
                let (values, index) = run_value(array, row)?;
                inspect_selected(values.as_ref(), Some(index), size)?;
            }
        }
        DataType::Union(fields, mode) => {
            size.charge(5, 0)?;
            let a = array.as_union();
            let selected_type = row
                .map(|row| a.type_id(row))
                .or_else(|| fields.iter().next().map(|(id, _)| id));
            for (id, _) in fields.iter() {
                if *mode == arrow_schema::UnionMode::Sparse || Some(id) == selected_type {
                    let child = row
                        .filter(|row| a.type_id(*row) == id)
                        .map(|row| a.value_offset(row));
                    inspect_selected(a.child(id).as_ref(), child, size)?;
                }
            }
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            for (index, _) in fields.iter().enumerate() {
                inspect_selected(array.column(index).as_ref(), row, size)?;
            }
        }
        DataType::FixedSizeList(_, width) => {
            let array = array.as_fixed_size_list();
            let width = usize::try_from(*width).map_err(|_| error("negative fixed list width"))?;
            // Refuse an oversized child cardinality before building indices or
            // walking any child, including a null parent's implicit slots.
            size.charge(0, width)?;
            for child in 0..width {
                inspect_selected(
                    array.values().as_ref(),
                    row.map(|r| array.value_offset(r) as usize + child),
                    size,
                )?;
            }
        }
        DataType::List(_) => {
            size.charge(8, 0)?;
            if let Some(row) = row {
                let array = array.as_list::<i32>();
                let offsets = array.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                size.charge(0, end - start)?;
                for child in start..end {
                    inspect_selected(array.values().as_ref(), Some(child), size)?;
                }
            }
        }
        DataType::LargeList(_) => {
            size.charge(16, 0)?;
            if let Some(row) = row {
                let array = array.as_list::<i64>();
                let offsets = array.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                size.charge(0, end - start)?;
                for child in start..end {
                    inspect_selected(array.values().as_ref(), Some(child), size)?;
                }
            }
        }
        DataType::Map(_, _) => {
            size.charge(8, 0)?;
            if let Some(row) = row {
                let array = array.as_map();
                let offsets = array.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                size.charge(0, end - start)?;
                for child in start..end {
                    inspect_selected(array.entries(), Some(child), size)?;
                }
            }
        }
        DataType::Utf8 => {
            size.charge(
                8 + row.map_or(0, |r| array.as_string::<i32>().value_length(r) as usize),
                0,
            )?;
        }
        DataType::LargeUtf8 => {
            size.charge(
                16 + row.map_or(0, |r| array.as_string::<i64>().value_length(r) as usize),
                0,
            )?;
        }
        DataType::Binary => {
            size.charge(
                8 + row.map_or(0, |r| array.as_binary::<i32>().value_length(r) as usize),
                0,
            )?;
        }
        DataType::LargeBinary => {
            size.charge(
                16 + row.map_or(0, |r| array.as_binary::<i64>().value_length(r) as usize),
                0,
            )?;
        }
        DataType::Utf8View => {
            size.charge(
                16 + row.map_or(0, |r| array.as_string_view().value(r).len()),
                0,
            )?;
        }
        DataType::BinaryView => {
            size.charge(
                16 + row.map_or(0, |r| array.as_binary_view().value(r).len()),
                0,
            )?;
        }
        DataType::FixedSizeBinary(width) => size.charge(
            usize::try_from(*width).map_err(|_| error("negative fixed binary width"))?,
            0,
        )?,
        _ => size.charge(kind.primitive_width().unwrap_or(16), 0)?,
    }
    Ok(())
}
fn padded_row_bytes(bytes: usize) -> Result<usize> {
    if bytes <= 32 {
        add(1, mul(bytes.div_ceil(8), 9)?)
    } else {
        add(4, mul(bytes.div_ceil(32), 33)?)
    }
}
fn null_order_size(kind: &DataType) -> Result<usize> {
    match kind {
        DataType::Struct(fields) => fields
            .iter()
            .try_fold(1, |n, f| add(n, null_order_size(f.data_type())?)),
        DataType::Dictionary(_, value) => null_order_size(value),
        DataType::RunEndEncoded(_, value) => null_order_size(value.data_type()),
        DataType::Null => Ok(0),
        DataType::Boolean => Ok(2),
        DataType::FixedSizeBinary(width) => add(
            1,
            usize::try_from(*width).map_err(|_| error("negative fixed binary width"))?,
        ),
        other => Ok(other.primitive_width().map_or(1, |width| width + 1)),
    }
}
fn order_size(array: &dyn Array, row: Option<usize>, limit: usize) -> Result<usize> {
    let row = row.filter(|row| !array.is_null(*row));
    let bytes = match array.data_type() {
        DataType::RunEndEncoded(_, value) => {
            if let Some(row) = row {
                let (values, index) = run_value(array, row)?;
                order_size(values.as_ref(), Some(index), limit)?
            } else {
                null_order_size(value.data_type())?
            }
        }
        DataType::Dictionary(_, value) => {
            if let Some(row) = row {
                let (values, key) = dictionary_value(array, row)?;
                order_size(values.as_ref(), key, limit)?
            } else {
                null_order_size(value)?
            }
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            fields.iter().enumerate().try_fold(1, |n, (i, _)| {
                add(n, order_size(array.column(i).as_ref(), row, limit)?)
            })?
        }
        DataType::List(_) => {
            if let Some(row) = row {
                let a = array.as_list::<i32>();
                let offsets = a.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                require(
                    end - start <= limit,
                    "grouped selector order child cardinality exceeds key allowance",
                )?;
                let mut n = 1;
                for child in start..end {
                    n = add(
                        n,
                        padded_row_bytes(order_size(a.values().as_ref(), Some(child), limit)?)?,
                    )?;
                    require(
                        n <= limit,
                        "grouped selector order key exceeds configured key allowance",
                    )?;
                }
                n
            } else {
                1
            }
        }
        DataType::LargeList(_) => {
            if let Some(row) = row {
                let a = array.as_list::<i64>();
                let offsets = a.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                require(
                    end - start <= limit,
                    "grouped selector order child cardinality exceeds key allowance",
                )?;
                let mut n = 1;
                for child in start..end {
                    n = add(
                        n,
                        padded_row_bytes(order_size(a.values().as_ref(), Some(child), limit)?)?,
                    )?;
                    require(
                        n <= limit,
                        "grouped selector order key exceeds configured key allowance",
                    )?;
                }
                n
            } else {
                1
            }
        }
        DataType::Utf8 => {
            if let Some(row) = row {
                padded_row_bytes(array.as_string::<i32>().value_length(row) as usize)?
            } else {
                1
            }
        }
        DataType::LargeUtf8 => {
            if let Some(row) = row {
                padded_row_bytes(array.as_string::<i64>().value_length(row) as usize)?
            } else {
                1
            }
        }
        DataType::Binary => {
            if let Some(row) = row {
                padded_row_bytes(array.as_binary::<i32>().value_length(row) as usize)?
            } else {
                1
            }
        }
        DataType::LargeBinary => {
            if let Some(row) = row {
                padded_row_bytes(array.as_binary::<i64>().value_length(row) as usize)?
            } else {
                1
            }
        }
        DataType::Utf8View => {
            if let Some(row) = row {
                padded_row_bytes(array.as_string_view().value(row).len())?
            } else {
                1
            }
        }
        DataType::BinaryView => {
            if let Some(row) = row {
                padded_row_bytes(array.as_binary_view().value(row).len())?
            } else {
                1
            }
        }
        other => null_order_size(other)?,
    };
    require(
        bytes <= limit,
        "grouped selector order key exceeds configured key allowance before materialization",
    )?;
    Ok(bytes)
}
/// Exact bounded Arrow-row size for a logical one-row argument/key tuple.
/// Reuses the shared ordering encoder's selected-row traversal before callers
/// allocate RowConverter output; unused backing buffers are never charged.
pub fn bounded_row_size(values: &[ArrayRef], limit: usize) -> Result<usize> {
    values.iter().try_fold(0usize, |total, value| {
        require(
            value.len() == 1,
            "bounded row encoding requires one logical row",
        )?;
        let bytes = order_size(value.as_ref(), Some(0), limit.saturating_sub(total))?;
        let total = add(total, bytes)?;
        require(
            total <= limit,
            "encoded row exceeds configured allowance before materialization",
        )?;
        Ok(total)
    })
}

fn compact_selected(array: &dyn Array, selected: &[Option<usize>]) -> Result<ArrayRef> {
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    let valid = selected
        .iter()
        .map(|row| row.is_some_and(|r| !array.is_null(r)))
        .collect::<Vec<_>>();
    let nulls = Some(NullBuffer::from(valid.clone()));
    let selected = selected
        .iter()
        .zip(&valid)
        .map(|(row, valid)| if *valid { *row } else { None })
        .collect::<Vec<_>>();
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(arrow_array::StringViewArray::from_iter(
            selected
                .iter()
                .map(|row| row.map(|row| array.as_string_view().value(row))),
        )),
        DataType::BinaryView => Arc::new(arrow_array::BinaryViewArray::from_iter(
            selected
                .iter()
                .map(|row| row.map(|row| array.as_binary_view().value(row))),
        )),
        DataType::RunEndEncoded(ends, value_field) => {
            let mut source = None;
            let mut indices = Vec::with_capacity(selected.len());
            for row in &selected {
                if let Some(row) = row {
                    let (values, index) = run_value(array, *row)?;
                    source = Some(values);
                    indices.push(Some(index));
                } else {
                    indices.push(None)
                }
            }
            let source =
                source.unwrap_or_else(|| arrow_array::new_empty_array(value_field.data_type()));
            let values = compact_selected(source.as_ref(), &indices)?;
            macro_rules! run {
                ($ty:ty,$native:ty) => {{
                    let ends = arrow_array::PrimitiveArray::<$ty>::from(
                        (1..=selected.len())
                            .map(|end| {
                                <$native>::try_from(end)
                                    .map_err(|_| error("selector run length overflow"))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    );
                    Arc::new(arrow_array::RunArray::<$ty>::try_new(
                        &ends,
                        values.as_ref(),
                    )?) as ArrayRef
                }};
            }
            match ends.data_type() {
                DataType::Int16 => run!(arrow_array::types::Int16Type, i16),
                DataType::Int32 => run!(arrow_array::types::Int32Type, i32),
                DataType::Int64 => run!(arrow_array::types::Int64Type, i64),
                _ => return Err(error("invalid selector run end type")),
            }
        }
        DataType::Union(fields, mode) => {
            let a = array.as_union();
            let first = fields
                .iter()
                .next()
                .map(|(id, _)| id)
                .ok_or_else(|| error("empty selector union"))?;
            let type_ids = selected
                .iter()
                .map(|row| row.map_or(first, |row| a.type_id(row)))
                .collect::<Vec<_>>();
            let mut offsets = vec![0i32; selected.len()];
            let mut children = Vec::new();
            for (id, _) in fields.iter() {
                let mut indices = Vec::new();
                for (index, row) in selected.iter().enumerate() {
                    if *mode == arrow_schema::UnionMode::Sparse {
                        indices.push(
                            row.filter(|row| a.type_id(*row) == id)
                                .map(|row| a.value_offset(row)),
                        );
                    } else if type_ids[index] == id {
                        offsets[index] = i32::try_from(indices.len())
                            .map_err(|_| error("selector union offset overflow"))?;
                        indices.push(row.map(|row| a.value_offset(row)));
                    }
                }
                children.push(compact_selected(a.child(id).as_ref(), &indices)?);
            }
            Arc::new(arrow_array::UnionArray::try_new(
                fields.clone(),
                ScalarBuffer::from(type_ids),
                (*mode == arrow_schema::UnionMode::Dense).then(|| ScalarBuffer::from(offsets)),
                children,
            )?)
        }
        DataType::Struct(fields) => {
            let a = array.as_struct();
            let children = a
                .columns()
                .iter()
                .map(|child| compact_selected(child.as_ref(), &selected))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(fields.clone(), children, nulls)?)
        }
        DataType::FixedSizeList(field, width) => {
            let a = array.as_fixed_size_list();
            let width_usize =
                usize::try_from(*width).map_err(|_| error("negative fixed list width"))?;
            let children = selected
                .iter()
                .flat_map(|row| {
                    (0..width_usize)
                        .map(move |child| row.map(|row| a.value_offset(row) as usize + child))
                })
                .collect::<Vec<_>>();
            Arc::new(arrow_array::FixedSizeListArray::try_new(
                field.clone(),
                *width,
                compact_selected(a.values().as_ref(), &children)?,
                nulls,
            )?)
        }
        DataType::List(field) => {
            let a = array.as_list::<i32>();
            let mut children = Vec::new();
            let mut offsets = vec![0i32];
            for row in &selected {
                if let Some(row) = row {
                    children.extend(
                        (a.value_offsets()[*row] as usize..a.value_offsets()[*row + 1] as usize)
                            .map(Some),
                    );
                }
                offsets.push(
                    i32::try_from(children.len())
                        .map_err(|_| error("selector list offset overflow"))?,
                );
            }
            Arc::new(arrow_array::ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                compact_selected(a.values().as_ref(), &children)?,
                nulls,
            )?)
        }
        DataType::LargeList(field) => {
            let a = array.as_list::<i64>();
            let mut children = Vec::new();
            let mut offsets = vec![0i64];
            for row in &selected {
                if let Some(row) = row {
                    children.extend(
                        (a.value_offsets()[*row] as usize..a.value_offsets()[*row + 1] as usize)
                            .map(Some),
                    );
                }
                offsets.push(
                    i64::try_from(children.len())
                        .map_err(|_| error("selector list offset overflow"))?,
                );
            }
            Arc::new(arrow_array::LargeListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                compact_selected(a.values().as_ref(), &children)?,
                nulls,
            )?)
        }
        DataType::Map(field, ordered) => {
            let a = array.as_map();
            let mut children = Vec::new();
            let mut offsets = vec![0i32];
            for row in &selected {
                if let Some(row) = row {
                    children.extend(
                        (a.value_offsets()[*row] as usize..a.value_offsets()[*row + 1] as usize)
                            .map(Some),
                    );
                }
                offsets.push(
                    i32::try_from(children.len())
                        .map_err(|_| error("selector map offset overflow"))?,
                );
            }
            let entries = compact_selected(a.entries(), &children)?;
            Arc::new(arrow_array::MapArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                entries.as_struct().clone(),
                nulls,
                *ordered,
            )?)
        }
        DataType::Dictionary(key, _) => {
            let mut values = None;
            let mut indices = Vec::new();
            let mut mapping = std::collections::BTreeMap::new();
            let mut compact_keys = Vec::new();
            for row in &selected {
                if let Some(row) = row {
                    let (v, key) = dictionary_value(array, *row)?;
                    values = Some(v);
                    if let Some(key) = key {
                        let next = indices.len();
                        let index = *mapping.entry(key).or_insert_with(|| {
                            indices.push(Some(key));
                            next
                        });
                        compact_keys.push(Some(index));
                    } else {
                        compact_keys.push(None)
                    }
                } else {
                    compact_keys.push(None)
                }
            }
            let values = values.unwrap_or_else(|| {
                arrow_array::new_empty_array(match array.data_type() {
                    DataType::Dictionary(_, v) => v,
                    _ => unreachable!(),
                })
            });
            let values = compact_selected(values.as_ref(), &indices)?;
            macro_rules! dictionary {
                ($ty:ty,$native:ty) => {{
                    let keys = arrow_array::PrimitiveArray::<$ty>::from(
                        compact_keys
                            .iter()
                            .map(|key| {
                                key.map(|k| {
                                    <$native>::try_from(k)
                                        .map_err(|_| error("selector dictionary key overflow"))
                                })
                            })
                            .map(|key| key.transpose())
                            .collect::<Result<Vec<_>>>()?,
                    );
                    Arc::new(arrow_array::DictionaryArray::<$ty>::try_new(keys, values)?)
                        as ArrayRef
                }};
            }
            match key.as_ref() {
                DataType::Int8 => dictionary!(arrow_array::types::Int8Type, i8),
                DataType::Int16 => dictionary!(arrow_array::types::Int16Type, i16),
                DataType::Int32 => dictionary!(arrow_array::types::Int32Type, i32),
                DataType::Int64 => dictionary!(arrow_array::types::Int64Type, i64),
                DataType::UInt8 => dictionary!(arrow_array::types::UInt8Type, u8),
                DataType::UInt16 => dictionary!(arrow_array::types::UInt16Type, u16),
                DataType::UInt32 => dictionary!(arrow_array::types::UInt32Type, u32),
                DataType::UInt64 => dictionary!(arrow_array::types::UInt64Type, u64),
                _ => return Err(error("invalid grouped selector dictionary key")),
            }
        }
        _ => {
            let data = array.to_data();
            let mut mutable =
                arrow::array::MutableArrayData::new(vec![&data], true, selected.len());
            for row in selected {
                if let Some(row) = row {
                    mutable.extend(0, row, row + 1)
                } else {
                    mutable.extend_nulls(1)
                }
            }
            arrow_array::make_array(mutable.freeze())
        }
    })
}

impl SelectorSpec {
    pub fn preflight_types(
        value_type: &DataType,
        order_types: &[DataType],
        budget: SelectorBudget,
    ) -> Result<()> {
        let mut types = vec![value_type.clone()];
        types.extend_from_slice(order_types);
        require(
            decoded_row_bound(&types, 0)? <= budget.decoded_bytes,
            "grouped selector schema exceeds configured decoded-state allowance",
        )
    }
    pub fn kind(&self) -> SelectorKind {
        self.kind
    }
    pub fn tie(&self) -> SelectorTie {
        self.tie
    }
    pub fn order_options(&self) -> &[SortOptions] {
        &self.order_options
    }
    pub fn argument_types(&self) -> &[DataType] {
        &self.argument_types
    }
    pub fn new(
        value_type: DataType,
        order_types: Vec<DataType>,
        order_options: Vec<SortOptions>,
        kind: SelectorKind,
        tie: SelectorTie,
        ignore_nulls: bool,
    ) -> Result<Self> {
        require(
            order_types.len() == order_options.len(),
            "grouped selector order type/options mismatch",
        )?;
        let mut argument_types = vec![value_type.clone()];
        argument_types.extend(order_types.iter().cloned());
        let mut identity_value = serde_json::to_value((
            &argument_types,
            order_options
                .iter()
                .map(|o| (o.descending, o.nulls_first))
                .collect::<Vec<_>>(),
            kind == SelectorKind::Last,
            tie == SelectorTie::LastWrite,
            ignore_nulls,
        ))
        .map_err(|e| error(format!("grouped selector schema encoding: {e}")))?;
        // Arrow Field metadata is a HashMap. Equal nested schemas must have
        // identical persisted identities across fresh processes/hash seeds.
        // Preserve positional arrays while canonicalizing every object key.
        identity_value.sort_all_objects();
        let identity_bytes = serde_json::to_vec(&identity_value)
            .map_err(|e| error(format!("grouped selector schema encoding: {e}")))?;
        let identity = xxhash_rust::xxh3::xxh3_128(&identity_bytes).to_be_bytes();
        let order_codec = RowConverter::new(
            order_types
                .iter()
                .zip(&order_options)
                .map(|(kind, options)| SortField::new_with_options(kind.clone(), *options))
                .collect(),
        )?;
        // LAST reverses SQL keys; the ordinal direction is an independent tie
        // policy, so explicit no-FOR ordering can retain its established ties.
        let order_codec = if kind == SelectorKind::Last {
            RowConverter::new(
                order_types
                    .iter()
                    .zip(&order_options)
                    .map(|(kind, options)| {
                        SortField::new_with_options(
                            kind.clone(),
                            SortOptions {
                                descending: !options.descending,
                                nulls_first: !options.nulls_first,
                            },
                        )
                    })
                    .collect(),
            )?
        } else {
            order_codec
        };
        let schema = Arc::new(Schema::new(
            argument_types
                .iter()
                .enumerate()
                .map(|(index, kind)| Field::new(format!("arg{index}"), kind.clone(), true))
                .collect::<Vec<_>>(),
        ));
        // A zero-row batch has the exact schema/dictionary/message metadata
        // without allocating a nullable fixed-width or nested candidate.
        let empty = RecordBatch::try_new(
            schema.clone(),
            argument_types
                .iter()
                .map(arrow_array::new_empty_array)
                .collect(),
        )?;
        let mut baseline = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut baseline, &schema)?;
            writer.write(&empty)?;
            writer.finish()?;
        }
        // Each nonempty buffer adds at most 63 bytes of IPC alignment padding.
        let ipc_overhead = add(baseline.len(), mul(schema_nodes(&argument_types)?, 4 * 64)?)?;
        let prefix_len = add(
            8,
            u32::from_le_bytes(baseline[4..8].try_into().unwrap()) as usize,
        )?;
        let ipc_prefix = baseline[..prefix_len].to_vec();
        Ok(Self {
            value_type,
            argument_types,
            order_types,
            order_options,
            kind,
            tie,
            ignore_nulls,
            order_codec,
            schema,
            ipc_overhead,
            ipc_prefix,
            identity,
        })
    }

    pub fn validate_budget(&self, budget: SelectorBudget) -> Result<()> {
        require(
            budget.key_bytes > 0 && budget.value_bytes > 0 && budget.decoded_bytes > 0,
            "grouped selector requires positive existing state limits",
        )?;
        require(
            decoded_row_bound(&self.argument_types, 0)? <= budget.decoded_bytes,
            "grouped selector schema exceeds configured decoded-state allowance",
        )
    }
    pub fn decoded_bound(&self, candidate: &SelectorCandidate) -> Result<usize> {
        require(
            candidate.args.starts_with(&self.ipc_prefix),
            "grouped selector IPC schema mismatch",
        )?;
        ipc_decoded_bound(&self.argument_types, &candidate.args)
    }
    pub fn encode(
        &self,
        values: &[ArrayRef],
        ordinal: u64,
        budget: SelectorBudget,
    ) -> Result<Option<SelectorCandidate>> {
        self.validate_budget(budget)?;
        require(
            values.len() == self.argument_types.len()
                && values
                    .iter()
                    .zip(&self.argument_types)
                    .all(|(v, t)| v.len() == 1 && v.data_type() == t),
            "grouped selector input type/cardinality mismatch",
        )?;
        if self.ignore_nulls && logical_null(values[0].as_ref(), 0)? {
            return Ok(None);
        }
        // Fixed Arrow/schema wrappers and IPC metadata coexist with compacted
        // logical payloads. One combined allowance must admit all of them
        // before allocating a compacted array, IPC body or row-order key.
        let fixed = add(
            decoded_row_bound(&self.argument_types, 0)?,
            mul(self.ipc_overhead, 3)?,
        )?;
        let mut sizes = LogicalSize::new(budget.value_bytes, budget.decoded_bytes, fixed);
        for value in values {
            inspect_selected(value.as_ref(), Some(0), &mut sizes)?;
        }
        let key_bound = values[1..].iter().try_fold(8usize, |size, value| {
            add(size, order_size(value.as_ref(), Some(0), budget.key_bytes)?)
        })?;
        require(
            key_bound <= budget.key_bytes,
            "grouped selector order key exceeds configured key allowance",
        )?;
        sizes.ensure_extra(mul(key_bound, 2)?)?;
        require(
            add(
                56,
                add(key_bound, add(self.ipc_overhead, sizes.payload_bound()?)?)?,
            )? <= budget.value_bytes,
            "grouped selector candidate exceeds configured encoded-state allowance before materialization",
        )?;
        let compacted = values
            .iter()
            .map(|value| compact_selected(value.as_ref(), &[Some(0)]))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), compacted.clone())?;
        let mut sink = CappedWriter {
            bytes: Vec::new(),
            limit: budget.value_bytes - 56 - key_bound,
        };
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut sink, &self.schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        let args = sink.bytes;
        let mut key = if self.order_types.is_empty() {
            Vec::new()
        } else {
            self.order_codec
                .convert_columns(&compacted[1..])?
                .row(0)
                .as_ref()
                .to_vec()
        };
        key.extend_from_slice(
            &(if self.tie == SelectorTie::LastWrite {
                !ordinal
            } else {
                ordinal
            })
            .to_be_bytes(),
        );
        let candidate = SelectorCandidate { key, args, ordinal };
        self.check_candidate(&candidate, budget)?;
        Ok(Some(candidate))
    }
    fn check_candidate(&self, candidate: &SelectorCandidate, budget: SelectorBudget) -> Result<()> {
        require(
            add(56, add(candidate.key.len(), candidate.args.len())?)? <= budget.value_bytes,
            "grouped selector framed candidate exceeds configured encoded-state allowance",
        )?;
        require(
            candidate.key.len() >= 8
                && candidate.key.len() <= budget.key_bytes
                && candidate.args.len() <= budget.value_bytes,
            "grouped selector candidate exceeds configured key/value allowance",
        )?;
        let expected = if self.tie == SelectorTie::LastWrite {
            !candidate.ordinal
        } else {
            candidate.ordinal
        };
        require(
            candidate.key.ends_with(&expected.to_be_bytes()),
            "grouped selector candidate ordinal disagrees with sort identity",
        )?;
        require(
            self.decoded_bound(candidate)? <= budget.decoded_bytes,
            "grouped selector candidate exceeds configured decoded-state allowance",
        )
    }
    pub fn choose(&self, current: &mut Option<SelectorCandidate>, next: SelectorCandidate) -> bool {
        if current.as_ref().is_none_or(|old| next.key < old.key) {
            *current = Some(next);
            true
        } else {
            false
        }
    }
    pub fn encode_candidate(
        &self,
        candidate: &SelectorCandidate,
        budget: SelectorBudget,
    ) -> Result<Vec<u8>> {
        self.check_candidate(candidate, budget)?;
        let size = add(56, add(candidate.key.len(), candidate.args.len())?)?;
        require(
            size <= budget.value_bytes,
            "grouped selector framed candidate exceeds configured encoded-state allowance",
        )?;
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.identity);
        bytes.extend_from_slice(&candidate.ordinal.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(candidate.key.len())
                .map_err(|_| error("selector key length overflow"))?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(candidate.args.len())
                .map_err(|_| error("selector args length overflow"))?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&candidate.key);
        bytes.extend_from_slice(&candidate.args);
        let checksum = xxhash_rust::xxh3::xxh3_128(&bytes).to_be_bytes();
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }
    pub fn decode_candidate(
        &self,
        bytes: &[u8],
        budget: SelectorBudget,
    ) -> Result<SelectorCandidate> {
        self.validate_budget(budget)?;
        require(
            bytes.len() >= 56 && bytes.len() <= budget.value_bytes,
            "grouped selector candidate framing/size mismatch",
        )?;
        require(
            &bytes[..8] == MAGIC && bytes[8..24] == self.identity,
            "grouped selector candidate codec/schema mismatch",
        )?;
        let ordinal = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        let key_len = u32::from_be_bytes(bytes[32..36].try_into().unwrap()) as usize;
        let args_len = u32::from_be_bytes(bytes[36..40].try_into().unwrap()) as usize;
        require(
            add(56, add(key_len, args_len)?)? == bytes.len(),
            "grouped selector candidate length mismatch",
        )?;
        require(
            xxhash_rust::xxh3::xxh3_128(&bytes[..bytes.len() - 16]).to_be_bytes()
                == bytes[bytes.len() - 16..],
            "grouped selector candidate checksum mismatch",
        )?;
        require(
            key_len <= budget.key_bytes
                && ipc_decoded_bound(&self.argument_types, &bytes[40 + key_len..bytes.len() - 16])?
                    <= budget.decoded_bytes,
            "grouped selector candidate exceeds configured decoded-state allowance",
        )?;
        let candidate = SelectorCandidate {
            key: bytes[40..40 + key_len].to_vec(),
            args: bytes[40 + key_len..bytes.len() - 16].to_vec(),
            ordinal,
        };
        self.check_candidate(&candidate, budget)?;
        Ok(candidate)
    }
    fn decode(
        &self,
        candidate: &SelectorCandidate,
        budget: SelectorBudget,
    ) -> Result<Vec<ArrayRef>> {
        self.check_candidate(candidate, budget)?;
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(Cursor::new(&candidate.args), None)?;
        require(
            reader.schema().as_ref() == self.schema.as_ref(),
            "grouped selector IPC schema mismatch",
        )?;
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| error("grouped selector IPC candidate has no batch"))?;
        require(
            batch.num_rows() == 1 && reader.next().is_none(),
            "grouped selector IPC requires exactly one candidate row",
        )?;
        Ok(batch.columns().to_vec())
    }
    pub fn evaluate(
        &self,
        candidate: Option<&SelectorCandidate>,
        budget: SelectorBudget,
    ) -> Result<ScalarValue> {
        match candidate {
            None => ScalarValue::try_from(&self.value_type),
            Some(candidate) => ScalarValue::try_from_array(&self.decode(candidate, budget)?[0], 0),
        }
    }
    pub fn validate_before(
        &self,
        candidate: &SelectorCandidate,
        values: &[ArrayRef],
        ignored_arg_indices: &[usize],
        budget: SelectorBudget,
    ) -> Result<()> {
        let stored = self.decode(candidate, budget)?;
        require(
            stored.len() == values.len()
                && stored
                    .iter()
                    .zip(values)
                    .enumerate()
                    .all(|(index, (old, new))| {
                        ignored_arg_indices.contains(&index) || old.as_ref() == new.as_ref()
                    }),
            "grouped selector retract before-image does not match retained row/version",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow_schema::{Field, TimeUnit};
    use std::sync::Arc;

    fn budget() -> SelectorBudget {
        SelectorBudget {
            key_bytes: 4096,
            value_bytes: 64 * 1024,
            decoded_bytes: 192 * 1024,
        }
    }
    fn values(value: Option<&str>, time: i64, priority: Option<i64>) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![value])),
            Arc::new(TimestampNanosecondArray::from(vec![time])),
            Arc::new(Int64Array::from(vec![priority])),
        ]
    }
    fn spec(kind: SelectorKind, secondary: SortOptions, ignore_nulls: bool) -> SelectorSpec {
        SelectorSpec::new(
            DataType::Utf8,
            vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Int64,
            ],
            vec![
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                secondary,
            ],
            kind,
            if kind == SelectorKind::Last {
                SelectorTie::LastWrite
            } else {
                SelectorTie::FirstWrite
            },
            ignore_nulls,
        )
        .unwrap()
    }
    #[test]
    fn nested_metadata_identity_and_candidate_survive_fresh_map_seeds() {
        fn nested(reverse: bool) -> DataType {
            let pairs = if reverse {
                [("z", "last"), ("a", "first")]
            } else {
                [("a", "first"), ("z", "last")]
            };
            let metadata = || {
                pairs
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect()
            };
            let child =
                Arc::new(Field::new("item", DataType::Int64, true).with_metadata(metadata()));
            DataType::Struct(
                vec![Arc::new(
                    Field::new("items", DataType::FixedSizeList(child, 2), true)
                        .with_metadata(metadata()),
                )]
                .into(),
            )
        }
        let original = scalar_spec(nested(false), false);
        let value = arrow_array::new_null_array(&nested(false), 1);
        let candidate = original.encode(&[value], 37, budget()).unwrap().unwrap();
        let frame = original.encode_candidate(&candidate, budget()).unwrap();
        for iteration in 0..32 {
            let fresh = scalar_spec(nested(iteration % 2 == 0), false);
            assert_eq!(original.argument_types, fresh.argument_types);
            assert_eq!(original.identity, fresh.identity);
            let restored = fresh.decode_candidate(&frame, budget()).unwrap();
            assert_eq!(restored.ordinal, 37);
            assert!(fresh.evaluate(Some(&restored), budget()).unwrap().is_null());
        }
    }

    #[test]
    fn bounded_row_arguments_reject_large_values_and_keep_small_shared_slices() {
        let backing: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
            "x".repeat(1024 * 1024),
            "tiny".into(),
        ]));
        assert!(bounded_row_size(&[backing.slice(0, 1)], 512).is_err());
        let tiny = backing.slice(1, 1);
        assert!(bounded_row_size(std::slice::from_ref(&tiny), 16).unwrap() <= 16);
        assert!(
            bounded_row_size(
                &[
                    tiny.clone(),
                    Arc::new(arrow_array::Int64Array::from(vec![Some(1)]))
                ],
                16
            )
            .is_err()
        );
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        assert_eq!(
            bounded_row_size(std::slice::from_ref(&tiny), 16).unwrap(),
            converter
                .convert_columns(&[tiny])
                .unwrap()
                .row(0)
                .as_ref()
                .len()
        );
    }

    #[test]
    fn raw_clock_secondary_options_and_original_write_identity_select_one_candidate() {
        for (kind, secondary, expected) in [
            (
                SelectorKind::First,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                "early",
            ),
            (
                SelectorKind::Last,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                "C",
            ),
            (
                SelectorKind::Last,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
                "A",
            ),
        ] {
            let spec = spec(kind, secondary, false);
            let mut winner = None;
            for (ordinal, (v, t, p)) in
                [("A", 20, 0), ("early", 10, 999), ("B", 20, 1), ("C", 20, 1)]
                    .into_iter()
                    .enumerate()
            {
                let candidate = spec
                    .encode(&values(Some(v), t, Some(p)), ordinal as u64, budget())
                    .unwrap()
                    .unwrap();
                // Every intermediate winner crosses the same durable framing
                // used by owner recovery and pane/session partial merges.
                spec.choose(&mut winner, candidate);
                let bytes = spec
                    .encode_candidate(winner.as_ref().unwrap(), budget())
                    .unwrap();
                winner = Some(spec.decode_candidate(&bytes, budget()).unwrap());
            }
            assert_eq!(
                spec.evaluate(winner.as_ref(), budget()).unwrap(),
                ScalarValue::Utf8(Some(expected.into()))
            );
        }
        for (nulls_first, expected) in [(true, "present"), (false, "null-priority")] {
            let spec = spec(
                SelectorKind::Last,
                SortOptions {
                    descending: false,
                    nulls_first,
                },
                false,
            );
            let mut winner = None;
            for (n, args) in [
                values(Some("null-priority"), 20, None),
                values(Some("present"), 20, Some(0)),
            ]
            .into_iter()
            .enumerate()
            {
                spec.choose(
                    &mut winner,
                    spec.encode(&args, n as u64, budget()).unwrap().unwrap(),
                );
            }
            assert_eq!(
                spec.evaluate(winner.as_ref(), budget()).unwrap(),
                ScalarValue::Utf8(Some(expected.into()))
            );
        }
    }
    #[test]
    fn null_treatment_is_explicit_and_false_zero_and_empty_are_values() {
        for ignore in [false, true] {
            let spec = spec(SelectorKind::Last, SortOptions::default(), ignore);
            let mut winner = spec
                .encode(&values(Some(""), 10, Some(0)), 0, budget())
                .unwrap();
            if let Some(candidate) = spec
                .encode(&values(None, 20, Some(0)), 1, budget())
                .unwrap()
            {
                spec.choose(&mut winner, candidate);
            }
            assert_eq!(
                spec.evaluate(winner.as_ref(), budget()).unwrap(),
                ScalarValue::Utf8(if ignore { Some("".into()) } else { None })
            );
        }
        for (ty, array, expected) in [
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                ScalarValue::Int64(Some(0)),
            ),
            (
                DataType::Boolean,
                Arc::new(arrow_array::BooleanArray::from(vec![false])) as ArrayRef,
                ScalarValue::Boolean(Some(false)),
            ),
        ] {
            let spec = SelectorSpec::new(
                ty,
                vec![],
                vec![],
                SelectorKind::Last,
                SelectorTie::LastWrite,
                true,
            )
            .unwrap();
            let candidate = spec.encode(&[array], 0, budget()).unwrap().unwrap();
            assert_eq!(spec.evaluate(Some(&candidate), budget()).unwrap(), expected);
        }
    }
    #[test]
    fn codec_schema_corruption_and_stale_secondary_version_fail_precisely() {
        let spec = spec(SelectorKind::Last, SortOptions::default(), false);
        let candidate = spec
            .encode(&values(Some("original"), 20, Some(1)), 7, budget())
            .unwrap()
            .unwrap();
        let bytes = spec.encode_candidate(&candidate, budget()).unwrap();
        let mut corrupted = bytes.clone();
        let index = corrupted.len() - 17;
        corrupted[index] ^= 1;
        assert!(
            spec.decode_candidate(&corrupted, budget())
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        let other = self::spec(SelectorKind::First, SortOptions::default(), false);
        assert!(
            other
                .decode_candidate(&bytes, budget())
                .unwrap_err()
                .to_string()
                .contains("codec/schema")
        );
        spec.validate_before(
            &candidate,
            &values(Some("original"), 99, Some(1)),
            &[1],
            budget(),
        )
        .unwrap();
        assert!(
            spec.validate_before(
                &candidate,
                &values(Some("stale"), 99, Some(1)),
                &[1],
                budget()
            )
            .is_err()
        );
        assert!(
            spec.validate_before(
                &candidate,
                &values(Some("original"), 99, Some(2)),
                &[1],
                budget()
            )
            .is_err()
        );
    }
    #[test]
    fn null_nested_shapes_are_admitted_before_decode_or_rejected_by_existing_limit() {
        let wide = DataType::Struct(
            (0..256)
                .map(|i| Arc::new(Field::new(format!("v{i}"), DataType::Int64, true)))
                .collect(),
        );
        assert!(
            SelectorSpec::preflight_types(
                &wide,
                &[],
                SelectorBudget {
                    key_bytes: 4096,
                    value_bytes: 4096,
                    decoded_bytes: 12288
                }
            )
            .unwrap_err()
            .to_string()
            .contains("schema exceeds configured decoded-state")
        );
        let nested = DataType::Struct(
            vec![
                Arc::new(Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
                Arc::new(Field::new(
                    "detail",
                    DataType::Struct(
                        vec![Arc::new(Field::new("label", DataType::Utf8, true))].into(),
                    ),
                    true,
                )),
            ]
            .into(),
        );
        SelectorSpec::preflight_types(&nested, &[], budget()).unwrap();
        let spec = SelectorSpec::new(
            nested.clone(),
            vec![],
            vec![],
            SelectorKind::First,
            SelectorTie::FirstWrite,
            false,
        )
        .unwrap();
        let candidate = spec
            .encode(&[arrow_array::new_null_array(&nested, 1)], 0, budget())
            .unwrap()
            .unwrap();
        assert!(spec.evaluate(Some(&candidate), budget()).unwrap().is_null());
    }
    fn scalar_spec(kind: DataType, ignore_nulls: bool) -> SelectorSpec {
        SelectorSpec::new(
            kind,
            vec![],
            vec![],
            SelectorKind::Last,
            SelectorTie::LastWrite,
            ignore_nulls,
        )
        .unwrap()
    }
    #[test]
    fn fixed_list_parent_null_and_nonnull_null_children_keep_sql_eligibility() {
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let null_parent = Arc::new(
            arrow_array::FixedSizeListArray::try_new(
                field.clone(),
                2,
                Arc::new(Int64Array::from(vec![None, None])),
                Some(arrow::buffer::NullBuffer::from(vec![false])),
            )
            .unwrap(),
        ) as ArrayRef;
        let nonnull = Arc::new(
            arrow_array::FixedSizeListArray::try_new(
                field,
                2,
                Arc::new(Int64Array::from(vec![None, None])),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let spec = scalar_spec(nonnull.data_type().clone(), true);
        assert!(spec.encode(&[null_parent], 0, budget()).unwrap().is_none());
        let candidate = spec
            .encode(std::slice::from_ref(&nonnull), 1, budget())
            .unwrap()
            .unwrap();
        let bytes = spec.encode_candidate(&candidate, budget()).unwrap();
        let candidate = spec.decode_candidate(&bytes, budget()).unwrap();
        let result = spec.evaluate(Some(&candidate), budget()).unwrap();
        assert!(!result.is_null());
        assert_eq!(result.to_array().unwrap().as_ref(), nonnull.as_ref());
        let fields = vec![Arc::new(Field::new("x", DataType::Int64, true))].into();
        let nonnull_struct = Arc::new(
            StructArray::try_new(fields, vec![Arc::new(Int64Array::from(vec![None]))], None)
                .unwrap(),
        ) as ArrayRef;
        let spec = scalar_spec(nonnull_struct.data_type().clone(), true);
        let candidate = spec
            .encode(&[nonnull_struct], 0, budget())
            .unwrap()
            .unwrap();
        assert!(!spec.evaluate(Some(&candidate), budget()).unwrap().is_null());
    }
    #[test]
    fn selected_dictionary_value_and_slices_ignore_unused_large_backing() {
        use arrow_array::types::Int8Type;
        use arrow_array::{DictionaryArray, Int8Array};
        let huge = "x".repeat(1024 * 1024);
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1]),
                Arc::new(StringArray::from(vec![
                    Some(huge.as_str()),
                    Some("small"),
                    None,
                ])),
            )
            .unwrap(),
        ) as ArrayRef;
        let spec = scalar_spec(dictionary.data_type().clone(), true);
        let candidate = spec
            .encode(std::slice::from_ref(&dictionary), 0, budget())
            .unwrap()
            .unwrap();
        assert!(candidate.args.len() < 4096);
        spec.validate_before(&candidate, &[dictionary], &[], budget())
            .unwrap();
        assert_eq!(
            spec.evaluate(Some(&candidate), budget()).unwrap(),
            ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(ScalarValue::Utf8(Some("small".into())))
            )
        );
        let null = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![2]),
                Arc::new(StringArray::from(vec![
                    Some(huge.as_str()),
                    Some("small"),
                    None,
                ])),
            )
            .unwrap(),
        ) as ArrayRef;
        assert!(spec.encode(&[null], 0, budget()).unwrap().is_none());
        let backing = Arc::new(StringArray::from(vec![
            huge.as_str(),
            "small",
            huge.as_str(),
        ])) as ArrayRef;
        let spec = scalar_spec(DataType::Utf8, false);
        let candidate = spec
            .encode(&[backing.slice(1, 1)], 0, budget())
            .unwrap()
            .unwrap();
        assert_eq!(
            spec.evaluate(Some(&candidate), budget()).unwrap(),
            ScalarValue::Utf8(Some("small".into()))
        );
        let view = Arc::new(arrow_array::StringViewArray::from_iter_values([
            huge.as_str(),
            "small",
            huge.as_str(),
        ])) as ArrayRef;
        let spec = scalar_spec(DataType::Utf8View, false);
        let candidate = spec
            .encode(&[view.slice(1, 1)], 0, budget())
            .unwrap()
            .unwrap();
        assert!(candidate.args.len() < 4096);
    }
    #[test]
    fn oversized_value_order_and_known_child_count_fail_before_materialization() {
        let huge = "x".repeat(1024 * 1024);
        let spec = scalar_spec(DataType::Utf8, false);
        assert!(
            spec.encode(
                &[Arc::new(StringArray::from(vec![huge.as_str()]))],
                0,
                budget()
            )
            .unwrap_err()
            .to_string()
            .contains("before materialization")
        );
        let spec = SelectorSpec::new(
            DataType::Int64,
            vec![DataType::Utf8],
            vec![SortOptions::default()],
            SelectorKind::Last,
            SelectorTie::LastWrite,
            false,
        )
        .unwrap();
        assert!(
            spec.encode(
                &[
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![huge.as_str()]))
                ],
                0,
                budget()
            )
            .unwrap_err()
            .to_string()
            .contains("before materialization")
        );
        let field = Arc::new(Field::new("item", DataType::Null, true));
        let list = Arc::new(
            arrow_array::ListArray::try_new(
                field,
                arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![
                    0i32, 1_000_000,
                ])),
                Arc::new(arrow_array::NullArray::new(1_000_000)),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let spec = scalar_spec(list.data_type().clone(), false);
        assert!(
            spec.encode(&[list], 0, budget())
                .unwrap_err()
                .to_string()
                .contains("before materialization")
        );
        let wide = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int64, true)),
            1_000_000,
        );
        assert!(SelectorSpec::preflight_types(&wide, &[], budget()).is_err());
    }
    #[test]
    fn ipc_schema_and_frame_lengths_fail_before_array_decode() {
        let spec = scalar_spec(DataType::Utf8, false);
        let candidate = spec
            .encode(&[Arc::new(StringArray::from(vec!["value"]))], 0, budget())
            .unwrap()
            .unwrap();
        let mut frame = spec.encode_candidate(&candidate, budget()).unwrap();
        let key_len = u32::from_be_bytes(frame[32..36].try_into().unwrap()) as usize;
        frame[40 + key_len + 8] ^= 1;
        let checksum = xxhash_rust::xxh3::xxh3_128(&frame[..frame.len() - 16]).to_be_bytes();
        let end = frame.len();
        frame[end - 16..].copy_from_slice(&checksum);
        assert!(spec.decode_candidate(&frame, budget()).is_err());
        let mut bad = candidate.clone();
        bad.args[0] ^= 1;
        assert!(
            spec.evaluate(Some(&bad), budget())
                .unwrap_err()
                .to_string()
                .contains("IPC schema")
        );
        let mut sink = CappedWriter {
            bytes: vec![],
            limit: 3,
        };
        assert!(sink.write_all(&[0; 4]).is_err());
        assert!(sink.bytes.is_empty());
    }
    #[test]
    fn selected_run_and_dense_union_nullness_uses_only_selected_child_offset() {
        use arrow::buffer::ScalarBuffer;
        use arrow_array::types::{Int8Type, Int32Type};
        use arrow_array::{DictionaryArray, Int8Array, Int32Array, RunArray, UnionArray};
        use arrow_schema::UnionFields;
        let count = 100_000;
        let mut keys = vec![Some(1i8); count];
        keys[0] = Some(2);
        let huge = "x".repeat(1024 * 1024);
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(keys),
                Arc::new(StringArray::from(vec![
                    Some(huge.as_str()),
                    Some("small"),
                    None,
                ])),
            )
            .unwrap(),
        ) as ArrayRef;
        let run = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from((1..=count as i32).collect::<Vec<_>>()),
                dictionary.as_ref(),
            )
            .unwrap(),
        ) as ArrayRef;
        let fields = UnionFields::new(
            vec![0],
            vec![Field::new("value", dictionary.data_type().clone(), true)],
        );
        let union = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![0i8, 0]),
                Some(ScalarBuffer::from(vec![0i32, count as i32 - 1])),
                vec![dictionary],
            )
            .unwrap(),
        ) as ArrayRef;
        for (selected, null) in [
            (run.slice(count - 1, 1), run.slice(0, 1)),
            (union.slice(1, 1), union.slice(0, 1)),
        ] {
            assert!(!logical_null(selected.as_ref(), 0).unwrap());
            assert!(logical_null(null.as_ref(), 0).unwrap());
            let spec = scalar_spec(selected.data_type().clone(), true);
            assert!(spec.encode(&[null], 0, budget()).unwrap().is_none());
            let candidate = spec.encode(&[selected], 1, budget()).unwrap().unwrap();
            assert!(candidate.args.len() < 4096);
            let decoded = spec.decode(&candidate, budget()).unwrap();
            assert!(!logical_null(decoded[0].as_ref(), 0).unwrap());
        }
    }
    #[test]
    fn schema_and_logical_workspace_share_one_pre_materialization_allowance() {
        let fields = (0..8)
            .map(|i| Arc::new(Field::new(format!("value{i}"), DataType::Utf8, true)))
            .collect();
        let text = "x".repeat(1024);
        let columns = (0..8)
            .map(|_| Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef)
            .collect();
        let value = Arc::new(StructArray::try_new(fields, columns, None).unwrap()) as ArrayRef;
        let spec = scalar_spec(value.data_type().clone(), false);
        let fixed = decoded_row_bound(&spec.argument_types, 0).unwrap() + spec.ipc_overhead * 3;
        let mut size = LogicalSize::new(usize::MAX, usize::MAX, fixed);
        inspect_selected(value.as_ref(), Some(0), &mut size).unwrap();
        let dynamic = size.workspace_bound().unwrap() - fixed;
        let combined = fixed + dynamic + 16; // two copies of the ordinal-only key
        let separate_only = fixed.max(dynamic) + 1;
        assert!(separate_only < combined);
        let mut limits = budget();
        limits.decoded_bytes = separate_only;
        SelectorSpec::preflight_types(value.data_type(), &[], limits).unwrap();
        assert!(
            spec.encode(std::slice::from_ref(&value), 0, limits)
                .unwrap_err()
                .to_string()
                .contains("before materialization")
        );
        limits.decoded_bytes = combined;
        let candidate = spec.encode(&[value], 0, limits).unwrap().unwrap();
        assert!(spec.evaluate(Some(&candidate), limits).is_ok());
    }
}
