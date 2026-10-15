//! Complex fields retain the pinned Arrow conversion and allocation behavior.
//! Framing validation below prevents known unchecked reader slices/counts; it
//! is not a new admission proof for inherited Arrow allocations.
use super::{Cursor, checked_add, is_complex};
use anyhow::{Context, Result, bail, ensure};
use arrow::buffer::Buffer;
use arrow::ipc::{
    reader::{read_dictionary, read_record_batch},
    writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions},
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, UnionMode};
use datafusion::common::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) fn schema_bytes(schema: &Schema) -> Vec<u8> {
    IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .ipc_message
}

/// Keep the Arrow messages until the sole final group buffer is allocated.
/// A second serialized complex-section byte buffer would duplicate every body.
pub(super) struct ComplexMessages {
    messages: Vec<EncodedData>,
    encoded_len: usize,
}
impl ComplexMessages {
    pub(super) fn encoded_len(&self) -> usize {
        self.encoded_len
    }
    pub(super) fn append_to(self, output: &mut Vec<u8>) -> Result<()> {
        if self.messages.is_empty() {
            return Ok(());
        }
        output.extend_from_slice(&u32::try_from(self.messages.len())?.to_le_bytes());
        for message in self.messages {
            output.extend_from_slice(&u32::try_from(message.ipc_message.len())?.to_le_bytes());
            output.extend_from_slice(&u32::try_from(message.arrow_data.len())?.to_le_bytes());
            output.extend_from_slice(&message.ipc_message);
            output.extend_from_slice(&message.arrow_data);
        }
        Ok(())
    }
}

pub(super) struct ComplexLayout {
    indices: Vec<usize>,
    schema: Arc<Schema>,
    dictionary_count: usize,
    // The pinned reader creates its own dictionary value schema. This cached
    // counterpart is only for borrowed preflight traversal, not extra per-read
    // schema cloning or a replacement dictionary implementation.
    dictionary_schemas: HashMap<i64, Arc<Schema>>,
}
impl ComplexLayout {
    pub(super) fn new<'a>(types: impl Iterator<Item = &'a DataType>) -> Result<Self> {
        let mut indices = Vec::new();
        let mut fields = Vec::new();
        for (i, t) in types.enumerate() {
            if is_complex(t) {
                indices.push(i);
                fields.push(Field::new(format!("c{i}"), t.clone(), true));
            }
        }
        let bytes = schema_bytes(&Schema::new(fields));
        let message = arrow::ipc::root_as_message(&bytes)
            .map_err(|e| anyhow::anyhow!("invalid aggregate schema: {e}"))?;
        let schema = Arc::new(arrow::ipc::convert::fb_to_schema(
            message
                .header_as_schema()
                .context("missing aggregate schema")?,
        ));
        let mut dictionary_count = 0;
        let mut dictionary_schemas = HashMap::new();
        for field in schema.flattened_fields() {
            if let DataType::Dictionary(_, value) = field.data_type() {
                dictionary_count += 1;
                #[allow(deprecated)]
                let id = field.dict_id().context("missing aggregate dictionary ID")?;
                dictionary_schemas.entry(id).or_insert_with(|| {
                    Arc::new(Schema::new(vec![Field::new(
                        "",
                        value.as_ref().clone(),
                        true,
                    )]))
                });
            }
        }
        Ok(Self {
            indices,
            schema,
            dictionary_count,
            dictionary_schemas,
        })
    }
    pub(super) fn encode<'a>(
        &self,
        values: impl Iterator<Item = &'a ScalarValue>,
        max: usize,
    ) -> Result<ComplexMessages> {
        if self.indices.is_empty() {
            return Ok(ComplexMessages {
                messages: Vec::new(),
                encoded_len: 0,
            });
        }
        let mut arrays = Vec::new();
        arrays.try_reserve_exact(self.indices.len())?;
        let mut indices = self.indices.iter().peekable();
        for (i, value) in values.enumerate() {
            if indices.peek().is_some_and(|n| **n == i) {
                // Preserve DataFusion's existing concat/null/union normalization.
                arrays.push(value.to_array()?);
                indices.next();
            }
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        let generator = IpcDataGenerator::default();
        let options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);
        if self.dictionary_count > 0 {
            generator.schema_to_bytes_with_dictionary_tracker(&self.schema, &mut tracker, &options);
        }
        let (dictionaries, batch) = generator.encoded_batch(&batch, &mut tracker, &options)?;
        let messages = dictionaries
            .into_iter()
            .chain(std::iter::once(batch))
            .collect::<Vec<_>>();
        u32::try_from(messages.len()).context("aggregate message count exceeds u32 framing")?;
        let mut encoded_len = 4usize;
        for message in &messages {
            encoded_len = checked_add(
                encoded_len,
                message_size(message.ipc_message.len(), message.arrow_data.len())?,
            )?;
        }
        ensure!(
            encoded_len <= max,
            "aggregate complex value exceeds configured limit"
        );
        Ok(ComplexMessages {
            messages,
            encoded_len,
        })
    }
    pub(super) fn decode(&self, input: &mut Cursor<'_>) -> Result<Option<RecordBatch>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        let count = input.u32()?;
        ensure!(
            count > 0 && count <= self.dictionary_count + 1,
            "invalid aggregate Arrow message count"
        );
        let mut probe = Cursor { bytes: input.bytes };
        for index in 0..count {
            let metadata_len = probe.u32()?;
            let body_len = probe.u32()?;
            let metadata = probe.take(metadata_len)?;
            probe.take(body_len)?;
            let message = arrow::ipc::root_as_message(metadata)
                .map_err(|e| anyhow::anyhow!("invalid aggregate Arrow message: {e}"))?;
            ensure!(
                message.version() == arrow::ipc::MetadataVersion::V5,
                "invalid aggregate Arrow version"
            );
            ensure!(
                usize::try_from(message.bodyLength())? == body_len,
                "aggregate Arrow body length changed"
            );
            match message.header_type() {
                arrow::ipc::MessageHeader::DictionaryBatch if index + 1 < count => {
                    let dictionary = message
                        .header_as_dictionary_batch()
                        .context("missing aggregate dictionary")?;
                    ensure!(!dictionary.isDelta(), "invalid aggregate dictionary delta");
                    let schema = self
                        .dictionary_schemas
                        .get(&dictionary.id())
                        .context("unknown aggregate dictionary")?;
                    let batch = dictionary
                        .data()
                        .context("missing aggregate dictionary data")?;
                    preflight(batch, schema.fields(), body_len)?;
                }
                arrow::ipc::MessageHeader::RecordBatch if index + 1 == count => {
                    let batch = message
                        .header_as_record_batch()
                        .context("missing aggregate batch")?;
                    ensure!(
                        batch.length() == 1,
                        "aggregate state must contain one Arrow row"
                    );
                    preflight(batch, self.schema.fields(), body_len)?;
                }
                _ => bail!("invalid aggregate Arrow message order"),
            }
        }
        let mut dictionaries = HashMap::new();
        let mut batch = None;
        for index in 0..count {
            let metadata_len = input.u32()?;
            let body_len = input.u32()?;
            let metadata = input.take(metadata_len)?;
            let body = input.take(body_len)?;
            let message = arrow::ipc::root_as_message(metadata)
                .map_err(|e| anyhow::anyhow!("invalid aggregate Arrow message: {e}"))?;
            let body = Buffer::from(body);
            if index + 1 < count {
                read_dictionary(
                    &body,
                    message
                        .header_as_dictionary_batch()
                        .context("missing aggregate dictionary")?,
                    &self.schema,
                    &mut dictionaries,
                    &message.version(),
                )?;
            } else {
                batch = Some(read_record_batch(
                    &body,
                    message
                        .header_as_record_batch()
                        .context("missing aggregate batch")?,
                    self.schema.clone(),
                    &dictionaries,
                    None,
                    &message.version(),
                )?);
            }
        }
        let batch = batch.context("missing aggregate complex batch")?;
        ensure!(
            batch.num_rows() == 1 && batch.num_columns() == self.indices.len(),
            "aggregate complex shape changed"
        );
        Ok(Some(batch))
    }
}

pub(super) fn message_size(metadata_len: usize, body_len: usize) -> Result<usize> {
    u32::try_from(metadata_len).context("aggregate metadata exceeds u32 framing")?;
    u32::try_from(body_len).context("aggregate body exceeds u32 framing")?;
    checked_add(8, checked_add(metadata_len, body_len)?)
}

/// Only the unchecked slicing/count hazards in Arrow55.2.0's reader are checked
/// here. Arrow's fallible builders retain responsibility for array semantics.
struct Preflight<'a> {
    batch: arrow::ipc::RecordBatch<'a>,
    node: usize,
    buffer: usize,
    variadic: usize,
    body_len: usize,
}
impl Preflight<'_> {
    fn buffer(&mut self) -> Result<usize> {
        let buffers = self.batch.buffers().context("missing aggregate buffers")?;
        ensure!(
            self.buffer < buffers.len(),
            "missing aggregate Arrow buffer"
        );
        let buffer = buffers.get(self.buffer);
        self.buffer += 1;
        let start = usize::try_from(buffer.offset())?;
        let len = usize::try_from(buffer.length())?;
        ensure!(
            checked_add(start, len)? <= self.body_len,
            "aggregate Arrow buffer exceeds body"
        );
        Ok(len)
    }
    fn field(&mut self, field: &Field) -> Result<()> {
        let nodes = self.batch.nodes().context("missing aggregate nodes")?;
        ensure!(self.node < nodes.len(), "missing aggregate Arrow node");
        let node = nodes.get(self.node);
        self.node += 1;
        let len = usize::try_from(node.length())?;
        let nulls = usize::try_from(node.null_count())?;
        ensure!(nulls <= len, "invalid aggregate null count");
        let t = field.data_type();
        if !matches!(
            t,
            DataType::Union(_, _) | DataType::Null | DataType::RunEndEncoded(_, _)
        ) {
            let validity = self.buffer()?;
            ensure!(
                nulls == 0 || validity >= len.div_ceil(8),
                "short aggregate validity buffer"
            );
        }
        match t {
            DataType::Null => {
                ensure!(nulls == len, "invalid aggregate Null count");
            }
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                self.buffer()?;
                self.buffer()?;
            }
            DataType::Utf8View | DataType::BinaryView => {
                let counts = self
                    .batch
                    .variadicBufferCounts()
                    .context("missing aggregate variadic counts")?;
                ensure!(
                    self.variadic < counts.len(),
                    "missing aggregate variadic count"
                );
                let count = usize::try_from(counts.get(self.variadic))?;
                self.variadic += 1;
                ensure!(
                    count
                        <= self
                            .batch
                            .buffers()
                            .context("missing aggregate buffers")?
                            .len(),
                    "invalid aggregate variadic count"
                );
                for _ in 0..checked_add(count, 1)? {
                    self.buffer()?;
                }
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
                self.buffer()?;
                self.field(child)?;
            }
            DataType::FixedSizeList(child, _) => {
                self.field(child)?;
            }
            DataType::Struct(fields) => {
                for child in fields {
                    self.field(child)?;
                }
            }
            DataType::Dictionary(_, _) => {
                self.buffer()?;
            }
            DataType::Union(fields, mode) => {
                ensure!(self.buffer()? >= len, "short aggregate union IDs");
                if *mode == UnionMode::Dense {
                    ensure!(
                        self.buffer()?
                            >= len
                                .checked_mul(4)
                                .context("aggregate union size overflow")?,
                        "short aggregate union offsets"
                    );
                }
                for (_, child) in fields.iter() {
                    self.field(child)?;
                }
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.field(run_ends)?;
                self.field(values)?;
            }
            _ => {
                self.buffer()?;
            }
        }
        Ok(())
    }
}
fn preflight(
    batch: arrow::ipc::RecordBatch<'_>,
    fields: &[Arc<Field>],
    body_len: usize,
) -> Result<()> {
    ensure!(
        batch.compression().is_none(),
        "compressed aggregate Arrow data is invalid"
    );
    ensure!(batch.length() >= 0, "invalid aggregate batch length");
    let mut scan = Preflight {
        batch,
        node: 0,
        buffer: 0,
        variadic: 0,
        body_len,
    };
    for field in fields {
        scan.field(field)?;
    }
    ensure!(
        scan.node == batch.nodes().context("missing aggregate nodes")?.len()
            && scan.buffer == batch.buffers().context("missing aggregate buffers")?.len(),
        "aggregate Arrow layout changed"
    );
    ensure!(
        scan.variadic == batch.variadicBufferCounts().map_or(0, |v| v.len()),
        "aggregate variadic counts changed"
    );
    Ok(())
}
