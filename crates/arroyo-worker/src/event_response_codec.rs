//! Registered response framing and IPC validation for private response edges.
//! Transport registration is explicit; no planner/Engine producer is enabled.
use anyhow::{Context, Result, bail, ensure};
use arrow::array::Array;
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::ipc::{MessageHeader, MetadataVersion};
use arrow_array::RecordBatch;
use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext, EventResponse};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

const ALIGNMENT: usize = arroyo_types::event_edge::NATIVE_IPC_ALIGNMENT;
const CONTEXT_BYTES: usize = 44;
pub(crate) const EXTENSION_BYTES: usize = CONTEXT_BYTES + 4;
const IPC_LENGTH_BYTES: usize = 4;
const MAX_KEYS: usize = 2;
const MAX_NATIVE_ROWS: usize = 4;

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).context("response size overflow")
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).context("response size overflow")
}
fn align(value: usize, alignment: usize) -> Result<usize> {
    Ok(add(value, alignment - 1)? & !(alignment - 1))
}
fn bitmap(rows: usize) -> Result<usize> {
    Ok(add(rows, 7)? / 8)
}

/// Upper bound for ONE actual FlatBuffer scalar push (not allocator capacity).
fn scalar_push(width: usize, alignment: usize) -> Result<usize> {
    add(width, alignment - 1)
}

/// Count exactly the writer's two vectors, two tables/vtables, and root offset.
/// Every scalar is charged even if a zero/default value would omit its slot.
/// Arrow55.2 writer.rs549..587; flatbuffers25.2.10 builder.rs555..648,693..736.
fn metadata_bound(nodes: usize, buffers: usize) -> Result<usize> {
    let vector = |count| add(add(4, ALIGNMENT - 1)?, mul(count, 16)?);
    let table_offset = scalar_push(4, 4)?;
    let rb_slots = add(scalar_push(8, 8)?, mul(2, scalar_push(4, 4)?)?)?;
    let rb_vtable = usize::from(arrow::ipc::RecordBatch::VT_BUFFERS) + 2;
    let msg_slots = add(
        add(scalar_push(2, 2)?, scalar_push(1, 1)?)?,
        add(scalar_push(8, 8)?, scalar_push(4, 4)?)?,
    )?;
    let msg_vtable = usize::from(arrow::ipc::Message::VT_BODYLENGTH) + 2;
    let rb = add(add(rb_slots, table_offset)?, rb_vtable)?;
    let msg = add(add(msg_slots, table_offset)?, msg_vtable)?;
    add(
        add(add(vector(nodes)?, vector(buffers)?)?, add(rb, msg)?)?,
        scalar_push(4, ALIGNMENT)?,
    )
}

#[derive(Debug, Clone)]
enum Leaf {
    Bool,
    Fixed(usize),
    Bytes { utf8: bool },
    Meta,
}
#[derive(Debug, Clone)]
struct Node {
    leaf: Leaf,
    nullable: bool,
}

fn append_nodes(field: &Field, out: &mut Vec<Node>) -> Result<()> {
    let leaf = match field.data_type() {
        DataType::Boolean => Leaf::Bool,
        DataType::Int8 | DataType::UInt8 => Leaf::Fixed(1),
        DataType::Int16 | DataType::UInt16 => Leaf::Fixed(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => Leaf::Fixed(4),
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => {
            Leaf::Fixed(8)
        }
        DataType::Utf8 => Leaf::Bytes { utf8: true },
        DataType::Binary => Leaf::Bytes { utf8: false },
        DataType::FixedSizeBinary(width) if *width > 0 => Leaf::Fixed(usize::try_from(*width)?),
        DataType::Struct(_) if field == arroyo_rpc::updating_meta_field().as_ref() => {
            // Canonical RPC child fields are nullable in the schema. Native
            // changelog values nevertheless require non-null parent/flag/id,
            // as checked by native_changelog_columns and UpdatingJoin::metadata.
            // Keep these value invariants independent of schema nullability.
            out.extend([
                Node {
                    leaf: Leaf::Meta,
                    nullable: false,
                },
                Node {
                    leaf: Leaf::Bool,
                    nullable: false,
                },
                Node {
                    leaf: Leaf::Fixed(16),
                    nullable: false,
                },
            ]);
            return Ok(());
        }
        _ => bail!(
            "response schema has an unproved native IPC type: {}",
            field.data_type()
        ),
    };
    out.push(Node {
        leaf,
        nullable: field.is_nullable(),
    });
    Ok(())
}

#[derive(Debug, Clone)]
enum Policy {
    /// Inherited ordinary raw IPC allowance. NOT a small decoded-memory proof.
    Raw,
    Native {
        owner_output_bytes: usize,
    },
    Keys {
        encoded_key_bytes: usize,
    },
}
#[derive(Debug, Clone)]
struct FrameSpec {
    schema: SchemaRef,
    max_rows: usize,
    nodes: Vec<Node>,
    buffers: usize,
    policy: Policy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodeShape {
    /// One aligned body copy retained by Arrow, plus original wire ownership.
    body_copy_capacity: usize,
    /// Schema-bounded cardinalities, not guessed allocator/header byte quotas.
    array_nodes: usize,
    buffer_handles: usize,
}
impl FrameSpec {
    fn native(schema: SchemaRef, max_rows: usize, owner_output_bytes: usize) -> Result<Self> {
        ensure!(
            (1..=MAX_NATIVE_ROWS).contains(&max_rows),
            "invalid native response row count"
        );
        Self::planned(schema, max_rows, Policy::Native { owner_output_bytes })
    }
    fn keys(encoded_key_bytes: usize) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "encoded_group_key",
            DataType::Binary,
            false,
        )]));
        Self::planned(schema, MAX_KEYS, Policy::Keys { encoded_key_bytes })
    }
    fn raw(schema: SchemaRef) -> Self {
        Self {
            schema,
            max_rows: 2,
            nodes: vec![],
            buffers: 0,
            policy: Policy::Raw,
        }
    }
    fn planned(schema: SchemaRef, max_rows: usize, policy: Policy) -> Result<Self> {
        ensure!(!schema.fields().is_empty(), "empty native response schema");
        let mut nodes = vec![];
        for field in schema.fields() {
            append_nodes(field, &mut nodes)?;
        }
        let mut buffers = 0;
        for node in &nodes {
            buffers = add(
                buffers,
                match node.leaf {
                    Leaf::Meta => 1,
                    Leaf::Bytes { .. } => 3,
                    _ => 2,
                },
            )?;
        }
        let result = Self {
            schema,
            max_rows,
            nodes,
            buffers,
            policy,
        };
        result.frame_bound()?;
        Ok(result)
    }
    fn body_bound(&self) -> Result<usize> {
        let unpadded = match self.policy {
            Policy::Raw => bail!("raw ordinary decoding has no native-owner body bound"),
            Policy::Native { owner_output_bytes } => {
                return arroyo_types::event_edge::native_body_bound(
                    owner_output_bytes,
                    self.max_rows,
                    self.nodes.len(),
                    self.buffers,
                )
                .map_err(anyhow::Error::msg);
            }
            Policy::Keys { encoded_key_bytes } => add(
                add(
                    mul(self.max_rows, encoded_key_bytes)?,
                    mul(add(self.max_rows, 1)?, 4)?,
                )?,
                bitmap(self.max_rows)?,
            )?,
        };
        add(unpadded, mul(self.buffers, ALIGNMENT - 1)?)
    }
    fn frame_bound(&self) -> Result<usize> {
        if matches!(self.policy, Policy::Raw) {
            return Ok(u32::MAX as usize);
        }
        let bound = add(
            IPC_LENGTH_BYTES,
            add(
                metadata_bound(self.nodes.len(), self.buffers)?,
                self.body_bound()?,
            )?,
        )?;
        ensure!(
            u32::try_from(bound).is_ok(),
            "native response derived frame does not fit existing u32 frame length"
        );
        Ok(bound)
    }
    fn options(&self) -> Result<IpcWriteOptions> {
        Ok(if matches!(self.policy, Policy::Raw) {
            IpcWriteOptions::default()
        } else {
            IpcWriteOptions::try_new(ALIGNMENT, false, MetadataVersion::V5)?
        })
    }
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        self.encode_with_ownership(batch, true)
    }
    fn encode_with_ownership(
        &self,
        batch: &RecordBatch,
        admit_original_owner: bool,
    ) -> Result<Vec<u8>> {
        ensure!(
            batch.schema() == self.schema,
            "response registered schema mismatch"
        );
        ensure!(
            batch.num_rows() > 0 && batch.num_rows() <= self.max_rows,
            "response row count outside registered stage"
        );
        if let Policy::Native { owner_output_bytes } = self.policy
            && admit_original_owner
        {
            ensure!(
                batch.get_array_memory_size() <= owner_output_bytes,
                "native sender exceeds its existing output allowance"
            );
        }
        if let Policy::Keys { encoded_key_bytes } = self.policy {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .context("invalid touched key array")?;
            ensure!(
                keys.null_count() == 0
                    && keys
                        .iter()
                        .all(|k| k.is_some_and(|key| key.len() <= encoded_key_bytes)),
                "touched key exceeds existing key allowance"
            );
        }
        if matches!(self.policy, Policy::Native { .. }) && !admit_original_owner {
            // Immutable backing can be shared by repeated projection columns.
            // IPC must copy each occurrence: prove that expanded body BEFORE
            // the canonical writer allocates any payload representation.
            let actual = arroyo_types::event_edge::native_canonical_body_len(batch)
                .map_err(anyhow::Error::msg)?;
            ensure!(
                actual <= self.body_bound()?,
                "native canonical body exceeds registered proof before encoding"
            );
        }
        let (dictionaries, data) = IpcDataGenerator {}.encoded_batch(
            batch,
            &mut DictionaryTracker::new(true),
            &self.options()?,
        )?;
        ensure!(
            dictionaries.is_empty(),
            "response dictionary framing is not implemented"
        );
        let mut encoded =
            Vec::with_capacity(add(4, add(data.ipc_message.len(), data.arrow_data.len())?)?);
        encoded.extend_from_slice(&u32::try_from(data.ipc_message.len())?.to_le_bytes());
        encoded.extend_from_slice(&data.ipc_message);
        encoded.extend_from_slice(&data.arrow_data);
        ensure!(
            encoded.len() <= self.frame_bound()?,
            "canonical writer exceeded derived response frame proof"
        );
        self.validate(&encoded)?;
        Ok(encoded)
    }
    fn validate<'a>(&self, frame: &'a [u8]) -> Result<Validated<'a>> {
        ensure!(
            frame.len() <= self.frame_bound()?,
            "response frame length exceeds registered proof"
        );
        let length =
            u32::from_le_bytes(frame.get(..4).context("missing IPC prefix")?.try_into()?) as usize;
        let metadata_end = add(4, length)?;
        let metadata = frame
            .get(4..metadata_end)
            .context("truncated IPC metadata")?;
        let body = frame.get(metadata_end..).context("truncated IPC body")?;
        if !matches!(self.policy, Policy::Raw) {
            ensure!(
                metadata.len() <= metadata_bound(self.nodes.len(), self.buffers)?,
                "response metadata exceeds schema proof"
            );
            ensure!(
                body.len() <= self.body_bound()?,
                "response body exceeds existing owner-derived proof"
            );
        }
        let message = arrow::ipc::root_as_message(metadata)
            .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
        ensure!(
            message.header_type() == MessageHeader::RecordBatch,
            "response requires one record batch"
        );
        let batch = message
            .header_as_record_batch()
            .context("missing response batch")?;
        ensure!(
            usize::try_from(message.bodyLength())? == body.len(),
            "response body length mismatch/trailing bytes"
        );
        let rows = usize::try_from(batch.length())?;
        ensure!(
            rows > 0 && rows <= self.max_rows,
            "response row count outside registered stage"
        );
        if matches!(self.policy, Policy::Raw) {
            return Ok(Validated {
                message,
                body,
                shape: None,
            });
        }
        ensure!(
            message.version() == MetadataVersion::V5 && message.custom_metadata().is_none(),
            "noncanonical response IPC metadata"
        );
        ensure!(
            batch.compression().is_none() && batch.variadicBufferCounts().is_none(),
            "compressed or variadic response IPC unsupported"
        );
        ensure!(
            metadata.len() <= metadata_bound(self.nodes.len(), self.buffers)?,
            "response metadata exceeds schema proof"
        );
        ensure!(
            body.len() <= self.body_bound()?,
            "response body exceeds existing owner-derived proof"
        );
        let nodes = batch.nodes().context("missing native nodes")?;
        let buffers = batch.buffers().context("missing native buffers")?;
        ensure!(
            nodes.len() == self.nodes.len() && buffers.len() == self.buffers,
            "response IPC shape differs from registered schema"
        );
        let mut ranges = std::iter::once(0..4).collect::<Vec<_>>();
        metadata_ranges(
            metadata,
            message._tab.loc(),
            message._tab.vtable().as_bytes(),
            message._tab.vtable().object_inline_num_bytes(),
            usize::from(arrow::ipc::Message::VT_BODYLENGTH) + 2,
            &mut ranges,
        )?;
        metadata_ranges(
            metadata,
            batch._tab.loc(),
            batch._tab.vtable().as_bytes(),
            batch._tab.vtable().object_inline_num_bytes(),
            usize::from(arrow::ipc::RecordBatch::VT_BUFFERS) + 2,
            &mut ranges,
        )?;
        for bytes in [nodes.bytes(), buffers.bytes()] {
            let start = (bytes.as_ptr() as usize)
                .checked_sub(metadata.as_ptr() as usize)
                .context("IPC vector before metadata")?;
            ranges.push(
                start.checked_sub(4).context("missing IPC vector prefix")?
                    ..add(start, bytes.len())?,
            );
        }
        ranges.sort_by_key(|r| r.start);
        let mut consumed = 0;
        for range in ranges {
            ensure!(
                range.start >= consumed && range.end <= metadata.len(),
                "overlapping/out-of-range response metadata"
            );
            ensure!(
                range.start - consumed < ALIGNMENT
                    && metadata[consumed..range.start].iter().all(|b| *b == 0),
                "unknown response metadata/padding"
            );
            consumed = range.end;
        }
        ensure!(consumed == metadata.len(), "unconsumed response metadata");
        let mut cursor = 0;
        let mut buffer_index = 0;
        for (node_index, spec) in self.nodes.iter().enumerate() {
            let node = nodes.get(node_index);
            let nulls = usize::try_from(node.null_count())?;
            ensure!(
                usize::try_from(node.length())? == rows
                    && nulls <= rows
                    && (spec.nullable || nulls == 0),
                "response node length/null count mismatch"
            );
            let mut take = |expected: Option<usize>| -> Result<&'a [u8]> {
                let buffer = buffers.get(buffer_index);
                buffer_index += 1;
                let start = usize::try_from(buffer.offset())?;
                let length = usize::try_from(buffer.length())?;
                ensure!(
                    start == cursor,
                    "response buffer is unaligned/overlapping/noncanonical"
                );
                if let Some(expected) = expected {
                    ensure!(
                        length == expected,
                        "response buffer length differs from schema"
                    );
                }
                let end = add(start, length)?;
                let next = align(end, ALIGNMENT)?;
                let value = body
                    .get(start..end)
                    .context("response buffer outside body")?;
                ensure!(
                    body.get(end..next)
                        .context("missing response padding")?
                        .iter()
                        .all(|b| *b == 0),
                    "nonzero response padding"
                );
                cursor = next;
                Ok(value)
            };
            let validity = take(Some(bitmap(rows)?))?;
            let valid = (0..rows)
                .filter(|i| validity[i / 8] & (1 << (i % 8)) != 0)
                .count();
            ensure!(
                rows - valid == nulls,
                "response bitmap disagrees with null count"
            );
            match spec.leaf {
                Leaf::Meta => {}
                Leaf::Bool => {
                    take(Some(bitmap(rows)?))?;
                }
                Leaf::Fixed(width) => {
                    take(Some(mul(rows, width)?))?;
                }
                Leaf::Bytes { utf8 } => {
                    let offsets = take(Some(mul(add(rows, 1)?, 4)?))?;
                    let values = take(None)?;
                    let mut previous = 0;
                    if utf8 {
                        std::str::from_utf8(values).context("response has invalid Utf8 payload")?;
                    }
                    for (index, raw) in offsets.chunks_exact(4).enumerate() {
                        let next = usize::try_from(i32::from_le_bytes(raw.try_into()?))?;
                        ensure!(
                            next >= previous && next <= values.len() && (index != 0 || next == 0),
                            "response byte offsets invalid"
                        );
                        if utf8 {
                            ensure!(
                                std::str::from_utf8(&values[previous..next]).is_ok(),
                                "response string offset splits codepoint"
                            );
                        }
                        if let Policy::Keys { encoded_key_bytes } = self.policy {
                            ensure!(
                                next - previous <= encoded_key_bytes,
                                "received key exceeds existing key allowance"
                            );
                        }
                        previous = next;
                    }
                    ensure!(
                        previous == values.len(),
                        "response has trailing variable payload"
                    );
                }
            }
        }
        ensure!(
            cursor == body.len() && buffer_index == self.buffers,
            "unconsumed response buffers/body"
        );
        Ok(Validated {
            message,
            body,
            shape: Some(DecodeShape {
                body_copy_capacity: body.len(),
                array_nodes: self.nodes.len(),
                buffer_handles: self.buffers,
            }),
        })
    }
    fn decode(&self, frame: &[u8]) -> Result<(RecordBatch, Option<DecodeShape>)> {
        let (batch, shape, _) = self.decode_with_body(frame)?;
        Ok((batch, shape))
    }
    fn decode_with_body(
        &self,
        frame: &[u8],
    ) -> Result<(RecordBatch, Option<DecodeShape>, arrow::buffer::Buffer)> {
        let validated = self.validate(frame)?;
        let mut buffer = MutableBuffer::from_len_zeroed(validated.body.len());
        buffer.copy_from_slice(validated.body);
        let body: arrow::buffer::Buffer = buffer.into();
        let batch = read_record_batch(
            &body,
            validated
                .message
                .header_as_record_batch()
                .context("missing batch")?,
            self.schema.clone(),
            &HashMap::new(),
            None,
            &validated.message.version(),
        )?;
        Ok((batch, validated.shape, body))
    }
}
#[derive(Debug)]
struct Validated<'a> {
    message: arrow::ipc::Message<'a>,
    body: &'a [u8],
    shape: Option<DecodeShape>,
}

fn metadata_ranges(
    metadata: &[u8],
    location: usize,
    vtable: &[u8],
    inline_bytes: usize,
    max_vtable_bytes: usize,
    out: &mut Vec<Range<usize>>,
) -> Result<()> {
    ensure!(
        vtable.len() <= max_vtable_bytes,
        "unsupported response table fields"
    );
    let start = (vtable.as_ptr() as usize)
        .checked_sub(metadata.as_ptr() as usize)
        .context("vtable before metadata")?;
    out.push(start..add(start, vtable.len())?);
    out.push(location..add(location, inline_bytes)?);
    Ok(())
}

#[derive(Clone)]
pub(crate) struct ResponseSpec {
    root_operator: u32,
    root_subtask: u32,
    rows: FrameSpec,
    keys: Option<FrameSpec>,
}
pub(crate) struct EncodedResponse {
    pub(crate) extension: [u8; EXTENSION_BYTES],
    pub(crate) rows: Vec<u8>,
    pub(crate) keys: Vec<u8>,
}
impl ResponseSpec {
    pub(crate) fn from_edge(edge: &arroyo_types::event_edge::EventEdgeSpec) -> Result<Self> {
        use arroyo_types::event_edge::EventLayout;
        match edge.layout {
            EventLayout::Raw => Ok(Self::raw(
                edge.root_operator,
                edge.root_subtask,
                edge.schema.clone(),
            )),
            EventLayout::Native {
                max_rows,
                owner_output_bytes,
                encoded_key_bytes,
            } => Self::native(
                edge.root_operator,
                edge.root_subtask,
                edge.schema.clone(),
                max_rows,
                owner_output_bytes,
                encoded_key_bytes,
            ),
        }
    }
    fn raw(root_operator: u32, root_subtask: u32, schema: SchemaRef) -> Self {
        Self {
            root_operator,
            root_subtask,
            rows: FrameSpec::raw(schema),
            keys: None,
        }
    }
    fn native(
        root_operator: u32,
        root_subtask: u32,
        schema: SchemaRef,
        rows: usize,
        owner_output_bytes: usize,
        key_bytes: usize,
    ) -> Result<Self> {
        Ok(Self {
            root_operator,
            root_subtask,
            rows: FrameSpec::native(schema, rows, owner_output_bytes)?,
            keys: Some(FrameSpec::keys(key_bytes)?),
        })
    }
    /// Pure preallocation check: caller must consult registered Quad/type first.
    /// Context/keys extension is OUTSIDE Header.len, which remains rows length.
    pub(crate) fn preflight(
        &self,
        extension: &[u8],
        row_bytes: u32,
    ) -> Result<(EventContext, usize)> {
        ensure!(
            extension.len() == EXTENSION_BYTES,
            "response extension length mismatch"
        );
        let get = |r: Range<usize>| -> Result<&[u8]> {
            extension.get(r).context("truncated response context")
        };
        let context = EventContext {
            version: u32::from_le_bytes(get(0..4)?.try_into()?),
            root_operator: u32::from_le_bytes(get(4..8)?.try_into()?),
            root_subtask: u32::from_le_bytes(get(8..12)?.try_into()?),
            attempt_nonce: get(12..28)?.try_into()?,
            sequence: u64::from_le_bytes(get(28..36)?.try_into()?),
            trigger_nanos: i64::from_le_bytes(get(36..44)?.try_into()?),
        };
        ensure!(
            context.version == EVENT_RESPONSE_VERSION
                && context.root_operator == self.root_operator
                && context.root_subtask == self.root_subtask,
            "response version/root mismatch"
        );
        ensure!(
            row_bytes as usize <= self.rows.frame_bound()?,
            "declared response rows exceed registered proof"
        );
        let keys = u32::from_le_bytes(get(44..48)?.try_into()?) as usize;
        match &self.keys {
            None => ensure!(keys == 0, "raw response carries native keys"),
            Some(spec) => ensure!(
                keys <= spec.frame_bound()?,
                "declared response keys exceed registered proof"
            ),
        }
        add(EXTENSION_BYTES, add(row_bytes as usize, keys)?)?;
        Ok((context, keys))
    }
    fn edge_spec(&self) -> arroyo_types::event_edge::EventEdgeSpec {
        use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
        let layout = match self.rows.policy {
            Policy::Raw => EventLayout::Raw,
            Policy::Native { owner_output_bytes } => EventLayout::Native {
                max_rows: self.rows.max_rows,
                owner_output_bytes,
                encoded_key_bytes: match self.keys.as_ref().map(|keys| &keys.policy) {
                    Some(Policy::Keys { encoded_key_bytes }) => *encoded_key_bytes,
                    _ => 0,
                },
            },
            Policy::Keys { .. } => unreachable!("response rows cannot use key policy"),
        };
        EventEdgeSpec {
            root_operator: self.root_operator,
            root_subtask: self.root_subtask,
            schema: self.rows.schema.clone(),
            layout,
        }
    }
    pub(crate) fn encode(&self, response: &EventResponse) -> Result<EncodedResponse> {
        let admit_original_owner = !response.is_native_forward(&self.edge_spec());
        let rows = response
            .rows
            .as_ref()
            .map(|b| self.rows.encode_with_ownership(b, admit_original_owner))
            .transpose()?
            .unwrap_or_default();
        let keys = match (&self.keys, &response.touched_keys) {
            (_, None) => vec![],
            (Some(spec), Some(b)) => spec.encode(b)?,
            (None, Some(_)) => bail!("raw response carries native keys"),
        };
        let c = response.context;
        let mut extension = [0; EXTENSION_BYTES];
        extension[..4].copy_from_slice(&c.version.to_le_bytes());
        extension[4..8].copy_from_slice(&c.root_operator.to_le_bytes());
        extension[8..12].copy_from_slice(&c.root_subtask.to_le_bytes());
        extension[12..28].copy_from_slice(&c.attempt_nonce);
        extension[28..36].copy_from_slice(&c.sequence.to_le_bytes());
        extension[36..44].copy_from_slice(&c.trigger_nanos.to_le_bytes());
        extension[44..].copy_from_slice(&u32::try_from(keys.len())?.to_le_bytes());
        self.preflight(&extension, u32::try_from(rows.len())?)?;
        Ok(EncodedResponse {
            extension,
            rows,
            keys,
        })
    }
    pub(crate) fn decode(&self, encoded: &EncodedResponse) -> Result<EventResponse> {
        let (context, key_bytes) =
            self.preflight(&encoded.extension, u32::try_from(encoded.rows.len())?)?;
        ensure!(
            encoded.keys.len() == key_bytes,
            "response key frame length mismatch"
        );
        let decoded = if encoded.rows.is_empty() {
            None
        } else {
            Some(self.rows.decode_with_body(&encoded.rows)?)
        };
        let rows = decoded.as_ref().map(|(batch, _, _)| batch.clone());
        let touched_keys = if encoded.keys.is_empty() {
            None
        } else {
            Some(
                self.keys
                    .as_ref()
                    .context("unexpected keys")?
                    .decode(&encoded.keys)?
                    .0,
            )
        };
        let response = EventResponse {
            native_body: None,
            context,
            rows,
            touched_keys,
        };
        if matches!(self.rows.policy, Policy::Native { .. })
            && let Some((_, _, body)) = decoded
        {
            response
                .with_native_body(&self.edge_spec(), body)
                .map_err(anyhow::Error::msg)
        } else {
            Ok(response)
        }
    }
}

/// Rebind native scalar column forwarding without double-counting IPC backing.
/// If the retained original body cannot fit the new stage, compact through the
/// SAME validated canonical encoder/decoder after exact body-size preflight.
pub(crate) fn rebind_response_rows(
    response: &EventResponse,
    rows: Option<RecordBatch>,
    next: &arroyo_types::event_edge::EventEdgeSpec,
) -> Result<EventResponse> {
    if let Ok(rebound) = response.try_rebind_native_rows(next, rows.clone()) {
        return Ok(rebound);
    }
    let rows = rows.context("native row rebind requires rows")?;
    next.validate_shape(&EventResponse {
        context: response.context,
        rows: Some(rows.clone()),
        touched_keys: response.touched_keys.clone(),
        native_body: None,
    })
    .map_err(anyhow::Error::msg)?;
    ensure!(
        response.proves_native_projection(next, &rows),
        "native row change requires original owner admission"
    );
    let next_codec = ResponseSpec::from_edge(next)?;
    let actual =
        arroyo_types::event_edge::native_canonical_body_len(&rows).map_err(anyhow::Error::msg)?;
    ensure!(
        actual <= next_codec.rows.body_bound()?,
        "native projected body exceeds registered proof"
    );
    let encoded = next_codec.rows.encode_with_ownership(&rows, false)?;
    let (rows, _, body) = next_codec.rows.decode_with_body(&encoded)?;
    EventResponse {
        context: response.context,
        rows: Some(rows),
        touched_keys: response.touched_keys.clone(),
        native_body: None,
    }
    .with_native_body(next, body)
    .map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int64Array, StringArray,
        StructArray, TimestampNanosecondArray,
    };

    fn context() -> EventContext {
        EventContext {
            version: EVENT_RESPONSE_VERSION,
            root_operator: 17,
            root_subtask: 0,
            attempt_nonce: [255; 16],
            sequence: u64::MAX,
            trigger_nanos: i64::MIN,
        }
    }
    fn keys(size: usize) -> RecordBatch {
        let a = vec![7u8; size];
        let b = vec![8u8; size];
        RecordBatch::try_new(
            FrameSpec::keys(size).unwrap().schema,
            vec![Arc::new(BinaryArray::from_vec(vec![&a, &b])) as ArrayRef],
        )
        .unwrap()
    }
    fn mixed(rows: usize, null_pattern: usize) -> RecordBatch {
        let present = |i: usize| null_pattern & (1 << i) == 0;
        let ints = Int64Array::from(
            (0..rows)
                .map(|i| present(i).then_some(if i % 2 == 0 { i64::MIN } else { i64::MAX }))
                .collect::<Vec<_>>(),
        );
        let strings = StringArray::from(
            (0..rows)
                .map(|i| present(i).then_some(if i % 2 == 0 { "é界" } else { "" }))
                .collect::<Vec<_>>(),
        );
        let bytes = BinaryArray::from(
            (0..rows)
                .map(|i| {
                    present(i).then_some(if i % 2 == 0 {
                        &[0, 255, 3][..]
                    } else {
                        &[][..]
                    })
                })
                .collect::<Vec<_>>(),
        );
        let times = TimestampNanosecondArray::from(
            (0..rows)
                .map(|i| present(i).then_some(i64::MAX - i as i64))
                .collect::<Vec<_>>(),
        );
        let meta_fields = arroyo_rpc::updating_meta_fields();
        let meta = StructArray::new(
            meta_fields.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![false; rows])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| &[3u8; 16][..])).unwrap(),
                ) as ArrayRef,
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                arroyo_rpc::UPDATING_META_FIELD,
                DataType::Struct(meta_fields),
                false,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ints),
                Arc::new(strings),
                Arc::new(bytes),
                Arc::new(times),
                Arc::new(meta),
            ],
        )
        .unwrap()
    }
    fn spec(batch: &RecordBatch) -> ResponseSpec {
        ResponseSpec::native(
            17,
            0,
            batch.schema(),
            MAX_NATIVE_ROWS,
            batch.get_array_memory_size(),
            512,
        )
        .unwrap()
    }
    fn body_start(frame: &[u8]) -> usize {
        4 + u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize
    }
    fn buffer_start(frame: &[u8], index: usize) -> usize {
        let end = body_start(frame);
        let m = arrow::ipc::root_as_message(&frame[4..end]).unwrap();
        end + m
            .header_as_record_batch()
            .unwrap()
            .buffers()
            .unwrap()
            .get(index)
            .offset() as usize
    }
    #[test]
    fn event_transport_body_preflight_matches_wide_canonical_writer_nulls_and_slices() -> Result<()>
    {
        for nulls in 0..16 {
            let batch = mixed(4, nulls);
            for (offset, rows) in [(0, 4), (1, 3), (2, 1)] {
                let batch = batch.slice(offset, rows);
                let encoded = spec(&batch).rows.encode(&batch)?;
                assert_eq!(
                    encoded.len() - body_start(&encoded),
                    arroyo_types::event_edge::native_canonical_body_len(&batch)
                        .map_err(anyhow::Error::msg)?
                );
            }
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(arrow_array::Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(i8::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::UInt8Array::from(vec![
                Some(u8::MIN),
                None,
                Some(u8::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(i16::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::UInt16Array::from(vec![
                Some(u16::MIN),
                None,
                Some(u16::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(i32::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::UInt32Array::from(vec![
                Some(u32::MIN),
                None,
                Some(u32::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::UInt64Array::from(vec![
                Some(u64::MIN),
                None,
                Some(u64::MAX),
                Some(0),
            ])),
            Arc::new(arrow_array::Float32Array::from(vec![
                Some(f32::MIN),
                None,
                Some(f32::MAX),
                Some(0.0),
            ])),
            Arc::new(arrow_array::Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(f64::MAX),
                Some(0.0),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    Some(&[1u8, 2, 3][..]),
                    None,
                    Some(&[0u8, 0, 0][..]),
                    Some(&[255u8, 255, 255][..]),
                ]
                .into_iter(),
                3,
            )?),
        ];
        let fields = columns
            .iter()
            .enumerate()
            .map(|(i, column)| Field::new(format!("v{i}"), column.data_type().clone(), true))
            .collect::<Vec<_>>();
        let wide = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        for (offset, rows) in [(0, 4), (1, 3), (2, 1)] {
            let wide = wide.slice(offset, rows);
            let encoded = spec(&wide).rows.encode(&wide)?;
            assert_eq!(
                encoded.len() - body_start(&encoded),
                arroyo_types::event_edge::native_canonical_body_len(&wide)
                    .map_err(anyhow::Error::msg)?
            );
        }
        Ok(())
    }

    #[test]
    fn event_response_codec_accepts_only_exact_canonical_metadata_fields() -> Result<()> {
        let canonical = arroyo_rpc::updating_meta_field();
        assert!(
            arroyo_rpc::updating_meta_fields()
                .iter()
                .all(|f| f.is_nullable())
        );
        let f = FrameSpec::native(Arc::new(Schema::new(vec![canonical.clone()])), 4, 1024)?;
        assert_eq!(f.nodes.len(), 3);
        assert_eq!(f.buffers, 5);
        assert!(f.nodes.iter().all(|n| !n.nullable));
        let fields = arroyo_rpc::updating_meta_fields();
        let wrong_fields = [
            vec![fields[1].clone(), fields[0].clone()],
            vec![
                Arc::new(fields[0].as_ref().clone().with_nullable(false)),
                fields[1].clone(),
            ],
            vec![
                fields[0].clone(),
                Arc::new(fields[1].as_ref().clone().with_nullable(false)),
            ],
            vec![
                Arc::new(Field::new("retract", DataType::Boolean, true)),
                fields[1].clone(),
            ],
            vec![
                fields[0].clone(),
                Arc::new(Field::new("id", DataType::FixedSizeBinary(8), true)),
            ],
        ];
        for children in wrong_fields {
            let forged = Field::new(
                arroyo_rpc::UPDATING_META_FIELD,
                DataType::Struct(children.into()),
                false,
            );
            assert!(FrameSpec::native(Arc::new(Schema::new(vec![forged])), 4, 1024).is_err());
        }
        for forged in [
            canonical.as_ref().clone().with_nullable(true),
            canonical.as_ref().clone().with_name("other"),
        ] {
            assert!(FrameSpec::native(Arc::new(Schema::new(vec![forged])), 4, 1024).is_err());
        }
        Ok(())
    }

    #[test]
    fn event_response_codec_rejects_actual_null_metadata_parent_flag_and_id() -> Result<()> {
        let canonical = arroyo_rpc::updating_meta_field();
        let f = FrameSpec::native(Arc::new(Schema::new(vec![canonical.clone()])), 4, 4096)?;
        // First prove a valid canonical metadata-only frame actually roundtrips.
        for null_part in [3, 0, 1, 2] {
            let flags = BooleanArray::from(vec![(null_part != 1).then_some(false)]);
            let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                std::iter::once((null_part != 2).then_some(&[3u8; 16][..])),
                16,
            )?;
            let parent_nulls =
                (null_part == 0).then(|| arrow::buffer::NullBuffer::from(vec![false]));
            let metadata = StructArray::new(
                arroyo_rpc::updating_meta_fields(),
                vec![Arc::new(flags) as ArrayRef, Arc::new(ids) as ArrayRef],
                parent_nulls,
            );
            // A nullable top-level fixture lets Arrow construct a real null
            // parent. IPC carries values, not schema: the receiver still uses
            // the exact registered, non-null canonical parent above.
            let fixture_field = canonical.as_ref().clone().with_nullable(null_part == 0);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![fixture_field])),
                vec![Arc::new(metadata)],
            )?;
            let (dictionaries, encoded) = IpcDataGenerator {}.encoded_batch(
                &batch,
                &mut DictionaryTracker::new(true),
                &f.options()?,
            )?;
            assert!(dictionaries.is_empty());
            let mut frame = u32::try_from(encoded.ipc_message.len())?
                .to_le_bytes()
                .to_vec();
            frame.extend_from_slice(&encoded.ipc_message);
            frame.extend_from_slice(&encoded.arrow_data);
            if null_part == 3 {
                assert_eq!(f.decode(&frame)?.0, batch);
            } else {
                let validation = f
                    .validate(&frame)
                    .expect_err("null metadata must fail validation");
                assert_eq!(
                    validation.to_string(),
                    "response node length/null count mismatch"
                );
                let decoding = f
                    .decode(&frame)
                    .expect_err("null metadata must fail before decoding");
                assert_eq!(
                    decoding.to_string(),
                    "response node length/null count mismatch"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn event_response_codec_roundtrips_all_native_row_null_patterns_and_slices() -> Result<()> {
        for rows in 1..=MAX_NATIVE_ROWS {
            for nulls in 0..1usize << rows {
                let batch = mixed(rows, nulls);
                let spec = spec(&batch);
                let response = EventResponse {
                    native_body: None,
                    context: context(),
                    rows: Some(batch.clone()),
                    touched_keys: Some(keys(512)),
                };
                let wire = spec.encode(&response)?;
                assert!(wire.rows.len() <= spec.rows.frame_bound()?);
                assert_eq!(spec.decode(&wire)?, response);
                let shape = spec.rows.validate(&wire.rows)?.shape.unwrap();
                assert_eq!(shape.array_nodes, 7);
                assert_eq!(shape.buffer_handles, 15);
                assert_eq!(
                    shape.body_copy_capacity,
                    spec.rows.validate(&wire.rows)?.body.len()
                );
            }
        }
        let batch = mixed(4, 0b1010).slice(1, 2);
        let spec = spec(&batch);
        let response = EventResponse {
            native_body: None,
            context: context(),
            rows: Some(batch),
            touched_keys: Some(keys(512).slice(1, 1)),
        };
        assert_eq!(spec.decode(&spec.encode(&response)?)?, response);
        Ok(())
    }

    #[test]
    fn event_response_codec_all_proved_primitive_widths_roundtrip() -> Result<()> {
        let base = Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef;
        for ty in [
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
        ] {
            let array = arrow::compute::cast(base.as_ref(), &ty)?;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", ty, false)])),
                vec![array],
            )?;
            let f = FrameSpec::native(batch.schema(), 4, batch.get_array_memory_size())?;
            assert_eq!(f.decode(&f.encode(&batch)?)?.0, batch);
        }
        Ok(())
    }
    #[test]
    fn event_response_codec_large_payloads_at_exact_owner_allowance_with_slices() -> Result<()> {
        let text = "é界".repeat(1024);
        let bytes = vec![255u8; 4097];
        let strings = StringArray::from(vec![
            Some("prefix"),
            Some(text.as_str()),
            None,
            Some(""),
            Some(text.as_str()),
            Some("suffix"),
        ]);
        let binary = BinaryArray::from(vec![
            Some(&[1u8][..]),
            Some(bytes.as_slice()),
            None,
            Some(&[][..]),
            Some(bytes.as_slice()),
            Some(&[2u8][..]),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(strings), Arc::new(binary)])?.slice(1, 4);
        let f = FrameSpec::native(batch.schema(), 4, batch.get_array_memory_size())?;
        let wire = f.encode(&batch)?;
        assert_eq!(f.decode(&wire)?.0, batch);
        assert!(wire.len() <= f.frame_bound()?);
        Ok(())
    }
    #[test]
    fn event_response_codec_distinguishes_empty_unchanged_keys_and_present_null() -> Result<()> {
        let batch = mixed(1, 1);
        let spec = spec(&batch);
        for response in [
            EventResponse {
                native_body: None,
                context: context(),
                rows: None,
                touched_keys: None,
            },
            EventResponse {
                native_body: None,
                context: context(),
                rows: None,
                touched_keys: Some(keys(512)),
            },
            EventResponse {
                native_body: None,
                context: context(),
                rows: Some(batch),
                touched_keys: Some(keys(512)),
            },
        ] {
            let wire = spec.encode(&response)?;
            assert_eq!(spec.decode(&wire)?, response);
        }
        Ok(())
    }
    #[test]
    fn event_response_codec_raw_keeps_full_u32_rows_outside_fixed_extension() -> Result<()> {
        let batch = mixed(2, 0);
        let raw = ResponseSpec::raw(17, 0, batch.schema());
        let response = EventResponse {
            native_body: None,
            context: context(),
            rows: Some(batch),
            touched_keys: None,
        };
        let wire = raw.encode(&response)?;
        assert_eq!(wire.extension.len(), 48);
        assert_eq!(raw.decode(&wire)?, response);
        raw.preflight(&wire.extension, u32::MAX)?;
        let (_, size) = raw.preflight(&wire.extension, 0)?;
        assert_eq!(size, 0);
        let mut bad = wire.extension;
        bad[44..48].copy_from_slice(&1u32.to_le_bytes());
        assert!(raw.preflight(&bad, 1).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_native_declared_bound_accepts_exact_rejects_one_more() -> Result<()> {
        let batch = mixed(4, 0);
        let spec = spec(&batch);
        let wire = spec.encode(&EventResponse {
            native_body: None,
            context: context(),
            rows: Some(batch),
            touched_keys: Some(keys(512)),
        })?;
        let bound = u32::try_from(spec.rows.frame_bound()?)?;
        spec.preflight(&wire.extension, bound)?;
        assert!(spec.preflight(&wire.extension, bound + 1).is_err());
        let mut bad = wire.extension;
        bad[..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(spec.preflight(&bad, 0).is_err());
        let mut bad = wire.extension;
        bad[4..8].copy_from_slice(&18u32.to_le_bytes());
        assert!(spec.preflight(&bad, 0).is_err());
        assert!(spec.preflight(&wire.extension[..47], 0).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_rejects_unproved_types_and_bound_overflow() {
        for ty in [
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Struct(vec![Field::new("v", DataType::Int64, true)].into()),
        ] {
            assert!(
                FrameSpec::native(
                    Arc::new(Schema::new(vec![Field::new("v", ty, true)])),
                    4,
                    1024
                )
                .is_err()
            );
        }
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        assert!(FrameSpec::native(schema.clone(), 4, usize::MAX).is_err());
        assert!(FrameSpec::native(schema, 5, 1024).is_err());
    }
    #[test]
    fn event_response_codec_rejects_owner_key_and_output_limit_violations() -> Result<()> {
        let batch = mixed(4, 0);
        let spec = spec(&batch);
        let too_small = FrameSpec::native(batch.schema(), 4, batch.get_array_memory_size() - 1)?;
        assert!(too_small.encode(&batch).is_err());
        assert!(FrameSpec::keys(511)?.encode(&keys(512)).is_err());
        let mut wire = spec.encode(&EventResponse {
            native_body: None,
            context: context(),
            rows: Some(batch),
            touched_keys: Some(keys(512)),
        })?;
        wire.keys.pop();
        assert!(spec.decode(&wire).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_rejects_body_truncation_trailing_metadata_and_nonzero_padding()
    -> Result<()> {
        let batch = mixed(2, 0);
        let frame = spec(&batch).rows.encode(&batch)?;
        let f = spec(&batch).rows;
        let mut bad = frame.clone();
        bad.pop();
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        bad.push(0);
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let end = body_start(&bad);
        bad.splice(end..end, [0u8; 8]);
        let len = u32::from_le_bytes(bad[..4].try_into()?);
        bad[..4].copy_from_slice(&(len + 8).to_le_bytes());
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let at = buffer_start(&bad, 0);
        bad[at + 1] = 1;
        assert!(f.validate(&bad).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_rejects_bitmap_offsets_utf8_and_node_shape_forgery() -> Result<()> {
        let batch = mixed(2, 0);
        let f = spec(&batch).rows;
        let frame = f.encode(&batch)?;
        let mut bad = frame.clone();
        let at = buffer_start(&bad, 0);
        bad[at] &= !1;
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let at = buffer_start(&bad, 3);
        bad[at..at + 4].copy_from_slice(&1i32.to_le_bytes());
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let at = buffer_start(&bad, 4);
        bad[at] = 255;
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let end = body_start(&bad);
        let at = {
            let m = arrow::ipc::root_as_message(&bad[4..end])
                .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
            (m.header_as_record_batch()
                .unwrap()
                .nodes()
                .unwrap()
                .bytes()
                .as_ptr() as usize)
                - (bad.as_ptr() as usize)
        };
        bad[at..at + 8].copy_from_slice(&i64::MAX.to_le_bytes());
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let end = body_start(&bad);
        let at = {
            let m = arrow::ipc::root_as_message(&bad[4..end])
                .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
            (m.header_as_record_batch()
                .unwrap()
                .buffers()
                .unwrap()
                .bytes()
                .as_ptr() as usize)
                - (bad.as_ptr() as usize)
                + 16
        };
        bad[at..at + 8].copy_from_slice(&0i64.to_le_bytes());
        assert!(f.validate(&bad).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_rejects_wrong_version_message_type_and_extra_buffer() -> Result<()> {
        let batch = mixed(1, 0);
        let f = spec(&batch).rows;
        let frame = f.encode(&batch)?;
        let mut bad = frame.clone();
        let end = body_start(&bad);
        let at = {
            let m = arrow::ipc::root_as_message(&bad[4..end])
                .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
            4 + m._tab.loc() + usize::from(m._tab.vtable().get(arrow::ipc::Message::VT_VERSION))
        };
        bad[at..at + 2].copy_from_slice(&MetadataVersion::V4.0.to_le_bytes());
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let at = {
            let m = arrow::ipc::root_as_message(&bad[4..end])
                .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
            4 + m._tab.loc() + usize::from(m._tab.vtable().get(arrow::ipc::Message::VT_HEADER_TYPE))
        };
        bad[at] = MessageHeader::Schema.0;
        assert!(f.validate(&bad).is_err());
        let mut bad = frame.clone();
        let at = {
            let m = arrow::ipc::root_as_message(&bad[4..end])
                .map_err(|error| anyhow::anyhow!("invalid IPC flatbuffer: {error:?}"))?;
            (m.header_as_record_batch()
                .unwrap()
                .buffers()
                .unwrap()
                .bytes()
                .as_ptr() as usize)
                - (bad.as_ptr() as usize)
                - 4
        };
        bad[at..at + 4].copy_from_slice(&99u32.to_le_bytes());
        assert!(f.validate(&bad).is_err());
        Ok(())
    }
    #[test]
    fn event_response_codec_rejects_compression_before_body_decode() -> Result<()> {
        let batch = mixed(4, 0);
        let f = spec(&batch).rows;
        let options = IpcWriteOptions::try_new(ALIGNMENT, false, MetadataVersion::V5)?
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))?;
        let (_, encoded) = IpcDataGenerator {}.encoded_batch(
            &batch,
            &mut DictionaryTracker::new(true),
            &options,
        )?;
        let mut bytes = vec![];
        bytes.extend_from_slice(&u32::try_from(encoded.ipc_message.len())?.to_le_bytes());
        bytes.extend_from_slice(&encoded.ipc_message);
        bytes.extend_from_slice(&encoded.arrow_data);
        assert!(f.validate(&bytes).is_err());
        Ok(())
    }
}
