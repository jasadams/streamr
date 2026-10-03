//! Paginated Arrow histories with an atomic expiry index. Each IPC chunk is a
//! separate primary record; no read concatenates a key's entire history.
use super::resources::{ResourcePermit, WorkerStateResources};
use super::{
    LiveStateBackend, LiveStateError, Result, ScanCursor, ScanRange, ScanRequest, StateKey,
    StateNamespace, StateSnapshot, WriteBatch, WriteOperation,
};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::{
    io::{self, Cursor, Write},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug)]
pub struct HistoryLimits {
    pub chunk_bytes: usize,
    pub chunk_rows: usize,
    pub page_bytes: usize,
    pub page_entries: usize,
    pub batch_bytes: usize,
}

/// The primary index orders full keys, signed timestamps and sequence numbers.
/// The expiry index orders timestamps first. Both indexes are changed together.
/// Sequence numbers are supplied by the operator and must be unique per key/time.
pub struct ArrowHistory {
    backend: Arc<dyn LiveStateBackend>,
    primary: StateNamespace,
    expiry: StateNamespace,
    schema: SchemaRef,
    limits: HistoryLimits,
    // Serializes this view's inserts with expiry, including across awaits.
    mutation: Mutex<()>,
    resources: Option<WorkerStateResources>,
}

pub struct HistorySnapshot {
    snapshot: StateSnapshot,
    primary: StateNamespace,
    schema: SchemaRef,
    limits: HistoryLimits,
    resources: Option<WorkerStateResources>,
}

pub struct HistoryChunk {
    pub timestamp: i64,
    pub sequence: u64,
    pub batch: RecordBatch,
}

pub struct HistoryPage {
    pub chunks: Vec<HistoryChunk>,
    pub next_cursor: Option<ScanCursor>,
    _decoded: Option<ResourcePermit>,
}

impl ArrowHistory {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        namespace: StateNamespace,
        schema: SchemaRef,
        limits: HistoryLimits,
    ) -> Result<Self> {
        if [
            limits.chunk_bytes,
            limits.chunk_rows,
            limits.page_bytes,
            limits.page_entries,
            limits.batch_bytes,
        ]
        .contains(&0)
        {
            return Err(LiveStateError::InvalidLimit);
        }
        // Routed namespaces need the operator's supplied routing hash on every key;
        // history v1 is explicitly partition local (or replicated/connector).
        if matches!(namespace.ownership, super::Ownership::Routed { .. }) {
            return Err(LiveStateError::InvalidEncoding(
                "routed histories require an operator routing adapter".into(),
            ));
        }
        let primary = index_namespace(&namespace, 0);
        let expiry = index_namespace(&namespace, 1);
        Ok(Self {
            backend,
            primary,
            expiry,
            schema,
            limits,
            mutation: Mutex::new(()),
            resources: None,
        })
    }

    /// Use the same worker resources as the backend. Retained decoded pages keep
    /// their reservation until dropped; callers own the memory of input batches.
    pub fn with_resources(mut self, resources: WorkerStateResources) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Commits one bounded chunk at a time. Cancellation may leave a committed
    /// prefix. Repeating the same key/time/sequence is an idempotent replacement.
    /// Timestamp units are caller-selected and must match the expiry cutoff.
    pub async fn append(
        &self,
        key: &[u8],
        timestamp: i64,
        sequence: u64,
        batch: &RecordBatch,
    ) -> Result<u64> {
        if batch.schema() != self.schema {
            return Err(LiveStateError::InvalidEncoding(
                "Arrow history schema mismatch".into(),
            ));
        }
        // Arrow IPC assembles a message body before writing to the sink. Bound
        // its source buffers first, in addition to capping the serialized sink.
        // Large caller inputs must be compacted into bounded batches upstream.
        let source_bytes = batch.get_array_memory_size();
        if source_bytes > self.limits.chunk_bytes {
            return Err(LiveStateError::ReadLimitExceeded {
                required: source_bytes,
                limit: self.limits.chunk_bytes,
            });
        }
        let _mutation = self.mutation.lock().await;
        let mut offset = 0;
        let mut next_sequence = sequence;
        while offset < batch.num_rows() {
            let mut rows = (batch.num_rows() - offset).min(self.limits.chunk_rows);
            let reservation = if let Some(resources) = &self.resources {
                Some(
                    resources
                        .decoded_value(
                            self.limits
                                .chunk_bytes
                                .checked_mul(3)
                                .ok_or(LiveStateError::InvalidLimit)?,
                        )
                        .await?,
                )
            } else {
                None
            };
            let encoded = loop {
                match encode_chunk(&batch.slice(offset, rows), self.limits.chunk_bytes) {
                    Ok(bytes) => break bytes,
                    Err(_) if rows > 1 => rows = (rows / 2).max(1),
                    Err(error) => return Err(error),
                }
            };
            let following = next_sequence.checked_add(1).ok_or_else(|| {
                LiveStateError::InvalidEncoding("history sequence overflow".into())
            })?;
            let primary_key = primary_key(key, timestamp, next_sequence);
            let expiry_key = expiry_key(key, timestamp, next_sequence);
            drop(reservation);
            self.backend
                .write_batch(WriteBatch {
                    operations: vec![
                        WriteOperation::Put {
                            key: state_key(&self.primary, primary_key.clone()),
                            value: encoded,
                        },
                        WriteOperation::Put {
                            key: state_key(&self.expiry, expiry_key),
                            value: primary_key,
                        },
                    ],
                    max_bytes: self.limits.batch_bytes,
                })
                .await?;
            offset += rows;
            next_sequence = following;
        }
        Ok(next_sequence)
    }

    pub async fn snapshot(&self) -> Result<HistorySnapshot> {
        let _mutation = self.mutation.lock().await;
        Ok(HistorySnapshot {
            snapshot: self.backend.snapshot().await?,
            primary: self.primary.clone(),
            schema: self.schema.clone(),
            limits: self.limits,
            resources: self.resources.clone(),
        })
    }

    /// Removes at most one page of chunks strictly older than cutoff. The cutoff
    /// boundary is retained, matching the legacy expiring table. Repeat until 0.
    /// A fresh snapshot per page avoids keeping a hot key's history in memory.
    pub async fn expire_page(&self, cutoff: i64) -> Result<usize> {
        let _mutation = self.mutation.lock().await;
        let snapshot = self.backend.snapshot().await?;
        let page = snapshot
            .scan(ScanRequest {
                range: ScanRange {
                    namespace: self.expiry.clone(),
                    prefix: None,
                    start: None,
                    end: Some(sortable_time(cutoff).to_vec()),
                },
                max_entries: self.limits.page_entries,
                max_bytes: self.limits.page_bytes,
                cursor: None,
            })
            .await?;
        let count = page.entries.len();
        let mut operations = Vec::with_capacity(count * 2);
        for entry in page.entries {
            // Never accept an index pointing outside its paired primary table.
            let (key, timestamp, sequence) = parse_primary(&entry.value)?;
            if expiry_key(&key, timestamp, sequence) != entry.key.key {
                return Err(LiveStateError::InvalidEncoding(
                    "inconsistent history expiry index".into(),
                ));
            }
            operations.push(WriteOperation::Delete {
                key: state_key(&self.primary, entry.value),
            });
            operations.push(WriteOperation::Delete { key: entry.key });
        }
        self.backend
            .write_batch(WriteBatch {
                operations,
                max_bytes: self.limits.batch_bytes,
            })
            .await?;
        Ok(count)
    }
}

impl HistorySnapshot {
    /// Scan a single key over [start, end). Even one hot key is paginated.
    pub async fn scan_key(
        &self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
        cursor: Option<ScanCursor>,
    ) -> Result<HistoryPage> {
        let decoded = if let Some(resources) = &self.resources {
            Some(
                resources
                    .decoded_value(
                        self.limits
                            .page_bytes
                            .checked_mul(3)
                            .ok_or(LiveStateError::InvalidLimit)?,
                    )
                    .await?,
            )
        } else {
            None
        };
        let prefix = key_prefix(key);
        let bound = |timestamp| {
            let mut value = prefix.clone();
            value.extend(sortable_time(timestamp));
            value
        };
        let start = start.map(bound);
        let end = end.map(bound);
        let page = self
            .snapshot
            .scan(ScanRequest {
                range: ScanRange {
                    namespace: self.primary.clone(),
                    prefix: Some(prefix),
                    start,
                    end,
                },
                max_entries: self.limits.page_entries,
                max_bytes: self.limits.page_bytes,
                cursor,
            })
            .await?;
        let mut chunks = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let (_, timestamp, sequence) = parse_primary(&entry.key.key)?;
            if entry.value.len() > self.limits.chunk_bytes {
                return Err(LiveStateError::ReadLimitExceeded {
                    required: entry.value.len(),
                    limit: self.limits.chunk_bytes,
                });
            }
            validate_ipc(&entry.value, self.limits.chunk_rows)?;
            let mut reader =
                StreamReader::try_new(Cursor::new(&entry.value), None).map_err(arrow_error)?;
            if reader.schema() != self.schema {
                return Err(LiveStateError::InvalidEncoding(
                    "stored Arrow schema mismatch".into(),
                ));
            }
            let batch = reader
                .next()
                .transpose()
                .map_err(arrow_error)?
                .ok_or_else(|| LiveStateError::InvalidEncoding("empty Arrow chunk".into()))?;
            if reader.next().is_some() {
                return Err(LiveStateError::InvalidEncoding(
                    "multiple batches in Arrow chunk".into(),
                ));
            }
            chunks.push(HistoryChunk {
                timestamp,
                sequence,
                batch,
            });
        }
        Ok(HistoryPage {
            chunks,
            next_cursor: page.next_cursor,
            _decoded: decoded,
        })
    }
}

fn index_namespace(namespace: &StateNamespace, index: u8) -> StateNamespace {
    let mut table = b"streamr.history.v1".to_vec();
    table.extend((namespace.table.len() as u64).to_be_bytes());
    table.extend(&namespace.table);
    table.push(index);
    StateNamespace {
        ownership: namespace.ownership.clone(),
        table,
    }
}
fn state_key(namespace: &StateNamespace, key: Vec<u8>) -> StateKey {
    StateKey {
        namespace: namespace.clone(),
        key,
        routing_hash: None,
    }
}
fn sortable_time(time: i64) -> [u8; 8] {
    ((time as u64) ^ (1 << 63)).to_be_bytes()
}
fn key_prefix(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 2);
    for byte in key {
        if *byte == 0 {
            encoded.extend([0, 255]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend([0, 0]);
    encoded
}
fn primary_key(key: &[u8], timestamp: i64, sequence: u64) -> Vec<u8> {
    let mut value = key_prefix(key);
    value.extend(sortable_time(timestamp));
    value.extend(sequence.to_be_bytes());
    value
}
fn expiry_key(key: &[u8], timestamp: i64, sequence: u64) -> Vec<u8> {
    let mut value = sortable_time(timestamp).to_vec();
    value.extend(key_prefix(key));
    value.extend(sequence.to_be_bytes());
    value
}
fn parse_primary(bytes: &[u8]) -> Result<(Vec<u8>, i64, u64)> {
    let invalid = || LiveStateError::InvalidEncoding("invalid history primary key".into());
    let mut key = Vec::new();
    let mut position = 0;
    loop {
        let byte = *bytes.get(position).ok_or_else(invalid)?;
        position += 1;
        if byte != 0 {
            key.push(byte);
            continue;
        }
        match bytes.get(position) {
            Some(255) => {
                key.push(0);
                position += 1;
            }
            Some(0) => {
                position += 1;
                break;
            }
            _ => return Err(invalid()),
        }
    }
    if bytes.len() - position != 16 {
        return Err(invalid());
    }
    let timestamp =
        (u64::from_be_bytes(bytes[position..position + 8].try_into().unwrap()) ^ (1 << 63)) as i64;
    let sequence = u64::from_be_bytes(bytes[position + 8..].try_into().unwrap());
    Ok((key, timestamp, sequence))
}
// StreamReader allocates the body length declared in a message. Verify every
// frame against the bounded stored bytes before allowing the decoder to allocate.
fn validate_ipc(bytes: &[u8], max_rows: usize) -> Result<()> {
    let invalid = || LiveStateError::InvalidEncoding("invalid bounded Arrow IPC frame".into());
    let mut offset = 0usize;
    while offset < bytes.len() {
        let read_len = |at: usize| -> Result<u32> {
            Ok(u32::from_le_bytes(
                bytes
                    .get(at..at.checked_add(4).ok_or_else(invalid)?)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            ))
        };
        let mut length = read_len(offset)?;
        offset += 4;
        if length == u32::MAX {
            length = read_len(offset)?;
            offset += 4;
        }
        if length == 0 {
            return if offset == bytes.len() {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let end = offset.checked_add(length as usize).ok_or_else(invalid)?;
        let message = arrow::ipc::root_as_message(bytes.get(offset..end).ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        // This writer emits uncompressed batches. Reject compression before
        // StreamReader can allocate a decompressed body from an attacker length.
        let record = message.header_as_record_batch().or_else(|| {
            message
                .header_as_dictionary_batch()
                .and_then(|dictionary| dictionary.data())
        });
        if let Some(record) = record {
            if record.compression().is_some() || record.length() < 0 {
                return Err(invalid());
            }
            // Dictionary values can outnumber rows, but are already bounded by
            // source buffers and serialized body bytes. Main batches must retain
            // the chunk row limit even when null columns carry no data buffer.
            if message.header_as_record_batch().is_some()
                && usize::try_from(record.length()).map_err(|_| invalid())? > max_rows
            {
                return Err(invalid());
            }
        }
        let body = usize::try_from(message.bodyLength()).map_err(|_| invalid())?;
        offset = end
            .checked_add(body)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(invalid)?;
    }
    Err(invalid())
}

fn arrow_error(error: arrow_schema::ArrowError) -> LiveStateError {
    LiveStateError::InvalidEncoding(error.to_string())
}
struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("Arrow chunk exceeds byte limit"));
        }
        self.bytes.extend(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode_chunk(batch: &RecordBatch, limit: usize) -> Result<Vec<u8>> {
    let mut buffer = LimitedBuffer {
        bytes: Vec::new(),
        limit,
    };
    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).map_err(arrow_error)?;
    writer.write(batch).map_err(arrow_error)?;
    writer.finish().map_err(arrow_error)?;
    drop(writer);
    Ok(buffer.bytes)
}

#[cfg(test)]
mod tests;
