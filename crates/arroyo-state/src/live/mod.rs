//! Owned, bounded live state independent of checkpoint storage.
//!
//! Backends apply batches in order and atomically. A completed write is visible to
//! subsequent reads; snapshots retain the state at their creation time.
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

pub mod checkpoint;
pub mod encoding;
pub mod lifecycle;
pub mod memory;
pub mod resources;
pub mod rocks;
pub mod table;
pub mod time;
pub mod worker;
pub mod write;

pub type Result<T> = std::result::Result<T, LiveStateError>;

#[derive(Debug)]
pub enum LiveStateError {
    InvalidLimit,
    ReadLimitExceeded { required: usize, limit: usize },
    BatchLimitExceeded { required: usize, limit: usize },
    InvalidCursor,
    InvalidEncoding(String),
    Backend(String),
    Resource(resources::ResourceError),
}
impl fmt::Display for LiveStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LiveStateError {}

/// Ownership is persisted with the key; routed hashes are supplied by callers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ownership {
    PartitionLocal {
        subtask: u32,
        parallelism: u32,
    },
    Routed {
        range_start: u64,
        range_end: u64,
    },
    Replicated {
        id: Vec<u8>,
    },
    Connector {
        connector: Vec<u8>,
        partition: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateNamespace {
    pub ownership: Ownership,
    pub table: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateKey {
    pub namespace: StateNamespace,
    /// Full logical key, never replaced with a routing hash.
    pub key: Vec<u8>,
    pub routing_hash: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
/// Returned buffers are owned by the caller. Backends bound individual reads;
/// callers must bound retained results to enforce an overall memory budget.
pub struct ReadOptions {
    /// Maximum combined returned value bytes (including duplicates in multi_get).
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
    Put { key: StateKey, value: Vec<u8> },
    Delete { key: StateKey },
}

#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub operations: Vec<WriteOperation>,
    /// Maximum encoded key and value bytes across every operation, including
    /// repeated keys. Checked before any mutation.
    pub max_bytes: usize,
}
impl WriteBatch {
    pub fn encoded_size(&self) -> Result<usize> {
        let mut size = 0usize;
        for operation in &self.operations {
            let (key, value_size) = match operation {
                WriteOperation::Put { key, value } => (key, encoding::encoded_value_size(value)?),
                WriteOperation::Delete { key } => (key, 0),
            };
            size = size
                .checked_add(encoding::encoded_key_size(key)?)
                .and_then(|size| size.checked_add(value_size))
                .ok_or(LiveStateError::BatchLimitExceeded {
                    required: usize::MAX,
                    limit: self.max_bytes,
                })?;
        }
        Ok(size)
    }
    pub fn validate(&self) -> Result<()> {
        let required = self.encoded_size()?;
        if required > self.max_bytes {
            return Err(LiveStateError::BatchLimitExceeded {
                required,
                limit: self.max_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRange {
    pub namespace: StateNamespace,
    pub prefix: Option<Vec<u8>>,
    /// Inclusive logical key bound; None is unbounded.
    pub start: Option<Vec<u8>>,
    /// Exclusive logical key bound; None is unbounded.
    pub end: Option<Vec<u8>>,
}

/// A cursor belongs to exactly one snapshot and range. Fields are backend-visible
/// but private to callers, who should pass back the returned cursor unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanCursor {
    pub(crate) snapshot_id: u64,
    pub(crate) range: ScanRange,
    pub(crate) last_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub range: ScanRange,
    pub max_entries: usize,
    /// Combined encoded key and value bytes in this page.
    pub max_bytes: usize,
    pub cursor: Option<ScanCursor>,
}
impl ScanRequest {
    pub(crate) fn validate(&self, snapshot_id: u64) -> Result<()> {
        if self.max_entries == 0 || self.max_bytes == 0 {
            return Err(LiveStateError::InvalidLimit);
        }
        if self
            .range
            .start
            .as_ref()
            .zip(self.range.end.as_ref())
            .is_some_and(|(a, b)| a > b)
        {
            return Err(LiveStateError::InvalidCursor);
        }
        if let Some(cursor) = &self.cursor
            && (cursor.snapshot_id != snapshot_id || cursor.range != self.range)
        {
            return Err(LiveStateError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: StateKey,
    pub value: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<ScanEntry>,
    pub next_cursor: Option<ScanCursor>,
}

#[async_trait]
pub trait SnapshotReader: Send + Sync {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>>;
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>>;
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage>;
}

/// Cloning retains a stable snapshot. Dropping the final clone releases it.
#[derive(Clone)]
pub struct StateSnapshot(pub Arc<dyn SnapshotReader>);
impl StateSnapshot {
    pub async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.0.get(key, options).await
    }
    pub async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.0.multi_get(keys, options).await
    }
    pub async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        self.0.scan(request).await
    }
}

#[async_trait]
pub trait LiveStateBackend: Send + Sync {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>>;
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>>;
    /// Owned inputs are caller allocations while awaiting admission. Producers
    /// requiring queued buffer accounting should reserve before assembling data
    /// (RocksLiveState::admitted_batch) and keep source/overlay memory bounded.
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;
    async fn snapshot(&self) -> Result<StateSnapshot>;
    async fn put(&self, key: StateKey, value: Vec<u8>, max_bytes: usize) -> Result<()> {
        self.write_batch(WriteBatch {
            operations: vec![WriteOperation::Put { key, value }],
            max_bytes,
        })
        .await
    }
    async fn delete(&self, key: StateKey, max_bytes: usize) -> Result<()> {
        self.write_batch(WriteBatch {
            operations: vec![WriteOperation::Delete { key }],
            max_bytes,
        })
        .await
    }
}

pub(crate) fn enforce_read_limit(required: usize, options: ReadOptions) -> Result<()> {
    if required > options.max_bytes {
        Err(LiveStateError::ReadLimitExceeded {
            required,
            limit: options.max_bytes,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
