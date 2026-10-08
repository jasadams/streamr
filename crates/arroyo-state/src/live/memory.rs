//! In-memory reference implementation for backend contract tests.
use super::*;
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) fn next_snapshot_id() -> u64 {
    SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed)
}
type Map = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Default)]
pub struct MemoryLiveState {
    data: RwLock<Map>,
}
impl MemoryLiveState {
    pub fn new() -> Self {
        Self::default()
    }
}
fn poisoned() -> LiveStateError {
    LiveStateError::Backend("live state lock poisoned".into())
}
fn read_many(data: &Map, keys: &[StateKey], options: ReadOptions) -> Result<Vec<Option<Vec<u8>>>> {
    let encoded = keys
        .iter()
        .map(encoding::encode_key)
        .collect::<Result<Vec<_>>>()?;
    let required = encoded
        .iter()
        .filter_map(|key| data.get(key))
        .try_fold(0usize, |n, v| n.checked_add(v.len() - 1))
        .ok_or(LiveStateError::ReadLimitExceeded {
            required: usize::MAX,
            limit: options.max_bytes,
        })?;
    enforce_read_limit(required, options)?;
    encoded
        .iter()
        .map(|key| data.get(key).map(|v| encoding::decode_value(v)).transpose())
        .collect()
}

#[async_trait]
impl LiveStateBackend for MemoryLiveState {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(read_many(
            &*self.data.read().map_err(|_| poisoned())?,
            std::slice::from_ref(key),
            options,
        )?
        .pop()
        .flatten())
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        read_many(&*self.data.read().map_err(|_| poisoned())?, keys, options)
    }
    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        batch.validate()?;
        // Encoding may fail; finish it before taking the lock or applying writes.
        let operations = batch
            .operations
            .into_iter()
            .map(|op| match op {
                WriteOperation::Put { key, value } => Ok((
                    encoding::encode_key(&key)?,
                    Some(encoding::encode_value(&value)),
                )),
                WriteOperation::Delete { key } => Ok((encoding::encode_key(&key)?, None)),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut data = self.data.write().map_err(|_| poisoned())?;
        for (key, value) in operations {
            if let Some(value) = value {
                data.insert(key, value);
            } else {
                data.remove(&key);
            }
        }
        Ok(())
    }
    async fn snapshot(&self) -> Result<StateSnapshot> {
        Ok(StateSnapshot(Arc::new(MemorySnapshot {
            id: next_snapshot_id(),
            data: self.data.read().map_err(|_| poisoned())?.clone(),
        })))
    }
}

struct MemorySnapshot {
    id: u64,
    data: Map,
}
#[async_trait]
impl SnapshotReader for MemorySnapshot {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(read_many(&self.data, std::slice::from_ref(key), options)?
            .pop()
            .flatten())
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        read_many(&self.data, keys, options)
    }
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        request.validate(self.id)?;
        let prefix = encoding::encode_namespace(&request.range.namespace)?;
        let mut entries = vec![];
        let mut size = 0usize;
        let mut more = false;
        for (encoded, value) in self.data.range(prefix.clone()..) {
            if !encoded.starts_with(&prefix) {
                break;
            }
            let key = encoding::decode_key(encoded)?;
            if request
                .range
                .prefix
                .as_ref()
                .is_some_and(|p| !key.key.starts_with(p))
                || request.range.start.as_ref().is_some_and(|s| key.key < *s)
                || request.range.end.as_ref().is_some_and(|e| key.key >= *e)
                || request
                    .cursor
                    .as_ref()
                    .is_some_and(|c| *encoded <= c.last_key)
            {
                continue;
            }
            let required = encoded.len().checked_add(value.len()).ok_or(
                LiveStateError::ReadLimitExceeded {
                    required: usize::MAX,
                    limit: request.max_bytes,
                },
            )?;
            if entries.len() == request.max_entries
                || required > request.max_bytes.saturating_sub(size)
            {
                if entries.is_empty() {
                    return Err(LiveStateError::ReadLimitExceeded {
                        required,
                        limit: request.max_bytes,
                    });
                }
                more = true;
                break;
            }
            size += required;
            entries.push(ScanEntry {
                key,
                value: encoding::decode_value(value)?,
            });
        }
        let next_cursor = if more {
            Some(ScanCursor {
                snapshot_id: self.id,
                range: request.range,
                last_key: encoding::encode_key(&entries.last().expect("nonempty page").key)?,
            })
        } else {
            None
        };
        Ok(ScanPage {
            entries,
            next_cursor,
        })
    }
}
