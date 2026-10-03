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
struct MemoryData {
    rows: Map,
    resident_bytes: usize,
}

#[derive(Default)]
pub struct MemoryLiveState {
    data: RwLock<MemoryData>,
    resources: Option<resources::WorkerStateResources>,
    max_resident_bytes: Option<usize>,
    _database: Option<resources::ResourcePermit>,
}
impl MemoryLiveState {
    pub fn new() -> Self {
        Self::default()
    }
    /// Accounted adapter for typed SQL tables; legacy reference construction is unchanged.
    pub fn bounded(
        resources: resources::WorkerStateResources,
        max_resident_bytes: usize,
    ) -> Result<Self> {
        if max_resident_bytes == 0 {
            return Err(LiveStateError::InvalidLimit);
        }
        Ok(Self {
            _database: Some(resources.try_database()?),
            data: RwLock::new(MemoryData::default()),
            resources: Some(resources),
            max_resident_bytes: Some(max_resident_bytes),
        })
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
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.get(key, options).await
    }
    fn try_admit_write(
        &self,
        bytes: usize,
        operations: usize,
    ) -> Result<write::AdmittedWriteBatch> {
        let resources = self.resources.clone().ok_or_else(|| {
            LiveStateError::Backend("memory adapter requires explicit worker resources".into())
        })?;
        write::AdmittedWriteBatch::try_reserve(resources, bytes, operations)
    }
    async fn close(self: Arc<Self>) -> Result<()> {
        Arc::try_unwrap(self)
            .map_err(|_| LiveStateError::Backend("live backend still has active handles".into()))?;
        Ok(())
    }
    async fn admit_write(
        &self,
        max_bytes: usize,
        max_operations: usize,
    ) -> Result<write::AdmittedWriteBatch> {
        let resources = self.resources.clone().ok_or_else(|| {
            LiveStateError::Backend(
                "memory adapter requires explicit worker resources for admission".into(),
            )
        })?;
        write::AdmittedWriteBatch::reserve(resources, max_bytes, max_operations).await
    }
    async fn write_admitted(&self, batch: write::AdmittedWriteBatch) -> Result<()> {
        let (batch, _permit, resources) = batch.into_parts();
        if !self
            .resources
            .as_ref()
            .is_some_and(|own| own.same_pool(&resources))
        {
            return Err(LiveStateError::Backend(
                "write admission resource pool mismatch".into(),
            ));
        }
        self.write_batch(batch).await
    }
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(read_many(
            &self.data.read().map_err(|_| poisoned())?.rows,
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
        read_many(
            &self.data.read().map_err(|_| poisoned())?.rows,
            keys,
            options,
        )
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
        // Only touched keys are visited. Failed batches leave map and charge unchanged.
        let mut final_values = BTreeMap::new();
        for (key, value) in &operations {
            final_values.insert(key, value);
        }
        let mut bytes = data.resident_bytes;
        for (key, value) in final_values {
            if let Some(old) = data.rows.get(key) {
                bytes -= key.len() + old.len() + 64;
            }
            if let Some(value) = value {
                bytes = bytes
                    .checked_add(key.len() + value.len() + 64)
                    .ok_or(LiveStateError::InvalidLimit)?;
            }
        }
        if let Some(limit) = self.max_resident_bytes
            && bytes > limit
        {
            return Err(LiveStateError::BatchLimitExceeded {
                required: bytes,
                limit,
            });
        }
        for (key, value) in operations {
            if let Some(value) = value {
                data.rows.insert(key, value);
            } else {
                data.rows.remove(&key);
            }
        }
        data.resident_bytes = bytes;
        Ok(())
    }
    async fn snapshot(&self) -> Result<StateSnapshot> {
        let permit = if let Some(resources) = &self.resources {
            Some(resources.snapshot().await?)
        } else {
            None
        };
        Ok(StateSnapshot(Arc::new(MemorySnapshot {
            id: next_snapshot_id(),
            _permit: permit,
            data: self.data.read().map_err(|_| poisoned())?.rows.clone(),
        })))
    }
}

struct MemorySnapshot {
    id: u64,
    _permit: Option<resources::ResourcePermit>,
    data: Map,
}
#[async_trait]
impl SnapshotReader for MemorySnapshot {
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.get(key, options).await
    }
    async fn try_scan(&self, request: ScanRequest) -> Result<ScanPage> {
        self.scan(request).await
    }
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

#[cfg(test)]
mod accounting_tests {
    use super::*;
    #[tokio::test]
    async fn resident_delta_handles_repeated_keys_and_failed_batches() {
        let backend = MemoryLiveState::new();
        let key = StateKey {
            namespace: StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: b"delta".to_vec(),
            },
            key: b"key".to_vec(),
            routing_hash: None,
        };
        let size = encoding::encoded_key_size(&key).unwrap()
            + encoding::encoded_value_size(b"first").unwrap()
            + 64;
        backend
            .put(key.clone(), b"first".to_vec(), 1024)
            .await
            .unwrap();
        assert_eq!(backend.data.read().unwrap().resident_bytes, size);
        backend
            .write_batch(WriteBatch {
                operations: vec![
                    WriteOperation::Delete { key: key.clone() },
                    WriteOperation::Put {
                        key: key.clone(),
                        value: b"second".to_vec(),
                    },
                    WriteOperation::Put {
                        key: key.clone(),
                        value: b"final".to_vec(),
                    },
                ],
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_eq!(backend.data.read().unwrap().resident_bytes, size);
        assert!(backend.put(key.clone(), vec![0; 1025], 1024).await.is_err());
        assert_eq!(backend.data.read().unwrap().resident_bytes, size);
        backend.delete(key, 1024).await.unwrap();
        assert_eq!(backend.data.read().unwrap().resident_bytes, 0);
    }
}
