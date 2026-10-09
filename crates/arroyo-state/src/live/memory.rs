//! In-memory reference implementation for backend contract tests.
use super::*;
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
tokio::task_local! {
    // Count entries yielded by the real map range, not comparison outcomes.
    static SCAN_VISITS: std::cell::Cell<usize>;
}

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
    health: Option<Arc<health::BackendHealth>>,
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
        let database = resources.try_database()?;
        let health = health::BackendHealth::new("memory", true);
        resources.observe_health(&health);
        Ok(Self {
            _database: Some(database),
            health: Some(health),
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
        let mut logical = self
            .health
            .as_ref()
            .and_then(|health| *health.logical.lock().unwrap_or_else(|e| e.into_inner()));
        for (key, value) in final_values {
            let logical_key_bytes = encoding::decode_key(key)?.key.len() as u64;
            if let Some(size) = &mut logical {
                if let Some(old) = data.rows.get(key) {
                    size.keys -= 1;
                    size.key_bytes -= logical_key_bytes;
                    size.value_bytes -= (old.len() - 1) as u64;
                }
                if let Some(value) = value {
                    size.keys += 1;
                    size.key_bytes += logical_key_bytes;
                    size.value_bytes += (value.len() - 1) as u64;
                }
            }
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
        if let Some(health) = &self.health {
            *health.logical.lock().unwrap_or_else(|e| e.into_inner()) = logical;
        }
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
        // Escaped logical bytes preserve key order. Do not terminate this
        // bound: an inclusive start must include every routing-hash variant
        // of the logical key, just as the RocksDB seek path does.
        let bound = |logical: &[u8]| {
            let mut encoded = prefix.clone();
            for byte in logical {
                if *byte == 0 {
                    encoded.extend_from_slice(&[0, 255]);
                } else {
                    encoded.push(*byte);
                }
            }
            encoded
        };
        let mut lower = bound(request.range.start.as_deref().unwrap_or_default());
        if let Some(logical_prefix) = &request.range.prefix {
            lower = lower.max(bound(logical_prefix));
        }
        if let Some(cursor) = &request.cursor {
            lower = lower.max(cursor.last_key.clone());
        }
        let mut entries = vec![];
        let mut size = 0usize;
        let mut more = false;
        for (encoded, value) in self.data.range(lower..) {
            #[cfg(test)]
            let _ = SCAN_VISITS.try_with(|visits| visits.set(visits.get() + 1));
            if !encoded.starts_with(&prefix) {
                break;
            }
            let key = encoding::decode_key(encoded)?;
            if request
                .range
                .prefix
                .as_ref()
                .is_some_and(|p| !key.key.starts_with(p))
                || request.range.end.as_ref().is_some_and(|e| key.key >= *e)
            {
                // The seek starts at or after the escaped logical prefix.
                // Escaping preserves logical order, so neither an exhausted
                // prefix nor an exclusive end can match any later entry.
                // Routing hashes follow the terminated logical key.
                break;
            }
            if request.range.start.as_ref().is_some_and(|s| key.key < *s)
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
    async fn scan_seek_preserves_escaped_bounds_pages_and_snapshot() {
        let backend = MemoryLiveState::new();
        let namespace = StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"scan".to_vec(),
        };
        let make_key = |logical: &[u8]| StateKey {
            namespace: namespace.clone(),
            key: logical.to_vec(),
            routing_hash: None,
        };
        for logical in [
            b"".as_slice(),
            b"a",
            b"a\0",
            b"a\0\0",
            b"a\0b",
            b"a\x01",
            b"b",
        ] {
            backend
                .put(make_key(logical), logical.to_vec(), 1024)
                .await
                .unwrap();
        }
        let mut other = make_key(b"a\0");
        other.namespace.table = b"other".to_vec();
        backend
            .put(other, b"wrong namespace".to_vec(), 1024)
            .await
            .unwrap();
        let snapshot = backend.snapshot().await.unwrap();
        backend.delete(make_key(b"a\0b"), 1024).await.unwrap();
        let range = ScanRange {
            namespace: namespace.clone(),
            prefix: Some(b"a\0".to_vec()),
            start: Some(b"a\0".to_vec()),
            end: Some(b"a\x01".to_vec()),
        };
        // Force byte-limited pages even though the entry-count limit is high.
        // Escaped zero bytes make a logical key larger on wire: a\0\0 is
        // encoded one byte wider than a\0b despite equal logical lengths.
        let entry_bytes: Vec<_> = [b"a\0".as_slice(), b"a\0\0", b"a\0b"]
            .into_iter()
            .map(|logical| {
                encoding::encoded_key_size(&make_key(logical)).unwrap()
                    + encoding::encoded_value_size(logical).unwrap()
            })
            .collect();
        let max_bytes = *entry_bytes.iter().max().unwrap();
        assert_eq!(max_bytes, 29);
        assert!(entry_bytes.iter().all(|bytes| 2 * bytes > max_bytes));
        let mut cursor = None;
        let mut returned = Vec::new();
        loop {
            let page = snapshot
                .try_scan(ScanRequest {
                    range: range.clone(),
                    max_entries: 8,
                    max_bytes,
                    cursor,
                })
                .await
                .unwrap();
            assert_eq!(page.entries.len(), 1);
            let entry = &page.entries[0];
            assert_eq!(entry.key.namespace, namespace);
            assert_eq!(entry.value, entry.key.key);
            returned.push(entry.key.key.clone());
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            returned,
            vec![b"a\0".to_vec(), b"a\0\0".to_vec(), b"a\0b".to_vec()]
        );
        assert!(
            backend
                .get(&make_key(b"a\0b"), ReadOptions { max_bytes: 1024 })
                .await
                .unwrap()
                .is_none()
        );
        let page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: namespace.clone(),
                    prefix: None,
                    start: Some(b"a\0".to_vec()),
                    end: Some(b"a\0b".to_vec()),
                },
                max_entries: 8,
                max_bytes: 1024,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.key.key.clone())
                .collect::<Vec<_>>(),
            vec![b"a\0".to_vec(), b"a\0\0".to_vec()]
        );
        assert!(page.next_cursor.is_none());
        let page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace,
                    prefix: Some(b"a".to_vec()),
                    start: Some(b"b".to_vec()),
                    end: None,
                },
                max_entries: 8,
                max_bytes: 1024,
                cursor: None,
            })
            .await
            .unwrap();
        assert!(page.entries.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn scan_seek_keeps_inclusive_routed_keys_and_strict_cursor() {
        let backend = MemoryLiveState::new();
        let namespace = StateNamespace {
            ownership: Ownership::Routed {
                range_start: 1,
                range_end: 3,
            },
            table: b"routed".to_vec(),
        };
        for (logical, hash) in [(b"a\0".as_slice(), 1), (b"a\0", 2), (b"a\0b", 3)] {
            backend
                .put(
                    StateKey {
                        namespace: namespace.clone(),
                        key: logical.to_vec(),
                        routing_hash: Some(hash),
                    },
                    vec![hash as u8],
                    1024,
                )
                .await
                .unwrap();
        }
        let snapshot = backend.snapshot().await.unwrap();
        let range = ScanRange {
            namespace,
            prefix: Some(b"a\0".to_vec()),
            start: Some(b"a\0".to_vec()),
            end: Some(b"a\0b".to_vec()),
        };
        let request = ScanRequest {
            range: range.clone(),
            max_entries: 1,
            max_bytes: 1024,
            cursor: None,
        };
        let first = snapshot.try_scan(request.clone()).await.unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].key.key, b"a\0");
        assert_eq!(first.entries[0].key.routing_hash, Some(1));
        let cursor = first.next_cursor.unwrap();
        let second = snapshot
            .try_scan(ScanRequest {
                cursor: Some(cursor.clone()),
                ..request.clone()
            })
            .await
            .unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].key.key, b"a\0");
        assert_eq!(second.entries[0].key.routing_hash, Some(2));
        assert!(second.next_cursor.is_none());
        let fresh = backend.snapshot().await.unwrap();
        assert!(matches!(
            fresh
                .try_scan(ScanRequest {
                    cursor: Some(cursor.clone()),
                    ..request.clone()
                })
                .await,
            Err(LiveStateError::InvalidCursor)
        ));
        let mut changed = request;
        changed.range.start = None;
        changed.cursor = Some(cursor);
        assert!(matches!(
            snapshot.try_scan(changed).await,
            Err(LiveStateError::InvalidCursor)
        ));
    }

    async fn counted_scan(snapshot: &StateSnapshot, request: ScanRequest) -> (ScanPage, usize) {
        SCAN_VISITS
            .scope(std::cell::Cell::new(0), async {
                let page = snapshot.try_scan(request).await.unwrap();
                (page, SCAN_VISITS.with(std::cell::Cell::get))
            })
            .await
    }

    #[tokio::test]
    async fn exhausted_prefix_and_end_do_not_visit_unrelated_tail() {
        // Grow only the unrelated tail. The expected rows and visit ceilings
        // are fixed independently of that growth; the old scan visits it all.
        for tail_rows in [16u32, 4096] {
            let backend = MemoryLiveState::new();
            let namespace = StateNamespace {
                ownership: Ownership::Routed {
                    range_start: 1,
                    range_end: 2,
                },
                table: b"scan\0bounds".to_vec(),
            };
            let key = |logical: &[u8], hash| StateKey {
                namespace: namespace.clone(),
                key: logical.to_vec(),
                routing_hash: Some(hash),
            };
            let expected = [
                ScanEntry {
                    key: key(b"a\0", 1),
                    value: b"first".to_vec(),
                },
                ScanEntry {
                    key: key(b"a\0", 2),
                    value: b"second".to_vec(),
                },
                ScanEntry {
                    key: key(b"a\0\0", 1),
                    value: b"zero".to_vec(),
                },
                ScanEntry {
                    key: key(b"a\0\xff", 2),
                    value: b"high".to_vec(),
                },
            ];
            for entry in &expected {
                backend
                    .put(entry.key.clone(), entry.value.clone(), 1024)
                    .await
                    .unwrap();
            }
            // Before the lower bound and in another namespace: neither may
            // enter the result, even when an identical logical key exists.
            backend
                .put(key(b"a", 1), b"before".to_vec(), 1024)
                .await
                .unwrap();
            let mut other = key(b"a\0", 1);
            other.namespace.table = b"scan\0boundt".to_vec();
            backend.put(other, b"other".to_vec(), 1024).await.unwrap();
            for ordinal in 0..tail_rows {
                let mut logical = vec![b'b'];
                logical.extend_from_slice(&ordinal.to_be_bytes());
                backend
                    .put(key(&logical, 1), ordinal.to_be_bytes().to_vec(), 1024)
                    .await
                    .unwrap();
            }
            let snapshot = backend.snapshot().await.unwrap();
            let request = ScanRequest {
                range: ScanRange {
                    namespace: namespace.clone(),
                    prefix: Some(b"a\0".to_vec()),
                    start: Some(b"a\0".to_vec()),
                    end: None,
                },
                max_entries: 2,
                max_bytes: 1024,
                cursor: None,
            };
            let (first, visits) = counted_scan(&snapshot, request.clone()).await;
            assert_eq!(first.entries, expected[..2]);
            assert!(visits <= 3, "first page visited {visits} entries");
            let (last, visits) = counted_scan(
                &snapshot,
                ScanRequest {
                    cursor: Some(first.next_cursor.unwrap()),
                    ..request.clone()
                },
            )
            .await;
            assert_eq!(last.entries, expected[2..]);
            assert!(last.next_cursor.is_none());
            // Two matches, at most one replayed cursor and one boundary.
            assert!(
                visits <= 4,
                "exhaustion visited {visits} entries with tail {tail_rows}"
            );

            for range in [
                ScanRange {
                    prefix: Some(b"a\x01".to_vec()),
                    start: None,
                    end: None,
                    namespace: namespace.clone(),
                },
                ScanRange {
                    prefix: Some(b"a\0".to_vec()),
                    start: Some(b"b".to_vec()),
                    end: None,
                    namespace: namespace.clone(),
                },
                ScanRange {
                    prefix: None,
                    start: Some(b"a\0".to_vec()),
                    end: Some(b"a\0".to_vec()),
                    namespace: namespace.clone(),
                },
                ScanRange {
                    prefix: Some(b"a\0".to_vec()),
                    start: None,
                    end: Some(b"a".to_vec()),
                    namespace: namespace.clone(),
                },
            ] {
                let (page, visits) = counted_scan(
                    &snapshot,
                    ScanRequest {
                        range,
                        ..request.clone()
                    },
                )
                .await;
                assert!(page.entries.is_empty());
                assert!(page.next_cursor.is_none());
                assert!(
                    visits <= 1,
                    "empty range visited {visits} entries with tail {tail_rows}"
                );
            }
            let (page, visits) = counted_scan(
                &snapshot,
                ScanRequest {
                    range: ScanRange {
                        namespace,
                        prefix: None,
                        start: Some(b"a\0".to_vec()),
                        end: Some(b"a\0\0".to_vec()),
                    },
                    ..request
                },
            )
            .await;
            assert_eq!(page.entries, expected[..2]);
            assert!(page.next_cursor.is_none());
            assert!(
                visits <= 3,
                "exclusive end visited {visits} entries with tail {tail_rows}"
            );
        }
    }

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
