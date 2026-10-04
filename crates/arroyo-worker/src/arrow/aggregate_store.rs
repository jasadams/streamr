//! Bounded, backend-neutral byte store for one native updating aggregate owner.
//!
//! The operator supplies versioned keys and values. This module owns only the
//! admission, read-your-writes overlay, prefix cursor, and ordered write batch.
use anyhow::{Context, Result, ensure};
use arroyo_state::live::{
    LiveStateBackend, ReadOptions, ScanRange, ScanRequest, StateSnapshot, encoding,
    resources::{ResourcePermit, WorkerStateResources},
    table::LiveTable,
    write::AdmittedWriteBatch,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateStoreLimits {
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub page_bytes: usize,
    pub page_entries: usize,
    pub write_bytes: usize,
    pub write_operations: usize,
    pub overlay_bytes: usize,
}

impl AggregateStoreLimits {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            self.key_bytes > 0
                && self.value_bytes > 0
                && self.page_bytes > 0
                && self.page_entries > 0
                && self.write_bytes > 0
                && self.write_operations > 0
                && self.overlay_bytes > 0,
            "native aggregate store limits must be positive"
        );
        ensure!(
            self.page_bytes >= self.key_bytes.saturating_add(self.value_bytes),
            "native aggregate scan page cannot fit one maximum key/value"
        );
        ensure!(
            self.write_bytes >= self.key_bytes.saturating_add(self.value_bytes),
            "native aggregate write batch cannot fit one maximum key/value"
        );
        Ok(self)
    }
}

pub(crate) struct AggregateStore {
    backend: Arc<dyn LiveStateBackend>,
    table: LiveTable,
    resources: WorkerStateResources,
    limits: AggregateStoreLimits,
    max_encoded_entry_bytes: usize,
}

pub(crate) struct AggregateScope<'a> {
    store: &'a AggregateStore,
    // Scan scopes retain the original stable view. Point-only scopes belong to
    // the serial aggregate owner and read live committed state plus `overlay`.
    snapshot: Option<StateSnapshot>,
    writes: AdmittedWriteBatch,
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    overlay_bytes: usize,
    _decoded_permit: ResourcePermit,
}

impl AggregateStore {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        table: LiveTable,
        resources: WorkerStateResources,
        limits: AggregateStoreLimits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let max_key = table.key(vec![0; limits.key_bytes], None);
        let encoded_key_bytes = encoding::encoded_key_size(&max_key)?;
        let one_write = encoded_key_bytes
            .checked_add(limits.value_bytes.saturating_add(1))
            .context("aggregate maximum write size overflow")?;
        ensure!(
            one_write <= limits.write_bytes,
            "native aggregate write budget cannot fit one encoded maximum key/value"
        );
        ensure!(
            one_write <= limits.page_bytes,
            "native aggregate scan page cannot fit one encoded maximum key/value"
        );
        ensure!(
            AdmittedWriteBatch::reservation_bytes(limits.write_bytes, limits.write_operations)?
                <= resources.config().queued_write_bytes,
            "native aggregate queued-write pool cannot admit one configured scope"
        );
        // The live backend admits each scan itself. Its Rocks adapter reserves
        // four copies of the page/request plus entry containers; account for
        // the full bounded request here so even a cursor at the maximum
        // encoded key size can make progress. Memory uses no larger reserve.
        let namespace_bytes = encoding::encoded_namespace_size(table.namespace())?;
        let request_bytes = namespace_bytes
            .saturating_mul(5)
            .saturating_add(limits.key_bytes.saturating_mul(2))
            .saturating_add(encoded_key_bytes);
        let entry_containers = limits
            .page_entries
            .min(limits.page_bytes / namespace_bytes.saturating_add(3))
            .saturating_mul(std::mem::size_of::<arroyo_state::live::ScanEntry>())
            .saturating_mul(2);
        let scan_admission = limits
            .page_bytes
            .saturating_mul(4)
            .saturating_add(request_bytes.saturating_mul(4))
            .saturating_add(entry_containers);
        ensure!(
            scan_admission <= resources.config().scan_page_bytes,
            "native aggregate scan pool cannot admit one configured page and cursor (requires {scan_admission} bytes)"
        );
        ensure!(
            limits
                .overlay_bytes
                .saturating_add(limits.value_bytes.saturating_mul(3))
                <= resources.config().decoded_value_bytes,
            "native aggregate decoded pool cannot admit one configured scope and read"
        );
        Ok(Self {
            backend,
            table,
            resources,
            limits,
            max_encoded_entry_bytes: one_write,
        })
    }

    pub async fn begin(&self) -> Result<AggregateScope<'_>> {
        self.begin_scope(true).await
    }

    /// Only for a serial owner that will perform keyed reads, never a scan.
    /// No public snapshot contract changes: `begin` remains stable-view.
    pub async fn begin_point(&self) -> Result<AggregateScope<'_>> {
        self.begin_scope(false).await
    }

    async fn begin_scope(&self, stable_scan: bool) -> Result<AggregateScope<'_>> {
        // Reserve every retained decoded value/overlay before processing input.
        let decoded = self.resources.try_decoded_value(
            self.limits
                .overlay_bytes
                .checked_add(self.limits.value_bytes.saturating_mul(3))
                .context("aggregate decoded scope size overflow")?,
        )?;
        let writes = AdmittedWriteBatch::try_reserve(
            self.resources.clone(),
            self.limits.write_bytes,
            self.limits.write_operations,
        )?;
        let snapshot = if stable_scan {
            Some(self.backend.snapshot().await?)
        } else {
            None
        };
        Ok(AggregateScope {
            store: self,
            snapshot,
            writes,
            overlay: BTreeMap::new(),
            overlay_bytes: 0,
            _decoded_permit: decoded,
        })
    }

    pub fn limits(&self) -> AggregateStoreLimits {
        self.limits
    }

    pub fn resources(&self) -> &WorkerStateResources {
        &self.resources
    }

    pub fn max_encoded_entry_bytes(&self) -> usize {
        self.max_encoded_entry_bytes
    }
}

impl AggregateScope<'_> {
    pub fn limits(&self) -> AggregateStoreLimits {
        self.store.limits
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        ensure!(
            !key.is_empty() && key.len() <= self.store.limits.key_bytes,
            "native aggregate key exceeds configured limit"
        );
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_key(key)?;
        if let Some(value) = self.overlay.get(key) {
            return Ok(value.clone());
        }
        let key = self.store.table.key(key.to_vec(), None);
        let options = ReadOptions {
            max_bytes: self.store.limits.value_bytes,
        };
        Ok(match &self.snapshot {
            Some(snapshot) => snapshot.try_get(&key, options).await?,
            None => self.store.backend.try_get(&key, options).await?,
        })
    }

    fn admit_overlay(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.check_key(key)?;
        let value_bytes = value.map_or(0, <[u8]>::len);
        ensure!(
            value_bytes <= self.store.limits.value_bytes,
            "native aggregate value exceeds configured limit"
        );
        let previous = self
            .overlay
            .get(key)
            .map_or(0, |old| key.len() + old.as_ref().map_or(0, Vec::len));
        let next = self
            .overlay_bytes
            .checked_sub(previous)
            .and_then(|n| n.checked_add(key.len() + value_bytes))
            .context("native aggregate overlay size overflow")?;
        ensure!(
            next <= self.store.limits.overlay_bytes,
            "native aggregate overlay exceeds configured limit"
        );
        self.overlay_bytes = next;
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.admit_overlay(key, Some(value))?;
        self.writes
            .put(&self.store.table.key(key.to_vec(), None), value)?;
        self.overlay.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.admit_overlay(key, None)?;
        self.writes
            .delete(&self.store.table.key(key.to_vec(), None))?;
        self.overlay.insert(key.to_vec(), None);
        Ok(())
    }

    /// First live logical entry in a prefix, including this scope's mutations.
    /// A page is released before requesting the next one. Tombstones may require
    /// several pages, but neither the page nor the overlay grows with history.
    pub async fn first(&self, prefix: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.first_from(prefix, None).await
    }

    pub async fn first_from(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self
            .snapshot
            .as_ref()
            .context("native aggregate point-read scope cannot scan")?;
        self.check_key(prefix)?;
        if let Some(after) = after {
            self.check_key(after)?;
            ensure!(
                after.starts_with(prefix),
                "aggregate scan start is outside prefix"
            );
        }
        let mut cursor = None;
        let mut backend_first = None;
        loop {
            let page = snapshot
                .try_scan(ScanRequest {
                    range: ScanRange {
                        namespace: self.store.table.namespace().clone(),
                        prefix: Some(prefix.to_vec()),
                        start: after.map(<[u8]>::to_vec),
                        end: None,
                    },
                    max_entries: self.store.limits.page_entries,
                    max_bytes: self.store.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in page.entries {
                let key = entry.key.key;
                if after.is_some_and(|after| key.as_slice() <= after) {
                    continue;
                }
                match self.overlay.get(&key) {
                    Some(None) => continue,
                    Some(Some(value)) => backend_first = Some((key, value.clone())),
                    None => backend_first = Some((key, entry.value)),
                }
                break;
            }
            if backend_first.is_some() || page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
        let overlay_first = self
            .overlay
            .range(after.unwrap_or(prefix).to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
            .find_map(|(key, value)| value.as_ref().map(|v| (key.clone(), v.clone())));
        Ok(match (backend_first, overlay_first) {
            (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
            (Some(found), None) | (None, Some(found)) => Some(found),
            (None, None) => None,
        })
    }

    pub async fn commit(self) -> Result<()> {
        self.store.backend.write_admitted(self.writes).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_state::live::{
        Ownership, StateKey, StateNamespace, lifecycle::RocksStateConfig, memory::MemoryLiveState,
        resources::ResourceConfig, rocks::RocksLiveState, table::LiveTableManager,
    };

    #[tokio::test]
    async fn rocks_scan_progresses_at_exact_composite_admission_boundary() {
        let limits = AggregateStoreLimits {
            key_bytes: 64,
            value_bytes: 128,
            page_bytes: 1024,
            page_entries: 2,
            write_bytes: 1024,
            write_operations: 4,
            overlay_bytes: 1024,
        };
        let ownership = Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        };
        let namespace = StateNamespace {
            ownership: ownership.clone(),
            table: b"aggregate-scan".to_vec(),
        };
        let namespace_bytes = encoding::encoded_namespace_size(&namespace).unwrap();
        let cursor_bytes = encoding::encoded_key_size(&StateKey {
            namespace,
            key: vec![0; limits.key_bytes],
            routing_hash: None,
        })
        .unwrap();
        let required = limits.page_bytes * 4
            + (namespace_bytes * 5 + limits.key_bytes * 2 + cursor_bytes) * 4
            + limits
                .page_entries
                .min(limits.page_bytes / (namespace_bytes + 3))
                * std::mem::size_of::<arroyo_state::live::ScanEntry>()
                * 2;
        let resource_config = ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: required,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        };
        let resources = WorkerStateResources::new(resource_config.clone()).unwrap();
        let root =
            std::env::temp_dir().join(format!("streamr-aggregate-scan-{}", uuid::Uuid::new_v4()));
        let backend: Arc<dyn LiveStateBackend> = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "aggregate-scan".into(),
                    operator_id: "aggregate-scan".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(backend.clone(), ownership).unwrap();
        let table = manager.register("aggregate-scan").unwrap();
        let too_small = WorkerStateResources::new(ResourceConfig {
            scan_page_bytes: required - 1,
            ..resource_config
        })
        .unwrap();
        assert!(AggregateStore::new(backend.clone(), table.clone(), too_small, limits).is_err());
        let store = AggregateStore::new(backend.clone(), table, resources, limits).unwrap();
        let mut scope = store.begin().await.unwrap();
        scope.put(b"a", b"one").unwrap();
        scope.put(b"b", b"two").unwrap();
        scope.commit().await.unwrap();
        let scope = store.begin().await.unwrap();
        assert_eq!(
            scope.first(b"a").await.unwrap(),
            Some((b"a".to_vec(), b"one".to_vec()))
        );
        drop(scope);
        drop(store);
        drop(manager);
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn prefix_reads_merge_pending_writes_and_drop_discards_them() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 2,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 1024 * 1024).unwrap());
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register("aggregate-test").unwrap();
        let store = AggregateStore::new(
            backend,
            table,
            resources,
            AggregateStoreLimits {
                key_bytes: 64,
                value_bytes: 128,
                page_bytes: 1024,
                page_entries: 1,
                write_bytes: 4096,
                write_operations: 8,
                overlay_bytes: 4096,
            },
        )
        .unwrap();
        let mut first = store.begin().await.unwrap();
        first.put(b"member/a", b"a").unwrap();
        first.put(b"member/b", b"b").unwrap();
        assert_eq!(first.get(b"member/a").await.unwrap(), Some(b"a".to_vec()));
        assert_eq!(
            first.first(b"member/").await.unwrap().unwrap().0,
            b"member/a"
        );
        first.commit().await.unwrap();

        let mut second = store.begin().await.unwrap();
        second.delete(b"member/a").unwrap();
        assert_eq!(
            second.first(b"member/").await.unwrap().unwrap().0,
            b"member/b"
        );
        drop(second);
        let third = store.begin().await.unwrap();
        assert_eq!(
            third.first(b"member/").await.unwrap().unwrap().0,
            b"member/a"
        );
    }

    #[tokio::test]
    async fn point_scope_reads_own_writes_without_consuming_snapshot_slot() {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 1,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 1024 * 1024).unwrap());
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register("point-scope").unwrap();
        let store = AggregateStore::new(
            backend.clone(),
            table,
            resources,
            AggregateStoreLimits {
                key_bytes: 64,
                value_bytes: 128,
                page_bytes: 1024,
                page_entries: 1,
                write_bytes: 4096,
                write_operations: 8,
                overlay_bytes: 4096,
            },
        )
        .unwrap();
        let held = backend.snapshot().await.unwrap();
        let mut scope =
            tokio::time::timeout(std::time::Duration::from_secs(1), store.begin_point())
                .await
                .expect("point scope must not wait for a snapshot slot")
                .unwrap();
        assert_eq!(scope.get(b"group").await.unwrap(), None);
        scope.put(b"group", b"first").unwrap();
        assert_eq!(scope.get(b"group").await.unwrap(), Some(b"first".to_vec()));
        assert!(scope.first(b"group").await.is_err());
        scope.commit().await.unwrap();
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), store.begin_point())
            .await
            .expect("point read must not wait for a snapshot slot")
            .unwrap();
        assert_eq!(read.get(b"group").await.unwrap(), Some(b"first".to_vec()));
        // The stable view remains unchanged while the point scope commits.
        assert_eq!(
            held.try_get(
                &store.table.key(b"group".to_vec(), None),
                ReadOptions { max_bytes: 128 }
            )
            .await
            .unwrap(),
            None
        );
    }
}
