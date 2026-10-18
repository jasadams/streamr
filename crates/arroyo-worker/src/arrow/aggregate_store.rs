//! Bounded, backend-neutral byte store for one native updating aggregate owner.
//!
//! The operator supplies versioned keys and values. This module owns only the
//! admission, read-your-writes overlay, prefix cursor, and ordered write batch.
use anyhow::{Context, Result, ensure};
use arroyo_state::live::{
    LiveStateBackend, ReadOptions, ScanRange, ScanRequest, StateKey, StateNamespace, StateSnapshot,
    checkpoint::checkpoint_decoded_reservation,
    encoding,
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
    selector_checkpoint_headroom: bool,
}

/// A logical expiry lower bound for one serial flush, never a backend cursor.
/// The optional permit covers keys, their clones/replacement and headers.
pub(crate) struct AggregateExpiryProgress {
    pub after: Option<Vec<u8>>,
    _permit: ResourcePermit,
}

/// One optional admitted logical duplicate-member bound, owned by a serial
/// input batch. Never a snapshot cursor and never retained across input batches.
pub(crate) struct AggregateMemberProgress {
    after: Option<Vec<u8>>,
    _permit: ResourcePermit,
}

pub(crate) struct AggregateScope<'a> {
    store: &'a AggregateStore,
    // Scan scopes retain the original stable view. Point-only scopes belong to
    // the serial aggregate owner and read live committed state plus `overlay`.
    snapshot: Option<StateSnapshot>,
    writes: AdmittedWriteBatch,
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    overlay_bytes: usize,
    member_progress: Option<AggregateMemberProgress>,
    _decoded_permit: ResourcePermit,
}

/// Maximum zero-escaped key framing, using the backend's namespace codec.
pub(crate) fn aggregate_encoded_key_bound(
    namespace: &StateNamespace,
    key_bytes: usize,
) -> Result<usize> {
    let empty = StateKey {
        namespace: namespace.clone(),
        key: Vec::new(),
        routing_hash: None,
    };
    encoding::encoded_key_size(&empty)?
        .checked_add(
            key_bytes
                .checked_mul(2)
                .context("aggregate encoded key bound overflow")?,
        )
        .context("aggregate encoded key bound overflow")
}

/// One serial owner's foreground peak. Point reads and candidate decoding are
/// sequential. The common bound also admits the RocksDB adapter's owned read.
pub(crate) fn aggregate_decoded_foreground(
    limits: AggregateStoreLimits,
    output_bytes: usize,
    selector: bool,
    encoded_key_bytes: usize,
) -> Result<usize> {
    let point = limits
        .value_bytes
        .checked_add(
            encoded_key_bytes
                .checked_mul(2)
                .context("aggregate point-read workspace overflow")?,
        )
        .and_then(|n| {
            n.checked_add(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>())
        })
        .context("aggregate point-read workspace overflow")?;
    let candidate = if selector {
        limits
            .value_bytes
            .checked_mul(3)
            .context("selector workspace overflow")?
    } else {
        0
    };
    limits
        .overlay_bytes
        .checked_add(
            limits
                .value_bytes
                .checked_mul(3)
                .context("aggregate scope workspace overflow")?,
        )
        .and_then(|n| n.checked_add(output_bytes.checked_mul(3)?))
        .and_then(|n| n.checked_add(candidate.max(point)))
        .context("aggregate foreground workspace overflow")
}

impl AggregateStore {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        table: LiveTable,
        resources: WorkerStateResources,
        limits: AggregateStoreLimits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let encoded_key_bytes = aggregate_encoded_key_bound(table.namespace(), limits.key_bytes)?;
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
            .saturating_add(limits.key_bytes.saturating_mul(3))
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
            selector_checkpoint_headroom: false,
        })
    }

    pub fn preserve_selector_checkpoint_headroom(&mut self) {
        self.selector_checkpoint_headroom = true;
    }

    pub fn optional_progress_peak(&self, output_bytes: usize, existing: usize) -> Option<usize> {
        if !self.selector_checkpoint_headroom {
            return Some(existing);
        }
        let encoded_key = self
            .max_encoded_entry_bytes
            .checked_sub(self.limits.value_bytes + 1)?;
        let foreground =
            aggregate_decoded_foreground(self.limits, output_bytes, true, encoded_key).ok()?;
        let checkpoint = checkpoint_decoded_reservation(self.resources.config()).ok()?;
        Some(existing.max(foreground.checked_add(checkpoint)?))
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
        let requested = self
            .limits
            .overlay_bytes
            .checked_add(
                self.limits
                    .value_bytes
                    .checked_mul(3)
                    .context("aggregate decoded scope size overflow")?,
            )
            .context("aggregate decoded scope size overflow")?;
        let decoded = self
            .resources
            .try_decoded_value(requested)
            .map_err(|error| {
                anyhow::anyhow!(
                    "aggregate scope decoded reservation requested={requested}: {error}"
                )
            })?;
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
            member_progress: None,
            _decoded_permit: decoded,
        })
    }

    pub fn try_member_progress(&self, output_bytes: usize) -> Option<AggregateMemberProgress> {
        Some(AggregateMemberProgress {
            after: None,
            _permit: self.try_progress_permit(
                output_bytes,
                std::mem::size_of::<AggregateMemberProgress>(),
            )?,
        })
    }

    pub fn try_expiry_progress(&self, output_bytes: usize) -> Option<AggregateExpiryProgress> {
        Some(AggregateExpiryProgress {
            after: None,
            _permit: self.try_progress_permit(
                output_bytes,
                std::mem::size_of::<AggregateExpiryProgress>(),
            )?,
        })
    }

    fn try_progress_permit(
        &self,
        output_bytes: usize,
        header_bytes: usize,
    ) -> Option<ResourcePermit> {
        // Native decoding can grow a Vec to up to twice its logical size;
        // cover replacement and shrinking as well as the retained/request key.
        let charge = self
            .limits
            .key_bytes
            .checked_mul(4)?
            .checked_add(header_bytes + 128)?;
        // Headroom is optional. Preserve configurations that can only admit
        // the original scope and dirty-output reservations without this key.
        let mandatory = self
            .limits
            .overlay_bytes
            .checked_add(self.limits.value_bytes.checked_mul(3)?)?
            .checked_add(output_bytes.checked_mul(3)?)?
            .checked_add(self.max_encoded_entry_bytes.checked_mul(2)?)?
            .checked_add(128)?;
        let mandatory = self.optional_progress_peak(output_bytes, mandatory)?;
        if mandatory.checked_add(charge)? > self.resources.config().decoded_value_bytes {
            return None;
        }
        self.resources.try_decoded_value(charge).ok()
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
    /// Resources belong to this scope's store, including when the serial owner
    /// temporarily takes the store out of its operator during input/flush.
    pub fn resources(&self) -> &WorkerStateResources {
        self.store.resources()
    }

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

    /// Transfer the serial input batch's admitted progress into this scope.
    pub fn set_member_progress(&mut self, progress: Option<AggregateMemberProgress>) {
        self.member_progress = progress;
    }

    pub fn reset_member_progress(&mut self) {
        if let Some(progress) = &mut self.member_progress {
            progress.after = None;
        }
    }

    /// Only duplicate-member retractions use this bound. Generic prefix scans
    /// retain their stable-view behavior. Exact tuple prefixes include group,
    /// generation, aggregate and the complete encoded argument (including NULL).
    pub async fn first_member(&self, prefix: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let after = self.member_progress.as_ref().and_then(|progress| {
            progress.after.as_deref().filter(|key| {
                key.len() == prefix.len().saturating_add(8) && key.starts_with(prefix)
            })
        });
        self.first_from(prefix, after).await
    }

    /// Stage only a successfully validated pair of primary/secondary deletes.
    /// Dropping the scope or its commit future also drops this staged bound.
    pub fn member_deleted(&mut self, key: &[u8]) {
        if let Some(progress) = &mut self.member_progress {
            progress.after = Some(key.to_vec());
        }
    }

    pub async fn commit_member_progress(mut self) -> Result<Option<AggregateMemberProgress>> {
        let progress = self.member_progress.take();
        self.commit().await?;
        Ok(progress)
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
        self.first_from_bounded(prefix, after, None).await
    }

    /// First logical entry with an optional exclusive native upper bound.
    pub async fn first_from_bounded(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.first_from_bounded_entries(prefix, after, end, self.store.limits.page_entries)
            .await
    }

    /// Dirty enumeration consumes only the first live marker, never the rest
    /// of a page. Include room for the inclusive `after` entry in a stable view;
    /// the ordinary overlay/pagination merge still handles additional deletes.
    pub async fn first_dirty_from(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.first_from_bounded_entries(b"D", after, None, 1 + usize::from(after.is_some()))
            .await
    }

    async fn first_from_bounded_entries(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        end: Option<&[u8]>,
        entries: usize,
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
        if let Some(end) = end {
            self.check_key(end)?;
            ensure!(
                end.starts_with(prefix),
                "aggregate scan end is outside prefix"
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
                        end: end.map(<[u8]>::to_vec),
                    },
                    max_entries: self.store.limits.page_entries.min(entries),
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
            .take_while(|(key, _)| {
                key.starts_with(prefix) && end.is_none_or(|end| key.as_slice() < end)
            })
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
            .find_map(|(key, value)| value.as_ref().map(|v| (key.clone(), v.clone())));
        Ok(match (backend_first, overlay_first) {
            (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
            (Some(found), None) | (None, Some(found)) => Some(found),
            (None, None) => None,
        })
    }

    /// Visit live prefix entries in logical order, merging this scope's writes.
    /// The callback borrows each value while its page and any backend scan
    /// reservation (or admitted overlay) remain alive. Only one page is retained.
    pub async fn visit_prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<()> + Send,
    ) -> Result<()> {
        self.check_key(prefix)?;
        let mut remaining = limit.unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(());
        }
        let snapshot = self
            .snapshot
            .as_ref()
            .context("native aggregate point-read scope cannot scan")?;
        let mut overlay = self.overlay.range(prefix.to_vec()..).peekable();
        let mut cursor = None;
        loop {
            let mut page = snapshot
                .try_scan(ScanRequest {
                    range: ScanRange {
                        namespace: self.store.table.namespace().clone(),
                        prefix: Some(prefix.to_vec()),
                        start: None,
                        end: None,
                    },
                    max_entries: self.store.limits.page_entries.min(remaining),
                    max_bytes: self.store.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in &page.entries {
                while overlay.peek().is_some_and(|(key, _)| {
                    key.starts_with(prefix) && key.as_slice() <= entry.key.key.as_slice()
                }) {
                    let (key, value) = overlay.next().expect("peeked overlay entry");
                    if let Some(value) = value {
                        visit(key, value)?;
                        remaining -= 1;
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                }
                // Any overlay value, including a deletion, supersedes the
                // stable backend entry. It was consumed above in key order.
                if !self.overlay.contains_key(&entry.key.key) {
                    visit(&entry.key.key, &entry.value)?;
                    remaining -= 1;
                    if remaining == 0 {
                        return Ok(());
                    }
                }
            }
            let next = page.next_cursor.take();
            // Release the scan reservation before asking for another page.
            drop(page);
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        for (key, value) in overlay {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(value) = value {
                visit(key, value)?;
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        Ok(())
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

    #[test]
    fn aggregate_foreground_uses_actual_zero_escaped_key_bound_and_checked_peak() {
        let namespace = StateNamespace {
            ownership: arroyo_state::live::Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"native-aggregate-v1".to_vec(),
        };
        for size in [0, 1, 8, 512] {
            let bound = aggregate_encoded_key_bound(&namespace, size).unwrap();
            for byte in [0, 1, 255] {
                let key = StateKey {
                    namespace: namespace.clone(),
                    key: vec![byte; size],
                    routing_hash: None,
                };
                let actual = encoding::encoded_key_size(&key).unwrap();
                assert!(actual <= bound);
                if byte == 0 {
                    assert_eq!(actual, bound);
                }
            }
        }
        assert!(aggregate_encoded_key_bound(&namespace, usize::MAX).is_err());
        let limits = AggregateStoreLimits {
            key_bytes: 512,
            value_bytes: 128,
            page_bytes: 2048,
            page_entries: 1,
            write_bytes: 4096,
            write_operations: 8,
            overlay_bytes: 4096,
        };
        let key = aggregate_encoded_key_bound(&namespace, limits.key_bytes).unwrap();
        let point = limits.value_bytes
            + 2 * key
            + std::mem::size_of::<Vec<u8>>()
            + std::mem::size_of::<Option<Vec<u8>>>();
        assert!(point > 3 * limits.value_bytes);
        assert_eq!(
            aggregate_decoded_foreground(limits, 256, true, key).unwrap(),
            limits.overlay_bytes + 3 * limits.value_bytes + 3 * 256 + point
        );
        assert!(aggregate_decoded_foreground(limits, usize::MAX, true, key).is_err());
    }

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
            + (namespace_bytes * 5 + limits.key_bytes * 3 + cursor_bytes) * 4
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
        assert!(
            scope
                .first_from_bounded(b"a", None, Some(b"a"))
                .await
                .unwrap()
                .is_none()
        );
        drop(scope);
        let mut scope = store.begin().await.unwrap();
        scope.put(b"aa", b"pending").unwrap();
        // Both committed and overlay entries respect the exclusive endpoint.
        assert_eq!(
            scope
                .first_from_bounded(b"a", None, Some(b"aa"))
                .await
                .unwrap(),
            Some((b"a".to_vec(), b"one".to_vec()))
        );
        assert!(
            scope
                .first_from_bounded(b"a", Some(b"a"), Some(b"aa"))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            scope
                .first_from_bounded(b"a", Some(b"a"), Some(b"ab"))
                .await
                .unwrap(),
            Some((b"aa".to_vec(), b"pending".to_vec()))
        );
        drop(scope);
        drop(store);
        drop(manager);
        backend.close().await.unwrap();
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
    #[tokio::test]
    async fn prefix_visitor_merges_pages_overlay_limits_and_error_retry() {
        for rocks in [false, true] {
            for page_entries in [1, 3] {
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
                let root = std::env::temp_dir()
                    .join(format!("aggregate-visitor-{}", uuid::Uuid::new_v4()));
                let backend: Arc<dyn LiveStateBackend> = if rocks {
                    Arc::new(
                        RocksLiveState::open(
                            RocksStateConfig {
                                root: root.clone(),
                                job_id: "visitor".into(),
                                operator_id: "visitor".into(),
                                subtask: 0,
                                generation: 0,
                                attempt: 0,
                            },
                            resources.clone(),
                        )
                        .await
                        .unwrap(),
                    )
                } else {
                    Arc::new(MemoryLiveState::bounded(resources.clone(), 1024 * 1024).unwrap())
                };
                let mut manager = LiveTableManager::new(
                    backend.clone(),
                    Ownership::PartitionLocal {
                        subtask: 0,
                        parallelism: 1,
                    },
                )
                .unwrap();
                let table = manager.register("aggregate-visitor").unwrap();
                let store = AggregateStore::new(
                    backend.clone(),
                    table,
                    resources.clone(),
                    AggregateStoreLimits {
                        key_bytes: 64,
                        value_bytes: 512,
                        page_bytes: 1024,
                        page_entries,
                        write_bytes: 4096,
                        write_operations: 16,
                        overlay_bytes: 4096,
                    },
                )
                .unwrap();
                let committed = [
                    (b"member-".as_slice(), b"left".as_slice()),
                    (b"member/b", b"old-b"),
                    (b"member/d", b"old-d"),
                    (b"member/f", b"old-f"),
                    (b"member/h", b"old-h"),
                    (b"member0", b"right"),
                ];
                let mut scope = store.begin().await.unwrap();
                for (key, value) in committed {
                    scope.put(key, value).unwrap();
                }
                scope.commit().await.unwrap();
                for i in 0..8 {
                    let mut scope = store.begin().await.unwrap();
                    scope
                        .put(format!("wide/{i}").as_bytes(), &vec![i as u8; 400])
                        .unwrap();
                    scope.commit().await.unwrap();
                }
                // Dirty first-read page reduction preserves stable snapshots,
                // overlay insertion/deletion order, short pages and cold retry.
                let mut dirty = store.begin().await.unwrap();
                for key in [b"D0", b"D2", b"D4", b"D6", b"D8"] {
                    dirty.put(key, b"marker").unwrap();
                }
                dirty.commit().await.unwrap();
                let mut dirty = store.begin().await.unwrap();
                dirty.delete(b"D0").unwrap();
                dirty.put(b"D1", b"new").unwrap();
                let mut after = None;
                let mut found = Vec::new();
                loop {
                    let next = dirty.first_dirty_from(after.as_deref()).await.unwrap();
                    assert_eq!(
                        next,
                        dirty.first_from(b"D", after.as_deref()).await.unwrap()
                    );
                    let Some((key, _)) = next else { break };
                    dirty.delete(&key).unwrap();
                    found.push(key.clone());
                    after = Some(key);
                }
                assert_eq!(found, [b"D1", b"D2", b"D4", b"D6", b"D8"]);
                drop(dirty);
                let mut dirty = store.begin().await.unwrap();
                assert_eq!(
                    dirty.first_dirty_from(None).await.unwrap().unwrap().0,
                    b"D0"
                );
                dirty.delete(b"D0").unwrap();
                dirty.commit().await.unwrap();
                let mut dirty = store.begin().await.unwrap();
                // A committed prior chunk's `after` need not exist in the new
                // snapshot. The inclusive request must still find its successor.
                assert_eq!(
                    dirty
                        .first_dirty_from(Some(b"D0"))
                        .await
                        .unwrap()
                        .unwrap()
                        .0,
                    b"D2"
                );
                for key in [b"D2", b"D4", b"D6", b"D8"] {
                    dirty.delete(key).unwrap();
                }
                dirty.commit().await.unwrap();
                let mut dirty = store.begin_point().await.unwrap();
                dirty.put(b"D0", b"earlier").unwrap();
                assert!(dirty.first_dirty_from(None).await.is_err());
                dirty.commit().await.unwrap();
                let dirty = store.begin().await.unwrap();
                assert_eq!(
                    dirty.first_dirty_from(None).await.unwrap().unwrap().0,
                    b"D0"
                );
                drop(dirty);
                let mut scope = store.begin().await.unwrap();
                if page_entries == 3 {
                    let page = scope
                        .snapshot
                        .as_ref()
                        .unwrap()
                        .try_scan(ScanRequest {
                            range: ScanRange {
                                namespace: store.table.namespace().clone(),
                                prefix: Some(b"wide/".to_vec()),
                                start: None,
                                end: None,
                            },
                            max_entries: 3,
                            max_bytes: 1024,
                            cursor: None,
                        })
                        .await
                        .unwrap();
                    assert!(
                        !page.entries.is_empty()
                            && page.entries.len() < 3
                            && page.next_cursor.is_some()
                    );
                    drop(page);
                }
                let mut wide = Vec::new();
                scope
                    .visit_prefix(b"wide/", None, |k, v| {
                        if rocks {
                            assert!(
                                resources
                                    .try_scan_page(resources.config().scan_page_bytes)
                                    .is_err(),
                                "callback must retain its Rocks page reservation"
                            );
                        }
                        wide.push((k.to_vec(), v.to_vec()));
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    wide,
                    (0..8)
                        .map(|i| (format!("wide/{i}").into_bytes(), vec![i as u8; 400]))
                        .collect::<Vec<_>>()
                );
                scope.put(b"member/a", b"insert-before").unwrap();
                scope.put(b"member/c", b"insert-between").unwrap();
                scope.put(b"member/z", b"insert-after").unwrap();
                scope.put(b"member/d", b"replacement").unwrap();
                scope.delete(b"member/b").unwrap();
                scope.delete(b"member/f").unwrap();
                scope.put(b"member.", b"overlay-left").unwrap();
                scope.put(b"member1", b"overlay-right").unwrap();
                let expected = [
                    (b"member/a".to_vec(), b"insert-before".to_vec()),
                    (b"member/c".to_vec(), b"insert-between".to_vec()),
                    (b"member/d".to_vec(), b"replacement".to_vec()),
                    (b"member/h".to_vec(), b"old-h".to_vec()),
                    (b"member/z".to_vec(), b"insert-after".to_vec()),
                ];
                // Holding the entire backend scan pool proves zero consumes no page.
                let held = resources
                    .try_scan_page(resources.config().scan_page_bytes)
                    .unwrap();
                scope
                    .visit_prefix(b"member/", Some(0), |_, _| {
                        anyhow::bail!("zero limit called visitor")
                    })
                    .await
                    .unwrap();
                drop(held);
                let point = store.begin_point().await.unwrap();
                point
                    .visit_prefix(b"member/", Some(0), |_, _| {
                        anyhow::bail!("zero limit called visitor")
                    })
                    .await
                    .unwrap();
                assert!(
                    point
                        .visit_prefix(b"member/", Some(1), |_, _| Ok(()))
                        .await
                        .is_err()
                );
                drop(point);
                if rocks {
                    // Occupy both native-worker slots before polling a visitor.
                    // The scan has admitted its page but cannot submit work;
                    // cancelling here must release the page immediately.
                    let mut blockers = Vec::new();
                    for _ in 0..resources.config().max_blocking_operations {
                        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                        let (release_tx, release_rx) = std::sync::mpsc::channel();
                        let blocking_resources = resources.clone();
                        let task = tokio::spawn(async move {
                            blocking_resources
                                .run_blocking(move || {
                                    started_tx.send(()).unwrap();
                                    release_rx
                                        .recv_timeout(std::time::Duration::from_secs(10))
                                        .unwrap();
                                })
                                .await
                                .unwrap();
                        });
                        tokio::time::timeout(std::time::Duration::from_secs(10), started_rx)
                            .await
                            .unwrap()
                            .unwrap();
                        blockers.push((release_tx, task));
                    }
                    for dirty_first in [false, true] {
                        let mut visiting = Box::pin(async {
                            if dirty_first {
                                scope.first_dirty_from(None).await.map(|_| ())
                            } else {
                                scope
                                    .visit_prefix(b"member/", None, |_, _| {
                                        anyhow::bail!("scan must still be waiting")
                                    })
                                    .await
                            }
                        });
                        assert!(futures::poll!(&mut visiting).is_pending());
                        assert!(
                            resources
                                .try_scan_page(resources.config().scan_page_bytes)
                                .is_err()
                        );
                        drop(visiting);
                        drop(
                            resources
                                .try_scan_page(resources.config().scan_page_bytes)
                                .unwrap(),
                        );
                    }
                    for (release, task) in blockers {
                        release.send(()).unwrap();
                        tokio::time::timeout(std::time::Duration::from_secs(10), task)
                            .await
                            .unwrap()
                            .unwrap();
                    }
                }
                for limit in [Some(1), Some(3), Some(5), Some(9), None] {
                    let mut actual = Vec::new();
                    scope
                        .visit_prefix(b"member/", limit, |k, v| {
                            actual.push((k.to_vec(), v.to_vec()));
                            Ok(())
                        })
                        .await
                        .unwrap();
                    assert_eq!(
                        actual,
                        expected[..limit.unwrap_or(expected.len()).min(expected.len())]
                    );
                }
                let mut calls = 0;
                let error = scope
                    .visit_prefix(b"member/", None, |_, _| {
                        calls += 1;
                        if calls == 2 {
                            anyhow::bail!("visitor stopped");
                        }
                        Ok(())
                    })
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("visitor stopped"));
                assert_eq!(calls, 2);
                // Error releases the admitted Rocks page even while scope lives.
                drop(
                    resources
                        .try_scan_page(resources.config().scan_page_bytes)
                        .unwrap(),
                );
                let mut retried = Vec::new();
                scope
                    .visit_prefix(b"member/", None, |k, v| {
                        retried.push((k.to_vec(), v.to_vec()));
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(retried, expected);
                // Visitor never commits or mutates the overlay. Dropping the
                // scope restores the original committed values on a fresh read.
                drop(scope);
                let scope = store.begin().await.unwrap();
                let mut original = Vec::new();
                scope
                    .visit_prefix(b"member/", None, |k, v| {
                        original.push((k.to_vec(), v.to_vec()));
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    original,
                    committed[1..5]
                        .iter()
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .collect::<Vec<_>>()
                );
                drop(scope);
                drop(store);
                drop(manager);
                if rocks {
                    backend.close().await.unwrap();
                    std::fs::remove_dir_all(root).unwrap();
                }
            }
        }
    }
}
