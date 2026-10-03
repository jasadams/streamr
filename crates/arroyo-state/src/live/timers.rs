//! Durable timers for one serial execution owner. Both indexes live inside the
//! registered table namespace, so logical checkpoints capture them together.
//! Logical key prefixes 0 and 1 are reserved for primary and deadline indexes.
use super::resources::{ResourcePermit, WorkerStateResources};
use super::{
    LiveStateBackend, LiveStateError, ReadOptions, Result, ScanCursor, ScanRange, ScanRequest,
    StateKey, StateNamespace, StateSnapshot, WriteBatch, WriteOperation,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerClock {
    Event,
    Processing,
}
impl TimerClock {
    fn byte(self) -> u8 {
        match self {
            Self::Event => 0,
            Self::Processing => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimerLimits {
    pub max_key_bytes: usize,
    pub max_payload_bytes: usize,
    pub page_entries: usize,
    pub page_bytes: usize,
    pub batch_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerEntry {
    pub clock: TimerClock,
    pub id: Vec<u8>,
    /// Caller-defined signed clock units, consistent with the scan cutoff.
    pub deadline: i64,
    pub payload: Vec<u8>,
}

pub struct TimerPage {
    entries: Vec<TimerEntry>,
    pub next_cursor: Option<ScanCursor>,
    _decoded: Option<ResourcePermit>,
}

impl TimerPage {
    /// Entries borrow the page, retaining its accounting permit. Cloned entries
    /// need separate caller admission if retained after this page is dropped.
    pub fn entries(&self) -> &[TimerEntry] {
        &self.entries
    }
}

/// Exactly one mutable view owns this namespace. Share that view with Arc rather
/// than constructing independent writers. Prepared batches require the caller's
/// serial owner to hold exclusivity from preparation through commit. Append any
/// related state operations to the same batch before committing; validation and
/// backend write_batch are atomic. Do not prepare multiple mutations for the same
/// timer against an unchanged backend and then combine them.
/// Input producers must reserve their owned payloads and assembled batches before
/// preparation; prepared operations are caller-owned until backend admission.
///
/// No wall clock is persisted or advanced here. Operators select event-watermark
/// and processing-clock cutoffs, revalidate due entries before firing, commit the
/// timer deletion with state changes, and apply their output/checkpoint contract.
pub struct DurableTimers {
    backend: Arc<dyn LiveStateBackend>,
    namespace: StateNamespace,
    limits: TimerLimits,
    mutation: Mutex<()>,
    resources: Option<WorkerStateResources>,
}

pub struct TimerSnapshot {
    snapshot: StateSnapshot,
    namespace: StateNamespace,
    limits: TimerLimits,
    resources: Option<WorkerStateResources>,
}

impl DurableTimers {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        namespace: StateNamespace,
        limits: TimerLimits,
    ) -> Result<Self> {
        if [
            limits.max_key_bytes,
            limits.max_payload_bytes,
            limits.page_entries,
            limits.page_bytes,
            limits.batch_bytes,
        ]
        .contains(&0)
        {
            return Err(LiveStateError::InvalidLimit);
        }
        super::encoding::encode_namespace(&namespace)?;
        if matches!(namespace.ownership, super::Ownership::Routed { .. }) {
            return Err(invalid("routed timers require an operator routing adapter"));
        }
        Ok(Self {
            backend,
            namespace,
            limits,
            mutation: Mutex::new(()),
            resources: None,
        })
    }

    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    /// Charge retained due pages to the same worker pool as the backend. Keep the
    /// TimerPage alive while using its entries to retain their reservation. Drop
    /// each page before requesting another unless the pool admits both at once.
    /// Admission reserves headroom for revalidating one held page; multiple held
    /// pages and concurrent producer reservations require caller admission.
    pub fn with_resources(mut self, resources: WorkerStateResources) -> Result<Self> {
        let namespace_bytes = super::encoding::encoded_namespace_size(&self.namespace)?;
        let page = page_reservation_bytes(self.limits, namespace_bytes)?;
        let read = revalidation_read_bytes(self.limits, namespace_bytes)?;
        let decoded = page.checked_add(read).ok_or(LiveStateError::InvalidLimit)?;
        if decoded > resources.config().decoded_value_bytes {
            return Err(invalid(&format!(
                "timer retained page plus revalidation read requires {decoded} decoded bytes; pool has {}",
                resources.config().decoded_value_bytes
            )));
        }
        let scan = native_scan_reservation_bytes(self.limits, namespace_bytes)?;
        if scan > resources.config().scan_page_bytes {
            return Err(invalid(&format!(
                "timer native scan requires {scan} bytes; scan pool has {}",
                resources.config().scan_page_bytes
            )));
        }
        self.resources = Some(resources);
        Ok(self)
    }

    fn key(&self, logical: Vec<u8>) -> StateKey {
        StateKey {
            namespace: self.namespace.clone(),
            key: logical,
            routing_hash: None,
        }
    }

    fn validate_id(&self, id: &[u8]) -> Result<()> {
        if id.is_empty() || id.len() > self.limits.max_key_bytes {
            return Err(invalid("timer ID is empty or exceeds max_key_bytes"));
        }
        Ok(())
    }

    async fn current(&self, clock: TimerClock, id: &[u8]) -> Result<Option<(i64, Vec<u8>)>> {
        self.validate_id(id)?;
        self.backend
            .get(
                &self.key(primary_key(clock, id)),
                ReadOptions {
                    max_bytes: self.limits.max_payload_bytes.saturating_add(8),
                },
            )
            .await?
            .map(|value| decode_primary(&value, self.limits.max_payload_bytes))
            .transpose()
    }

    /// Prepare an atomic replacement, including deletion of the previous index.
    /// The caller owns serialization until the returned batch is committed.
    pub async fn prepare_replace(
        &self,
        clock: TimerClock,
        id: &[u8],
        deadline: i64,
        payload: &[u8],
    ) -> Result<WriteBatch> {
        self.validate_id(id)?;
        if payload.len() > self.limits.max_payload_bytes {
            return Err(invalid("timer payload exceeds max_payload_bytes"));
        }
        let old = self.current(clock, id).await?;
        let mut operations = Vec::with_capacity(3);
        if let Some((previous, _)) = old {
            operations.push(WriteOperation::Delete {
                key: self.key(due_key(clock, previous, id)),
            });
        }
        let mut value = deadline.to_be_bytes().to_vec();
        value.extend_from_slice(payload);
        operations.push(WriteOperation::Put {
            key: self.key(primary_key(clock, id)),
            value,
        });
        operations.push(WriteOperation::Put {
            key: self.key(due_key(clock, deadline, id)),
            value: payload.to_vec(),
        });
        self.batch(operations)
    }

    pub async fn prepare_cancel(&self, clock: TimerClock, id: &[u8]) -> Result<WriteBatch> {
        let current = self.current(clock, id).await?;
        self.cancel_batch(clock, id, current.map(|(deadline, _)| deadline))
    }

    /// Revalidate a stable due page against live state before firing. None means
    /// the timer was cancelled or replaced. An identical replacement represents
    /// the same logical timer (clock, ID, deadline and payload).
    pub async fn prepare_cancel_current(&self, entry: &TimerEntry) -> Result<Option<WriteBatch>> {
        if entry.payload.len() > self.limits.max_payload_bytes {
            return Err(invalid("timer payload exceeds max_payload_bytes"));
        }
        match self.current(entry.clock, &entry.id).await? {
            Some((deadline, payload)) if deadline == entry.deadline && payload == entry.payload => {
                Ok(Some(self.cancel_batch(
                    entry.clock,
                    &entry.id,
                    Some(deadline),
                )?))
            }
            _ => Ok(None),
        }
    }

    fn cancel_batch(
        &self,
        clock: TimerClock,
        id: &[u8],
        deadline: Option<i64>,
    ) -> Result<WriteBatch> {
        let operations = deadline.map_or_else(Vec::new, |deadline| {
            vec![
                WriteOperation::Delete {
                    key: self.key(primary_key(clock, id)),
                },
                WriteOperation::Delete {
                    key: self.key(due_key(clock, deadline, id)),
                },
            ]
        });
        self.batch(operations)
    }

    fn batch(&self, operations: Vec<WriteOperation>) -> Result<WriteBatch> {
        let batch = WriteBatch {
            operations,
            max_bytes: self.limits.batch_bytes,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub async fn replace(
        &self,
        clock: TimerClock,
        id: &[u8],
        deadline: i64,
        payload: &[u8],
    ) -> Result<()> {
        let _guard = self.mutation.lock().await;
        self.backend
            .write_batch(self.prepare_replace(clock, id, deadline, payload).await?)
            .await
    }

    pub async fn cancel(&self, clock: TimerClock, id: &[u8]) -> Result<()> {
        let _guard = self.mutation.lock().await;
        self.backend
            .write_batch(self.prepare_cancel(clock, id).await?)
            .await
    }

    pub async fn snapshot(&self) -> Result<TimerSnapshot> {
        Ok(TimerSnapshot {
            snapshot: self.backend.snapshot().await?,
            namespace: self.namespace.clone(),
            limits: self.limits,
            resources: self.resources.clone(),
        })
    }
}

impl TimerSnapshot {
    /// A cursor belongs to this snapshot, clock and cutoff. Page limits apply to
    /// encoded index bytes and entry count, even for many IDs at one timestamp.
    pub async fn due(
        &self,
        clock: TimerClock,
        through: i64,
        cursor: Option<ScanCursor>,
    ) -> Result<TimerPage> {
        // Reserve before scan returns owned buffers or the index is decoded.
        // Account for native-decoded scan entries, timer structs, copied IDs and
        // cursor/range namespaces; payload vectors transfer ownership in place.
        let required = page_reservation_bytes(
            self.limits,
            super::encoding::encoded_namespace_size(&self.namespace)?,
        )?;
        let decoded = if let Some(resources) = &self.resources {
            Some(resources.decoded_value(required).await?)
        } else {
            None
        };
        let prefix = vec![1, clock.byte()];
        let end = if let Some(next) = through.checked_add(1) {
            let mut bound = prefix.clone();
            bound.extend(sortable_time(next));
            bound
        } else {
            vec![1, clock.byte() + 1]
        };
        let page = self
            .snapshot
            .scan(ScanRequest {
                range: ScanRange {
                    namespace: self.namespace.clone(),
                    prefix: Some(prefix),
                    start: None,
                    end: Some(end),
                },
                max_entries: self.limits.page_entries,
                max_bytes: self.limits.page_bytes,
                cursor,
            })
            .await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let key = &entry.key.key;
            if key.len() <= 10
                || key[0..2] != [1, clock.byte()]
                || key.len() - 10 > self.limits.max_key_bytes
                || entry.value.len() > self.limits.max_payload_bytes
            {
                return Err(invalid("invalid durable timer index"));
            }
            let deadline = (u64::from_be_bytes(key[2..10].try_into().expect("checked timer key"))
                ^ (1 << 63)) as i64;
            entries.push(TimerEntry {
                clock,
                deadline,
                id: key[10..].to_vec(),
                payload: entry.value,
            });
        }
        Ok(TimerPage {
            entries,
            next_cursor: page.next_cursor,
            _decoded: decoded,
        })
    }
}

// Conservative admission bounds match Rocks get/scan request accounting. They
// avoid allocating worst-case escaped timer IDs merely to measure their sizes.
fn page_reservation_bytes(limits: TimerLimits, namespace_bytes: usize) -> Result<usize> {
    limits
        .page_entries
        .min(limits.page_bytes / 11)
        .checked_mul(std::mem::size_of::<TimerEntry>() * 2)
        .and_then(|containers| limits.page_bytes.checked_mul(4)?.checked_add(containers))
        .and_then(|bytes| bytes.checked_add(namespace_bytes.checked_mul(4)?))
        .ok_or(LiveStateError::InvalidLimit)
}
fn revalidation_read_bytes(limits: TimerLimits, namespace_bytes: usize) -> Result<usize> {
    // Primary logical prefix is two bytes; both may escape, as may every ID byte.
    let key_bytes = limits
        .max_key_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(namespace_bytes))
        .and_then(|bytes| bytes.checked_add(6))
        .ok_or(LiveStateError::InvalidLimit)?;
    limits
        .max_payload_bytes
        .checked_add(8)
        .and_then(|bytes| bytes.checked_add(key_bytes.checked_mul(2)?))
        .and_then(|bytes| {
            bytes.checked_add(
                std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>(),
            )
        })
        .ok_or(LiveStateError::InvalidLimit)
}
fn native_scan_reservation_bytes(limits: TimerLimits, namespace_bytes: usize) -> Result<usize> {
    let cursor_bytes = limits
        .max_key_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(namespace_bytes))
        .and_then(|bytes| bytes.checked_add(22))
        .ok_or(LiveStateError::InvalidLimit)?;
    let request_bytes = namespace_bytes
        .checked_mul(5)
        .and_then(|bytes| bytes.checked_add(12)) // ten-byte end + two-byte prefix
        .and_then(|bytes| bytes.checked_add(cursor_bytes))
        .ok_or(LiveStateError::InvalidLimit)?;
    let min_entry = namespace_bytes
        .checked_add(3)
        .ok_or(LiveStateError::InvalidLimit)?;
    let containers = limits
        .page_entries
        .min(limits.page_bytes / min_entry)
        .checked_mul(std::mem::size_of::<super::ScanEntry>() * 2)
        .ok_or(LiveStateError::InvalidLimit)?;
    limits
        .page_bytes
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(request_bytes.checked_mul(4)?))
        .and_then(|bytes| bytes.checked_add(containers))
        .ok_or(LiveStateError::InvalidLimit)
}

fn primary_key(clock: TimerClock, id: &[u8]) -> Vec<u8> {
    let mut key = vec![0, clock.byte()];
    key.extend_from_slice(id);
    key
}
fn sortable_time(time: i64) -> [u8; 8] {
    ((time as u64) ^ (1 << 63)).to_be_bytes()
}
fn due_key(clock: TimerClock, deadline: i64, id: &[u8]) -> Vec<u8> {
    let mut key = vec![1, clock.byte()];
    key.extend(sortable_time(deadline));
    key.extend_from_slice(id);
    key
}
fn decode_primary(value: &[u8], max_payload: usize) -> Result<(i64, Vec<u8>)> {
    if value.len() < 8 || value.len() - 8 > max_payload {
        return Err(invalid("invalid durable timer primary record"));
    }
    Ok((
        i64::from_be_bytes(value[..8].try_into().expect("checked primary")),
        value[8..].to_vec(),
    ))
}
fn invalid(message: &str) -> LiveStateError {
    LiveStateError::InvalidEncoding(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{
        Ownership, checkpoint,
        lifecycle::RocksStateConfig,
        memory::MemoryLiveState,
        resources::{ResourceConfig, WorkerStateResources},
        rocks::RocksLiveState,
    };
    use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
    use arroyo_storage::{StorageProvider, StorageProviderRef};

    fn namespace() -> StateNamespace {
        StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"timers".to_vec(),
        }
    }
    fn limits() -> TimerLimits {
        TimerLimits {
            max_key_bytes: 64,
            max_payload_bytes: 256,
            page_entries: 7,
            page_bytes: 2048,
            batch_bytes: 4096,
        }
    }
    fn timers(backend: Arc<dyn LiveStateBackend>) -> DurableTimers {
        DurableTimers::new(backend, namespace(), limits()).unwrap()
    }
    async fn all_due(view: &TimerSnapshot, clock: TimerClock, through: i64) -> Vec<TimerEntry> {
        let mut values = Vec::new();
        let mut cursor = None;
        loop {
            let page = view.due(clock, through, cursor).await.unwrap();
            assert!(page.entries.len() <= 7);
            values.extend(page.entries);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        values
    }
    fn resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 * 1024 * 1024,
            memtable_bytes: 2 * 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 8 * 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 8,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }
    async fn rocks(root: &std::path::Path, generation: u64) -> Arc<dyn LiveStateBackend> {
        Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.to_path_buf(),
                    job_id: "timer-job".into(),
                    operator_id: "timer-operator".into(),
                    subtask: 0,
                    generation,
                    attempt: 0,
                },
                resources(),
            )
            .await
            .unwrap(),
        )
    }

    async fn replacement_and_cancel(backend: Arc<dyn LiveStateBackend>) {
        let view = timers(backend.clone());
        view.replace(TimerClock::Event, b"profile", -20, b"session")
            .await
            .unwrap();
        view.replace(TimerClock::Processing, b"profile", 5, b"debounce")
            .await
            .unwrap();
        let before = view.snapshot().await.unwrap();
        view.replace(TimerClock::Event, b"profile", 30, b"decay")
            .await
            .unwrap();
        assert_eq!(
            all_due(&before, TimerClock::Event, 0).await[0].payload,
            b"session"
        );
        let stale = all_due(&before, TimerClock::Event, 0).await.remove(0);
        assert!(view.prepare_cancel_current(&stale).await.unwrap().is_none());
        let after = view.snapshot().await.unwrap();
        assert!(all_due(&after, TimerClock::Event, 29).await.is_empty());
        assert_eq!(all_due(&after, TimerClock::Event, 30).await.len(), 1);
        assert_eq!(all_due(&after, TimerClock::Processing, 5).await.len(), 1);
        view.cancel(TimerClock::Event, b"profile").await.unwrap();
        view.cancel(TimerClock::Event, b"profile").await.unwrap();
        let snapshot = view.snapshot().await.unwrap();
        assert!(
            all_due(&snapshot, TimerClock::Event, i64::MAX)
                .await
                .is_empty()
        );
        assert_eq!(
            all_due(&snapshot, TimerClock::Processing, i64::MAX)
                .await
                .len(),
            1
        );
        // Cancel removes both the primary and due record; it leaves no tombstone.
        assert!(
            backend
                .get(
                    &view.key(primary_key(TimerClock::Event, b"profile")),
                    ReadOptions { max_bytes: 1024 }
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn replacement_cancel_and_clock_isolation_on_memory_and_rocks() {
        replacement_and_cancel(Arc::new(MemoryLiveState::new())).await;
        let root = tempfile::tempdir().unwrap();
        replacement_and_cancel(rocks(root.path(), 0).await).await;
    }

    #[tokio::test]
    async fn same_timestamp_hot_key_pages_are_complete_and_cursors_are_bound() {
        let view = timers(Arc::new(MemoryLiveState::new()));
        for id in 0u32..300 {
            let mut key = b"hot-profile\0".to_vec();
            key.extend(id.to_be_bytes());
            view.replace(TimerClock::Event, &key, i64::MAX, &[id as u8])
                .await
                .unwrap();
        }
        let snapshot = view.snapshot().await.unwrap();
        let first = snapshot
            .due(TimerClock::Event, i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(first.entries.len(), 7);
        let cursor = first.next_cursor.clone().unwrap();
        assert!(
            snapshot
                .due(TimerClock::Processing, i64::MAX, Some(cursor.clone()))
                .await
                .is_err()
        );
        assert!(
            snapshot
                .due(TimerClock::Event, i64::MAX - 1, Some(cursor.clone()))
                .await
                .is_err()
        );
        assert!(
            view.snapshot()
                .await
                .unwrap()
                .due(TimerClock::Event, i64::MAX, Some(cursor))
                .await
                .is_err()
        );
        let due = all_due(&snapshot, TimerClock::Event, i64::MAX).await;
        assert_eq!(due.len(), 300);
        for (index, entry) in due.iter().enumerate() {
            assert_eq!(entry.id[12..], (index as u32).to_be_bytes());
        }
        assert!(
            all_due(&snapshot, TimerClock::Event, i64::MAX - 1)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn prepared_timer_and_state_changes_commit_atomically_and_revalidate() {
        let backend = Arc::new(MemoryLiveState::new());
        let view = timers(backend.clone());
        let state = StateKey {
            namespace: namespace(),
            key: b"profile-state".to_vec(),
            routing_hash: None,
        };
        let mut batch = view
            .prepare_replace(TimerClock::Processing, b"profile", 10, b"v1")
            .await
            .unwrap();
        batch.operations.push(WriteOperation::Put {
            key: state.clone(),
            value: b"v1".to_vec(),
        });
        backend.write_batch(batch).await.unwrap();
        let entry = all_due(&view.snapshot().await.unwrap(), TimerClock::Processing, 10)
            .await
            .remove(0);
        // Even a replacement at the same timestamp invalidates a stale payload.
        view.replace(TimerClock::Processing, b"profile", 10, b"v2")
            .await
            .unwrap();
        assert!(view.prepare_cancel_current(&entry).await.unwrap().is_none());
        let mut failing = view
            .prepare_replace(TimerClock::Processing, b"profile", 20, b"v3")
            .await
            .unwrap();
        failing.operations.push(WriteOperation::Put {
            key: state.clone(),
            value: vec![0; 8192],
        });
        assert!(backend.write_batch(failing).await.is_err());
        let current = all_due(&view.snapshot().await.unwrap(), TimerClock::Processing, 10)
            .await
            .remove(0);
        assert_eq!(current.payload, b"v2");
        assert_eq!(
            backend
                .get(&state, ReadOptions { max_bytes: 2 })
                .await
                .unwrap(),
            Some(b"v1".to_vec())
        );
        let mut firing = view
            .prepare_cancel_current(&current)
            .await
            .unwrap()
            .unwrap();
        firing.operations.push(WriteOperation::Put {
            key: state.clone(),
            value: b"fired".to_vec(),
        });
        backend.write_batch(firing).await.unwrap();
        assert!(
            all_due(
                &view.snapshot().await.unwrap(),
                TimerClock::Processing,
                i64::MAX
            )
            .await
            .is_empty()
        );
        assert_eq!(
            backend
                .get(&state, ReadOptions { max_bytes: 5 })
                .await
                .unwrap(),
            Some(b"fired".to_vec())
        );
        assert!(
            view.replace(TimerClock::Event, b"oversize", 0, &[0; 257])
                .await
                .is_err()
        );
        assert!(
            view.replace(TimerClock::Event, &[0; 65], 0, b"")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retained_due_page_keeps_its_worker_reservation_until_dropped() {
        let resources = resources();
        let _occupied = resources
            .decoded_value(resources.config().decoded_value_bytes - 16 * 1024)
            .await
            .unwrap();
        let view = timers(Arc::new(MemoryLiveState::new()))
            .with_resources(resources.clone())
            .unwrap();
        view.replace(TimerClock::Event, b"profile", 1, b"payload")
            .await
            .unwrap();
        let page = view
            .snapshot()
            .await
            .unwrap()
            .due(TimerClock::Event, 1, None)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                resources.decoded_value(16 * 1024)
            )
            .await
            .is_err()
        );
        drop(page);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            resources.decoded_value(16 * 1024),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[test]
    fn resource_validation_requires_revalidation_and_native_scan_headroom() {
        let namespace_bytes = super::super::encoding::encoded_namespace_size(&namespace()).unwrap();
        let page = page_reservation_bytes(limits(), namespace_bytes).unwrap();
        let read = revalidation_read_bytes(limits(), namespace_bytes).unwrap();
        let scan = native_scan_reservation_bytes(limits(), namespace_bytes).unwrap();
        let mut config = resources().config().clone();
        config.decoded_value_bytes = page;
        assert!(
            timers(Arc::new(MemoryLiveState::new()))
                .with_resources(WorkerStateResources::new(config.clone()).unwrap())
                .err()
                .unwrap()
                .to_string()
                .contains("revalidation read")
        );
        config.decoded_value_bytes = page + read;
        config.scan_page_bytes = scan - 1;
        assert!(
            timers(Arc::new(MemoryLiveState::new()))
                .with_resources(WorkerStateResources::new(config).unwrap())
                .err()
                .unwrap()
                .to_string()
                .contains("native scan")
        );
    }

    #[tokio::test]
    async fn rocks_revalidation_progresses_with_a_retained_page_in_tight_valid_pool() {
        let limits = TimerLimits {
            max_key_bytes: 16,
            max_payload_bytes: 32,
            page_entries: 2,
            page_bytes: 128,
            batch_bytes: 1024,
        };
        let namespace_bytes = super::super::encoding::encoded_namespace_size(&namespace()).unwrap();
        let mut config = resources().config().clone();
        config.decoded_value_bytes = page_reservation_bytes(limits, namespace_bytes).unwrap()
            + revalidation_read_bytes(limits, namespace_bytes).unwrap();
        config.scan_page_bytes = native_scan_reservation_bytes(limits, namespace_bytes).unwrap();
        let resources = WorkerStateResources::new(config).unwrap();
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.path().to_path_buf(),
                    job_id: "timer-job".into(),
                    operator_id: "timer-operator".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let view = DurableTimers::new(backend.clone(), namespace(), limits)
            .unwrap()
            .with_resources(resources)
            .unwrap();
        // All-zero IDs exercise maximum key escaping and native read admission.
        view.replace(TimerClock::Event, &[0; 16], 1, &[1; 32])
            .await
            .unwrap();
        view.replace(TimerClock::Event, &[0; 15], 2, &[2; 32])
            .await
            .unwrap();
        let snapshot = view.snapshot().await.unwrap();
        let page = snapshot.due(TimerClock::Event, 2, None).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert!(page.next_cursor.is_some());
        let batch = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            view.prepare_cancel_current(&page.entries[0]),
        )
        .await
        .expect("held timer page must leave revalidation headroom")
        .unwrap()
        .unwrap();
        backend.write_batch(batch).await.unwrap();
        let cursor = page.next_cursor.clone();
        drop(page);
        let next = snapshot.due(TimerClock::Event, 2, cursor).await.unwrap();
        assert_eq!(next.entries.len(), 1);
        assert_eq!(next.entries[0].deadline, 2);
    }

    #[tokio::test]
    async fn full_logical_checkpoint_restores_timer_indexes_into_fresh_generation() {
        let root = tempfile::tempdir().unwrap();
        let backend = rocks(root.path(), 0).await;
        let view = timers(backend.clone());
        for i in 0u32..300 {
            view.replace(TimerClock::Event, &i.to_be_bytes(), 100, &i.to_be_bytes())
                .await
                .unwrap();
        }
        view.replace(TimerClock::Processing, b"debounce", 5, b"payload")
            .await
            .unwrap();
        let barrier = backend.snapshot().await.unwrap();
        view.cancel(TimerClock::Event, &0u32.to_be_bytes())
            .await
            .unwrap();
        view.replace(TimerClock::Processing, b"debounce", 999, b"post-barrier")
            .await
            .unwrap();
        let remote = tempfile::tempdir().unwrap();
        let storage: StorageProviderRef = Arc::new(
            StorageProvider::for_url(&format!("file://{}", remote.path().display()))
                .await
                .unwrap(),
        );
        let config = DiskKeyedTableConfig {
            table_name: "timers".into(),
            encoding_version: 1,
            schema_identity: b"timers-v1".to_vec(),
        };
        let metadata = checkpoint::export(
            &barrier,
            &namespace(),
            &config,
            &storage,
            "checkpoints/checkpoint-0000001/timers",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        assert!(metadata.files.len() > 1);
        let restored = rocks(root.path(), 1).await;
        checkpoint::restore(
            restored.as_ref(),
            &namespace(),
            &config,
            &metadata,
            &storage,
        )
        .await
        .unwrap();
        let restored_view = timers(restored);
        let due = all_due(
            &restored_view.snapshot().await.unwrap(),
            TimerClock::Event,
            100,
        )
        .await;
        assert_eq!(due.len(), 300);
        let debounce = all_due(
            &restored_view.snapshot().await.unwrap(),
            TimerClock::Processing,
            5,
        )
        .await;
        assert_eq!(debounce.len(), 1);
        assert_eq!(debounce[0].payload, b"payload");
        restored_view
            .cancel(TimerClock::Event, &0u32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(
            all_due(
                &restored_view.snapshot().await.unwrap(),
                TimerClock::Event,
                100
            )
            .await
            .len(),
            299
        );
    }
}
