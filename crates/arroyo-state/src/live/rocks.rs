//! RocksDB live state with lifetime-safe owned native read snapshots.
//! Pipeline checkpoint export and recovery remain separate from these read views.
use super::lifecycle::RocksStateConfig;
use super::resources::{CleanupPermit, ResourcePermit, WorkerStateResources};
use super::{
    encoding::{decode_key, decode_value, encode_key, encode_value},
    *,
};
use async_trait::async_trait;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, DB, DBRecoveryMode, Options, WriteOptions};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

pub struct RocksLiveState {
    db: Arc<NativeDb>,
    completion: oneshot::Receiver<Result<()>>,
    path: PathBuf,
    resources: WorkerStateResources,
    durability: WriteDurability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteDurability {
    /// Disposable working state; only a published pipeline checkpoint is durable.
    Checkpoint,
    /// Explicit local open/reopen supports recovery of acknowledged writes.
    SynchronousWal,
}

impl WriteDurability {
    fn options(self) -> WriteOptions {
        let mut options = WriteOptions::default();
        options.disable_wal(self == Self::Checkpoint);
        options.set_sync(self == Self::SynchronousWal);
        options
    }
}

fn backend(error: impl std::fmt::Display) -> LiveStateError {
    LiveStateError::Backend(error.to_string())
}

fn options(resources: &WorkerStateResources, create: bool) -> Options {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    // A damaged WAL must surface as an error, never be skipped silently.
    options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
    options.set_write_buffer_manager(resources.write_buffer_manager());
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(resources.cache());
    table.set_cache_index_and_filter_blocks(true);
    table.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    table.set_partition_filters(true);
    table.set_bloom_filter(10.0, false);
    table.set_pin_top_level_index_and_filter(false);
    table.set_pin_l0_filter_and_index_blocks_in_cache(false);
    options.set_max_open_files(128);
    options.set_write_buffer_size(resources.config().memtable_bytes);
    options.set_block_based_table_factory(&table);
    options
}

impl RocksLiveState {
    /// Worker entry point: every operator uses the configured process-wide pool.
    /// This opens fresh live storage; checkpoint restore remains caller-managed.
    /// Exhausted database slots fail immediately to avoid readiness deadlocks.
    /// Writes complete atomically without WAL logging or per-row fsync;
    /// recovery must restore a committed checkpoint into this fresh attempt.
    pub async fn open_worker(config: RocksStateConfig) -> Result<Self> {
        let resources = super::worker::configured_worker_resources()?.ok_or_else(|| {
            LiveStateError::Backend("worker live-state resource budgets are not configured".into())
        })?;
        Self::open_worker_with_resources(config, resources).await
    }

    pub(crate) async fn open_worker_with_resources(
        config: RocksStateConfig,
        resources: WorkerStateResources,
    ) -> Result<Self> {
        let permit = resources.try_database()?;
        Self::open_mode(
            config,
            resources,
            false,
            permit,
            WriteDurability::Checkpoint,
        )
        .await
    }

    pub async fn open(config: RocksStateConfig, resources: WorkerStateResources) -> Result<Self> {
        let permit = resources.database().await?;
        Self::open_mode(
            config,
            resources,
            false,
            permit,
            WriteDurability::SynchronousWal,
        )
        .await
    }

    /// Explicit reuse of precisely the supplied attempt. Missing or corrupt
    /// databases fail instead of being replaced with empty state.
    pub async fn reopen(config: RocksStateConfig, resources: WorkerStateResources) -> Result<Self> {
        let permit = resources.database().await?;
        Self::open_mode(
            config,
            resources,
            true,
            permit,
            WriteDurability::SynchronousWal,
        )
        .await
    }

    async fn open_mode(
        config: RocksStateConfig,
        resources: WorkerStateResources,
        reopen: bool,
        database_permit: ResourcePermit,
        durability: WriteDurability,
    ) -> Result<Self> {
        let cleanup = resources.cleanup().await.map_err(LiveStateError::from)?;
        let task_resources = resources.clone();
        resources
            .run_blocking(move || {
                let _latency = task_resources.operation_timer("open");
                let path = config.prepare(reopen)?;
                let opened = (|| {
                    task_resources.ensure_disk_space(&path, 0)?;
                    DB::open(&options(&task_resources, !reopen), &path).map_err(backend)
                })();
                let db = match opened {
                    Ok(db) => db,
                    Err(error) => {
                        // Fresh preparation exclusively created this directory.
                        // Reopen failures must preserve existing state for diagnosis.
                        if !reopen {
                            let _ = std::fs::remove_dir_all(&path);
                        }
                        return Err(error);
                    }
                };
                let health = health::BackendHealth::new("rocksdb", !reopen);
                health.sample_native(&db, &path);
                task_resources.observe_health(&health);
                let (finished, completion) = oneshot::channel();
                Ok(Self {
                    db: Arc::new(NativeDb {
                        db: Some(db),
                        write_lock: Mutex::new(()),
                        health: Some(health),
                        path: path.clone(),
                        remove: AtomicBool::new(false),
                        permit: Some(database_permit),
                        resources: Some(task_resources.clone()),
                        cleanup: Some(cleanup),
                        finished: Some(finished),
                    }),
                    completion,
                    path,
                    resources: task_resources,
                    durability,
                })
            })
            .await
            .map_err(LiveStateError::from)?
    }

    /// Reserve producer bytes before constructing owned pending writes.
    pub async fn admitted_batch(
        &self,
        max_bytes: usize,
        max_operations: usize,
    ) -> Result<super::write::AdmittedWriteBatch> {
        super::write::AdmittedWriteBatch::reserve(self.resources.clone(), max_bytes, max_operations)
            .await
    }

    pub async fn write_admitted(&self, admitted: super::write::AdmittedWriteBatch) -> Result<()> {
        let (batch, permit, resources) = admitted.into_parts();
        if !self.resources.same_pool(&resources) {
            return Err(LiveStateError::Backend(
                "write admission belongs to another worker pool".into(),
            ));
        }
        self.write_with_permit(batch, permit).await
    }

    async fn write_with_permit(&self, batch: WriteBatch, permit: ResourcePermit) -> Result<()> {
        let bytes = batch.encoded_size()?;
        let db = self.db.clone();
        let path = self.path.clone();
        let resources = self.resources.clone();
        let durability = self.durability;
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("write");
                let _permit = permit;
                resources
                    .ensure_disk_space(&path, bytes as u64)
                    .map_err(LiveStateError::from)?;
                // Serialize touched-key observations with the native commit. The
                // map contains only the final operation per admitted key, so
                // repeated puts/deletes within one atomic batch count once.
                let _write = db.write_lock.lock().map_err(backend)?;
                let mut final_values = std::collections::BTreeMap::new();
                for operation in batch.operations {
                    match operation {
                        WriteOperation::Put { key, value } => {
                            final_values.insert(
                                encode_key(&key)?,
                                (key.key.len(), Some(encode_value(&value))),
                            );
                        }
                        WriteOperation::Delete { key } => {
                            final_values.insert(encode_key(&key)?, (key.key.len(), None));
                        }
                    }
                }
                let mut logical = db
                    .health
                    .as_ref()
                    .and_then(|health| *health.logical.lock().unwrap_or_else(|e| e.into_inner()));
                let mut native = rocksdb::WriteBatch::default();
                for (key, (key_bytes, value)) in final_values {
                    if logical.is_some() {
                        // Pinned access avoids copying an old value merely to
                        // measure it. Telemetry read failures make the baseline
                        // unavailable; they never veto an otherwise valid write.
                        match db.get_pinned(&key) {
                            Ok(old) => {
                                let size = logical.as_mut().expect("known logical baseline");
                                if let Some(old) = old {
                                    if let Some(old_bytes) = old.len().checked_sub(1) {
                                        size.keys -= 1;
                                        size.key_bytes -= key_bytes as u64;
                                        size.value_bytes -= old_bytes as u64;
                                    } else {
                                        logical = None;
                                    }
                                }
                                if let Some(size) = &mut logical
                                    && let Some(value) = &value
                                {
                                    size.keys += 1;
                                    size.key_bytes += key_bytes as u64;
                                    size.value_bytes += (value.len() - 1) as u64;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "logical state measurement unavailable");
                                logical = None;
                            }
                        }
                    }
                    if let Some(value) = value {
                        native.put(key, value);
                    } else {
                        native.delete(key);
                    }
                }
                let options = durability.options();
                let result = db.write_opt(native, &options).map_err(backend);
                if let Some(health) = &db.health {
                    if result.is_ok() {
                        *health.logical.lock().unwrap_or_else(|e| e.into_inner()) = logical;
                    }
                    health.sample_native(&db, &path);
                }
                result
            })
            .await
            .map_err(LiveStateError::from)?
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Attempt-local SQL databases are disposable caches of committed remote
    /// checkpoints. Mark before restore so all failure paths remove this attempt
    /// after its final native reader releases it.
    pub fn remove_on_drop(&self) {
        self.db.remove.store(true, Ordering::Relaxed);
    }

    /// Wait until all native users have released the database and cleanup finishes.
    pub async fn close(self) -> Result<()> {
        let Self { db, completion, .. } = self;
        drop(db);
        completion.await.map_err(backend)?
    }

    /// Remove only this attempt's database after its last native read view
    /// releases the shared owner. No sibling snapshot databases are created.
    pub async fn close_and_remove(self) -> Result<()> {
        self.db.remove.store(true, Ordering::Relaxed);
        self.close().await
    }

    async fn read_many(
        &self,
        keys: &[StateKey],
        read: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        read_many(
            DbHandle::Live(self.db.clone()),
            self.resources.clone(),
            keys,
            read,
            false,
        )
        .await
    }
}

async fn read_many(
    db: DbHandle,
    resources: WorkerStateResources,
    keys: &[StateKey],
    read: ReadOptions,
    fail_fast: bool,
) -> Result<Vec<Option<Vec<u8>>>> {
    let mut request_bytes = keys
        .len()
        .saturating_mul(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>());
    for key in keys {
        request_bytes =
            request_bytes.saturating_add(encoding::encoded_key_size(key)?.saturating_mul(2));
    }
    let bytes = read.max_bytes.saturating_add(request_bytes);
    let permit = if fail_fast {
        resources.try_decoded_value(bytes)
    } else {
        resources.decoded_value(bytes).await
    }?;
    let keys = keys.iter().map(encode_key).collect::<Result<Vec<_>>>()?;
    let task_resources = resources.clone();
    resources
        .run_blocking(move || {
            let _latency = task_resources.operation_timer("read");
            let _permit = permit;
            // Live multi-get takes one local sequence for the entire request;
            // stable readers retain the sequence selected when the view began.
            let live_snapshot = match &db {
                DbHandle::Live(db) => Some(db.snapshot()),
                DbHandle::Snapshot(_) => None,
            };
            let snapshot = match &db {
                DbHandle::Live(_) => live_snapshot.as_ref().expect("live read snapshot"),
                DbHandle::Snapshot(view) => view.snapshot.borrow_dependent(),
            };
            let mut native_read = rocksdb::ReadOptions::default();
            native_read.set_snapshot(snapshot);
            let mut used = 0usize;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                // Pin native storage first so the limit is enforced before copying
                // a potentially oversized value into an owned result.
                let value = db.get_pinned_opt(key, &native_read).map_err(backend)?;
                if let Some(value) = value {
                    // Check the version without allocating, then bound decoded bytes.
                    if value.first() != Some(&1) {
                        return Err(LiveStateError::InvalidEncoding(
                            "unsupported value version".into(),
                        ));
                    }
                    enforce_read_limit(used.saturating_add(value.len() - 1), read)?;
                    let value = decode_value(&value)?;
                    used = used.saturating_add(value.len());
                    values.push(Some(value));
                } else {
                    values.push(None);
                }
            }
            Ok(values)
        })
        .await
        .map_err(LiveStateError::from)?
}

#[async_trait]
impl LiveStateBackend for RocksLiveState {
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(read_many(
            DbHandle::Live(self.db.clone()),
            self.resources.clone(),
            std::slice::from_ref(key),
            options,
            true,
        )
        .await?
        .pop()
        .flatten())
    }
    fn try_admit_write(
        &self,
        bytes: usize,
        operations: usize,
    ) -> Result<super::write::AdmittedWriteBatch> {
        super::write::AdmittedWriteBatch::try_reserve(self.resources.clone(), bytes, operations)
    }
    async fn close(self: Arc<Self>) -> Result<()> {
        let backend = Arc::try_unwrap(self)
            .map_err(|_| LiveStateError::Backend("live backend still has active handles".into()))?;
        RocksLiveState::close(backend).await
    }
    async fn admit_write(
        &self,
        max_bytes: usize,
        max_operations: usize,
    ) -> Result<super::write::AdmittedWriteBatch> {
        self.admitted_batch(max_bytes, max_operations).await
    }
    async fn write_admitted(&self, batch: super::write::AdmittedWriteBatch) -> Result<()> {
        RocksLiveState::write_admitted(self, batch).await
    }
    async fn get(&self, key: &StateKey, read: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(self
            .read_many(std::slice::from_ref(key), read)
            .await?
            .pop()
            .flatten())
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        read: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.read_many(keys, read).await
    }
    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        batch.validate()?;
        let bytes = batch.encoded_size()?;
        // Caller buffers, temporary encoding, and native WriteBatch coexist.
        let admitted_bytes = bytes
            .saturating_mul(5)
            .saturating_add(batch.operations.len().saturating_mul(32));
        let permit = self
            .resources
            .queued_write(admitted_bytes)
            .await
            .map_err(LiveStateError::from)?;
        self.write_with_permit(batch, permit).await
    }

    async fn snapshot(&self) -> Result<StateSnapshot> {
        let permit = self
            .resources
            .snapshot()
            .await
            .map_err(LiveStateError::from)?;
        let id = super::memory::next_snapshot_id();
        let db = self.db.clone();
        let resources = self.resources.clone();
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("snapshot");
                let snapshot = OwnedNativeSnapshot::new(db, |owner| owner.snapshot());
                Ok(StateSnapshot(Arc::new(RocksSnapshot {
                    view: Arc::new(NativeReadView {
                        snapshot,
                        _permit: permit,
                    }),
                    id,
                    resources,
                })))
            })
            .await
            .map_err(LiveStateError::from)?
    }
}

enum DbHandle {
    Live(Arc<NativeDb>),
    Snapshot(Arc<NativeReadView>),
}
impl std::ops::Deref for DbHandle {
    type Target = DB;
    fn deref(&self) -> &DB {
        match self {
            Self::Live(db) => db,
            Self::Snapshot(view) => view.snapshot.borrow_owner(),
        }
    }
}
struct NativeDb {
    db: Option<DB>,
    write_lock: Mutex<()>,
    health: Option<Arc<health::BackendHealth>>,
    path: PathBuf,
    remove: AtomicBool,
    permit: Option<ResourcePermit>,
    resources: Option<WorkerStateResources>,
    cleanup: Option<CleanupPermit>,
    finished: Option<oneshot::Sender<Result<()>>>,
}
impl std::ops::Deref for NativeDb {
    type Target = DB;
    fn deref(&self) -> &DB {
        self.db.as_ref().expect("live native database")
    }
}
impl Drop for NativeDb {
    fn drop(&mut self) {
        let db = self.db.take();
        let path = self.path.clone();
        let remove = self.remove.load(Ordering::Relaxed);
        let permit = self.permit.take();
        let resources = self.resources.take();
        let finished = self.finished.take();
        // Every native owner reserves queue capacity before creation, so Drop
        // submits without blocking a reactor or starting unbounded threads.
        self.cleanup
            .take()
            .expect("reserved native cleanup")
            .submit(move || {
                drop(db);
                let result = if remove {
                    std::fs::remove_dir_all(path).map_err(lifecycle::io_error)
                } else {
                    Ok(())
                };
                drop(permit);
                drop(resources);
                if let Some(finished) = finished {
                    if let Err(Err(error)) = finished.send(result) {
                        tracing::error!(%error, "live-state cleanup failed after close cancellation");
                    }
                } else if let Err(error) = result {
                    tracing::error!(%error, "live-state database cleanup failed");
                }
            });
    }
}
type NativeSnapshot<'a> = rocksdb::Snapshot<'a>;

self_cell::self_cell!(
    struct OwnedNativeSnapshot {
        owner: Arc<NativeDb>,
        #[covariant]
        dependent: NativeSnapshot,
    }
);

/// The permit follows every blocking reader, including cancelled futures.
/// self_cell drops the native snapshot before releasing its database owner.
struct NativeReadView {
    snapshot: OwnedNativeSnapshot,
    _permit: ResourcePermit,
}

struct RocksSnapshot {
    view: Arc<NativeReadView>,
    id: u64,
    resources: WorkerStateResources,
}

#[async_trait]
impl SnapshotReader for RocksSnapshot {
    async fn get(&self, key: &StateKey, read: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(self
            .multi_get(std::slice::from_ref(key), read)
            .await?
            .pop()
            .flatten())
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        read: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        read_many(
            DbHandle::Snapshot(self.view.clone()),
            self.resources.clone(),
            keys,
            read,
            false,
        )
        .await
    }
    async fn try_get(&self, key: &StateKey, read: ReadOptions) -> Result<Option<Vec<u8>>> {
        Ok(read_many(
            DbHandle::Snapshot(self.view.clone()),
            self.resources.clone(),
            std::slice::from_ref(key),
            read,
            true,
        )
        .await?
        .pop()
        .flatten())
    }
    async fn try_scan(&self, request: ScanRequest) -> Result<ScanPage> {
        self.scan_admission(request, true).await
    }
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        self.scan_admission(request, false).await
    }
}
// Exclusive ceiling of an encoded prefix, including prefixes ending in 0xff.
fn encoded_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut end = prefix[..=last].to_vec();
    end[last] += 1;
    Some(end)
}

fn set_scan_upper_bound(
    read: &mut rocksdb::ReadOptions,
    namespace: &[u8],
    prefix: Option<&[u8]>,
    end: Option<&[u8]>,
) {
    // An encoded prefix includes its namespace, so its successor is already
    // no greater than the namespace successor. Allocate only that candidate.
    let prefix = prefix.unwrap_or(namespace);
    debug_assert!(prefix.starts_with(namespace));
    let mut upper = encoded_prefix_end(prefix);
    if let Some(end) = end
        && upper
            .as_ref()
            .is_none_or(|current| end < current.as_slice())
    {
        upper = Some(end.to_vec());
    }
    if let Some(upper) = upper {
        read.set_iterate_upper_bound(upper);
    }
}

impl RocksSnapshot {
    async fn scan_admission(&self, request: ScanRequest, fail_fast: bool) -> Result<ScanPage> {
        request.validate(self.id)?;
        let id = self.id;
        let db = DbHandle::Snapshot(self.view.clone());
        let namespace_bytes = encoding::encoded_namespace_size(&request.range.namespace)?;
        let request_bytes = namespace_bytes.saturating_mul(5).saturating_add(
            request
                .range
                .start
                .as_ref()
                .map_or(0, Vec::len)
                .saturating_add(request.range.end.as_ref().map_or(0, Vec::len))
                .saturating_add(request.range.prefix.as_ref().map_or(0, Vec::len))
                .saturating_add(
                    request
                        .cursor
                        .as_ref()
                        .map_or(0, |cursor| cursor.last_key.len()),
                ),
        );
        let container_bytes = request
            .max_entries
            .min(request.max_bytes / namespace_bytes.saturating_add(3))
            .saturating_mul(std::mem::size_of::<ScanEntry>())
            .saturating_mul(2);
        let bytes = request
            .max_bytes
            .saturating_mul(4)
            .saturating_add(request_bytes.saturating_mul(4))
            .saturating_add(container_bytes);
        let permit = if fail_fast {
            self.resources.try_scan_page(bytes)
        } else {
            self.resources.scan_page(bytes).await
        }?;
        let resources = self.resources.clone();
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("scan");
                let namespace = encoding::encode_namespace(&request.range.namespace)?;
                let bound = |logical: &[u8]| {
                    let mut encoded = namespace.clone();
                    for byte in logical {
                        if *byte == 0 {
                            encoded.extend_from_slice(&[0, 255]);
                        } else {
                            encoded.push(*byte);
                        }
                    }
                    encoded
                };
                let prefix = request.range.prefix.as_ref().map(|prefix| bound(prefix));
                let end = request.range.end.as_ref().map(|end| bound(end));
                let start = bound(request.range.start.as_deref().unwrap_or_default());
                // Borrow the greatest lower bound rather than cloning an
                // escaped prefix/cursor. That spare copy covers the owned
                // native upper bound within the existing request admission:
                // start/prefix/end expand at most 2x, plus at most one extra
                // prefix and end while choosing the upper bound (4x total).
                let mut lower = start.as_slice();
                if let Some(prefix) = &prefix
                    && prefix.as_slice() > lower
                {
                    lower = prefix;
                }
                if let Some(cursor) = &request.cursor
                    && cursor.last_key.as_slice() > lower
                {
                    lower = &cursor.last_key;
                }
                let mut native_read = rocksdb::ReadOptions::default();
                let DbHandle::Snapshot(view) = &db else {
                    unreachable!("stable scan requires a snapshot view")
                };
                native_read.set_snapshot(view.snapshot.borrow_dependent());
                // Rust checks below define the same range, but run only after
                // native Seek/Next. Bound those operations too, so an empty
                // prefix cannot walk unrelated deleted keys to find a live row.
                set_scan_upper_bound(
                    &mut native_read,
                    &namespace,
                    prefix.as_deref(),
                    end.as_deref(),
                );
                let mut iterator = db.raw_iterator_opt(native_read);
                iterator.seek(lower);
                let mut first = true;
                let mut entries = Vec::new();
                let mut used = 0usize;
                let mut next_cursor = None;
                loop {
                    if !first {
                        iterator.next();
                    }
                    first = false;
                    if !iterator.valid() {
                        break;
                    }
                    let encoded_key = iterator.key().expect("valid iterator key");
                    let encoded_value = iterator.value().expect("valid iterator value");
                    if !encoded_key.starts_with(&namespace) {
                        break;
                    }
                    if prefix
                        .as_ref()
                        .is_some_and(|prefix| !encoded_key.starts_with(prefix))
                    {
                        break;
                    }
                    if end
                        .as_ref()
                        .is_some_and(|end| encoded_key >= end.as_slice())
                    {
                        break;
                    }
                    if request
                        .cursor
                        .as_ref()
                        .is_some_and(|c| encoded_key <= c.last_key.as_slice())
                    {
                        continue;
                    }
                    let size = encoded_key.len().saturating_add(encoded_value.len());
                    if size > request.max_bytes && entries.is_empty() {
                        return Err(LiveStateError::ReadLimitExceeded {
                            required: size,
                            limit: request.max_bytes,
                        });
                    }
                    if entries.len() >= request.max_entries
                        || used.saturating_add(size) > request.max_bytes
                    {
                        next_cursor = entries.last().map(|entry: &ScanEntry| ScanCursor {
                            snapshot_id: id,
                            range: request.range.clone(),
                            last_key: encode_key(&entry.key).expect("validated key"),
                        });
                        break;
                    }
                    // Native key/value lengths are checked before allocating either decode.
                    let key = decode_key(encoded_key)?;
                    if key.namespace != request.range.namespace {
                        break;
                    }
                    used += size;
                    entries.push(ScanEntry {
                        key,
                        value: decode_value(encoded_value)?,
                    });
                }
                iterator.status().map_err(backend)?;
                Ok(ScanPage {
                    entries,
                    next_cursor,
                    _reservation: Some(permit),
                })
            })
            .await
            .map_err(LiveStateError::from)?
    }
}

#[cfg(test)]
mod tests {
    use super::super::resources::ResourceConfig;
    use super::*;

    fn resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 * 1024 * 1024,
            memtable_bytes: 2 * 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 8,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }
    fn health_value(registry: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
        registry
            .gather()
            .iter()
            .find(|family| family.name() == name)
            .unwrap()
            .get_metric()
            .iter()
            .find(|metric| {
                labels.iter().all(|(name, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == *name && label.value() == *value)
                })
            })
            .unwrap()
            .get_gauge()
            .as_ref()
            .unwrap()
            .value()
    }

    async fn exercise_logical_accounting(
        state: &dyn LiveStateBackend,
        registry: &prometheus::Registry,
        backend: &str,
    ) {
        let assert_size = |keys, key_bytes, value_bytes| {
            assert_eq!(
                health_value(
                    registry,
                    "arroyo_live_state_logical_keys",
                    &[("backend", backend)]
                ),
                keys
            );
            for (part, expected) in [("key", key_bytes), ("value", value_bytes)] {
                assert_eq!(
                    health_value(
                        registry,
                        "arroyo_live_state_logical_bytes",
                        &[("backend", backend), ("part", part)]
                    ),
                    expected
                );
            }
        };
        let key_bytes = key().key.len() as f64;
        assert_size(0.0, 0.0, 0.0);
        state.put(key(), vec![1; 7], 1024).await.unwrap();
        assert_size(1.0, key_bytes, 7.0);
        state.put(key(), vec![2; 3], 1024).await.unwrap();
        assert_size(1.0, key_bytes, 3.0);
        state
            .write_batch(WriteBatch {
                operations: vec![
                    WriteOperation::Put {
                        key: key(),
                        value: vec![3; 11],
                    },
                    WriteOperation::Delete { key: key() },
                    WriteOperation::Put {
                        key: key(),
                        value: vec![4; 5],
                    },
                ],
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_size(1.0, key_bytes, 5.0);
        assert!(
            state
                .write_batch(WriteBatch {
                    operations: vec![WriteOperation::Put {
                        key: key(),
                        value: vec![5; 8]
                    }],
                    max_bytes: 1
                })
                .await
                .is_err()
        );
        assert_size(1.0, key_bytes, 5.0);
        state
            .write_batch(WriteBatch {
                operations: vec![
                    WriteOperation::Delete { key: key() },
                    WriteOperation::Delete { key: key() },
                ],
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_size(0.0, 0.0, 0.0);
        state
            .write_batch(WriteBatch {
                operations: vec![WriteOperation::Delete { key: key() }],
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_size(0.0, 0.0, 0.0);
    }

    #[tokio::test]
    async fn health_accounting_matches_atomic_memory_and_rocks_batches() {
        for backend in ["memory", "rocksdb"] {
            let resources = resources();
            let registry = prometheus::Registry::new();
            resources.register_metrics(&registry).unwrap();
            if backend == "memory" {
                let state = memory::MemoryLiveState::bounded(resources, 1024).unwrap();
                exercise_logical_accounting(&state, &registry, backend).await;
                state.put(key(), vec![1; 4], 1024).await.unwrap();
                drop(state);
            } else {
                let config = config();
                let state = RocksLiveState::open(config.clone(), resources.clone())
                    .await
                    .unwrap();
                exercise_logical_accounting(&state, &registry, backend).await;
                state.put(key(), vec![1; 4], 1024).await.unwrap();
                // WAL-backed logical payloads are independent of SST files.
                assert_eq!(
                    health_value(
                        &registry,
                        "arroyo_live_state_health_observations",
                        &[("measurement", "sst_bytes"), ("outcome", "available")],
                    ),
                    1.0
                );
                assert_eq!(
                    health_value(
                        &registry,
                        "arroyo_live_state_native_health",
                        &[("measurement", "sst_bytes")]
                    ),
                    0.0
                );
                assert!(
                    health_value(
                        &registry,
                        "arroyo_live_state_native_health",
                        &[("measurement", "free_bytes")]
                    ) > 0.0
                );
                let weak = Arc::downgrade(state.db.health.as_ref().unwrap());
                let snapshot = state.snapshot().await.unwrap();
                assert_eq!(
                    health_value(
                        &registry,
                        "arroyo_live_state_logical_keys",
                        &[("backend", backend)]
                    ),
                    1.0
                );
                drop(snapshot);
                state.close().await.unwrap();
                assert!(weak.upgrade().is_none());
                // Reopen deliberately does not invent a baseline by scanning
                // an arbitrarily large existing database during collection.
                let state = RocksLiveState::reopen(config.clone(), resources.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    health_value(
                        &registry,
                        "arroyo_live_state_health_observations",
                        &[("measurement", "logical"), ("outcome", "unavailable")]
                    ),
                    1.0
                );
                state.put(key(), vec![2; 2], 1024).await.unwrap();
                assert_eq!(
                    health_value(
                        &registry,
                        "arroyo_live_state_health_observations",
                        &[("measurement", "logical"), ("outcome", "unavailable")]
                    ),
                    1.0
                );
                state.close_and_remove().await.unwrap();
                drain_cleanup(&resources).await;
                std::fs::remove_dir_all(config.root).unwrap();
            }
            assert_eq!(
                health_value(
                    &registry,
                    "arroyo_live_state_logical_keys",
                    &[("backend", backend)]
                ),
                0.0
            );
            assert_eq!(
                health_value(
                    &registry,
                    "arroyo_live_state_health_observations",
                    &[("measurement", "logical"), ("outcome", "available")]
                ),
                0.0
            );
        }
    }

    async fn drain_cleanup(resources: &WorkerStateResources) {
        let (tx, rx) = oneshot::channel();
        resources.cleanup().await.unwrap().submit(move || {
            let _ = tx.send(());
        });
        rx.await.unwrap();
    }
    fn config() -> RocksStateConfig {
        RocksStateConfig {
            root: std::env::temp_dir().join(format!(
                "arroyo-live-test-{}-{}",
                std::process::id(),
                super::super::memory::next_snapshot_id()
            )),
            job_id: "job/../one".into(),
            operator_id: "operator".into(),
            subtask: 0,
            generation: 1,
            attempt: 1,
        }
    }
    fn key() -> StateKey {
        StateKey {
            namespace: StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: b"table".to_vec(),
            },
            key: b"key".to_vec(),
            routing_hash: None,
        }
    }
    #[tokio::test]
    async fn decoded_scan_page_retains_admission_through_waiter_cancellation_and_drop() {
        let config = config();
        let resources = resources();
        let registry = prometheus::Registry::new();
        resources.register_metrics(&registry).unwrap();
        let usage = |measurement: &str| {
            registry
                .gather()
                .into_iter()
                .find(|family| family.name() == "arroyo_live_state_resources")
                .unwrap()
                .get_metric()
                .iter()
                .find(|metric| {
                    metric.get_label().iter().any(|label| {
                        label.name() == "resource" && label.value() == "scan_page_bytes"
                    }) && metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "measurement" && label.value() == measurement)
                })
                .unwrap()
                .get_gauge()
                .as_ref()
                .unwrap()
                .value() as usize
        };
        let state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        state
            .put(key(), b"decoded-page".to_vec(), 1024)
            .await
            .unwrap();
        let snapshot = state.snapshot().await.unwrap();
        let request = ScanRequest {
            range: ScanRange {
                namespace: key().namespace,
                prefix: None,
                start: None,
                end: None,
            },
            max_entries: 1,
            max_bytes: 1024,
            cursor: None,
        };
        let page = snapshot.scan(request.clone()).await.unwrap();
        assert_eq!(page.entries[0].value, b"decoded-page");
        let retained = usage("used");
        assert!(retained >= 4 * request.max_bytes);
        let remainder = resources
            .try_scan_page(resources.config().scan_page_bytes - retained)
            .unwrap();
        assert!(matches!(
            snapshot.try_scan(request.clone()).await,
            Err(LiveStateError::Resource(
                super::super::resources::ResourceError::ResourceExhausted {
                    resource: "scan_page_bytes",
                    ..
                }
            ))
        ));
        {
            let mut waiting = Box::pin(snapshot.scan(request.clone()));
            assert!(futures::poll!(&mut waiting).is_pending());
            assert_eq!(usage("waiting"), 1);
            assert_eq!(usage("used"), resources.config().scan_page_bytes);
        }
        assert_eq!(usage("waiting"), 0);
        assert_eq!(usage("used"), resources.config().scan_page_bytes);
        drop(remainder);
        assert_eq!(usage("used"), retained);
        // The returned data outlives its snapshot while retaining its admission.
        drop(snapshot);
        assert_eq!(page.entries[0].value, b"decoded-page");
        assert_eq!(usage("used"), retained);
        drop(page);
        assert_eq!(usage("used"), 0);
        let snapshot = state.snapshot().await.unwrap();
        let mut oversized = request.clone();
        oversized.max_bytes = resources.config().scan_page_bytes;
        assert!(matches!(
            snapshot.scan(oversized).await,
            Err(LiveStateError::Resource(
                super::super::resources::ResourceError::RequestTooLarge {
                    resource: "scan_page_bytes",
                    ..
                }
            ))
        ));
        assert_eq!(usage("used"), 0);
        let retried = snapshot.scan(request).await.unwrap();
        assert_eq!(usage("used"), retained);
        drop(retried);
        assert_eq!(usage("used"), 0);
        drop(snapshot);
        state.close_and_remove().await.unwrap();
        drain_cleanup(&resources).await;
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn worker_database_open_rejects_saturation_before_native_preparation() {
        let mut limits = resources().config().clone();
        limits.max_open_databases = 1;
        let resources = WorkerStateResources::new(limits).unwrap();
        let first_config = config();
        let first =
            RocksLiveState::open_worker_with_resources(first_config.clone(), resources.clone())
                .await
                .unwrap();
        let second_config = config();
        let rejected =
            RocksLiveState::open_worker_with_resources(second_config.clone(), resources.clone());
        tokio::pin!(rejected);
        assert!(matches!(
            futures::poll!(&mut rejected),
            std::task::Poll::Ready(Err(LiveStateError::Resource(
                super::super::resources::ResourceError::ResourceExhausted {
                    resource: "databases",
                    limit: 1,
                }
            )))
        ));
        assert!(!second_config.root.exists());

        let diagnostic = RocksLiveState::open(second_config.clone(), resources.clone());
        tokio::pin!(diagnostic);
        assert!(futures::poll!(&mut diagnostic).is_pending());
        first.close_and_remove().await.unwrap();
        let second = diagnostic.await.unwrap();
        second.close_and_remove().await.unwrap();
        std::fs::remove_dir_all(first_config.root).unwrap();
        std::fs::remove_dir_all(second_config.root).unwrap();
    }

    #[tokio::test]
    async fn worker_checkpoint_captures_unlogged_memtable_updates_and_deletes() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open_worker_with_resources(config.clone(), resources.clone())
            .await
            .unwrap();
        let mut deleted = key();
        deleted.key = b"deleted".to_vec();
        state.put(key(), b"old".to_vec(), 1024).await.unwrap();
        state
            .put(deleted.clone(), b"old".to_vec(), 1024)
            .await
            .unwrap();
        // Establish older SST values, then leave the changes entirely in RAM.
        state.db.flush().unwrap();
        state
            .put(key(), b"checkpoint".to_vec(), 1024)
            .await
            .unwrap();
        state.delete(deleted.clone(), 1024).await.unwrap();
        assert!(
            state
                .db
                .property_int_value("rocksdb.num-entries-active-mem-table")
                .unwrap()
                .unwrap()
                >= 2
        );
        let wal_bytes = || {
            std::fs::read_dir(state.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
                .map(|path| std::fs::metadata(path).unwrap().len())
                .sum::<u64>()
        };
        assert_eq!(
            wal_bytes(),
            0,
            "worker mutations must not append WAL records"
        );
        let snapshot = state.snapshot().await.unwrap();
        state.put(key(), b"newer".to_vec(), 1024).await.unwrap();
        state
            .put(deleted.clone(), b"newer".to_vec(), 1024)
            .await
            .unwrap();
        let mut closing = Box::pin(state.close_and_remove());
        assert!(futures::poll!(&mut closing).is_pending());
        let read = ReadOptions { max_bytes: 1024 };
        assert_eq!(
            snapshot.get(&key(), read).await.unwrap(),
            Some(b"checkpoint".to_vec())
        );
        assert_eq!(snapshot.get(&deleted, read).await.unwrap(), None);
        drop(snapshot);
        closing.await.unwrap();
        drain_cleanup(&resources).await;
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn worker_writes_are_visible_and_snapshots_survive_without_per_write_sync() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open_worker_with_resources(config.clone(), resources.clone())
            .await
            .unwrap();
        assert_eq!(state.durability, WriteDurability::Checkpoint);
        // Covers atomic batch rejection, operation ordering, immediate reads,
        // namespace isolation, and snapshot pagination on the worker write mode.
        super::super::tests::backend_contract(&state).await;
        state.put(key(), b"committed".to_vec(), 1024).await.unwrap();
        let snapshot = state.snapshot().await.unwrap();
        state.put(key(), b"newer".to_vec(), 1024).await.unwrap();
        assert_eq!(
            state
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"newer".to_vec())
        );
        let mut closing = Box::pin(state.close_and_remove());
        assert!(futures::poll!(&mut closing).is_pending());
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"committed".to_vec())
        );
        drop(snapshot);
        closing.await.unwrap();
        drain_cleanup(&resources).await;
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn rocks_conforms_and_reopens_explicitly() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        assert_eq!(state.durability, WriteDurability::SynchronousWal);
        super::super::tests::backend_contract(&state).await;
        state.put(key(), b"persisted".to_vec(), 1024).await.unwrap();
        assert!(
            RocksLiveState::open(config.clone(), resources.clone())
                .await
                .is_err()
        );
        let snapshot = state.snapshot().await.unwrap();
        let mut closing = Box::pin(state.close());
        assert!(futures::poll!(&mut closing).is_pending());
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"persisted".to_vec())
        );
        drop(snapshot);
        closing.await.unwrap();
        let reopened = RocksLiveState::reopen(config.clone(), resources.clone())
            .await
            .unwrap();
        assert_eq!(reopened.durability, WriteDurability::SynchronousWal);
        assert_eq!(
            reopened
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"persisted".to_vec())
        );
        let snapshot = reopened.snapshot().await.unwrap();
        reopened
            .put(key(), b"changed".to_vec(), 1024)
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"persisted".to_vec())
        );
        drop(snapshot);
        reopened.close().await.unwrap();
        drain_cleanup(&resources).await;
        std::fs::remove_dir_all(config.root).unwrap();
    }
    #[tokio::test]
    async fn operators_are_independent_and_corruption_is_visible() {
        let config = config();
        let mut other = config.clone();
        other.operator_id = "other".into();
        let resources = resources();
        let (left, right) = tokio::join!(
            RocksLiveState::open(config.clone(), resources.clone()),
            RocksLiveState::open(other.clone(), resources.clone())
        );
        let left = left.unwrap();
        let right = right.unwrap();
        let (a, b) = tokio::join!(
            left.put(key(), b"left".to_vec(), 1024),
            right.put(key(), b"right".to_vec(), 1024)
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(
            left.get(&key(), ReadOptions { max_bytes: 10 })
                .await
                .unwrap(),
            Some(b"left".to_vec())
        );
        assert_eq!(
            right
                .get(&key(), ReadOptions { max_bytes: 10 })
                .await
                .unwrap(),
            Some(b"right".to_vec())
        );
        left.db
            .put(encode_key(&key()).unwrap(), b"\xffinvalid")
            .unwrap();
        assert!(matches!(
            left.get(&key(), ReadOptions { max_bytes: 100 }).await,
            Err(LiveStateError::InvalidEncoding(_))
        ));
        left.close().await.unwrap();
        right.close().await.unwrap();
        std::fs::write(config.path().join("CURRENT"), b"broken manifest\n").unwrap();
        assert!(
            RocksLiveState::reopen(config.clone(), resources.clone())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(config.root).unwrap();
    }
    #[tokio::test]
    async fn native_scan_bounds_preserve_encoded_ranges_and_pages() {
        let config = config();
        let state = RocksLiveState::open_worker_with_resources(config.clone(), resources())
            .await
            .unwrap();
        let logical = |bytes: &[u8]| {
            let mut k = key();
            k.key = bytes.to_vec();
            k
        };
        let keys: Vec<Vec<u8>> = [
            b"".as_slice(),
            b"\0",
            b"\0\0",
            b"\0\xff",
            b"a",
            b"a\0",
            b"a\xff",
            b"b",
            b"\xff",
            b"\xff\0",
            b"\xff\xff",
        ]
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect();
        for k in &keys {
            state.put(logical(k), b"old".to_vec(), 1024).await.unwrap();
        }
        let mut other = logical(b"a");
        other.namespace.table = b"other".to_vec();
        state
            .put(other.clone(), b"outside".to_vec(), 1024)
            .await
            .unwrap();
        let snapshot = state.snapshot().await.unwrap();
        state.delete(logical(b"a"), 1024).await.unwrap();
        state
            .put(logical(b"a!"), b"new".to_vec(), 1024)
            .await
            .unwrap();
        let ranges = [
            (None, None, None),
            (Some(b"".to_vec()), None, None),
            (Some(b"a".to_vec()), None, None),
            (
                Some(b"a".to_vec()),
                Some(b"a\0".to_vec()),
                Some(b"a\xff".to_vec()),
            ),
            (Some(b"a".to_vec()), None, Some(b"a\0".to_vec())),
            (Some(b"a".to_vec()), Some(b"b".to_vec()), None),
            (Some(b"a".to_vec()), None, Some(b"\0".to_vec())),
            (Some(b"\0".to_vec()), None, Some(b"\0\xff".to_vec())),
            (Some(b"\xff".to_vec()), None, None),
            (Some(b"\xff\xff".to_vec()), None, None),
            (None, Some(b"a".to_vec()), Some(b"a".to_vec())),
        ];
        for (prefix, start, end) in ranges {
            let expected: Vec<_> = keys
                .iter()
                .filter(|k| {
                    prefix.as_ref().is_none_or(|p| k.starts_with(p))
                        && start.as_ref().is_none_or(|s| *k >= s)
                        && end.as_ref().is_none_or(|e| *k < e)
                })
                .cloned()
                .collect();
            let request = ScanRequest {
                range: ScanRange {
                    namespace: key().namespace,
                    prefix,
                    start,
                    end,
                },
                max_entries: 2,
                max_bytes: 1024,
                cursor: None,
            };
            let one_entry_bytes = keys
                .iter()
                .map(|k| encoding::encoded_key_size(&logical(k)).unwrap() + 4)
                .max()
                .unwrap();
            for (max_entries, max_bytes) in [(2, 1024), (64, one_entry_bytes)] {
                for fail_fast in [false, true] {
                    let mut cursor = None;
                    let mut actual = Vec::new();
                    loop {
                        let next = ScanRequest {
                            cursor,
                            max_entries,
                            max_bytes,
                            ..request.clone()
                        };
                        let page = if fail_fast {
                            snapshot.try_scan(next).await
                        } else {
                            snapshot.scan(next).await
                        }
                        .unwrap();
                        for entry in &page.entries {
                            assert_eq!(entry.value, b"old");
                            actual.push(entry.key.key.clone());
                        }
                        cursor = page.next_cursor.clone();
                        if cursor.is_none() {
                            break;
                        }
                    }
                    assert_eq!(actual, expected);
                }
            }
        }
        let request = ScanRequest {
            range: ScanRange {
                namespace: key().namespace,
                prefix: None,
                start: Some(b"b".to_vec()),
                end: Some(b"a".to_vec()),
            },
            max_entries: 1,
            max_bytes: 1024,
            cursor: None,
        };
        assert!(matches!(
            snapshot.try_scan(request.clone()).await,
            Err(LiveStateError::InvalidCursor)
        ));
        let small = ScanRequest {
            range: ScanRange {
                start: None,
                end: None,
                ..request.range
            },
            max_bytes: 1,
            ..request
        };
        assert!(matches!(
            snapshot.try_scan(small).await,
            Err(LiveStateError::ReadLimitExceeded { .. })
        ));
        let other_page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: other.namespace,
                    prefix: None,
                    start: None,
                    end: None,
                },
                max_entries: 8,
                max_bytes: 1024,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(other_page.entries.len(), 1);
        assert_eq!(other_page.entries[0].value, b"outside");
        drop(other_page);
        drop(snapshot);
        state.close_and_remove().await.unwrap();
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[test]
    fn native_scan_upper_bound_skips_no_unrelated_tombstones() {
        use rocksdb::perf::{PerfContext, PerfMetric, PerfStatsLevel, set_perf_stats};
        let config = config();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_disable_auto_compactions(true);
        let db = DB::open(&options, &config.root).unwrap();
        let logical = |bytes: &[u8]| {
            let mut k = key();
            k.key = bytes.to_vec();
            k
        };
        let namespace = encoding::encode_namespace(&key().namespace).unwrap();
        let deleted: Vec<_> = (0..1000)
            .map(|i| encode_key(&logical(format!("D/{i:06}").as_bytes())).unwrap())
            .collect();
        let live = encode_key(&logical(b"G/live")).unwrap();
        let mut initial = rocksdb::WriteBatch::default();
        for k in &deleted {
            initial.put(k, b"v");
        }
        initial.put(&live, b"g");
        db.write(initial).unwrap();
        db.flush().unwrap();
        let before = db.snapshot();
        let mut removed = rocksdb::WriteBatch::default();
        for k in &deleted {
            removed.delete(k);
        }
        db.write(removed).unwrap();
        db.flush().unwrap();
        let after = db.snapshot();
        assert!(before.get(&deleted[0]).unwrap().is_some());
        assert!(after.get(&deleted[0]).unwrap().is_none());
        let mut prefix = namespace.clone();
        prefix.extend_from_slice(b"B/");
        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        for bounded in [false, true] {
            let mut read = rocksdb::ReadOptions::default();
            read.set_snapshot(&after);
            if bounded {
                set_scan_upper_bound(&mut read, &namespace, Some(&prefix), None);
            }
            let mut iterator = db.raw_iterator_opt(read);
            perf.reset();
            iterator.seek(&prefix);
            iterator.status().unwrap();
            let skipped = perf.metric(PerfMetric::InternalDeleteSkippedCount);
            if bounded {
                assert_eq!(
                    skipped, 0,
                    "empty prefix must not traverse unrelated D deletions"
                );
                assert!(!iterator.valid());
            } else {
                assert_eq!(skipped, deleted.len() as u64);
                assert_eq!(iterator.key(), Some(live.as_slice()));
            }
        }
        set_perf_stats(PerfStatsLevel::Disable);
        drop(after);
        drop(before);
        drop(db);
        std::fs::remove_dir_all(config.root).unwrap();
    }

    /// Finite traversal diagnostic. Native options deliberately retain deleted
    /// versions; this is not SQL throughput or production resource qualification.
    #[test]
    #[ignore = "opt-in STR-114 bounded native traversal diagnostic"]
    fn native_scan_churn_diagnostic() {
        use rocksdb::perf::{PerfContext, PerfMetric, PerfStatsLevel, set_perf_stats};
        const ROWS: usize = 600;
        for repeat in 0usize..3 {
            for (key_bytes, value_bytes, page_entries) in [(16, 32, 1), (96, 1024, 16)] {
                let modes = if repeat.is_multiple_of(2) {
                    ["minimum", "stable", "advancing"]
                } else {
                    ["advancing", "stable", "minimum"]
                };
                for mode in modes {
                    let config = config();
                    let mut options = Options::default();
                    options.create_if_missing(true);
                    options.set_disable_auto_compactions(true);
                    let db = DB::open(&options, &config.root).unwrap();
                    let namespace = encoding::encode_namespace(&key().namespace).unwrap();
                    let logical = |family: u8, i: usize| {
                        let mut k = key();
                        k.key = vec![0; key_bytes];
                        k.key[0] = family;
                        k.key[1..9].copy_from_slice(&(i as u64).to_be_bytes());
                        encode_key(&k).unwrap()
                    };
                    let keys: Vec<_> = (0..ROWS).map(|i| logical(b'B', i)).collect();
                    let unrelated: Vec<_> = (0..ROWS).map(|i| logical(b'D', i)).collect();
                    let value = vec![7; value_bytes];
                    let mut initial = rocksdb::WriteBatch::default();
                    for k in keys.iter().chain(&unrelated) {
                        initial.put(k, &value);
                    }
                    db.write(initial).unwrap();
                    db.flush().unwrap();
                    // Keep pre-delete versions pinned in every arm. Stable
                    // continuation reads this view; fresh views see tombstones.
                    let pinned = db.snapshot();
                    let mut removed = rocksdb::WriteBatch::default();
                    for k in &unrelated {
                        removed.delete(k);
                    }
                    db.write(removed).unwrap();
                    db.flush().unwrap();
                    let mut prefix = namespace.clone();
                    prefix.push(b'B');
                    set_perf_stats(PerfStatsLevel::EnableTimeAndCPUTimeExceptForMutex);
                    let mut perf = PerfContext::default();
                    let mut completed = 0;
                    let mut issued_seeks = 0;
                    let mut after: Option<Vec<u8>> = None;
                    let mut started = std::time::Instant::now();
                    perf.reset();
                    while completed < ROWS {
                        let fresh = db.snapshot();
                        let mut read = rocksdb::ReadOptions::default();
                        read.set_snapshot(if mode == "stable" { &pinned } else { &fresh });
                        set_scan_upper_bound(&mut read, &namespace, Some(&prefix), None);
                        let mut iterator = db.raw_iterator_opt(read);
                        iterator.seek(after.as_deref().unwrap_or(&prefix));
                        issued_seeks += 1;
                        if after
                            .as_ref()
                            .is_some_and(|last| iterator.key() == Some(last.as_slice()))
                        {
                            iterator.next();
                        }
                        let mut page = rocksdb::WriteBatch::default();
                        let mut count = 0;
                        while iterator.valid()
                            && count < page_entries.min(ROWS / 3 - completed % (ROWS / 3))
                        {
                            let k = iterator.key().unwrap();
                            assert_eq!(k, keys[completed + count].as_slice());
                            assert_eq!(iterator.value().unwrap(), value.as_slice());
                            page.delete(k);
                            if mode != "minimum" {
                                after = Some(k.to_vec());
                            }
                            count += 1;
                            iterator.next();
                        }
                        iterator.status().unwrap();
                        assert!(count > 0);
                        drop(iterator);
                        drop(fresh);
                        db.write(page).unwrap();
                        completed += count;
                        if completed.is_multiple_of(ROWS / 3) {
                            println!(
                                "STR114 repeat={repeat} mode={mode} keys={key_bytes} values={value_bytes} page={page_entries} completed={completed} elapsed_us={} seeks={issued_seeks} child_seeks={} memtable_seeks={} internal_skips={} deleted_skips={} recent_skips={} block_reads={} block_bytes={} seek_ns={} next_ns={}",
                                started.elapsed().as_micros(),
                                perf.metric(PerfMetric::SeekChildSeekCount),
                                perf.metric(PerfMetric::SeekOnMemtableCount),
                                perf.metric(PerfMetric::InternalKeySkippedCount),
                                perf.metric(PerfMetric::InternalDeleteSkippedCount),
                                perf.metric(PerfMetric::InternalRecentSkippedCount),
                                perf.metric(PerfMetric::BlockReadCount),
                                perf.metric(PerfMetric::BlockReadByte),
                                perf.metric(PerfMetric::SeekInternalSeekTime),
                                perf.metric(PerfMetric::FindNextUserEntryTime)
                            );
                            issued_seeks = 0;
                            perf.reset();
                            started = std::time::Instant::now();
                        }
                    }
                    let final_view = db.snapshot();
                    for k in &keys {
                        assert!(final_view.get(k).unwrap().is_none());
                        assert_eq!(pinned.get(k).unwrap().unwrap(), value);
                    }
                    set_perf_stats(PerfStatsLevel::Disable);
                    drop(final_view);
                    drop(pinned);
                    drop(db);
                    std::fs::remove_dir_all(config.root).unwrap();
                }
            }
        }
    }

    /// Exercises the actual admitted/decode/page adapter with a stable read
    /// view, including deletion history and native upper-bound exhaustion.
    #[tokio::test]
    #[ignore = "opt-in STR-114 bounded admitted scan diagnostic"]
    async fn admitted_scan_churn_diagnostic() {
        for repeat in 0usize..3 {
            for (key_bytes, value_bytes, page_entries) in [(16, 32, 1), (96, 1024, 16)] {
                let config = config();
                let resources = WorkerStateResources::new(ResourceConfig {
                    block_cache_bytes: 1024 * 1024,
                    memtable_bytes: 1024 * 1024,
                    queued_write_bytes: 128 * 1024,
                    decoded_value_bytes: 64 * 1024,
                    scan_page_bytes: 128 * 1024,
                    max_blocking_operations: 1,
                    max_snapshots: 2,
                    max_open_databases: 1,
                    disk_reserve_bytes: 0,
                })
                .unwrap();
                let state =
                    RocksLiveState::open_worker_with_resources(config.clone(), resources.clone())
                        .await
                        .unwrap();
                let logical = |i: usize| {
                    let mut k = key();
                    k.key = vec![0; key_bytes];
                    k.key[0] = b'B';
                    k.key[1..9].copy_from_slice(&(i as u64).to_be_bytes());
                    k
                };
                let value = vec![7; value_bytes];
                for start in (0..600).step_by(8) {
                    let mut batch = WriteBatch {
                        operations: Vec::new(),
                        max_bytes: 128 * 1024,
                    };
                    for i in start..(start + 8).min(600) {
                        batch.operations.push(WriteOperation::Put {
                            key: logical(i),
                            value: value.clone(),
                        });
                    }
                    state.write_batch(batch).await.unwrap();
                }
                let view = state.snapshot().await.unwrap();
                let mut request = ScanRequest {
                    range: ScanRange {
                        namespace: key().namespace,
                        prefix: Some(vec![b'B']),
                        start: None,
                        end: None,
                    },
                    max_entries: page_entries,
                    max_bytes: page_entries * (2 * key_bytes + value_bytes + 128),
                    cursor: None,
                };
                let mut completed = 0;
                let mut pages = 0;
                let mut scan_us = 0;
                let mut write_us = 0;
                let process_work = || {
                    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
                    let fields: Vec<_> = stat
                        .rsplit_once(')')
                        .unwrap()
                        .1
                        .split_whitespace()
                        .collect();
                    let io = std::fs::read_to_string("/proc/self/io").unwrap();
                    let io_bytes = |name: &str| -> u64 {
                        io.lines()
                            .find_map(|line| line.strip_prefix(name))
                            .unwrap()
                            .trim()
                            .parse()
                            .unwrap()
                    };
                    (
                        fields[11].parse::<u64>().unwrap(),
                        fields[12].parse::<u64>().unwrap(),
                        io_bytes("read_bytes:"),
                        io_bytes("write_bytes:"),
                    )
                };
                let mut prior_work = process_work();
                let mut started = std::time::Instant::now();
                loop {
                    request.max_entries = page_entries.min(200 - completed % 200);
                    let scan_started = std::time::Instant::now();
                    let page = view.try_scan(request.clone()).await.unwrap();
                    scan_us += scan_started.elapsed().as_micros();
                    let mut batch = WriteBatch {
                        operations: Vec::new(),
                        max_bytes: 128 * 1024,
                    };
                    for (offset, entry) in page.entries.iter().enumerate() {
                        assert_eq!(entry.key, logical(completed + offset));
                        assert_eq!(entry.value, value);
                        batch.operations.push(WriteOperation::Delete {
                            key: entry.key.clone(),
                        });
                    }
                    completed += page.entries.len();
                    pages += 1;
                    request.cursor = page.next_cursor.clone();
                    drop(page); // release scan reservation before write
                    let write_started = std::time::Instant::now();
                    state.write_batch(batch).await.unwrap();
                    write_us += write_started.elapsed().as_micros();
                    if completed.is_multiple_of(200) {
                        let status = std::fs::read_to_string("/proc/self/status").unwrap();
                        let rss = status
                            .lines()
                            .find(|line| line.starts_with("VmRSS:"))
                            .unwrap();
                        let work = process_work();
                        println!(
                            "STR114_ADAPTER repeat={repeat} keys={key_bytes} values={value_bytes} page={page_entries} completed={completed} pages={pages} scan_us={scan_us} write_us={write_us} elapsed_us={} cpu_user_ticks={} cpu_system_ticks={} {rss} read_bytes={} write_bytes={}",
                            started.elapsed().as_micros(),
                            work.0 - prior_work.0,
                            work.1 - prior_work.1,
                            work.2 - prior_work.2,
                            work.3 - prior_work.3
                        );
                        prior_work = work;
                        pages = 0;
                        scan_us = 0;
                        write_us = 0;
                        started = std::time::Instant::now();
                    }
                    if request.cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(completed, 600);
                drop(view);
                let empty = state.snapshot().await.unwrap();
                request.cursor = None;
                assert!(empty.try_scan(request).await.unwrap().entries.is_empty());
                drop(empty);
                state.close_and_remove().await.unwrap();
                std::fs::remove_dir_all(config.root).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn native_snapshot_preserves_point_multiget_and_paged_scan_sequence() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open_worker_with_resources(config.clone(), resources.clone())
            .await
            .unwrap();
        let logical_key = |name: &[u8]| {
            let mut value = key();
            value.key = name.to_vec();
            value
        };
        let keys = [
            logical_key(b"a"),
            logical_key(b"b"),
            logical_key(b"c"),
            logical_key(b"d"),
        ];
        for (key, value) in keys[..3].iter().zip([b"old-a", b"old-b", b"old-c"]) {
            state.put(key.clone(), value.to_vec(), 1024).await.unwrap();
        }
        let snapshot = state.snapshot().await.unwrap();
        let request = ScanRequest {
            range: ScanRange {
                namespace: keys[0].namespace.clone(),
                prefix: None,
                start: None,
                end: None,
            },
            max_entries: 1,
            max_bytes: 1024,
            cursor: None,
        };
        let first = snapshot.scan(request.clone()).await.unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].key, keys[0]);
        assert_eq!(first.entries[0].value, b"old-a");
        state
            .write_batch(WriteBatch {
                operations: vec![
                    WriteOperation::Delete {
                        key: keys[0].clone(),
                    },
                    WriteOperation::Put {
                        key: keys[1].clone(),
                        value: b"new-b".to_vec(),
                    },
                    WriteOperation::Put {
                        key: keys[3].clone(),
                        value: b"new-d".to_vec(),
                    },
                ],
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .get(&keys[0], ReadOptions { max_bytes: 1024 })
                .await
                .unwrap(),
            Some(b"old-a".to_vec())
        );
        assert_eq!(
            snapshot
                .multi_get(&keys, ReadOptions { max_bytes: 1024 })
                .await
                .unwrap(),
            vec![
                Some(b"old-a".to_vec()),
                Some(b"old-b".to_vec()),
                Some(b"old-c".to_vec()),
                None
            ]
        );
        let mut cursor = first.next_cursor.clone();
        drop(first);
        let mut tail = Vec::new();
        while let Some(next) = cursor {
            let page = snapshot
                .scan(ScanRequest {
                    cursor: Some(next),
                    ..request.clone()
                })
                .await
                .unwrap();
            for entry in &page.entries {
                tail.push((entry.key.clone(), entry.value.clone()));
            }
            cursor = page.next_cursor.clone();
        }
        assert_eq!(
            tail,
            vec![
                (keys[1].clone(), b"old-b".to_vec()),
                (keys[2].clone(), b"old-c".to_vec())
            ]
        );
        assert_eq!(
            state
                .multi_get(&keys, ReadOptions { max_bytes: 1024 })
                .await
                .unwrap(),
            vec![
                None,
                Some(b"new-b".to_vec()),
                Some(b"old-c".to_vec()),
                Some(b"new-d".to_vec())
            ]
        );
        assert_eq!(
            std::fs::read_dir(config.path().parent().unwrap())
                .unwrap()
                .count(),
            1,
            "native snapshot must not create a filesystem checkpoint database"
        );
        drop(snapshot);
        state.close_and_remove().await.unwrap();
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn removal_preserves_snapshot_until_last_reader_and_drop_unlocks_db() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        state.put(key(), b"retained".to_vec(), 1024).await.unwrap();
        let snapshot = state.snapshot().await.unwrap();
        let mut closing = Box::pin(state.close_and_remove());
        assert!(futures::poll!(&mut closing).is_pending());
        assert!(config.path().exists());
        assert_eq!(
            std::fs::read_dir(config.path().parent().unwrap())
                .unwrap()
                .count(),
            1,
            "native read views must not create sibling database directories"
        );
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 8 })
                .await
                .unwrap(),
            Some(b"retained".to_vec())
        );
        drop(snapshot);
        closing.await.unwrap();
        assert!(!config.path().exists());
        drain_cleanup(&resources).await;
        assert_eq!(
            std::fs::read_dir(config.path().parent().unwrap())
                .unwrap()
                .count(),
            0
        );
        let state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        drop(state);
        drain_cleanup(&resources).await;
        RocksLiveState::reopen(config.clone(), resources)
            .await
            .unwrap()
            .close_and_remove()
            .await
            .unwrap();
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn ownership_mismatch_and_fresh_open_failure_are_safe() {
        let config = config();
        let resources = resources();
        RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        std::fs::write(config.path().join("OWNERSHIP"), b"different identity").unwrap();
        assert!(
            RocksLiveState::reopen(config.clone(), resources.clone())
                .await
                .is_err()
        );
        assert!(config.path().join("CURRENT").exists());
        std::fs::remove_dir_all(&config.root).unwrap();
        let mut impossible = resources.config().clone();
        impossible.disk_reserve_bytes = u64::MAX;
        let impossible = WorkerStateResources::new(impossible).unwrap();
        assert!(
            RocksLiveState::open(config.clone(), impossible)
                .await
                .is_err()
        );
        assert!(!config.path().exists());
        std::fs::remove_dir_all(config.root).unwrap();
    }

    #[tokio::test]
    async fn read_request_buffers_are_admitted_before_encoding() {
        let config = config();
        let mut limits = resources().config().clone();
        limits.decoded_value_bytes = 128;
        let resources = WorkerStateResources::new(limits).unwrap();
        let state = RocksLiveState::open(config.clone(), resources)
            .await
            .unwrap();
        let mut huge = key();
        huge.key = vec![0; 1024];
        assert!(matches!(
            state.get(&huge, ReadOptions { max_bytes: 1 }).await,
            Err(LiveStateError::Resource(
                super::super::resources::ResourceError::RequestTooLarge { .. }
            ))
        ));
        assert!(matches!(
            state
                .multi_get(&vec![key(); 100], ReadOptions { max_bytes: 1 })
                .await,
            Err(LiveStateError::Resource(
                super::super::resources::ResourceError::RequestTooLarge { .. }
            ))
        ));
        state.close_and_remove().await.unwrap();
        std::fs::remove_dir_all(config.root).unwrap();
    }
    #[tokio::test]
    async fn explicit_removal_reports_filesystem_failures() {
        let config = config();
        let resources = resources();
        let mut state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        // Simulate a cleanup path removed by an external actor. The actual DB
        // is still closed first, so subsequent reopen demonstrates lock release.
        Arc::get_mut(&mut state.db).unwrap().path = config.root.join("missing-cleanup-path");
        assert!(matches!(
            state.close_and_remove().await,
            Err(LiveStateError::Backend(_))
        ));
        RocksLiveState::reopen(config.clone(), resources)
            .await
            .unwrap()
            .close_and_remove()
            .await
            .unwrap();
        std::fs::remove_dir_all(config.root).unwrap();
    }
}
