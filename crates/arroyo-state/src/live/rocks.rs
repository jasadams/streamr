//! RocksDB live state. Local checkpoints implement owned stable snapshots;
//! they are not durable distributed checkpoints or restore artifacts.
use super::lifecycle::{RocksStateConfig, directory_bytes};
use super::resources::{CleanupPermit, ResourcePermit, WorkerStateResources};
use super::{
    encoding::{decode_key, decode_value, encode_key, encode_value},
    *,
};
use async_trait::async_trait;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, DB, DBRecoveryMode, Options, WriteOptions};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

pub struct RocksLiveState {
    db: Arc<NativeDb>,
    completion: oneshot::Receiver<Result<()>>,
    path: PathBuf,
    resources: WorkerStateResources,
    sync_writes: bool,
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
    /// Writes complete atomically with WAL enabled, without per-row fsync;
    /// recovery must restore a committed checkpoint into this fresh attempt.
    pub async fn open_worker(config: RocksStateConfig) -> Result<Self> {
        let resources = super::worker::configured_worker_resources()?.ok_or_else(|| {
            LiveStateError::Backend("worker live-state resource budgets are not configured".into())
        })?;
        Self::open_worker_with_resources(config, resources).await
    }

    async fn open_worker_with_resources(
        config: RocksStateConfig,
        resources: WorkerStateResources,
    ) -> Result<Self> {
        let permit = resources.try_database()?;
        Self::open_mode(config, resources, false, permit, false).await
    }

    pub async fn open(config: RocksStateConfig, resources: WorkerStateResources) -> Result<Self> {
        let permit = resources.database().await?;
        Self::open_mode(config, resources, false, permit, true).await
    }

    /// Explicit reuse of precisely the supplied attempt. Missing or corrupt
    /// databases fail instead of being replaced with empty state.
    pub async fn reopen(config: RocksStateConfig, resources: WorkerStateResources) -> Result<Self> {
        let permit = resources.database().await?;
        Self::open_mode(config, resources, true, permit, true).await
    }

    async fn open_mode(
        config: RocksStateConfig,
        resources: WorkerStateResources,
        reopen: bool,
        database_permit: ResourcePermit,
        sync_writes: bool,
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
                let (finished, completion) = oneshot::channel();
                Ok(Self {
                    db: Arc::new(NativeDb {
                        db: Some(db),
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
                    sync_writes,
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
        let sync_writes = self.sync_writes;
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("write");
                let _permit = permit;
                resources
                    .ensure_disk_space(&path, bytes as u64)
                    .map_err(LiveStateError::from)?;
                let mut native = rocksdb::WriteBatch::default();
                for operation in batch.operations {
                    match operation {
                        WriteOperation::Put { key, value } => {
                            native.put(encode_key(&key)?, encode_value(&value))
                        }
                        WriteOperation::Delete { key } => native.delete(encode_key(&key)?),
                    }
                }
                let mut options = WriteOptions::default();
                options.disable_wal(false);
                options.set_sync(sync_writes);
                db.write_opt(native, &options).map_err(backend)
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

    /// Remove only this attempt's live database. Snapshot checkpoints are siblings
    /// and retain their own cleanup guard until their last reader finishes.
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
        )
        .await
    }
}

async fn read_many(
    db: DbHandle,
    resources: WorkerStateResources,
    keys: &[StateKey],
    read: ReadOptions,
) -> Result<Vec<Option<Vec<u8>>>> {
    let mut request_bytes = keys
        .len()
        .saturating_mul(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>());
    for key in keys {
        request_bytes =
            request_bytes.saturating_add(encoding::encoded_key_size(key)?.saturating_mul(2));
    }
    let permit = resources
        .decoded_value(read.max_bytes.saturating_add(request_bytes))
        .await
        .map_err(LiveStateError::from)?;
    let keys = keys.iter().map(encode_key).collect::<Result<Vec<_>>>()?;
    let task_resources = resources.clone();
    resources
        .run_blocking(move || {
            let _latency = task_resources.operation_timer("read");
            let _permit = permit;
            let snapshot = db.snapshot();
            let mut native_read = rocksdb::ReadOptions::default();
            native_read.set_snapshot(&snapshot);
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
        let cleanup = self
            .resources
            .cleanup()
            .await
            .map_err(LiveStateError::from)?;
        let id = super::memory::next_snapshot_id();
        let path = self.path.with_file_name(format!(
            "{}-snapshot-{}-{id}",
            self.path
                .file_name()
                .expect("attempt directory")
                .to_string_lossy(),
            std::process::id()
        ));
        let db = self.db.clone();
        let resources = self.resources.clone();
        let live_path = self.path.clone();
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("snapshot");
                resources
                    .ensure_disk_space(&live_path, directory_bytes(&live_path)?)
                    .map_err(LiveStateError::from)?;
                if path.try_exists().map_err(lifecycle::io_error)? {
                    return Err(backend("snapshot directory already exists"));
                }
                if let Err(error) =
                    Checkpoint::new(&db).and_then(|checkpoint| checkpoint.create_checkpoint(&path))
                {
                    let _ = std::fs::remove_dir_all(&path);
                    return Err(backend(error));
                }
                let snapshot_db =
                    match DB::open_for_read_only(&options(&resources, false), &path, false) {
                        Ok(db) => db,
                        Err(error) => {
                            let _ = std::fs::remove_dir_all(&path);
                            return Err(backend(error));
                        }
                    };
                Ok(StateSnapshot(Arc::new(RocksSnapshot {
                    db: Arc::new(NativeDb {
                        db: Some(snapshot_db),
                        path,
                        remove: AtomicBool::new(true),
                        permit: Some(permit),
                        resources: Some(resources.clone()),
                        cleanup: Some(cleanup),
                        finished: None,
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
    Snapshot(Arc<NativeDb>),
}
impl std::ops::Deref for DbHandle {
    type Target = DB;
    fn deref(&self) -> &DB {
        match self {
            Self::Live(db) | Self::Snapshot(db) => db,
        }
    }
}
struct NativeDb {
    db: Option<DB>,
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
                    tracing::error!(%error, "live-state snapshot cleanup failed");
                }
            });
    }
}
struct RocksSnapshot {
    db: Arc<NativeDb>,
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
            DbHandle::Snapshot(self.db.clone()),
            self.resources.clone(),
            keys,
            read,
        )
        .await
    }
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        request.validate(self.id)?;
        let id = self.id;
        let db = DbHandle::Snapshot(self.db.clone());
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
        let permit = self
            .resources
            .scan_page(
                request
                    .max_bytes
                    .saturating_mul(4)
                    .saturating_add(request_bytes.saturating_mul(4))
                    .saturating_add(container_bytes),
            )
            .await
            .map_err(LiveStateError::from)?;
        let resources = self.resources.clone();
        self.resources
            .run_blocking(move || {
                let _latency = resources.operation_timer("scan");
                let _permit = permit;
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
                let mut lower = bound(request.range.start.as_deref().unwrap_or_default());
                if let Some(prefix) = &prefix
                    && *prefix > lower
                {
                    lower = prefix.clone();
                }
                if let Some(cursor) = &request.cursor
                    && cursor.last_key > lower
                {
                    lower = cursor.last_key.clone();
                }
                let mut iterator = db.raw_iterator();
                iterator.seek(&lower);
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
    async fn worker_writes_are_visible_and_snapshots_survive_without_per_write_sync() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open_worker_with_resources(config.clone(), resources.clone())
            .await
            .unwrap();
        assert!(!state.sync_writes);
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
        state.close_and_remove().await.unwrap();
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"committed".to_vec())
        );
        drop(snapshot);
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
        assert!(state.sync_writes);
        super::super::tests::backend_contract(&state).await;
        state.put(key(), b"persisted".to_vec(), 1024).await.unwrap();
        assert!(
            RocksLiveState::open(config.clone(), resources.clone())
                .await
                .is_err()
        );
        let snapshot = state.snapshot().await.unwrap();
        state.close().await.unwrap();
        let reopened = RocksLiveState::reopen(config.clone(), resources.clone())
            .await
            .unwrap();
        assert!(reopened.sync_writes);
        assert_eq!(
            reopened
                .get(&key(), ReadOptions { max_bytes: 9 })
                .await
                .unwrap(),
            Some(b"persisted".to_vec())
        );
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
        reopened.close().await.unwrap();
        drop(snapshot);
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
    async fn removal_preserves_snapshot_until_last_reader_and_drop_unlocks_db() {
        let config = config();
        let resources = resources();
        let state = RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap();
        state.put(key(), b"retained".to_vec(), 1024).await.unwrap();
        let snapshot = state.snapshot().await.unwrap();
        state.close_and_remove().await.unwrap();
        assert!(!config.path().exists());
        assert_eq!(
            snapshot
                .get(&key(), ReadOptions { max_bytes: 8 })
                .await
                .unwrap(),
            Some(b"retained".to_vec())
        );
        drop(snapshot);
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
