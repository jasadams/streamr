//! Worker-wide resources for live state. Configure from measured worker budgets;
//! RocksDB's cache and write-buffer limits do not constitute a process RSS limit.
use std::{
    ffi::CString,
    fmt,
    path::Path,
    sync::{
        Arc, Mutex, OnceLock,
        mpsc::{self, SyncSender},
    },
};

use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntGaugeVec, Opts, Registry};
use rocksdb::{Cache, WriteBufferManager};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// No production defaults: deployment qualification must supply every budget.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ResourceConfig {
    /// Includes memtables charged to this cache by the write-buffer manager.
    pub block_cache_bytes: usize,
    pub memtable_bytes: usize,
    pub queued_write_bytes: usize,
    pub decoded_value_bytes: usize,
    pub scan_page_bytes: usize,
    pub max_blocking_operations: usize,
    pub max_snapshots: usize,
    pub max_open_databases: usize,
    /// Free disk kept available for compaction and other worker activity.
    pub disk_reserve_bytes: u64,
}

#[derive(Debug)]
pub enum ResourceError {
    InvalidConfig(String),
    RequestTooLarge {
        resource: &'static str,
        requested: usize,
        limit: usize,
    },
    Closed {
        resource: &'static str,
    },
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    BlockingTask(tokio::task::JoinError),
    DiskIo(std::io::Error),
    CleanupThread(std::io::Error),
    DiskReserve {
        available: u64,
        required: u64,
    },
}
impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(s) => write!(f, "invalid live-state resource configuration: {s}"),
            Self::RequestTooLarge {
                resource,
                requested,
                limit,
            } => write!(
                f,
                "{resource} request of {requested} exceeds budget {limit}"
            ),
            Self::Closed { resource } => write!(f, "{resource} admission closed"),
            Self::ResourceExhausted { resource, limit } => write!(
                f,
                "{resource} budget exhausted (limit {limit}); worker startup cannot wait for a database slot"
            ),
            Self::BlockingTask(e) => write!(f, "live-state blocking task failed: {e}"),
            Self::DiskIo(e) => write!(f, "cannot inspect live-state disk: {e}"),
            Self::CleanupThread(e) => write!(f, "cannot start live-state cleanup thread: {e}"),
            Self::DiskReserve {
                available,
                required,
            } => write!(
                f,
                "insufficient live-state disk: {available} available, {required} required including reserve"
            ),
        }
    }
}
impl std::error::Error for ResourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BlockingTask(e) => Some(e),
            Self::DiskIo(e) | Self::CleanupThread(e) => Some(e),
            _ => None,
        }
    }
}
impl From<ResourceError> for super::LiveStateError {
    fn from(error: ResourceError) -> Self {
        Self::Resource(error)
    }
}

struct Budget {
    name: &'static str,
    limit: usize,
    semaphore: Arc<Semaphore>,
    metrics: IntGaugeVec,
}
impl Budget {
    fn new(name: &'static str, limit: usize, metrics: &IntGaugeVec) -> Self {
        metrics
            .with_label_values(&[name, "limit"])
            .set(limit as i64);
        Self {
            name,
            limit,
            semaphore: Arc::new(Semaphore::new(limit)),
            metrics: metrics.clone(),
        }
    }
    async fn acquire(&self, amount: usize) -> Result<ResourcePermit, ResourceError> {
        // acquire_many takes u32; reject impossible requests rather than wait forever.
        let count = u32::try_from(amount)
            .ok()
            .filter(|_| amount <= self.limit)
            .ok_or(ResourceError::RequestTooLarge {
                resource: self.name,
                requested: amount,
                limit: self.limit.min(u32::MAX as usize),
            })?;
        let waiting = self.metrics.with_label_values(&[self.name, "waiting"]);
        waiting.inc();
        let _waiting = GaugeGuard(waiting);
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(count)
            .await
            .map_err(|_| ResourceError::Closed {
                resource: self.name,
            })?;
        let used = self.metrics.with_label_values(&[self.name, "used"]);
        used.add(amount as i64);
        Ok(ResourcePermit {
            _permit: permit,
            used,
            amount,
        })
    }
    fn try_acquire_one(&self) -> Result<ResourcePermit, ResourceError> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => ResourceError::Closed {
                    resource: self.name,
                },
                tokio::sync::TryAcquireError::NoPermits => ResourceError::ResourceExhausted {
                    resource: self.name,
                    limit: self.limit,
                },
            })?;
        let used = self.metrics.with_label_values(&[self.name, "used"]);
        used.inc();
        Ok(ResourcePermit {
            _permit: permit,
            used,
            amount: 1,
        })
    }
}
struct GaugeGuard(prometheus::IntGauge);
impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Hold until the corresponding buffers, snapshot, or blocking work are released.
/// Admission bounds only accounted operations: returning an owned value without
/// its permit transfers responsibility for retained memory to the caller.
pub struct ResourcePermit {
    _permit: OwnedSemaphorePermit,
    used: prometheus::IntGauge,
    amount: usize,
}
impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.used.sub(self.amount as i64);
    }
}

type CleanupWork = Box<dyn FnOnce() + Send + 'static>;
struct CleanupJob {
    work: CleanupWork,
    // The slot is retained through execution, including panic unwinding.
    _slot: ResourcePermit,
}
struct CleanupExecutor {
    sender: SyncSender<CleanupJob>,
    slots: Budget,
}
impl CleanupExecutor {
    fn new(capacity: usize, metrics: &IntGaugeVec) -> Result<Self, ResourceError> {
        let (sender, receiver) = mpsc::sync_channel::<CleanupJob>(capacity);
        std::thread::Builder::new()
            .name("live-state-cleanup".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    // A cleanup panic must not strand subsequent native resources.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let CleanupJob { work, _slot } = job;
                        let _slot = _slot;
                        work();
                    }));
                }
            })
            .map_err(ResourceError::CleanupThread)?;
        Ok(Self {
            sender,
            slots: Budget::new("cleanup_slots", capacity, metrics),
        })
    }
    async fn reserve(&self) -> Result<CleanupPermit, ResourceError> {
        let slot = self.slots.acquire(1).await?;
        Ok(CleanupPermit {
            sender: self.sender.clone(),
            slot,
        })
    }
}

/// Reserve before creating a database or snapshot. Dropping an unused permit
/// releases admission. Submitting transfers admission to the dedicated cleanup
/// thread until the resource has actually been destroyed.
pub struct CleanupPermit {
    sender: SyncSender<CleanupJob>,
    slot: ResourcePermit,
}
impl CleanupPermit {
    pub fn submit<F>(self, work: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = CleanupJob {
            work: Box::new(work),
            _slot: self.slot,
        };
        // Each queued/executing job and each unsent permit owns one slot. Since
        // queue capacity equals total slots, an owner can always enqueue its job.
        // This permit also keeps the receiver's channel alive through submission.
        if let Err(error) = self.sender.try_send(job) {
            // Never destroy native captures on this calling (potentially async)
            // thread, even if an internal invariant has been violated.
            std::mem::forget(error);
            panic!("live-state cleanup queue reservation invariant violated");
        }
    }
}

struct Inner {
    config: ResourceConfig,
    cache: Cache,
    manager: WriteBufferManager,
    queued: Budget,
    decoded: Budget,
    scans: Budget,
    blocking: Budget,
    snapshots: Budget,
    databases: Budget,
    cleanup: CleanupExecutor,
    metrics: IntGaugeVec,
    operation_latency: HistogramVec,
}
/// Create once per worker, then clone into all task-local databases.
#[derive(Clone)]
pub struct WorkerStateResources(Arc<Inner>);
impl WorkerStateResources {
    /// Production workers share exactly one pool. Incompatible budgets are an
    /// error rather than silently creating another cache and admission domain.
    pub fn for_worker(config: ResourceConfig) -> Result<Self, ResourceError> {
        static WORKER: OnceLock<Mutex<Option<WorkerStateResources>>> = OnceLock::new();
        let mut worker = WORKER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .map_err(|_| ResourceError::InvalidConfig("worker resource lock poisoned".into()))?;
        if let Some(resources) = worker.as_ref() {
            if resources.config() != &config {
                return Err(ResourceError::InvalidConfig(
                    "worker resources already initialized with different budgets".into(),
                ));
            }
            return Ok(resources.clone());
        }
        let resources = Self::new(config)?;
        *worker = Some(resources.clone());
        Ok(resources)
    }

    /// Isolated pools are useful in tests; production must use `for_worker`.
    pub fn new(config: ResourceConfig) -> Result<Self, ResourceError> {
        for (name, limit) in [
            ("block_cache_bytes", config.block_cache_bytes),
            ("memtable_bytes", config.memtable_bytes),
            ("queued_write_bytes", config.queued_write_bytes),
            ("decoded_value_bytes", config.decoded_value_bytes),
            ("scan_page_bytes", config.scan_page_bytes),
            ("max_blocking_operations", config.max_blocking_operations),
            ("max_snapshots", config.max_snapshots),
            ("max_open_databases", config.max_open_databases),
        ] {
            if limit == 0 || limit > Semaphore::MAX_PERMITS || limit > i64::MAX as usize {
                return Err(ResourceError::InvalidConfig(format!(
                    "{name} must be positive and fit semaphore/metric capacity"
                )));
            }
        }
        for (name, count) in [
            ("max_open_databases", config.max_open_databases),
            ("max_snapshots", config.max_snapshots),
            ("max_blocking_operations", config.max_blocking_operations),
        ] {
            if count > u32::MAX as usize {
                return Err(ResourceError::InvalidConfig(format!("{name} must fit u32")));
            }
        }
        let cleanup_capacity = config
            .max_open_databases
            .checked_add(config.max_snapshots)
            .and_then(|n| n.checked_add(config.max_blocking_operations))
            .filter(|n| *n <= Semaphore::MAX_PERMITS && *n <= i64::MAX as usize)
            .ok_or_else(|| ResourceError::InvalidConfig("cleanup capacity overflow".into()))?;
        if config.memtable_bytes > config.block_cache_bytes {
            return Err(ResourceError::InvalidConfig(
                "memtable budget must fit the cache budget it is charged to".into(),
            ));
        }
        let metrics = IntGaugeVec::new(
            Opts::new(
                "arroyo_live_state_resources",
                "Worker live-state admission limits, usage and waiting requests",
            ),
            &["resource", "measurement"],
        )
        .map_err(|e| ResourceError::InvalidConfig(e.to_string()))?;
        let operation_latency = HistogramVec::new(
            HistogramOpts::new(
                "arroyo_live_state_operation_latency_seconds",
                "Live-state native operation latency after blocking admission",
            ),
            &["operation"],
        )
        .map_err(|e| ResourceError::InvalidConfig(e.to_string()))?;
        for operation in ["read", "write", "scan", "snapshot", "open"] {
            operation_latency.with_label_values(&[operation]);
        }
        let cache = Cache::new_lru_cache(config.block_cache_bytes);
        let manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            config.memtable_bytes,
            true,
            cache.clone(),
        );
        Ok(Self(Arc::new(Inner {
            queued: Budget::new("queued_write_bytes", config.queued_write_bytes, &metrics),
            decoded: Budget::new("decoded_value_bytes", config.decoded_value_bytes, &metrics),
            scans: Budget::new("scan_page_bytes", config.scan_page_bytes, &metrics),
            blocking: Budget::new(
                "blocking_operations",
                config.max_blocking_operations,
                &metrics,
            ),
            snapshots: Budget::new("snapshots", config.max_snapshots, &metrics),
            databases: Budget::new("databases", config.max_open_databases, &metrics),
            cleanup: CleanupExecutor::new(cleanup_capacity, &metrics)?,
            config,
            cache,
            manager,
            metrics,
            operation_latency,
        })))
    }
    pub(crate) fn same_pool(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn config(&self) -> &ResourceConfig {
        &self.0.config
    }
    pub fn cache(&self) -> &Cache {
        &self.0.cache
    }
    pub fn write_buffer_manager(&self) -> &WriteBufferManager {
        &self.0.manager
    }
    pub async fn queued_write(&self, bytes: usize) -> Result<ResourcePermit, ResourceError> {
        self.0.queued.acquire(bytes).await
    }
    pub async fn decoded_value(&self, bytes: usize) -> Result<ResourcePermit, ResourceError> {
        self.0.decoded.acquire(bytes).await
    }
    pub async fn scan_page(&self, bytes: usize) -> Result<ResourcePermit, ResourceError> {
        self.0.scans.acquire(bytes).await
    }
    pub async fn snapshot(&self) -> Result<ResourcePermit, ResourceError> {
        self.0.snapshots.acquire(1).await
    }

    pub async fn database(&self) -> Result<ResourcePermit, ResourceError> {
        self.0.databases.acquire(1).await
    }

    /// Worker startup must fail instead of waiting for other operators that
    /// may themselves be waiting for the worker readiness barrier.
    pub fn try_database(&self) -> Result<ResourcePermit, ResourceError> {
        self.0.databases.try_acquire_one()
    }
    pub async fn cleanup(&self) -> Result<CleanupPermit, ResourceError> {
        self.0.cleanup.reserve().await
    }

    /// Permit belongs to the blocking closure, so cancelling the awaiting future
    /// cannot release capacity while the uncancellable native call still runs.
    pub async fn run_blocking<F, T>(&self, work: F) -> Result<T, ResourceError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.0.blocking.acquire(1).await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(ResourceError::BlockingTask)
    }
    /// Start before admission and retain through native completion. These are
    /// whole-operation timings, rather than isolated native I/O measurements.
    pub(crate) fn operation_timer(&self, operation: &'static str) -> HistogramTimer {
        self.0
            .operation_latency
            .with_label_values(&[operation])
            .start_timer()
    }

    /// Registration is explicit to avoid duplicate global collectors in tests.
    /// The collector refreshes native usage on each Prometheus scrape.
    pub fn register_metrics(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.clone()))
    }
    pub fn refresh_native_metrics(&self) {
        for (name, used, limit) in [
            (
                "block_cache_bytes",
                self.0.cache.get_usage(),
                self.0.config.block_cache_bytes,
            ),
            (
                "memtable_bytes",
                self.0.manager.get_usage(),
                self.0.config.memtable_bytes,
            ),
        ] {
            self.0
                .metrics
                .with_label_values(&[name, "used"])
                .set(used as i64);
            self.0
                .metrics
                .with_label_values(&[name, "limit"])
                .set(limit as i64);
        }
        self.0
            .metrics
            .with_label_values(&["pinned_cache_bytes", "used"])
            .set(self.0.cache.get_pinned_usage() as i64);
    }
    /// Checks current filesystem headroom; this is not a reservation against
    /// concurrent external writers or RocksDB compaction amplification.
    pub fn ensure_disk_space(
        &self,
        path: &Path,
        additional_bytes: u64,
    ) -> Result<(), ResourceError> {
        let available = available_disk_bytes(path).map_err(ResourceError::DiskIo)?;
        self.0
            .metrics
            .with_label_values(&["disk_available_bytes", "used"])
            .set(available.min(i64::MAX as u64) as i64);
        let result = check_disk_reserve(
            available,
            additional_bytes,
            self.0.config.disk_reserve_bytes,
        );
        if result.is_err() {
            self.0
                .metrics
                .with_label_values(&["disk_refusals", "count"])
                .inc();
        }
        result
    }
}
impl prometheus::core::Collector for WorkerStateResources {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut descriptions = prometheus::core::Collector::desc(&self.0.metrics);
        descriptions.extend(prometheus::core::Collector::desc(&self.0.operation_latency));
        descriptions
    }
    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.refresh_native_metrics();
        let mut families = prometheus::core::Collector::collect(&self.0.metrics);
        families.extend(prometheus::core::Collector::collect(
            &self.0.operation_latency,
        ));
        families
    }
}
fn check_disk_reserve(available: u64, additional: u64, reserve: u64) -> Result<(), ResourceError> {
    match additional.checked_add(reserve) {
        Some(required) if available >= required => Ok(()),
        required => Err(ResourceError::DiskReserve {
            available,
            required: required.unwrap_or(u64::MAX),
        }),
    }
}
#[cfg(unix)]
#[allow(
    clippy::unnecessary_cast,
    reason = "statvfs field widths vary across Unix targets"
)]
fn available_disk_bytes(path: &Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "disk path contains NUL")
    })?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL terminated and stat points to valid writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: successful statvfs initialized the complete structure.
    let stat = unsafe { stat.assume_init() };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}
#[cfg(not(unix))]
fn available_disk_bytes(_path: &Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "live-state disk accounting requires Unix",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ResourceConfig {
        ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 512 * 1024,
            queued_write_bytes: 16,
            decoded_value_bytes: 16,
            scan_page_bytes: 16,
            max_blocking_operations: 1,
            max_snapshots: 1,
            max_open_databases: 1,
            disk_reserve_bytes: 1,
        }
    }
    #[tokio::test]
    async fn worker_database_admission_fails_fast_but_diagnostic_admission_waits() {
        let resources = WorkerStateResources::new(config()).unwrap();
        let permit = resources.try_database().unwrap();
        assert!(matches!(
            resources.try_database(),
            Err(ResourceError::ResourceExhausted {
                resource: "databases",
                limit: 1,
            })
        ));
        let diagnostic = resources.database();
        tokio::pin!(diagnostic);
        assert!(futures::poll!(&mut diagnostic).is_pending());
        drop(permit);
        drop(diagnostic.await.unwrap());
        assert!(resources.try_database().is_ok());
        assert_eq!(resources.0.databases.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn shared_admission_blocks_until_owner_releases() {
        let resources = WorkerStateResources::new(config()).unwrap();
        let clone = resources.clone();
        assert!(Arc::ptr_eq(&resources.0, &clone.0));
        let permit = resources.queued_write(16).await.unwrap();
        let next = clone.queued_write(1);
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        drop(permit);
        assert!(next.await.is_ok());
        assert!(matches!(
            resources.scan_page(17).await,
            Err(ResourceError::RequestTooLarge { .. })
        ));
    }
    #[tokio::test]
    async fn snapshots_and_decoded_buffers_are_bounded() {
        let resources = WorkerStateResources::new(config()).unwrap();
        let snapshot = resources.snapshot().await.unwrap();
        let second = resources.snapshot();
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());
        let value = resources.decoded_value(16).await.unwrap();
        let another = resources.decoded_value(1);
        tokio::pin!(another);
        assert!(futures::poll!(&mut another).is_pending());
        drop(snapshot);
        drop(value);
        assert!(second.await.is_ok());
        assert!(another.await.is_ok());
        let page = resources.scan_page(16).await.unwrap();
        let next_page = resources.scan_page(1);
        tokio::pin!(next_page);
        assert!(futures::poll!(&mut next_page).is_pending());
        drop(page);
        assert!(next_page.await.is_ok());
    }
    #[tokio::test]
    async fn cancelled_waiter_does_not_release_running_blocking_work() {
        let resources = WorkerStateResources::new(config()).unwrap();
        let worker = resources.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            worker
                .run_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let next = resources.run_blocking(|| 42);
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        release_tx.send(()).unwrap();
        assert_eq!(next.await.unwrap(), 42);
        assert_eq!(resources.0.blocking.semaphore.available_permits(), 1);
    }
    fn cleanup_executor(capacity: usize) -> CleanupExecutor {
        let metrics = IntGaugeVec::new(
            Opts::new("test_cleanup", "test cleanup admission"),
            &["resource", "measurement"],
        )
        .unwrap();
        CleanupExecutor::new(capacity, &metrics).unwrap()
    }

    #[tokio::test]
    async fn cleanup_slot_is_retained_until_destruction_finishes() {
        // Native-free: a blocked destructor models database teardown.
        let executor = cleanup_executor(1);
        let permit = executor.reserve().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let calling_thread = std::thread::current().id();
        permit.submit(move || {
            assert_ne!(std::thread::current().id(), calling_thread);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        let next = executor.reserve();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        release_tx.send(()).unwrap();
        drop(next.await.unwrap());
        assert_eq!(executor.slots.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn unused_and_cancelled_cleanup_reservations_release_capacity() {
        let executor = cleanup_executor(1);
        let permit = executor.reserve().await.unwrap();
        {
            let waiting = executor.reserve();
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            // Cancelling a waiter must not release the live owner's slot.
        }
        assert_eq!(executor.slots.semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(executor.slots.semaphore.available_permits(), 1);
        drop(executor.reserve().await.unwrap());
        assert_eq!(executor.slots.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn cleanup_queue_is_bounded_while_work_is_blocked() {
        let executor = cleanup_executor(2);
        let first = executor.reserve().await.unwrap();
        let second = executor.reserve().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        first.submit(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        second.submit(move || {
            let _ = finished_tx.send(());
        });
        assert_eq!(executor.slots.semaphore.available_permits(), 0);
        {
            let third = executor.reserve();
            tokio::pin!(third);
            assert!(futures::poll!(&mut third).is_pending());
            // No thread per resource: both reservations feed the same bounded queue.
        }
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_permit_keeps_worker_alive_until_submission() {
        let executor = cleanup_executor(1);
        let permit = executor.reserve().await.unwrap();
        drop(executor);
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        permit.submit(move || {
            let _ = finished_tx.send(());
        });
        finished_rx.await.unwrap();
    }

    #[test]
    fn disk_reserve_refuses_growth_and_overflow() {
        assert!(check_disk_reserve(100, 40, 60).is_ok());
        assert!(matches!(
            check_disk_reserve(99, 40, 60),
            Err(ResourceError::DiskReserve { required: 100, .. })
        ));
        assert!(check_disk_reserve(u64::MAX, 1, u64::MAX).is_err());
        let mut config = config();
        config.disk_reserve_bytes = u64::MAX;
        let resources = WorkerStateResources::new(config).unwrap();
        assert!(matches!(
            resources.ensure_disk_space(&std::env::temp_dir(), 1),
            Err(ResourceError::DiskReserve { .. })
        ));
    }
    #[test]
    fn registry_collects_native_usage_and_operation_latency() {
        let resources = WorkerStateResources::new(config()).unwrap();
        let registry = Registry::new();
        resources.register_metrics(&registry).unwrap();
        drop(resources.operation_timer("read"));
        let families = registry.gather();
        let usage = families
            .iter()
            .find(|family| family.name() == "arroyo_live_state_resources")
            .unwrap();
        assert!(usage.get_metric().iter().any(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.value() == "block_cache_bytes")
        }));
        let latency = families
            .iter()
            .find(|family| family.name() == "arroyo_live_state_operation_latency_seconds")
            .unwrap();
        let read = latency
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.value() == "read")
            })
            .unwrap();
        assert_eq!(read.get_histogram().get_sample_count(), 1);
        assert_eq!(latency.get_metric().len(), 5);
    }

    #[test]
    fn rejects_unusable_database_limits_before_native_allocation() {
        let mut invalid = config();
        invalid.max_open_databases = 0;
        assert!(matches!(
            WorkerStateResources::new(invalid),
            Err(ResourceError::InvalidConfig(_))
        ));
        if let Some(too_many) = (u32::MAX as usize).checked_add(1) {
            let mut invalid = config();
            invalid.max_open_databases = too_many;
            assert!(matches!(
                WorkerStateResources::new(invalid),
                Err(ResourceError::InvalidConfig(_))
            ));
        }
    }

    #[test]
    fn rejects_unusable_configuration() {
        let mut config = config();
        config.memtable_bytes = config.block_cache_bytes + 1;
        assert!(matches!(
            WorkerStateResources::new(config),
            Err(ResourceError::InvalidConfig(_))
        ));
    }
}
