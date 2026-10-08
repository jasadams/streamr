//! Shared cooperative execution limits for the selected milestone 3 paths.
//! Spilling is disabled until its filesystem capacity and lifecycle are qualified.
//! Unsupported allocations fail rather than silently spill outside that contract.
use arroyo_rpc::config::ExecutionResourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(crate) struct ExecutionResources {
    pub runtime: Arc<RuntimeEnv>,
    pub limits: ExecutionResourceConfig,
}

#[cfg(test)]
tokio::task_local! {
    static TEST_EXECUTION_RESOURCES: Arc<ExecutionResources>;
}

#[cfg(test)]
pub(super) async fn with_test_execution_resources<T>(
    resources: Arc<ExecutionResources>,
    future: impl std::future::Future<Output = T>,
) -> T {
    TEST_EXECUTION_RESOURCES.scope(resources, future).await
}

impl ExecutionResources {
    pub fn reserve_batch(
        &self,
        name: &str,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::common::Result<datafusion::execution::memory_pool::MemoryReservation> {
        self.check_batch(name, batch)?;
        self.reserve_bytes(name, batch.get_array_memory_size())
    }

    pub(crate) fn check_batch(
        &self,
        name: &str,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::common::Result<()> {
        let bytes = batch.get_array_memory_size();
        if bytes > self.limits.max_batch_bytes {
            return Err(datafusion::common::DataFusionError::ResourcesExhausted(
                format!(
                    "{name} batch requires {bytes} bytes; max-batch-bytes is {}",
                    self.limits.max_batch_bytes,
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn reserve_bytes(
        &self,
        name: &str,
        bytes: usize,
    ) -> datafusion::common::Result<datafusion::execution::memory_pool::MemoryReservation> {
        let mut reservation = datafusion::execution::memory_pool::MemoryConsumer::new(name)
            .register(&self.runtime.memory_pool);
        reservation.try_grow(bytes)?;
        Ok(reservation)
    }

    pub fn new(limits: ExecutionResourceConfig) -> anyhow::Result<Self> {
        limits.validate()?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(limits.memory_bytes)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        Ok(Self { runtime, limits })
    }

    pub fn task_context(&self) -> Arc<TaskContext> {
        SessionContext::new_with_config_rt(Default::default(), self.runtime.clone()).task_ctx()
    }
}

/// Scrape the actual configured pool without extending its admission lifetime.
/// A reservation may outlive RuntimeEnv, so observe the pool itself weakly.
#[derive(Clone)]
struct ExecutionPoolMetrics {
    observed: Arc<Mutex<Option<ObservedExecutionPool>>>,
    bytes: prometheus::IntGaugeVec,
}

struct ObservedExecutionPool {
    pool: Weak<dyn MemoryPool>,
    limits: ExecutionResourceConfig,
}

impl ExecutionPoolMetrics {
    fn register(registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let bytes = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "arroyo_worker_execution_memory_bytes",
                "Active shared execution pool reserved bytes and configured memory/batch limits; not process RSS",
            ),
            &["measurement"],
        )?;
        for measurement in ["used", "limit", "max_batch"] {
            bytes.with_label_values(&[measurement]);
        }
        let metrics = Self {
            observed: Arc::new(Mutex::new(None)),
            bytes,
        };
        registry.register(Box::new(metrics.clone()))?;
        Ok(metrics)
    }

    fn observe(&self, resources: &ExecutionResources) -> anyhow::Result<()> {
        let mut observed = self
            .observed
            .lock()
            .map_err(|_| anyhow::anyhow!("execution metrics observation mutex poisoned"))?;
        *observed = Some(ObservedExecutionPool {
            pool: Arc::downgrade(&resources.runtime.memory_pool),
            limits: resources.limits.clone(),
        });
        Ok(())
    }
}

impl prometheus::core::Collector for ExecutionPoolMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        prometheus::core::Collector::desc(&self.bytes)
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        // Serialize refresh and collection so concurrent scrapes cannot mix
        // measurements from different observed runtimes. No work is awaited.
        let observed = self.observed.lock().unwrap_or_else(|e| e.into_inner());
        let (used, limit, max_batch) = observed
            .as_ref()
            .and_then(|observed| {
                observed.pool.upgrade().map(|pool| {
                    (
                        pool.reserved(),
                        observed.limits.memory_bytes,
                        observed.limits.max_batch_bytes,
                    )
                })
            })
            .unwrap_or((0, 0, 0));
        for (measurement, value) in [("used", used), ("limit", limit), ("max_batch", max_batch)] {
            self.bytes
                .with_label_values(&[measurement])
                .set(value.min(i64::MAX as usize) as i64);
        }
        prometheus::core::Collector::collect(&self.bytes)
    }
}

fn observe_configured_execution_pool(resources: &ExecutionResources) -> anyhow::Result<()> {
    static METRICS: OnceLock<Result<ExecutionPoolMetrics, String>> = OnceLock::new();
    let metrics = METRICS.get_or_init(|| {
        ExecutionPoolMetrics::register(prometheus::default_registry()).map_err(|e| e.to_string())
    });
    match metrics {
        Ok(metrics) => metrics.observe(resources),
        Err(error) => anyhow::bail!("execution metrics registration: {error}"),
    }
}

/// Live operators share one pool. Reject budget changes while any operator still
/// uses the old pool; otherwise a configuration reload could double admission.
pub(crate) fn configured_execution_resources() -> anyhow::Result<Option<Arc<ExecutionResources>>> {
    #[cfg(test)]
    if let Ok(resources) = TEST_EXECUTION_RESOURCES.try_with(Arc::clone) {
        return Ok(Some(resources));
    }
    let limits = arroyo_rpc::config::config()
        .worker
        .execution_resources
        .clone();
    type CurrentRuntime = Option<(ExecutionResourceConfig, Weak<RuntimeEnv>)>;
    static RESOURCES: OnceLock<Mutex<CurrentRuntime>> = OnceLock::new();
    let mut current = RESOURCES
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| anyhow::anyhow!("worker execution resource mutex poisoned"))?;
    let Some(limits) = limits else {
        anyhow::ensure!(
            current
                .as_ref()
                .and_then(|(_, runtime)| runtime.upgrade())
                .is_none(),
            "worker execution budgets disabled while operators are active"
        );
        return Ok(None);
    };
    if let Some((previous, runtime)) = &*current
        && let Some(runtime) = runtime.upgrade()
    {
        anyhow::ensure!(
            previous == &limits,
            "worker execution budgets changed while operators are active"
        );
        return Ok(Some(Arc::new(ExecutionResources {
            runtime,
            limits: limits.clone(),
        })));
    }
    let resources = Arc::new(ExecutionResources::new(limits.clone())?);
    observe_configured_execution_pool(&resources)?;
    *current = Some((limits.clone(), Arc::downgrade(&resources.runtime)));
    Ok(Some(resources))
}

#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;

#[cfg(test)]
mod core_tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    #[test]
    fn task_contexts_share_one_memory_pool_and_release_reservations() {
        let resources = ExecutionResources::new(ExecutionResourceConfig {
            memory_bytes: 1024,
            max_batch_bytes: 512,
        })
        .unwrap();
        let a = resources.task_context();
        let b = resources.task_context();
        assert!(Arc::ptr_eq(
            &a.runtime_env().memory_pool,
            &b.runtime_env().memory_pool
        ));
        let mut left = MemoryConsumer::new("left").register(&a.runtime_env().memory_pool);
        let mut right = MemoryConsumer::new("right").register(&b.runtime_env().memory_pool);
        left.try_grow(700).unwrap();
        assert!(right.try_grow(400).is_err());
        drop(left);
        right.try_grow(400).unwrap();
        drop(right);
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        assert!(
            resources
                .runtime
                .disk_manager
                .create_tmp_file("unqualified spill")
                .is_err()
        );
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;
    use std::collections::BTreeMap;

    fn measurements(registry: &prometheus::Registry) -> BTreeMap<String, u64> {
        let families = registry.gather();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name(), "arroyo_worker_execution_memory_bytes");
        let values: BTreeMap<_, _> = families[0]
            .get_metric()
            .iter()
            .map(|metric| {
                assert_eq!(metric.get_label().len(), 1);
                let label = &metric.get_label()[0];
                assert_eq!(label.name(), "measurement");
                (
                    label.value().to_owned(),
                    metric.get_gauge().as_ref().unwrap().value() as u64,
                )
            })
            .collect();
        assert_eq!(values.len(), 3);
        values
    }

    fn expected(used: u64, limit: u64, max_batch: u64) -> BTreeMap<String, u64> {
        [("used", used), ("limit", limit), ("max_batch", max_batch)]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect()
    }

    #[test]
    fn registry_scrapes_actual_reservations_without_retaining_runtime_or_pool() {
        let registry = prometheus::Registry::new();
        let metrics = ExecutionPoolMetrics::register(&registry).unwrap();
        assert_eq!(measurements(&registry), expected(0, 0, 0));
        let resources = ExecutionResources::new(ExecutionResourceConfig {
            memory_bytes: 1024,
            max_batch_bytes: 512,
        })
        .unwrap();
        let runtime = Arc::downgrade(&resources.runtime);
        let pool = Arc::downgrade(&resources.runtime.memory_pool);
        metrics.observe(&resources).unwrap();
        let mut reservation =
            MemoryConsumer::new("metric lifetime test").register(&resources.runtime.memory_pool);
        reservation.try_grow(384).unwrap();
        assert_eq!(measurements(&registry), expected(384, 1024, 512));
        assert!(reservation.try_grow(1024).is_err());
        assert_eq!(measurements(&registry), expected(384, 1024, 512));
        reservation.shrink(128);
        assert_eq!(measurements(&registry), expected(256, 1024, 512));
        drop(resources);
        assert!(runtime.upgrade().is_none());
        assert_eq!(measurements(&registry), expected(256, 1024, 512));
        drop(reservation);
        assert!(pool.upgrade().is_none());
        assert_eq!(measurements(&registry), expected(0, 0, 0));
    }

    #[test]
    fn registry_observation_reuses_fixed_series_after_pool_replacement() {
        let registry = prometheus::Registry::new();
        let metrics = ExecutionPoolMetrics::register(&registry).unwrap();
        for memory_bytes in [1024, 2048] {
            let resources = ExecutionResources::new(ExecutionResourceConfig {
                memory_bytes,
                max_batch_bytes: memory_bytes / 2,
            })
            .unwrap();
            metrics.observe(&resources).unwrap();
            metrics.observe(&resources).unwrap();
            let held = resources.reserve_bytes("replacement test", 128).unwrap();
            assert_eq!(
                measurements(&registry),
                expected(128, memory_bytes as u64, (memory_bytes / 2) as u64)
            );
            drop(held);
            drop(resources);
            assert_eq!(measurements(&registry), expected(0, 0, 0));
        }
    }
}
