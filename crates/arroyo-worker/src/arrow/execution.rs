//! Shared cooperative execution limits for the selected milestone 3 paths.
//! Spilling is disabled until its filesystem capacity and lifecycle are qualified.
//! Unsupported allocations fail rather than silently spill outside that contract.
use arroyo_rpc::config::ExecutionResourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(crate) struct ExecutionResources {
    pub runtime: Arc<RuntimeEnv>,
    pub limits: ExecutionResourceConfig,
}

impl ExecutionResources {
    pub fn reserve_batch(
        &self,
        name: &str,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::common::Result<datafusion::execution::memory_pool::MemoryReservation> {
        let bytes = batch.get_array_memory_size();
        if bytes > self.limits.max_batch_bytes {
            return Err(datafusion::common::DataFusionError::ResourcesExhausted(
                format!(
                    "{name} batch requires {bytes} bytes; max-batch-bytes is {}",
                    self.limits.max_batch_bytes,
                ),
            ));
        }
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

/// Live operators share one pool. Reject budget changes while any operator still
/// uses the old pool; otherwise a configuration reload could double admission.
pub(crate) fn configured_execution_resources() -> anyhow::Result<Option<Arc<ExecutionResources>>> {
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
