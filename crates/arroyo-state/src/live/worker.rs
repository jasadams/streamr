//! Worker configuration adapter. No default budget and no per-operator pool.
use super::{
    Result,
    resources::{ResourceConfig, WorkerStateResources},
};

pub fn configured_worker_resources() -> Result<Option<WorkerStateResources>> {
    let Some(config) = &arroyo_rpc::config::config().worker.live_state_resources else {
        return Ok(None);
    };
    let resources = WorkerStateResources::for_worker(ResourceConfig {
        block_cache_bytes: config.block_cache_bytes,
        memtable_bytes: config.memtable_bytes,
        queued_write_bytes: config.queued_write_bytes,
        decoded_value_bytes: config.decoded_value_bytes,
        scan_page_bytes: config.scan_page_bytes,
        max_blocking_operations: config.max_blocking_operations,
        max_snapshots: config.max_snapshots,
        max_open_databases: config.max_open_databases,
        disk_reserve_bytes: config.disk_reserve_bytes,
    })?;
    static METRICS: std::sync::OnceLock<std::result::Result<(), String>> =
        std::sync::OnceLock::new();
    if let Err(error) = METRICS.get_or_init(|| {
        resources
            .register_metrics(prometheus::default_registry())
            .map_err(|e| e.to_string())
    }) {
        return Err(super::LiveStateError::Backend(format!(
            "live-state metrics registration: {error}"
        )));
    }
    Ok(Some(resources))
}

/// Backend selection occurs once at construction. SQL kernels receive only the
/// returned generic handle. Future adapters register here and run conformance.
pub enum BackendConstruction {
    Memory { max_resident_bytes: usize },
    Rocksdb(super::lifecycle::RocksStateConfig),
}

/// Opaque task ownership passed to the backend adapter. SQL operators never
/// select a physical backend or construct storage paths themselves.
pub struct ConfiguredBackendOwner {
    pub job_id: String,
    pub operator_id: String,
    pub subtask: u32,
    pub generation: u64,
}

/// Select the configured backend in one place for every native SQL owner.
/// Physical adapters are still implemented by `construct_backend`; adding a
/// backend does not add branches to aggregate/window/table SQL operators.
pub async fn construct_configured_backend(
    owner: ConfiguredBackendOwner,
    max_resident_bytes: usize,
    resources: WorkerStateResources,
) -> Result<std::sync::Arc<dyn super::LiveStateBackend>> {
    use arroyo_rpc::config::SqlStateBackend;
    let configured = arroyo_rpc::config::config();
    let worker = &configured.worker;
    let construction = match worker.sql_state_backend {
        SqlStateBackend::Memory => BackendConstruction::Memory { max_resident_bytes },
        SqlStateBackend::Rocksdb => {
            let disk = worker.disk_sql_state.as_ref().ok_or_else(|| {
                super::LiveStateError::Backend(
                    "configured RocksDB SQL state requires worker.disk-sql-state".into(),
                )
            })?;
            BackendConstruction::Rocksdb(super::lifecycle::RocksStateConfig {
                root: disk.directory.join(uuid::Uuid::new_v4().to_string()),
                job_id: owner.job_id,
                operator_id: owner.operator_id,
                subtask: owner.subtask,
                generation: owner.generation,
                attempt: 0,
            })
        }
    };
    construct_backend(construction, resources).await
}

pub async fn construct_backend(
    construction: BackendConstruction,
    resources: WorkerStateResources,
) -> Result<std::sync::Arc<dyn super::LiveStateBackend>> {
    match construction {
        BackendConstruction::Memory { max_resident_bytes } => Ok(std::sync::Arc::new(
            super::memory::MemoryLiveState::bounded(resources, max_resident_bytes)?,
        )),
        BackendConstruction::Rocksdb(config) => {
            let backend =
                super::rocks::RocksLiveState::open_worker_with_resources(config, resources).await?;
            backend.remove_on_drop();
            Ok(std::sync::Arc::new(backend))
        }
    }
}
