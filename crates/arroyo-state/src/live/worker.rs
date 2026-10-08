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
