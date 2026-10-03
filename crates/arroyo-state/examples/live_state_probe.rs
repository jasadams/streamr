//! Native live-state smoke/stress probe, not an operator memory qualification.
//! Run: cargo run -p arroyo-state --example live_state_probe --release -- 128
//! The argument is logical MiB. Each input batch and scan page is bounded; RSS
//! includes process baseline, allocator/native overhead, and concurrent compaction.
use arroyo_state::live::{
    LiveStateBackend, Ownership, ScanRange, ScanRequest, StateKey, StateNamespace,
    lifecycle::RocksStateConfig,
    resources::{ResourceConfig, WorkerStateResources},
    rocks::RocksLiveState,
};
use std::time::Instant;

fn payload(index: usize) -> Vec<u8> {
    let mut random = (index as u64).wrapping_add(0x9e3779b97f4a7c15);
    let mut value = vec![0; 4096];
    for bytes in value.chunks_exact_mut(8) {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        bytes.copy_from_slice(&random.to_le_bytes());
    }
    value
}

fn memory_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn report(
    stage: &str,
    count: usize,
    started: Instant,
    limit_kib: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{stage},{count},{},{},{},{}",
        count * 4096,
        started.elapsed().as_millis(),
        memory_kib("VmRSS:").map_or("unavailable".into(), |v| v.to_string()),
        memory_kib("VmHWM:").map_or("unavailable".into(), |v| v.to_string())
    );
    if memory_kib("VmHWM:").is_some_and(|peak| peak > limit_kib) {
        return Err(format!("process RSS exceeded declared envelope of {limit_kib} KiB").into());
    }
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mib: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "128".into())
        .parse()?;
    let envelope_mib: u64 = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "192".into())
        .parse()?;
    let limit_kib = envelope_mib.checked_mul(1024).ok_or("RSS limit overflow")?;
    let count = mib.checked_mul(256).ok_or("logical size overflow")?;
    let root = tempfile::tempdir()?;
    let resources = WorkerStateResources::for_worker(ResourceConfig {
        block_cache_bytes: 8 << 20,
        memtable_bytes: 4 << 20,
        queued_write_bytes: 1 << 20,
        decoded_value_bytes: 1 << 20,
        scan_page_bytes: 1 << 20,
        max_blocking_operations: 2,
        max_snapshots: 2,
        max_open_databases: 4,
        disk_reserve_bytes: 0,
    })?;
    const OPERATORS: usize = 4;
    const BATCH_ROWS: usize = 32;
    let mut configs = Vec::with_capacity(OPERATORS);
    let mut backends = Vec::with_capacity(OPERATORS);
    for index in 0..OPERATORS {
        let config = RocksStateConfig {
            root: root.path().into(),
            job_id: "probe".into(),
            operator_id: format!("native-{index}"),
            subtask: 0,
            generation: 1,
            attempt: 1,
        };
        backends.push(RocksLiveState::open(config.clone(), resources.clone()).await?);
        configs.push(config);
    }
    let namespace = StateNamespace {
        ownership: Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
        table: b"probe".to_vec(),
    };
    println!("stage,entries,logical_bytes,elapsed_ms,rss_kib,peak_rss_kib");
    let started = Instant::now();
    report("baseline", 0, started, limit_kib)?;
    for base in (0..count).step_by(BATCH_ROWS) {
        let backend = &backends[(base / BATCH_ROWS) % OPERATORS];
        let mut admitted = backend.admitted_batch(136 << 10, BATCH_ROWS).await?;
        for index in base..(base + BATCH_ROWS).min(count) {
            // Vary payloads so compression cannot turn the workload into zeros.
            let value = payload(index);
            let key = StateKey {
                namespace: namespace.clone(),
                key: (index as u64).to_be_bytes().to_vec(),
                routing_hash: None,
            };
            admitted.put(&key, &value)?;
        }
        backend.write_admitted(admitted).await?;
        if (base + BATCH_ROWS).is_multiple_of(4096) {
            report("write", (base + BATCH_ROWS).min(count), started, limit_kib)?;
        }
    }
    report("written", count, started, limit_kib)?;
    for backend in backends {
        backend.close().await?;
    }
    let mut total = 0;
    for (operator, config) in configs.into_iter().enumerate() {
        let backend = RocksLiveState::reopen(config, resources.clone()).await?;
        let snapshot = backend.snapshot().await?;
        let mut cursor = None;
        let mut seen = 0usize;
        loop {
            let page = snapshot
                .scan(ScanRequest {
                    range: ScanRange {
                        namespace: namespace.clone(),
                        prefix: None,
                        start: None,
                        end: None,
                    },
                    max_entries: 16,
                    max_bytes: 128 << 10,
                    cursor,
                })
                .await?;
            for entry in &page.entries {
                let expected =
                    (seen / BATCH_ROWS * OPERATORS + operator) * BATCH_ROWS + seen % BATCH_ROWS;
                assert_eq!(entry.key.key, (expected as u64).to_be_bytes());
                assert_eq!(entry.value, payload(expected));
                seen += 1;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        total += seen;
        drop(snapshot);
        backend.close_and_remove().await?;
    }
    assert_eq!(total, count);
    report("reopened_scanned", total, started, limit_kib)?;
    Ok(())
}
