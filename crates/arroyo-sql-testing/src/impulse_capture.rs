//! Observation-only helpers for the finite native Impulse capture route.
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::MetadataOrManifest;
use arroyo_state::{
    BackingStore, StateBackend, StorageProviderFor, tables::table_manager::TableManager,
};
use arroyo_worker::engine::{Program, SubtaskOrQueueNode};
use std::{
    path::Path,
    time::{Duration, SystemTime},
};

pub async fn wait_for_rows(path: &Path, minimum: usize, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let bytes = match tokio::fs::read(path).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => panic!(
                    "cannot observe finite Impulse output {}: {error}",
                    path.display()
                ),
            };
            if bytes.iter().filter(|byte| **byte == b'\n').count() >= minimum {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("finite Impulse did not reach checkpoint prefix");
}

/// Restore an independent read-only view of the selected source table. This
/// leaves the actual reconstructed source's cache and behavior untouched. The
/// tuple has the same bincode field sequence as ImpulseSourceState; no alternate
/// source state format or source clock is installed.
pub async fn record_saved_source_state(program: &Program, job: &str, epoch: u32, output: &Path) {
    let (task, tables, control_tx) = {
        let graph = program.graph.read().unwrap();
        graph
            .node_weights()
            .find_map(|node| {
                if let SubtaskOrQueueNode::SubtaskNode(node) = node
                    && let OperatorNode::Source(source) = &node.node
                    && source.operator.name() == "impulse-source"
                {
                    Some((
                        source.context.task_info.clone(),
                        source.operator.tables(),
                        source.context.control_tx.clone(),
                    ))
                } else {
                    None
                }
            })
            .expect("missing reconstructed Impulse source")
    };
    let metadata = StateBackend::load_checkpoint_metadata(&StorageProviderFor::Worker, job, epoch)
        .await
        .unwrap();
    let (mut reader, _) = TableManager::load(
        task.clone(),
        tables,
        control_tx,
        Some(&MetadataOrManifest::Metadata(metadata)),
    )
    .await
    .unwrap();
    let saved = reader
        .get_global_keyed_state::<u32, (usize, SystemTime)>("i")
        .await
        .unwrap()
        .get(&task.task_index)
        .copied()
        .expect("Impulse checkpoint state missing");
    let value = serde_json::json!({
        "epoch": epoch, "counter": saved.0,
        "start_nanos": saved.1.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos().to_string(),
    });
    tokio::fs::write(
        output.with_extension("impulse-state.json"),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .await
    .unwrap();
    println!("CAPTURE_IMPULSE_STATE {value}");
}
