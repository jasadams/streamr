use crate::tables::disk_keyed_map::DiskKeyedTable;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::{BackingStore, StorageProviderFor, get_storage_provider};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableCheckpointMetadata,
};
use arroyo_types::CheckpointFilePathLayout;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info, warn};

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

pub struct ParquetBackend;

/// Only the existing cleanup operations. The private seam permits failure tests
/// against a real filesystem without changing configured storage or GC policy.
#[async_trait::async_trait]
trait CleanupStore: Sync {
    async fn load_operator(
        &self,
        job: &str,
        operator: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError>;
    async fn delete(&self, path: &str) -> Result<(), StateError>;
    async fn prune_empty_directories(&self, prefix: &str) -> Result<(), StateError>;
    async fn publish(&self, metadata: CheckpointMetadata) -> Result<(), StateError>;
    fn qualified_prefix(&self, prefix: &str) -> String;
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<
        futures::stream::BoxStream<'_, Result<object_store::path::Path, object_store::Error>>,
        StateError,
    >;
}

#[async_trait::async_trait]
impl CleanupStore for arroyo_storage::StorageProvider {
    async fn load_operator(
        &self,
        job: &str,
        operator: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError> {
        self.get_if_present(metadata_path(&operator_path(job, epoch, operator)).as_str())
            .await?
            .map(|bytes| OperatorCheckpointMetadata::decode(bytes.as_ref()).map_err(Into::into))
            .transpose()
    }

    async fn delete(&self, path: &str) -> Result<(), StateError> {
        self.delete_if_present(path).await?;
        Ok(())
    }

    async fn prune_empty_directories(&self, prefix: &str) -> Result<(), StateError> {
        arroyo_storage::StorageProvider::prune_empty_directories(self, prefix.into()).await?;
        Ok(())
    }

    async fn publish(&self, metadata: CheckpointMetadata) -> Result<(), StateError> {
        self.put(
            metadata_path(&base_path(&metadata.job_id, metadata.epoch)),
            metadata.encode_to_vec(),
        )
        .await?;
        Ok(())
    }

    fn qualified_prefix(&self, prefix: &str) -> String {
        self.qualify_path(&prefix.into()).to_string()
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<
        futures::stream::BoxStream<'_, Result<object_store::path::Path, object_store::Error>>,
        StateError,
    > {
        Ok(
            arroyo_storage::StorageProvider::list_prefix(self, prefix.into())
                .await?
                .boxed(),
        )
    }
}

async fn cleanup_checkpoint_files<S: CleanupStore + ?Sized>(
    storage: &S,
    mut metadata: CheckpointMetadata,
    old_min_epoch: u32,
    min_epoch: u32,
) -> Result<(), StateError> {
    info!(
        message = "Cleaning checkpoint",
        min_epoch,
        job_id = metadata.job_id
    );
    let mut futures: FuturesUnordered<_> = metadata
        .operator_ids
        .iter()
        .map(|operator| {
            ParquetBackend::cleanup_operator_files(
                storage,
                metadata.job_id.clone(),
                operator.clone(),
                old_min_epoch,
                min_epoch,
            )
        })
        .collect();
    // Move each existing retained-path set, including empty declared owners.
    // This is bounded by the selected graph/retained descriptors, not historical
    // filesystem objects. No checkpoint payload or path inventory is copied.
    let mut retained = HashMap::with_capacity(metadata.operator_ids.len());
    while let Some(result) = futures.next().await {
        let (operator, paths) = result?;
        retained.insert(operator, paths);
    }
    cleanup_abandoned_controller_job_disk_files(storage, &metadata.job_id, min_epoch, &retained)
        .await?;
    // Preserve all old metadata if known-file cleanup or orphan traversal fails.
    // After this point deletion can partially complete; retry is idempotent and
    // the retained minimum's descriptors remain outside this deletion range.
    for operator in &metadata.operator_ids {
        for epoch in old_min_epoch..min_epoch {
            storage
                .delete(&metadata_path(&operator_path(
                    &metadata.job_id,
                    epoch,
                    operator,
                )))
                .await?;
        }
        debug!(
            message = "Finished cleaning operator",
            job_id = metadata.job_id,
            operator_id = operator,
            min_epoch
        );
    }
    for epoch in old_min_epoch..min_epoch {
        storage
            .delete(&metadata_path(&base_path(&metadata.job_id, epoch)))
            .await?;
    }
    metadata.min_epoch = min_epoch;
    storage.publish(metadata).await
}

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{job_id}/checkpoints/checkpoint-{epoch:0>7}")
}

fn metadata_path(path: &str) -> String {
    format!("{path}/metadata")
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    async fn load_checkpoint_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        epoch: u32,
    ) -> Result<CheckpointMetadata, StateError> {
        let storage_client = get_storage_provider(role).await?;
        let data = storage_client
            .get(metadata_path(&base_path(job_id, epoch)).as_str())
            .await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        Ok(metadata)
    }

    async fn load_operator_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError> {
        let storage_client = get_storage_provider(role).await?;
        storage_client
            .get_if_present(metadata_path(&operator_path(job_id, epoch, operator_id)).as_str())
            .await?
            .map(|data| Ok(OperatorCheckpointMetadata::decode(&data[..])?))
            .transpose()
    }

    async fn write_operator_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<(), StateError> {
        let storage_client = get_storage_provider(role).await?;
        let operator_metadata =
            metadata
                .operator_metadata
                .as_ref()
                .ok_or_else(|| StateError::Other {
                    table: "".to_string(),
                    error: "missing operator metadata".to_string(),
                })?;
        let path = metadata_path(&operator_path(
            &operator_metadata.job_id,
            operator_metadata.epoch,
            &operator_metadata.operator_id,
        ));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        // TODO: propagate error
        Ok(())
    }

    async fn write_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: CheckpointMetadata,
    ) -> Result<(), StateError> {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider(role).await?;
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        Ok(())
    }

    async fn cleanup_checkpoint(
        role: &StorageProviderFor,
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
    ) -> Result<(), StateError> {
        let storage = get_storage_provider(role).await?;
        cleanup_checkpoint_files(storage.as_ref(), metadata, old_min_epoch, min_epoch).await
    }
}

impl ParquetBackend {
    /// Called after a checkpoint is committed
    pub async fn compact_operator(
        role: &StorageProviderFor,
        job_id: Arc<String>,
        operator_id: &str,
        epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>, StateError> {
        let min_files_to_compact = config().pipeline.compaction.checkpoints_to_compact as usize;

        let operator_checkpoint_metadata =
            Self::load_operator_metadata(role, &job_id, operator_id, epoch)
                .await?
                .expect("expect operator metadata to still be present");
        let storage_provider = get_storage_provider(role).await?;
        let compaction_config = CompactionConfig {
            compact_generations: vec![0].into_iter().collect(),
            min_compaction_epochs: min_files_to_compact,
            storage_provider: Arc::clone(&storage_provider),
            file_path_layout: CheckpointFilePathLayout::Legacy,
        };
        let operator_metadata = operator_checkpoint_metadata.operator_metadata.unwrap();

        let mut result = HashMap::new();

        for (table, table_metadata) in operator_checkpoint_metadata.table_checkpoint_metadata {
            let table_config = operator_checkpoint_metadata
                .table_configs
                .get(&table)
                .unwrap()
                .clone();
            if let Some(compacted_metadata) = match table_metadata.table_type() {
                rpc::TableEnum::MissingTableType => {
                    return Err(StateError::Other {
                        table: table.clone(),
                        error: "should have table type".to_string(),
                    });
                }
                rpc::TableEnum::DiskKeyedMap => None,
                rpc::TableEnum::TypedStateTable => None,
                rpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
            } {
                result.insert(table, compacted_metadata);
            }
        }
        Ok(result)
    }

    /// Delete known obsolete files; the job owner performs orphan enumeration.
    async fn cleanup_operator_files<S: CleanupStore + ?Sized>(
        storage: &S,
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<(String, HashSet<String>), StateError> {
        let operator_metadata = storage
            .load_operator(&job_id, &operator_id, new_min_epoch)
            .await?
            .expect("expect new_min_epoch metadata to still be present");
        let mut paths_to_keep = HashSet::new();
        for (table_name, metadata) in &operator_metadata.table_checkpoint_metadata {
            let table_config = operator_metadata
                .table_configs
                .get(table_name)
                .ok_or_else(|| StateError::Other {
                    table: table_name.clone(),
                    error: "missing retained checkpoint table configuration".into(),
                })?
                .clone();
            let files = match table_config.table_type() {
                rpc::TableEnum::MissingTableType => {
                    return Err(StateError::Other {
                        table: table_name.clone(),
                        error: "missing retained checkpoint table type".into(),
                    });
                }
                rpc::TableEnum::DiskKeyedMap => {
                    DiskKeyedTable::files_to_keep(table_config, metadata.clone())?
                }
                rpc::TableEnum::TypedStateTable => {
                    arroyo_state_protocol::typed_checkpoint::files_to_keep(
                        table_config,
                        metadata.clone(),
                    )
                    .map_err(|error| StateError::Other {
                        table: table_name.clone(),
                        error,
                    })?
                }
                rpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::files_to_keep(table_config, metadata.clone())?
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone())?
                }
            };
            paths_to_keep.extend(files);
        }

        let mut deleted_paths = HashSet::new();

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(operator_metadata) = storage
                .load_operator(&job_id, &operator_id, epoch_to_remove)
                .await?
            else {
                continue;
            };

            // delete any files that are not in the new min epoch
            let mut files = HashSet::new();
            for (table_name, metadata) in operator_metadata.table_checkpoint_metadata.iter() {
                let table_config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .ok_or_else(|| StateError::Other {
                        table: table_name.clone(),
                        error: format!("missing table config for operator {operator_id}, table {table_name}, metadata is {metadata:?}, operator_metadata is {operator_metadata:?}")
                    })?
                    .clone();

                files.extend(match table_config.table_type() {
                    rpc::TableEnum::MissingTableType => {
                        warn!("found table without table type: {:?}", table_name);
                        HashSet::new()
                    }
                    rpc::TableEnum::DiskKeyedMap => {
                        DiskKeyedTable::files_to_keep(table_config, metadata.clone())?
                    }
                    rpc::TableEnum::TypedStateTable => {
                        arroyo_state_protocol::typed_checkpoint::files_to_keep(
                            table_config,
                            metadata.clone(),
                        )
                        .map_err(|error| StateError::Other {
                            table: table_name.clone(),
                            error,
                        })?
                    }
                    rpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::files_to_keep(table_config, metadata.clone())?
                    }
                    rpc::TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone())?
                    }
                });
            }

            for file in files {
                if !paths_to_keep.contains(&file) && !deleted_paths.contains(&file) {
                    deleted_paths.insert(file.clone());
                    storage.delete(&file).await?;
                }
            }
        }

        Ok((operator_id, paths_to_keep))
    }
}

#[derive(Debug)]
pub struct ParquetStats {
    pub max_timestamp: SystemTime,
    pub min_routing_key: u64,
    pub max_routing_key: u64,
}

impl Default for ParquetStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl ParquetStats {
    pub fn merge(&mut self, other: ParquetStats) {
        self.max_timestamp = self.max_timestamp.max(other.max_timestamp);
        self.min_routing_key = self.min_routing_key.min(other.min_routing_key);
        self.max_routing_key = self.max_routing_key.max(other.max_routing_key);
    }
}

/// Full logical disk pages have exclusive epoch ownership. Once the controller
/// advances its retained minimum, pages in older epochs cannot be referenced by
/// a retained checkpoint or an active publication, including pages uploaded by
/// workers that crashed before reporting completion.
async fn cleanup_abandoned_controller_job_disk_files<S: CleanupStore + ?Sized>(
    storage: &S,
    job_id: &str,
    retained_min_epoch: u32,
    retained_by_operator: &HashMap<String, HashSet<String>>,
) -> Result<(), StateError> {
    use futures::TryStreamExt;
    let namespace = format!("{job_id}/checkpoints/");
    let qualified = storage.qualified_prefix(&namespace);
    let prefix = format!("{}/", qualified.trim_end_matches('/'));
    // Remove historical local empty trees before the complete late-upload scan.
    // Files deleted below (or by metadata cleanup) can leave empties until the
    // next pass; no directory inventory or old-epoch exclusion survives a pass.
    storage.prune_empty_directories(&namespace).await?;
    let listing = storage.list_prefix(&namespace).await?;
    futures::pin_mut!(listing);
    while let Some(object) = listing
        .try_next()
        .await
        .map_err(arroyo_rpc::errors::StorageError::from)?
    {
        let object = object.to_string();
        let Some(suffix) = object.strip_prefix(&prefix) else {
            continue;
        };
        let parts: Vec<_> = suffix.split('/').collect();
        if parts.len() != 4 || !parts[2].starts_with("table-") || !parts[3].starts_with("disk-") {
            continue;
        }
        let Some(retained_files) = parts[1]
            .strip_prefix("operator-")
            .and_then(|operator| retained_by_operator.get(operator))
        else {
            continue;
        };
        let Some(epoch) = parts[0].strip_prefix("checkpoint-") else {
            continue;
        };
        let Ok(epoch) = epoch.parse::<u32>() else {
            continue;
        };
        if epoch < retained_min_epoch && !retained_files.contains(&format!("{namespace}{suffix}")) {
            storage.delete(&format!("{namespace}{suffix}")).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod disk_cleanup_tests;
