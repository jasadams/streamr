use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, DiskKeyedTableTaskCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use prost::Message;
use std::collections::HashSet;
use std::path::Path;
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct CheckpointOwner {
    pub generation: Generation,
    pub epoch: Epoch,
}

pub async fn cleanup_leader_checkpoints<S>(
    store: &S,
    paths: &ProtocolPaths,
    head: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let head_manifest: CheckpointManifest = read_protobuf(store, &head)
        .await?
        .ok_or_else(|| StoreError::ExistingObjectMissing { path: head.clone() })?;
    if *new_min_epoch > head_manifest.epoch {
        return Err(StoreError::InvalidProtobuf {
            path: head,
            msg: "retention minimum would prune the active checkpoint head".into(),
        });
    }
    let history =
        collect_history_and_clean_checkpoint_files(store, paths, head, new_min_epoch).await?;

    for c in history.iter().rev() {
        debug!(
            generation = c.generation.0,
            epoch = c.epoch.0,
            "cleaning checkpoint"
        );

        let path = paths.checkpoint_manifest(c.generation, c.epoch);
        store.delete_object(&path).await?;

        let dir = paths.checkpoint_dir(c.generation, c.epoch);
        store.delete_directory(dir.as_str()).await;
    }

    Ok(())
}

/// Traverses backwards through the checkpoint history, doing two things:
/// 1. Finding and cleaning data files for GC'able checkpoints
/// 2. Collecting a record of ownership for each checkpoint
///
/// We do it in this order because we must delete metadata files by going forward, to prevent
/// gaps from forming in case of failure which would orphan older files. However, we do not
/// want to re-read or retain the metadata in order to do file deletion as that could exhaust
/// memory, so we opportunistically delete data files.
async fn collect_history_and_clean_checkpoint_files<S>(
    store: &S,
    paths: &ProtocolPaths,
    current: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<Vec<CheckpointOwner>, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut history = Vec::new();
    let mut seen = HashSet::new();
    let mut next = Some(current);

    while let Some(checkpoint_ref) = next {
        // TODO: use a metadata cache here so we're not re-reading checkpoints we've just written
        let Some(manifest): Option<CheckpointManifest> =
            read_protobuf(store, &checkpoint_ref).await?
        else {
            // `history` contains only prunable checkpoints; a retained head can
            // have an already deleted parent on an idempotent cleanup retry.
            if seen.is_empty() {
                return Err(StoreError::ExistingObjectMissing {
                    path: checkpoint_ref,
                });
            }
            break;
        };

        let owner = CheckpointOwner {
            generation: Generation(manifest.generation),
            epoch: Epoch(manifest.epoch),
        };

        if !seen.insert(owner) {
            return Err(ProtocolError::CheckpointCycle {
                generation: owner.generation,
                epoch: owner.epoch,
            }
            .into());
        }

        if manifest.epoch < *new_min_epoch {
            history.push(owner);
            clean_checkpoint(store, paths, &checkpoint_ref, &manifest).await?;
        }

        next = manifest
            .parent_checkpoint_ref
            .map(CheckpointRef::new)
            .transpose()?;
    }

    Ok(history)
}

/// cleans a checkpoint but leaves the metadata file in place
async fn clean_checkpoint<S>(
    store: &S,
    paths: &ProtocolPaths,
    manifest_path: &CheckpointRef,
    checkpoint: &CheckpointManifest,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    crate::disk::validate_manifest(checkpoint).map_err(|msg| StoreError::InvalidProtobuf {
        path: manifest_path.clone(),
        msg,
    })?;
    if !crate::ready::preserve(store, paths, manifest_path, checkpoint).await? {
        return Err(StoreError::InvalidProtobuf {
            path: manifest_path.clone(),
            msg: "refusing to prune a checkpoint without canonical commit completion".into(),
        });
    }
    let mut to_delete = vec![];
    for operator in &checkpoint.operators {
        for (table_name, metadata) in &operator.table_checkpoint_metadata {
            let op_metadata =
                operator
                    .operator_metadata
                    .as_ref()
                    .ok_or_else(|| StoreError::InvalidProtobuf {
                        path: manifest_path.clone(),
                        msg: "missing OperatorMetadata field".to_string(),
                    })?;

            table_checkpoint_data_files(
                &op_metadata.operator_id,
                table_name,
                manifest_path,
                metadata,
                &mut to_delete,
            )?;
        }
    }

    let directories: HashSet<_> = to_delete
        .iter()
        .filter_map(|f| Path::new(f.as_str()).parent().and_then(|p| p.to_str()))
        .map(|f| f.to_string())
        .collect();

    // Canonical claims and commit completion are small immutable fences. Keep
    // them so stale publishers cannot recreate pruned epochs or replay a commit.

    for file in &to_delete {
        store.delete_object(file).await?;
    }

    for d in directories {
        // this will loop over duplicate directory when there are multiples tables per operator,
        // but it's a no-op if the directory is already deleted
        store.delete_directory(&d).await;
    }

    Ok(())
}

fn table_checkpoint_data_files(
    operator_id: &str,
    table_name: &str,
    metadata_path: &CheckpointRef,
    metadata: &TableCheckpointMetadata,
    files: &mut Vec<CheckpointRef>,
) -> Result<(), StoreError> {
    match metadata.table_type() {
        TableEnum::MissingTableType => {
            return Err(StoreError::InvalidProtobuf {
                path: metadata_path.clone(),
                msg: format!(
                    "table metadata for operator '{}' table '{}' is missing table type",
                    operator_id, table_name
                ),
            });
        }
        TableEnum::DiskKeyedMap => {
            let metadata = DiskKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .map_err(|source| StoreError::DecodeProtobuf {
                path: metadata_path.clone(),
                source,
            })?;
            for subtask in metadata.subtasks.into_values() {
                for file in subtask.files {
                    files.push(CheckpointRef::new(file.path)?);
                }
            }
        }
        TableEnum::TypedStateTable => {
            let metadata = arroyo_rpc::grpc::rpc::TypedStateTableTaskCheckpointMetadata::decode(
                metadata.data.as_slice(),
            )
            .map_err(|source| StoreError::DecodeProtobuf {
                path: metadata_path.clone(),
                source,
            })?;
            for subtask in metadata.subtasks.into_values() {
                for file in subtask.files {
                    files.push(CheckpointRef::new(file.path)?);
                }
            }
        }
        TableEnum::GlobalKeyValue => {
            let metadata = GlobalKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .map_err(|e| StoreError::DecodeProtobuf {
                    path: metadata_path.clone(),
                    source: e,
                })?;

            for file in metadata.files {
                files.push(CheckpointRef::new(file.clone())?);
            }
        }
        TableEnum::ExpiringKeyedTimeTable => {
            return Err(StoreError::InvalidProtobuf {
                path: metadata_path.clone(),
                msg: format!(
                    "table metadata for operator '{}' table '{}' has table type \
                ExpiringKeyedTimeTable, which is not yet supported in leader mode",
                    operator_id, table_name
                ),
            });
        }
    }

    Ok(())
}

/// Deletes abandoned uploads after a newer generation is fenced in and closure
/// atomically excludes this checkpoint from publication. Legacy generations
/// without a closure log require a different canonical owner for the same epoch.
/// Callers supply bounded listings; files outside the exclusive owner are rejected.
pub async fn cleanup_abandoned_disk_uploads<S: ProtocolStore + ?Sized>(
    store: &S,
    paths: &ProtocolPaths,
    generation: Generation,
    epoch: Epoch,
    files: &[CheckpointRef],
) -> Result<bool, StoreError> {
    use crate::store::read_json;
    use crate::types::{CurrentGeneration, EpochRecord, GenerationManifest};
    let Some(current): Option<CurrentGeneration> =
        read_json(store, &paths.current_generation()).await?
    else {
        return Ok(false);
    };
    if current.generation <= generation {
        return Ok(false);
    }
    let owner = paths.checkpoint_manifest(generation, epoch);
    let record: Option<EpochRecord> = read_json(store, &paths.epoch_record(epoch)).await?;
    if record
        .as_ref()
        .is_some_and(|record| record.checkpoint_ref == owner)
    {
        return Ok(false);
    }
    let old_manifest: Option<GenerationManifest> =
        read_json(store, &paths.generation_manifest(generation)).await?;
    let excluded_by_closure =
        if let Some(manifest) = old_manifest.filter(|m| m.publication_log_version != 0) {
            !crate::publication::close(store, &manifest, Some(&owner))
                .await?
                .target_published
        } else {
            false
        };
    if !excluded_by_closure && record.is_none() {
        return Ok(false);
    }
    let Some(mut active): Option<GenerationManifest> =
        read_json(store, &current.generation_manifest_ref).await?
    else {
        return Ok(false);
    };
    if active.publication_log_version != 0 {
        let frontier = crate::publication::inspect(store, &active, None).await?;
        active.base_checkpoint_ref = frontier.base_checkpoint_ref;
        active.latest_checkpoint_ref = frontier.latest_checkpoint_ref;
    }
    if active.base_checkpoint_ref.as_ref() == Some(&owner)
        || active.latest_checkpoint_ref.as_ref() == Some(&owner)
    {
        return Ok(false);
    }
    let prefix = format!("{}/operator-", paths.checkpoint_dir(generation, epoch));
    for file in files {
        if !file.as_str().starts_with(&prefix) || !file.as_str().contains("/disk-") {
            return Err(StoreError::InvalidProtobuf {
                path: owner,
                msg: "abandoned upload is outside its exclusive disk checkpoint namespace".into(),
            });
        }
    }
    for file in files {
        store.delete_object(file).await?;
    }
    Ok(true)
}

/// Reconciles abandoned logical pages with a streaming listing. Each candidate
/// uses the fenced, canonical-owner check above, so a list racing publication
/// cannot delete the winning checkpoint or an upload authorized by its log.
pub async fn reconcile_abandoned_disk_uploads(
    storage: &arroyo_storage::StorageProvider,
    paths: &ProtocolPaths,
) -> Result<(), StoreError> {
    use futures::TryStreamExt;
    let namespace = format!("{}/{}/generations/", paths.pipeline_id(), paths.job_id());
    let qualified = storage.qualify_path(&namespace.as_str().into()).to_string();
    let prefix = format!("{}/", qualified.trim_end_matches('/'));
    let listing = storage.list(true).await?;
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
        if parts.len() != 6
            || parts[1] != "checkpoints"
            || !parts[3].starts_with("operator-")
            || !parts[4].starts_with("table-")
            || !parts[5].starts_with("disk-")
        {
            continue;
        }
        let (Ok(generation), Some(epoch)) = (
            parts[0].parse::<u64>(),
            parts[2].strip_prefix("checkpoint-"),
        ) else {
            continue;
        };
        let Ok(epoch) = epoch.parse::<u64>() else {
            continue;
        };
        let file = CheckpointRef::new(format!("{namespace}{suffix}"))?;
        cleanup_abandoned_disk_uploads(
            storage,
            paths,
            Generation(generation),
            Epoch(epoch),
            &[file],
        )
        .await?;
    }
    // Only after the complete page pass succeeds may we discard unlogged
    // manifests. A failed deletion leaves their ownership metadata for retry.
    let manifests = storage.list(true).await?;
    futures::pin_mut!(manifests);
    while let Some(object) = manifests
        .try_next()
        .await
        .map_err(arroyo_rpc::errors::StorageError::from)?
    {
        let object = object.to_string();
        let Some(suffix) = object.strip_prefix(&prefix) else {
            continue;
        };
        let parts: Vec<_> = suffix.split('/').collect();
        if parts.len() != 4 || parts[1] != "checkpoints" || parts[3] != "checkpoint-manifest.pb" {
            continue;
        }
        let (Ok(generation), Some(epoch)) = (
            parts[0].parse::<u64>(),
            parts[2].strip_prefix("checkpoint-"),
        ) else {
            continue;
        };
        let Ok(epoch) = epoch.parse::<u64>() else {
            continue;
        };
        let generation = Generation(generation);
        let epoch = Epoch(epoch);
        let owner = paths.checkpoint_manifest(generation, epoch);
        let Some(manifest): Option<crate::types::GenerationManifest> =
            crate::store::read_json(storage, &paths.generation_manifest(generation)).await?
        else {
            continue;
        };
        // Legacy generations have no terminal publication proof; retain their
        // manifest even when their disk uploads have a conflicting epoch owner.
        if manifest.publication_log_version == 0 {
            continue;
        }
        if cleanup_abandoned_disk_uploads(storage, paths, generation, epoch, &[]).await?
            && !crate::publication::inspect(storage, &manifest, Some(&owner))
                .await?
                .target_published
        {
            storage.delete_object(&owner).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::MemoryProtocolStore;
    use crate::store::{CreateResult, put_protobuf};
    use arroyo_rpc::grpc::rpc::{
        DiskCheckpointFile, DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
        DiskKeyedTableTaskCheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata,
        TableConfig,
    };
    use arroyo_types::{JobId, PipelineId};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailOnce {
        inner: MemoryProtocolStore,
        fail: AtomicBool,
    }
    #[async_trait::async_trait]
    impl ProtocolStore for FailOnce {
        async fn read_bytes(&self, p: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
            self.inner.read_bytes(p).await
        }
        async fn put_bytes(&self, p: &CheckpointRef, b: Vec<u8>) -> Result<(), StoreError> {
            self.inner.put_bytes(p, b).await
        }
        async fn create_bytes(
            &self,
            p: &CheckpointRef,
            b: Vec<u8>,
        ) -> Result<CreateResult<Vec<u8>>, StoreError> {
            self.inner.create_bytes(p, b).await
        }
        async fn delete_object(&self, p: &CheckpointRef) -> Result<(), StoreError> {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(StoreError::InvalidProtobuf {
                    path: p.clone(),
                    msg: "injected deletion interruption".into(),
                });
            }
            self.inner.delete_object(p).await
        }
        async fn delete_directory(&self, p: &str) {
            self.inner.delete_directory(p).await
        }
    }
    fn checkpoint(epoch: u32, parent: Option<String>) -> (CheckpointManifest, CheckpointRef) {
        let path = CheckpointRef::new(format!(
            "P/J/generations/1/checkpoints/checkpoint-{epoch:07}/operator-o/table-m-000/disk-0.bin"
        ))
        .unwrap();
        let mut namespace = vec![1, 0];
        namespace.extend_from_slice(&0u32.to_be_bytes());
        namespace.extend_from_slice(&1u32.to_be_bytes());
        namespace.extend_from_slice(&1u32.to_be_bytes());
        namespace.push(b'm');
        let metadata = DiskKeyedTableTaskCheckpointMetadata {
            format_version: 1,
            subtasks: [(
                0,
                DiskKeyedTableSubtaskCheckpointMetadata {
                    subtask_index: 0,
                    format_version: 1,
                    encoding_version: 1,
                    schema_identity: vec![1],
                    namespace,
                    generation: 1,
                    epoch,
                    empty: false,
                    files: vec![DiskCheckpointFile {
                        path: path.to_string(),
                        size_bytes: 1,
                        row_count: 1,
                        checksum: vec![0; 32],
                    }],
                },
            )]
            .into(),
        };
        let config = DiskKeyedTableConfig {
            table_name: "m".into(),
            encoding_version: 1,
            schema_identity: vec![1],
        };
        let manifest = CheckpointManifest {
            pipeline_id: "P".into(),
            job_id: "J".into(),
            generation: 1,
            epoch: u64::from(epoch),
            parent_checkpoint_ref: parent,
            operators: vec![OperatorCheckpointMetadata {
                operator_metadata: Some(OperatorMetadata {
                    job_id: "J".into(),
                    operator_id: "o".into(),
                    epoch,
                    parallelism: 1,
                    ..Default::default()
                }),
                table_configs: [(
                    "m".into(),
                    TableConfig {
                        table_type: TableEnum::DiskKeyedMap.into(),
                        config: config.encode_to_vec(),
                        state_version: 1,
                    },
                )]
                .into(),
                table_checkpoint_metadata: [(
                    "m".into(),
                    TableCheckpointMetadata {
                        table_type: TableEnum::DiskKeyedMap.into(),
                        data: metadata.encode_to_vec(),
                    },
                )]
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        (manifest, path)
    }
    #[tokio::test]
    async fn retained_disk_checkpoint_survives_interrupted_cleanup_and_retry() {
        let store = FailOnce {
            inner: MemoryProtocolStore::default(),
            fail: AtomicBool::new(true),
        };
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let first_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let second_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let (first, first_file) = checkpoint(1, None);
        let (second, second_file) = checkpoint(2, Some(first_ref.to_string()));
        put_protobuf(&store, &first_ref, &first).await.unwrap();
        put_protobuf(&store, &second_ref, &second).await.unwrap();
        for (reference, checkpoint) in [(&first_ref, &first), (&second_ref, &second)] {
            let record = crate::types::EpochRecord::for_checkpoint(
                PipelineId::new("P"),
                Generation(1),
                reference.clone(),
                checkpoint,
                std::time::SystemTime::UNIX_EPOCH,
            )
            .unwrap();
            crate::store::put_json(
                &store,
                &paths.epoch_record(Epoch(checkpoint.epoch)),
                &record,
            )
            .await
            .unwrap();
        }
        store.put_bytes(&first_file, vec![1]).await.unwrap();
        store.put_bytes(&second_file, vec![2]).await.unwrap();
        assert!(
            cleanup_leader_checkpoints(&store, &paths, second_ref.clone(), Epoch(2))
                .await
                .is_err()
        );
        assert!(store.read_bytes(&first_ref).await.unwrap().is_some());
        assert_eq!(store.read_bytes(&second_file).await.unwrap(), Some(vec![2]));
        cleanup_leader_checkpoints(&store, &paths, second_ref.clone(), Epoch(2))
            .await
            .unwrap();
        assert!(store.read_bytes(&first_file).await.unwrap().is_none());
        assert!(store.read_bytes(&first_ref).await.unwrap().is_none());
        assert!(store.read_bytes(&second_ref).await.unwrap().is_some());
        assert_eq!(store.read_bytes(&second_file).await.unwrap(), Some(vec![2]));
        cleanup_leader_checkpoints(&store, &paths, second_ref.clone(), Epoch(2))
            .await
            .unwrap();
        let mut retained = crate::types::GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            None,
            0,
        );
        retained.latest_checkpoint_ref = Some(second_ref.clone());
        assert_eq!(
            crate::workflow::resolve_generation_manifest(&store, &retained, Generation(1))
                .await
                .unwrap(),
            crate::workflow::GenerationResolution::Ready {
                checkpoint_ref: second_ref
            }
        );
    }
    #[tokio::test]
    async fn abandoned_upload_cleanup_requires_fencing_and_another_canonical_owner() {
        use crate::store::put_json;
        use crate::types::{CurrentGeneration, EpochRecord, GenerationManifest};
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let (mut canonical, abandoned_file) = checkpoint(1, None);
        store.put_bytes(&abandoned_file, vec![1]).await.unwrap();
        let current = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            std::time::SystemTime::UNIX_EPOCH,
        );
        put_json(&store, &paths.current_generation(), &current)
            .await
            .unwrap();
        let active = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            None,
            0,
        );
        put_json(&store, &paths.generation_manifest(Generation(2)), &active)
            .await
            .unwrap();
        assert!(
            !cleanup_abandoned_disk_uploads(
                &store,
                &paths,
                Generation(1),
                Epoch(1),
                std::slice::from_ref(&abandoned_file)
            )
            .await
            .unwrap()
        );
        let old_record = EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(1),
            paths.checkpoint_manifest(Generation(1), Epoch(1)),
            &canonical,
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        put_json(&store, &paths.epoch_record(Epoch(1)), &old_record)
            .await
            .unwrap();
        assert!(
            !cleanup_abandoned_disk_uploads(
                &store,
                &paths,
                Generation(1),
                Epoch(1),
                std::slice::from_ref(&abandoned_file)
            )
            .await
            .unwrap()
        );
        canonical.generation = 2;
        let record = EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(2),
            paths.checkpoint_manifest(Generation(2), Epoch(1)),
            &canonical,
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        put_json(&store, &paths.epoch_record(Epoch(1)), &record)
            .await
            .unwrap();
        let foreign = CheckpointRef::new("P/J/generations/1/checkpoints/checkpoint-0000002/operator-o/table-m-000/disk-foreign.bin").unwrap();
        assert!(
            cleanup_abandoned_disk_uploads(&store, &paths, Generation(1), Epoch(1), &[foreign])
                .await
                .is_err()
        );
        assert!(store.read_bytes(&abandoned_file).await.unwrap().is_some());
        assert!(
            cleanup_abandoned_disk_uploads(
                &store,
                &paths,
                Generation(1),
                Epoch(1),
                std::slice::from_ref(&abandoned_file)
            )
            .await
            .unwrap()
        );
        assert!(store.read_bytes(&abandoned_file).await.unwrap().is_none());
        assert!(
            cleanup_abandoned_disk_uploads(
                &store,
                &paths,
                Generation(1),
                Epoch(1),
                &[abandoned_file]
            )
            .await
            .unwrap()
        );
    }
}
