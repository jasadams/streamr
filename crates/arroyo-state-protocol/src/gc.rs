use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, DiskKeyedTableTaskCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use prost::Message;
use tracing::debug;

#[derive(PartialEq, Eq)]
struct FrozenRoots {
    current: Option<crate::types::CurrentGeneration>,
    frontier: Option<crate::types::GenerationManifest>,
    roots: Vec<CheckpointRef>,
}

fn validate_gc_owner(
    paths: &ProtocolPaths,
    reference: &CheckpointRef,
    manifest: &CheckpointManifest,
) -> Result<(), StoreError> {
    if manifest.pipeline_id.as_str() != paths.pipeline_id().as_str()
        || manifest.job_id.as_str() != paths.job_id().as_str()
        || *reference
            != paths.checkpoint_manifest(Generation(manifest.generation), Epoch(manifest.epoch))
    {
        return Err(StoreError::InvalidProtobuf {
            path: reference.clone(),
            msg: "cleanup checkpoint belongs to another job, generation or epoch".into(),
        });
    }
    Ok(())
}

async fn freeze_roots<S: ProtocolStore + ?Sized>(
    store: &S,
    paths: &ProtocolPaths,
) -> Result<FrozenRoots, StoreError> {
    use crate::store::read_json;
    use crate::types::{CommittedMarker, CurrentGeneration, EpochRecord, GenerationManifest};
    let current: Option<CurrentGeneration> = read_json(store, &paths.current_generation()).await?;
    let Some(owner) = current.as_ref() else {
        return Ok(FrozenRoots {
            current,
            frontier: None,
            roots: vec![],
        });
    };
    if owner.pipeline_id != *paths.pipeline_id()
        || owner.job_id != *paths.job_id()
        || owner.generation_manifest_ref != paths.generation_manifest(owner.generation)
    {
        return Err(StoreError::InvalidProtobuf {
            path: paths.current_generation(),
            msg: "cleanup current generation identity mismatch".into(),
        });
    }
    let mut active: GenerationManifest = read_json(store, &owner.generation_manifest_ref)
        .await?
        .ok_or_else(|| StoreError::ExistingObjectMissing {
            path: owner.generation_manifest_ref.clone(),
        })?;
    if active.generation != owner.generation
        || active.pipeline_id != *paths.pipeline_id()
        || active.job_id != *paths.job_id()
    {
        return Err(StoreError::InvalidProtobuf {
            path: owner.generation_manifest_ref.clone(),
            msg: "cleanup generation manifest identity mismatch".into(),
        });
    }
    if active.publication_log_version != 0 {
        let frontier = crate::publication::inspect(store, &active, None).await?;
        if frontier.closed {
            return Err(StoreError::InvalidProtobuf {
                path: owner.generation_manifest_ref.clone(),
                msg: "cleanup observed closed current generation".into(),
            });
        }
        active.base_checkpoint_ref = frontier.base_checkpoint_ref;
        active.latest_checkpoint_ref = frontier.latest_checkpoint_ref;
    }
    let mut roots = Vec::with_capacity(2);
    let mut latest_ready = false;
    if let Some(reference) = &active.latest_checkpoint_ref {
        let manifest: CheckpointManifest =
            read_protobuf(store, reference).await?.ok_or_else(|| {
                StoreError::ExistingObjectMissing {
                    path: reference.clone(),
                }
            })?;
        validate_gc_owner(paths, reference, &manifest)?;
        let record: Option<EpochRecord> =
            read_json(store, &paths.epoch_record(Epoch(manifest.epoch))).await?;
        let marker: Option<CommittedMarker> = if manifest.needs_commit {
            read_json(
                store,
                &paths.committed_marker(Generation(manifest.generation), Epoch(manifest.epoch)),
            )
            .await?
        } else {
            None
        };
        let state = crate::state::derive_checkpoint_state(
            reference,
            Some(&manifest),
            record,
            marker.as_ref(),
        )?;
        if matches!(state, crate::state::CheckpointState::Orphaned { .. }) {
            return Err(StoreError::InvalidProtobuf {
                path: reference.clone(),
                msg: "cleanup active frontier lost canonical ownership".into(),
            });
        }
        latest_ready = state.is_ready();
        roots.push(reference.clone());
    }
    if !latest_ready
        && let Some(base) = &active.base_checkpoint_ref
        && !roots.contains(base)
    {
        roots.push(base.clone());
    }
    Ok(FrozenRoots {
        current,
        frontier: Some(active),
        roots,
    })
}

/// Builds the full retained reference union before any payload deletion. Every
/// checkpoint is a complete file inventory; inherited SST objects survive until
/// their final retained reference disappears.
pub async fn cleanup_leader_checkpoints<S: ProtocolStore + ?Sized>(
    store: &S,
    paths: &ProtocolPaths,
    head: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<(), StoreError> {
    use crate::gc_spool::ReachabilitySpool;
    use futures::TryStreamExt;
    let frozen = freeze_roots(store, paths).await?;
    let mut spool = ReachabilitySpool::configured()?;
    let mut next = Some(head.clone());
    let mut previous_epoch = None;
    let mut have_native = false;
    let mut visited = 0u64;
    while let Some(reference) = next {
        let Some(manifest): Option<CheckpointManifest> = read_protobuf(store, &reference).await?
        else {
            let missing_epoch = reference
                .as_str()
                .strip_suffix("/checkpoint-manifest.pb")
                .and_then(|directory| directory.rsplit_once("/checkpoint-"))
                .and_then(|(_, epoch)| epoch.parse::<u64>().ok());
            if visited == 0 || missing_epoch.is_none_or(|epoch| epoch >= *new_min_epoch) {
                return Err(StoreError::ExistingObjectMissing { path: reference });
            }
            break;
        };
        validate_gc_owner(paths, &reference, &manifest)?;
        spool.descriptor_bytes(manifest.encoded_len())?;
        visited += 1;
        if visited > 16_000_000 {
            return Err(StoreError::InvalidProtobuf {
                path: reference,
                msg: "checkpoint ancestry exceeds cleanup bounds".into(),
            });
        }
        if previous_epoch.is_some_and(|epoch| manifest.epoch >= epoch) {
            // Valid parents are strictly older. Keep the established typed
            // cycle error without an unbounded in-memory owner inventory;
            // corrupt increasing ancestry fails closed through the same guard.
            return Err(ProtocolError::CheckpointCycle {
                generation: Generation(manifest.generation),
                epoch: Epoch(manifest.epoch),
            }
            .into());
        }
        if visited == 1 && *new_min_epoch > manifest.epoch {
            return Err(StoreError::InvalidProtobuf {
                path: reference,
                msg: "retention minimum would prune the active checkpoint head".into(),
            });
        }
        previous_epoch = Some(manifest.epoch);
        crate::disk::validate_manifest(&manifest).map_err(|msg| StoreError::InvalidProtobuf {
            path: reference.clone(),
            msg,
        })?;
        let prune = manifest.epoch < *new_min_epoch && !frozen.roots.contains(&reference);
        if prune {
            if !crate::ready::preserve(store, paths, &reference, &manifest).await? {
                return Err(StoreError::InvalidProtobuf {
                    path: reference,
                    msg: "refusing to prune a checkpoint without canonical commit completion"
                        .into(),
                });
            }
            spool.metadata(&format!("{:020}|{}", manifest.epoch, reference))?;
        }
        for operator in &manifest.operators {
            let owner =
                operator
                    .operator_metadata
                    .as_ref()
                    .ok_or_else(|| StoreError::InvalidProtobuf {
                        path: reference.clone(),
                        msg: "operator checkpoint missing ownership".into(),
                    })?;
            let mut files = Vec::new();
            for (name, metadata) in &operator.table_checkpoint_metadata {
                table_checkpoint_data_files(
                    &owner.operator_id,
                    name,
                    &reference,
                    metadata,
                    &mut files,
                )?;
            }
            if let Some(native) = &operator.native_database {
                have_native = true;
                for file in &native.files {
                    if prune {
                        spool.candidate(&file.path)?;
                    } else {
                        spool.retain(&file.path)?;
                    }
                }
            }
            for file in files {
                if prune {
                    spool.candidate(file.as_str())?;
                } else {
                    spool.retain(file.as_str())?;
                }
            }
        }
        next = manifest
            .parent_checkpoint_ref
            .map(CheckpointRef::new)
            .transpose()?;
    }
    // A replacement coordinator may select a recovery root outside the old
    // cleanup head. Protect its complete inventory and its metadata explicitly.
    for reference in &frozen.roots {
        let manifest: CheckpointManifest =
            read_protobuf(store, reference).await?.ok_or_else(|| {
                StoreError::ExistingObjectMissing {
                    path: reference.clone(),
                }
            })?;
        validate_gc_owner(paths, reference, &manifest)?;
        spool.descriptor_bytes(manifest.encoded_len())?;
        crate::disk::validate_manifest(&manifest).map_err(|msg| StoreError::InvalidProtobuf {
            path: reference.clone(),
            msg,
        })?;
        for operator in &manifest.operators {
            let owner =
                operator
                    .operator_metadata
                    .as_ref()
                    .ok_or_else(|| StoreError::InvalidProtobuf {
                        path: reference.clone(),
                        msg: "active cleanup root missing operator ownership".into(),
                    })?;
            let mut files = Vec::new();
            for (name, metadata) in &operator.table_checkpoint_metadata {
                table_checkpoint_data_files(
                    &owner.operator_id,
                    name,
                    reference,
                    metadata,
                    &mut files,
                )?;
            }
            for file in files {
                spool.retain(file.as_str())?;
            }
            if let Some(native) = &operator.native_database {
                have_native = true;
                for file in &native.files {
                    spool.retain(&file.path)?;
                }
            }
        }
    }
    if have_native && frozen.current.is_none() {
        return Err(StoreError::InvalidProtobuf {
            path: head.clone(),
            msg: "native cleanup missing authoritative generation frontier".into(),
        });
    }
    {
        let namespace = format!("{}/{}/generations/", paths.pipeline_id(), paths.job_id());
        let listing = store.list_objects(&namespace).await?;
        if listing.is_none() && have_native {
            return Err(StoreError::InvalidProtobuf {
                path: head,
                msg: "native cleanup requires streaming object enumeration".into(),
            });
        }
        if let Some(mut listing) = listing {
            let mut last_owner: Option<(u64, u64, bool)> = None;
            while let Some(object) = listing.try_next().await? {
                let Some(suffix) = object.strip_prefix(&namespace) else {
                    continue;
                };
                let parts = suffix.split('/').collect::<Vec<_>>();
                if parts.len() != 6
                    || parts[1] != "checkpoints"
                    || !parts[3].starts_with("operator-")
                    || !parts[4].starts_with("native-")
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
                if frozen.current.is_none() {
                    return Err(StoreError::InvalidProtobuf {
                        path: head.clone(),
                        msg: "native orphan cleanup missing authoritative generation frontier"
                            .into(),
                    });
                }
                if epoch >= *new_min_epoch {
                    continue;
                }
                let owner = paths.checkpoint_manifest(Generation(generation), Epoch(epoch));
                // Ready proofs survive manifest pruning. Otherwise only a fenced,
                // publication-excluded owner can contribute abandoned candidates.
                let eligible = if let Some((g, e, result)) =
                    last_owner.filter(|(g, e, _)| *g == generation && *e == epoch)
                {
                    let _ = (g, e);
                    result
                } else {
                    let result = crate::ready::exists(store, paths, &owner).await?
                        || cleanup_abandoned_disk_uploads(
                            store,
                            paths,
                            Generation(generation),
                            Epoch(epoch),
                            &[],
                        )
                        .await?;
                    last_owner = Some((generation, epoch, result));
                    result
                };
                if eligible {
                    spool.candidate(&object)?;
                }
            }
        }
    }
    let mut plan = spool.finish()?;
    if freeze_roots(store, paths).await? != frozen {
        return Err(StoreError::InvalidProtobuf {
            path: head.clone(),
            msg: "cleanup authoritative generation frontier changed during preflight".into(),
        });
    }
    for file in plan.by_ref() {
        let file = CheckpointRef::new(file?)?;
        store.delete_object(&file).await?;
        if let Some((directory, _)) = file.as_str().rsplit_once('/') {
            store.delete_directory(directory).await;
        }
    }
    while let Some(record) = plan.next_metadata()? {
        let (_, path) = record
            .split_once('|')
            .ok_or_else(|| StoreError::InvalidProtobuf {
                path: head.clone(),
                msg: "invalid cleanup metadata spool".into(),
            })?;
        let reference = CheckpointRef::new(path.to_string())?;
        debug!(checkpoint=%reference,"cleaning checkpoint metadata");
        store.delete_object(&reference).await?;
        if let Some((directory, _)) = path.rsplit_once('/') {
            store.delete_directory(directory).await;
        }
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

#[cfg(test)]
mod native_tests {
    use super::*;
    use crate::store::{put_json, put_protobuf};
    use crate::types::{CurrentGeneration, EpochRecord, GenerationManifest};
    use arroyo_rpc::grpc::rpc::{
        NativeDatabaseCheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata,
    };
    use arroyo_types::{JobId, PipelineId};
    async fn record(
        store: &arroyo_storage::StorageProvider,
        paths: &ProtocolPaths,
        epoch: u32,
        parent: Option<&CheckpointRef>,
        mut native: NativeDatabaseCheckpointMetadata,
    ) -> CheckpointRef {
        let (_, configs) = crate::native_checkpoint::tests::fixture(epoch, 1, Some("P"));
        native.epoch = epoch;
        let reference = paths.checkpoint_manifest(Generation(1), Epoch(u64::from(epoch)));
        let manifest = CheckpointManifest {
            pipeline_id: "P".into(),
            job_id: "J".into(),
            generation: 1,
            epoch: u64::from(epoch),
            parent_checkpoint_ref: parent.map(ToString::to_string),
            operators: vec![OperatorCheckpointMetadata {
                operator_metadata: Some(OperatorMetadata {
                    job_id: "J".into(),
                    operator_id: "o".into(),
                    epoch,
                    parallelism: 1,
                    ..Default::default()
                }),
                table_configs: configs,
                native_database: Some(native.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };
        crate::disk::validate_manifest(&manifest).unwrap();
        for file in &native.files {
            if !store.exists(file.path.clone()).await.unwrap() {
                store
                    .put_if_not_exists(file.path.clone(), vec![1])
                    .await
                    .unwrap();
            }
        }
        put_protobuf(store, &reference, &manifest).await.unwrap();
        let record = EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(1),
            reference.clone(),
            &manifest,
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        put_json(store, &paths.epoch_record(Epoch(u64::from(epoch))), &record)
            .await
            .unwrap();
        reference
    }
    async fn frontier(
        store: &arroyo_storage::StorageProvider,
        paths: &ProtocolPaths,
        generation: u64,
        base: Option<CheckpointRef>,
        latest: Option<CheckpointRef>,
    ) {
        let current = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(generation),
            std::time::SystemTime::UNIX_EPOCH,
        );
        let mut active = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(generation),
            base,
            0,
        );
        active.latest_checkpoint_ref = latest;
        put_json(
            store,
            &paths.generation_manifest(Generation(generation)),
            &active,
        )
        .await
        .unwrap();
        put_json(store, &paths.current_generation(), &current)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn real_storage_native_shared_files_survive_every_retained_root_then_final_reference_gc()
    {
        let directory = tempfile::tempdir().unwrap();
        let store = arroyo_storage::StorageProvider::for_url(&format!(
            "file://{}",
            directory.path().display()
        ))
        .await
        .unwrap();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let (first, _) = crate::native_checkpoint::tests::fixture(1, 1, Some("P"));
        let shared_a = first.files[2].path.clone();
        let private = first.files[0].path.clone();
        let r1 = record(&store, &paths, 1, None, first.clone()).await;
        let (mut second, _) = crate::native_checkpoint::tests::fixture(2, 1, Some("P"));
        second.files[2].local_name = "000002.sst".into();
        second.files[2].path = second.files[2].path.replace("000001.sst", "000002.sst");
        let shared_b = second.files[2].path.clone();
        second.files.push(first.files[2].clone());
        second.completed_base_epoch = Some(1);
        let r2 = record(&store, &paths, 2, Some(&r1), second.clone()).await;
        let (mut third, _) = crate::native_checkpoint::tests::fixture(3, 1, Some("P"));
        third.files[2] = second.files[2].clone();
        third.completed_base_epoch = Some(2);
        let r3 = record(&store, &paths, 3, Some(&r2), third.clone()).await;
        frontier(&store, &paths, 1, None, Some(r3.clone())).await;
        cleanup_leader_checkpoints(&store, &paths, r3.clone(), Epoch(2))
            .await
            .unwrap();
        assert!(store.exists(shared_a.clone()).await.unwrap());
        assert!(!store.exists(private).await.unwrap());
        assert!(!store.exists(r1.to_string()).await.unwrap());
        // The remaining chain still names the already-pruned epoch-one
        // manifest. Retrying the same cutoff must retain inherited SSTs and
        // recognize that missing ancestor by its directory epoch.
        cleanup_leader_checkpoints(&store, &paths, r3.clone(), Epoch(2))
            .await
            .unwrap();
        assert!(store.exists(shared_a.clone()).await.unwrap());
        assert!(store.exists(shared_b.clone()).await.unwrap());
        cleanup_leader_checkpoints(&store, &paths, r3.clone(), Epoch(3))
            .await
            .unwrap();
        assert!(!store.exists(shared_a).await.unwrap());
        assert!(store.exists(shared_b.clone()).await.unwrap());
        let (fourth, _) = crate::native_checkpoint::tests::fixture(4, 1, Some("P"));
        let r4 = record(&store, &paths, 4, Some(&r3), fourth).await;
        frontier(&store, &paths, 1, None, Some(r4.clone())).await;
        cleanup_leader_checkpoints(&store, &paths, r4, Epoch(4))
            .await
            .unwrap();
        assert!(!store.exists(shared_b).await.unwrap());
    }
    #[tokio::test]
    async fn replacement_recovery_base_below_prune_cutoff_is_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let store = arroyo_storage::StorageProvider::for_url(&format!(
            "file://{}",
            directory.path().display()
        ))
        .await
        .unwrap();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let (base, _) = crate::native_checkpoint::tests::fixture(1, 1, Some("P"));
        let shared = base.files[2].path.clone();
        let r1 = record(&store, &paths, 1, None, base).await;
        let (next, _) = crate::native_checkpoint::tests::fixture(2, 1, Some("P"));
        let r2 = record(&store, &paths, 2, Some(&r1), next).await;
        // Deliberately present a stale cleaner with a distinct replacement base.
        frontier(&store, &paths, 2, Some(r1.clone()), None).await;
        cleanup_leader_checkpoints(&store, &paths, r2, Epoch(2))
            .await
            .unwrap();
        assert!(store.exists(shared).await.unwrap());
        assert!(store.exists(r1.to_string()).await.unwrap());
        store.delete_object(&r1).await.unwrap();
        let deleted_before = store
            .exists(
                paths
                    .checkpoint_manifest(Generation(1), Epoch(2))
                    .to_string(),
            )
            .await
            .unwrap();
        assert!(deleted_before);
        assert!(
            cleanup_leader_checkpoints(
                &store,
                &paths,
                paths.checkpoint_manifest(Generation(1), Epoch(2)),
                Epoch(2)
            )
            .await
            .is_err()
        );
        assert!(
            store
                .exists(
                    paths
                        .checkpoint_manifest(Generation(1), Epoch(2))
                        .to_string()
                )
                .await
                .unwrap()
        );
    }
}

#[cfg(test)]
mod native_frontier_race_tests {
    use super::*;
    use crate::store::{CreateResult, put_json, put_protobuf};
    use crate::types::{CurrentGeneration, EpochRecord, GenerationManifest};
    use arroyo_types::{JobId, PipelineId};
    use std::sync::atomic::{AtomicBool, Ordering};
    struct ChangeDuringListing {
        inner: arroyo_storage::StorageProvider,
        next: CurrentGeneration,
        changed: AtomicBool,
    }
    #[async_trait::async_trait]
    impl ProtocolStore for ChangeDuringListing {
        async fn read_bytes(&self, p: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
            self.inner.read_bytes(p).await
        }
        async fn put_bytes(&self, p: &CheckpointRef, b: Vec<u8>) -> Result<(), StoreError> {
            ProtocolStore::put_bytes(&self.inner, p, b).await
        }
        async fn create_bytes(
            &self,
            p: &CheckpointRef,
            b: Vec<u8>,
        ) -> Result<CreateResult<Vec<u8>>, StoreError> {
            self.inner.create_bytes(p, b).await
        }
        async fn delete_object(&self, p: &CheckpointRef) -> Result<(), StoreError> {
            self.inner.delete_object(p).await
        }
        async fn delete_directory(&self, p: &str) {
            ProtocolStore::delete_directory(&self.inner, p).await;
        }
        async fn list_objects(
            &self,
            prefix: &str,
        ) -> Result<Option<futures::stream::BoxStream<'_, Result<String, StoreError>>>, StoreError>
        {
            if !self.changed.swap(true, Ordering::SeqCst) {
                let paths =
                    ProtocolPaths::new(self.next.pipeline_id.clone(), self.next.job_id.clone());
                put_json(&self.inner, &paths.current_generation(), &self.next).await?;
            }
            self.inner.list_objects(prefix).await
        }
    }
    #[tokio::test]
    async fn generation_change_during_preflight_deletes_no_payload_and_closed_frontier_fails_closed()
     {
        let directory = tempfile::tempdir().unwrap();
        let inner = arroyo_storage::StorageProvider::for_url(&format!(
            "file://{}",
            directory.path().display()
        ))
        .await
        .unwrap();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let mut previous = None;
        let mut first_file = String::new();
        for epoch in 1..=2 {
            let (native, configs) = crate::native_checkpoint::tests::fixture(epoch, 1, Some("P"));
            if epoch == 1 {
                first_file = native.files[2].path.clone();
            }
            for file in &native.files {
                inner
                    .put_if_not_exists(file.path.clone(), vec![1])
                    .await
                    .unwrap();
            }
            let reference = paths.checkpoint_manifest(Generation(1), Epoch(epoch.into()));
            let manifest = CheckpointManifest {
                pipeline_id: "P".into(),
                job_id: "J".into(),
                generation: 1,
                epoch: epoch.into(),
                parent_checkpoint_ref: previous.clone(),
                operators: vec![arroyo_rpc::grpc::rpc::OperatorCheckpointMetadata {
                    operator_metadata: Some(arroyo_rpc::grpc::rpc::OperatorMetadata {
                        job_id: "J".into(),
                        operator_id: "o".into(),
                        epoch,
                        parallelism: 1,
                        ..Default::default()
                    }),
                    table_configs: configs,
                    native_database: Some(native),
                    ..Default::default()
                }],
                ..Default::default()
            };
            put_protobuf(&inner, &reference, &manifest).await.unwrap();
            let record = EpochRecord::for_checkpoint(
                PipelineId::new("P"),
                Generation(1),
                reference.clone(),
                &manifest,
                std::time::SystemTime::UNIX_EPOCH,
            )
            .unwrap();
            put_json(&inner, &paths.epoch_record(Epoch(epoch.into())), &record)
                .await
                .unwrap();
            previous = Some(reference.to_string());
        }
        let head = CheckpointRef::new(previous.unwrap()).unwrap();
        let mut active = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            None,
            0,
        );
        active.latest_checkpoint_ref = Some(head.clone());
        put_json(&inner, &paths.generation_manifest(Generation(1)), &active)
            .await
            .unwrap();
        let current = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            std::time::SystemTime::UNIX_EPOCH,
        );
        put_json(&inner, &paths.current_generation(), &current)
            .await
            .unwrap();
        let mut replacement = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            Some(head.clone()),
            0,
        );
        put_json(
            &inner,
            &paths.generation_manifest(Generation(2)),
            &replacement,
        )
        .await
        .unwrap();
        let next = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            std::time::SystemTime::UNIX_EPOCH,
        );
        let store = ChangeDuringListing {
            inner,
            next,
            changed: AtomicBool::new(false),
        };
        assert!(
            cleanup_leader_checkpoints(&store, &paths, head.clone(), Epoch(2))
                .await
                .is_err()
        );
        assert!(store.inner.exists(first_file.clone()).await.unwrap());
        replacement.publication_log_version = crate::publication::PUBLICATION_LOG_VERSION;
        put_json(
            &store.inner,
            &paths.generation_manifest(Generation(2)),
            &replacement,
        )
        .await
        .unwrap();
        crate::publication::open(&store.inner, &replacement)
            .await
            .unwrap();
        crate::publication::close(&store.inner, &replacement, None)
            .await
            .unwrap();
        assert!(
            cleanup_leader_checkpoints(&store.inner, &paths, head, Epoch(2))
                .await
                .is_err()
        );
        assert!(store.inner.exists(first_file).await.unwrap());
    }
}

#[cfg(test)]
mod native_orphan_frontier_tests {
    use super::*;
    use crate::store::put_protobuf;
    use arroyo_types::{JobId, PipelineId};
    #[tokio::test]
    async fn native_orphan_after_logical_head_requires_authoritative_frontier() {
        let directory = tempfile::tempdir().unwrap();
        let storage = arroyo_storage::StorageProvider::for_url(&format!(
            "file://{}",
            directory.path().display()
        ))
        .await
        .unwrap();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let head = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let logical = CheckpointManifest {
            pipeline_id: "P".into(),
            job_id: "J".into(),
            generation: 1,
            epoch: 2,
            ..Default::default()
        };
        put_protobuf(&storage, &head, &logical).await.unwrap();
        let orphan = "P/J/generations/1/checkpoints/checkpoint-0000001/operator-o/native-owned/000001.sst.upload-owned";
        storage.put_if_not_exists(orphan, vec![1]).await.unwrap();
        let error = cleanup_leader_checkpoints(&storage, &paths, head.clone(), Epoch(2))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("native orphan cleanup missing authoritative")
        );
        assert!(storage.exists(orphan).await.unwrap());
        assert!(storage.exists(head.to_string()).await.unwrap());
    }
}
