//! Atomic publication/closure ordering using immutable conditional-create slots.
//!
//! Files and a manifest become eligible for canonical epoch claiming only after
//! their reference wins a publication slot. A replacement generation closes the
//! same append-only log: publication either wins first and is included in the
//! recovery frontier, or observes closure and cannot claim its epoch. This uses
//! conditional create, including file:// stores; no cross-object CAS is assumed.
use crate::ProtocolPaths;
use crate::store::{CreateResult, ProtocolStore, StoreError, create_json_if_not_exist, read_json};
use crate::types::{CheckpointRef, GenerationManifest};
use serde::{Deserialize, Serialize};

pub const PUBLICATION_LOG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Entry {
    Open {
        base_checkpoint_ref: Option<CheckpointRef>,
    },
    Published {
        checkpoint_ref: CheckpointRef,
    },
    Closed {
        latest_checkpoint_ref: Option<CheckpointRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    version: u32,
    pipeline_id: arroyo_types::PipelineId,
    job_id: arroyo_types::JobId,
    generation: crate::types::Generation,
    sequence: u64,
    entry: Entry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationFrontier {
    pub base_checkpoint_ref: Option<CheckpointRef>,
    pub latest_checkpoint_ref: Option<CheckpointRef>,
    pub closed: bool,
    pub target_published: bool,
    sequence: u64,
}

fn record(manifest: &GenerationManifest, sequence: u64, entry: Entry) -> Record {
    Record {
        version: PUBLICATION_LOG_VERSION,
        pipeline_id: manifest.pipeline_id.clone(),
        job_id: manifest.job_id.clone(),
        generation: manifest.generation,
        sequence,
        entry,
    }
}
fn paths(manifest: &GenerationManifest) -> ProtocolPaths {
    ProtocolPaths::new(manifest.pipeline_id.clone(), manifest.job_id.clone())
}
fn invalid(manifest: &GenerationManifest, message: &str) -> StoreError {
    StoreError::InvalidProtobuf {
        path: paths(manifest).generation_manifest(manifest.generation),
        msg: message.into(),
    }
}

pub async fn open<S: ProtocolStore + ?Sized>(
    store: &S,
    manifest: &GenerationManifest,
) -> Result<(), StoreError> {
    let origin = record(
        manifest,
        0,
        Entry::Open {
            base_checkpoint_ref: manifest.base_checkpoint_ref.clone(),
        },
    );
    match create_json_if_not_exist(
        store,
        &paths(manifest).publication_record(manifest.generation, 0),
        &origin,
    )
    .await?
    {
        CreateResult::Created => Ok(()),
        CreateResult::AlreadyExists(existing) if existing == origin => Ok(()),
        CreateResult::AlreadyExists(_) => {
            Err(invalid(manifest, "publication log origin/owner mismatch"))
        }
    }
}

/// Read one bounded log record at a time. Slots are never deleted: keeping the
/// terminal closure and its immutable prefix prevents stale writers from
/// recreating an old sequence and forking the publication order.
pub async fn inspect<S: ProtocolStore + ?Sized>(
    store: &S,
    manifest: &GenerationManifest,
    target: Option<&CheckpointRef>,
) -> Result<PublicationFrontier, StoreError> {
    if manifest.publication_log_version != PUBLICATION_LOG_VERSION {
        return Err(invalid(manifest, "unsupported publication log version"));
    }
    let paths = paths(manifest);
    let mut frontier = PublicationFrontier {
        base_checkpoint_ref: None,
        latest_checkpoint_ref: None,
        closed: false,
        target_published: false,
        sequence: 0,
    };
    let mut sequence = 0;
    loop {
        let path = paths.publication_record(manifest.generation, sequence);
        let Some(entry): Option<Record> = read_json(store, &path).await? else {
            if sequence == 0 {
                return Err(StoreError::ExistingObjectMissing { path });
            }
            return Ok(frontier);
        };
        if entry.version != PUBLICATION_LOG_VERSION
            || entry.pipeline_id != manifest.pipeline_id
            || entry.job_id != manifest.job_id
            || entry.generation != manifest.generation
            || entry.sequence != sequence
        {
            return Err(invalid(
                manifest,
                "publication log identity/version mismatch",
            ));
        }
        match entry.entry {
            Entry::Open {
                base_checkpoint_ref,
            } if sequence == 0 => {
                if base_checkpoint_ref != manifest.base_checkpoint_ref {
                    return Err(invalid(
                        manifest,
                        "publication log base differs from generation ownership",
                    ));
                }
                if let Some(reference) = &base_checkpoint_ref {
                    CheckpointRef::new(reference.to_string())?;
                }
                frontier.base_checkpoint_ref = base_checkpoint_ref;
            }
            Entry::Published { checkpoint_ref } if sequence > 0 => {
                CheckpointRef::new(checkpoint_ref.to_string())?;
                let prefix = format!(
                    "{}/{}/generations/{}/checkpoints/checkpoint-",
                    manifest.pipeline_id, manifest.job_id, manifest.generation
                );
                if !checkpoint_ref.as_str().starts_with(&prefix)
                    || !checkpoint_ref.as_str().ends_with("/checkpoint-manifest.pb")
                {
                    return Err(invalid(
                        manifest,
                        "publication reference belongs to another generation",
                    ));
                }
                frontier.target_published |= target == Some(&checkpoint_ref);
                frontier.latest_checkpoint_ref = Some(checkpoint_ref);
            }
            Entry::Closed {
                latest_checkpoint_ref,
            } if sequence > 0 && latest_checkpoint_ref == frontier.latest_checkpoint_ref => {
                frontier.closed = true;
                frontier.sequence = sequence;
                return Ok(frontier);
            }
            _ => {
                return Err(invalid(
                    manifest,
                    "invalid publication log sequence or closure frontier",
                ));
            }
        }
        frontier.sequence = sequence;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid(manifest, "publication log sequence overflow"))?;
    }
}

/// `false` means closure won the slot; callers must stop before writing a
/// generation pointer or claiming the checkpoint's canonical epoch record.
pub async fn publish<S: ProtocolStore + ?Sized>(
    store: &S,
    manifest: &GenerationManifest,
    checkpoint_ref: &CheckpointRef,
    parent_ref: Option<&CheckpointRef>,
) -> Result<bool, StoreError> {
    loop {
        let frontier = inspect(store, manifest, Some(checkpoint_ref)).await?;
        if frontier.latest_checkpoint_ref.as_ref() == Some(checkpoint_ref) {
            return Ok(true);
        }
        if frontier.target_published || frontier.closed {
            return Ok(false);
        }
        if frontier
            .latest_checkpoint_ref
            .as_ref()
            .or(frontier.base_checkpoint_ref.as_ref())
            != parent_ref
        {
            return Err(invalid(
                manifest,
                "checkpoint parent differs from publication frontier",
            ));
        }
        let sequence = frontier
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid(manifest, "publication log sequence overflow"))?;
        let entry = record(
            manifest,
            sequence,
            Entry::Published {
                checkpoint_ref: checkpoint_ref.clone(),
            },
        );
        match create_json_if_not_exist(
            store,
            &paths(manifest).publication_record(manifest.generation, sequence),
            &entry,
        )
        .await?
        {
            CreateResult::Created => return Ok(true),
            CreateResult::AlreadyExists(existing) if existing == entry => return Ok(true),
            CreateResult::AlreadyExists(_) => {}
        }
    }
}

pub async fn close<S: ProtocolStore + ?Sized>(
    store: &S,
    manifest: &GenerationManifest,
    target: Option<&CheckpointRef>,
) -> Result<PublicationFrontier, StoreError> {
    loop {
        let frontier = inspect(store, manifest, target).await?;
        if frontier.closed {
            return Ok(frontier);
        }
        let sequence = frontier
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid(manifest, "publication log sequence overflow"))?;
        let entry = record(
            manifest,
            sequence,
            Entry::Closed {
                latest_checkpoint_ref: frontier.latest_checkpoint_ref.clone(),
            },
        );
        match create_json_if_not_exist(
            store,
            &paths(manifest).publication_record(manifest.generation, sequence),
            &entry,
        )
        .await?
        {
            CreateResult::Created => {}
            CreateResult::AlreadyExists(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::MemoryProtocolStore;
    use crate::store::{ProtocolStore, put_json, put_protobuf};
    use crate::types::{Epoch, Generation};
    use crate::workflow::{
        GenerationInitialization, GenerationRecovery, GenerationResolution,
        InitializeGenerationRequest, complete_commit, initialize_generation,
        resolve_generation_manifest,
    };
    use arroyo_rpc::grpc::rpc::CheckpointManifest;
    use arroyo_types::{JobId, PipelineId};
    use std::time::SystemTime;

    fn paths() -> ProtocolPaths {
        ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"))
    }
    async fn initialize<S: ProtocolStore + ?Sized>(
        store: &S,
        generation: u64,
    ) -> (GenerationManifest, GenerationRecovery) {
        match initialize_generation(
            store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(generation),
                updated_at: SystemTime::UNIX_EPOCH,
            },
            true,
        )
        .await
        .unwrap()
        {
            GenerationInitialization::Initialized {
                generation_manifest,
                recovery,
            } => (generation_manifest, recovery),
            other => panic!("unexpected initialization: {other:?}"),
        }
    }
    fn checkpoint(needs_commit: bool) -> CheckpointManifest {
        CheckpointManifest {
            pipeline_id: "P".into(),
            job_id: "J".into(),
            generation: 1,
            epoch: 1,
            needs_commit,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn crash_after_manifest_before_log_is_excluded_and_unclaimed_uploads_are_cleaned() {
        let store = MemoryProtocolStore::default();
        let (manifest, _) = initialize(&store, 1).await;
        let checkpoint_ref = paths().checkpoint_manifest(Generation(1), Epoch(1));
        put_protobuf(&store, &checkpoint_ref, &checkpoint(false))
            .await
            .unwrap();
        let file = CheckpointRef::new("P/J/generations/1/checkpoints/checkpoint-0000001/operator-o/table-m-000/disk-orphan.bin").unwrap();
        store.put_bytes(&file, vec![1]).await.unwrap();
        let (_, recovery) = initialize(&store, 2).await;
        assert_eq!(recovery, GenerationRecovery::NoCheckpoint);
        assert!(
            !publish(&store, &manifest, &checkpoint_ref, None)
                .await
                .unwrap()
        );
        assert!(
            crate::gc::cleanup_abandoned_disk_uploads(
                &store,
                &paths(),
                Generation(1),
                Epoch(1),
                std::slice::from_ref(&file)
            )
            .await
            .unwrap()
        );
        assert!(store.read_bytes(&file).await.unwrap().is_none());
        assert!(
            crate::gc::cleanup_abandoned_disk_uploads(
                &store,
                &paths(),
                Generation(1),
                Epoch(1),
                &[file]
            )
            .await
            .unwrap()
        );
        assert!(
            store
                .read_bytes(&paths().epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn crash_after_log_before_epoch_claim_recovers_from_log_and_ignores_regressed_pointer() {
        let store = MemoryProtocolStore::default();
        let (manifest, _) = initialize(&store, 1).await;
        let checkpoint_ref = paths().checkpoint_manifest(Generation(1), Epoch(1));
        put_protobuf(&store, &checkpoint_ref, &checkpoint(false))
            .await
            .unwrap();
        assert!(
            publish(&store, &manifest, &checkpoint_ref, None)
                .await
                .unwrap()
        );
        assert!(
            store
                .read_bytes(&paths().epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_none()
        );
        let (new_manifest, recovery) = initialize(&store, 2).await;
        assert_eq!(
            recovery,
            GenerationRecovery::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        assert_eq!(
            new_manifest.base_checkpoint_ref,
            Some(checkpoint_ref.clone())
        );
        // A paused old publisher can update its advisory pointer after closure.
        // The immutable frontier must continue to win over that stale write.
        put_json(
            &store,
            &paths().generation_manifest(Generation(1)),
            &manifest,
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_generation_manifest(&store, &manifest, Generation(2))
                .await
                .unwrap(),
            GenerationResolution::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        let file = CheckpointRef::new(
            "P/J/generations/1/checkpoints/checkpoint-0000001/operator-o/table-m-000/disk-keep.bin",
        )
        .unwrap();
        store.put_bytes(&file, vec![1]).await.unwrap();
        assert!(
            !crate::gc::cleanup_abandoned_disk_uploads(
                &store,
                &paths(),
                Generation(1),
                Epoch(1),
                std::slice::from_ref(&file)
            )
            .await
            .unwrap()
        );
        assert!(store.read_bytes(&file).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn closed_frontier_preserves_connector_commit_replay() {
        let store = MemoryProtocolStore::default();
        let (manifest, _) = initialize(&store, 1).await;
        let checkpoint_ref = paths().checkpoint_manifest(Generation(1), Epoch(1));
        put_protobuf(&store, &checkpoint_ref, &checkpoint(true))
            .await
            .unwrap();
        assert!(
            publish(&store, &manifest, &checkpoint_ref, None)
                .await
                .unwrap()
        );
        let (next, recovery) = initialize(&store, 2).await;
        let GenerationRecovery::ReplayCommit {
            checkpoint_ref: recovered,
            commit_permit,
        } = recovery
        else {
            panic!("connector commit replay was lost")
        };
        assert_eq!(recovered, checkpoint_ref);
        complete_commit(&store, &commit_permit, Generation(2))
            .await
            .unwrap();
        assert_eq!(
            resolve_generation_manifest(&store, &next, Generation(2))
                .await
                .unwrap(),
            GenerationResolution::Ready { checkpoint_ref }
        );
    }

    struct RaceStore {
        inner: MemoryProtocolStore,
        barrier: tokio::sync::Barrier,
    }
    #[async_trait::async_trait]
    impl ProtocolStore for RaceStore {
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
            if p.as_str().ends_with("record-00000000000000000001.json") {
                self.barrier.wait().await;
            }
            self.inner.create_bytes(p, b).await
        }
        async fn delete_object(&self, p: &CheckpointRef) -> Result<(), StoreError> {
            self.inner.delete_object(p).await
        }
        async fn delete_directory(&self, p: &str) {
            self.inner.delete_directory(p).await
        }
    }
    #[tokio::test]
    async fn closure_and_publication_race_on_the_same_immutable_slot() {
        let store = RaceStore {
            inner: MemoryProtocolStore::default(),
            barrier: tokio::sync::Barrier::new(2),
        };
        let mut manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            None,
            0,
        );
        manifest.publication_log_version = 1;
        open(&store, &manifest).await.unwrap();
        let checkpoint_ref = paths().checkpoint_manifest(Generation(1), Epoch(1));
        let (published, closed) = tokio::join!(
            publish(&store, &manifest, &checkpoint_ref, None),
            close(&store, &manifest, Some(&checkpoint_ref))
        );
        let published = published.unwrap();
        let closed = closed.unwrap();
        assert!(closed.closed);
        assert_eq!(closed.target_published, published);
        assert_eq!(
            closed.latest_checkpoint_ref,
            published.then_some(checkpoint_ref.clone())
        );
        let different = paths().checkpoint_manifest(Generation(1), Epoch(2));
        assert!(
            !publish(&store, &manifest, &different, Some(&checkpoint_ref))
                .await
                .unwrap()
        );
        assert_eq!(
            close(&store, &manifest, Some(&checkpoint_ref))
                .await
                .unwrap(),
            closed
        );
    }

    #[tokio::test]
    async fn local_store_supports_publication_and_terminal_closure_without_cas() {
        let root = std::env::temp_dir().join(format!(
            "streamr-publication-log-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.display()))
            .await
            .unwrap();
        let (manifest, _) = initialize(&store, 1).await;
        let checkpoint_ref = paths().checkpoint_manifest(Generation(1), Epoch(1));
        put_protobuf(&store, &checkpoint_ref, &checkpoint(false))
            .await
            .unwrap();
        assert!(
            publish(&store, &manifest, &checkpoint_ref, None)
                .await
                .unwrap()
        );
        let (_, recovery) = initialize(&store, 2).await;
        assert_eq!(
            recovery,
            GenerationRecovery::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        assert!(
            inspect(&store, &manifest, Some(&checkpoint_ref))
                .await
                .unwrap()
                .closed
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
    #[tokio::test]
    async fn initialization_retry_uses_current_log_instead_of_resetting_to_empty() {
        let store = MemoryProtocolStore::default();
        let (manifest, _) = initialize(&store, 0).await;
        let checkpoint_ref = paths().checkpoint_manifest(Generation(0), Epoch(1));
        let mut checkpoint = checkpoint(false);
        checkpoint.generation = 0;
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();
        assert!(
            publish(&store, &manifest, &checkpoint_ref, None)
                .await
                .unwrap()
        );
        let (_, recovery) = initialize(&store, 0).await;
        assert_eq!(recovery, GenerationRecovery::Ready { checkpoint_ref });
    }
    #[tokio::test]
    async fn historical_publication_retry_cannot_regress_frontier_or_reopen_closure() {
        let store = MemoryProtocolStore::default();
        let (manifest, _) = initialize(&store, 1).await;
        let first = paths().checkpoint_manifest(Generation(1), Epoch(1));
        let second = paths().checkpoint_manifest(Generation(1), Epoch(2));
        assert!(publish(&store, &manifest, &first, None).await.unwrap());
        assert!(publish(&store, &manifest, &second, None).await.is_err());
        assert!(
            publish(&store, &manifest, &second, Some(&first))
                .await
                .unwrap()
        );
        assert!(!publish(&store, &manifest, &first, None).await.unwrap());
        let closed = close(&store, &manifest, None).await.unwrap();
        assert_eq!(closed.latest_checkpoint_ref, Some(second));
        assert!(!publish(&store, &manifest, &first, None).await.unwrap());
        assert!(
            initialize_generation(
                &store,
                InitializeGenerationRequest {
                    pipeline_id: PipelineId::new("P"),
                    job_id: JobId::new("J"),
                    generation: Generation(1),
                    updated_at: SystemTime::UNIX_EPOCH,
                },
                true
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn local_reconciliation_prunes_unlogged_manifest_after_pages_and_preserves_logged_head() {
        let root = std::env::temp_dir().join(format!(
            "streamr-orphan-reconcile-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.display()))
            .await
            .unwrap();
        let (manifest, _) = initialize(&store, 1).await;
        let kept = paths().checkpoint_manifest(Generation(1), Epoch(1));
        put_protobuf(&store, &kept, &checkpoint(false))
            .await
            .unwrap();
        assert!(publish(&store, &manifest, &kept, None).await.unwrap());
        let orphan = paths().checkpoint_manifest(Generation(1), Epoch(2));
        let mut unlogged = checkpoint(false);
        unlogged.epoch = 2;
        put_protobuf(&store, &orphan, &unlogged).await.unwrap();
        let file = CheckpointRef::new("P/J/generations/1/checkpoints/checkpoint-0000002/operator-o/table-m-000/disk-abandoned.bin").unwrap();
        ProtocolStore::put_bytes(&store, &file, vec![1])
            .await
            .unwrap();
        initialize(&store, 2).await;
        crate::gc::reconcile_abandoned_disk_uploads(&store, &paths())
            .await
            .unwrap();
        assert!(store.read_bytes(&file).await.unwrap().is_none());
        assert!(store.read_bytes(&orphan).await.unwrap().is_none());
        assert!(store.read_bytes(&kept).await.unwrap().is_some());
        crate::gc::reconcile_abandoned_disk_uploads(&store, &paths())
            .await
            .unwrap();
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
