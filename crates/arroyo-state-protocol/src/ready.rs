//! Small immutable ancestry proof retained when old snapshot files and large
//! manifests are pruned. Epoch ownership and connector commit completion remain
//! verifiable without retaining the full file list for every historical epoch.
use crate::{
    ProtocolPaths,
    state::{CheckpointState, derive_checkpoint_state},
    store::{CreateResult, ProtocolStore, StoreError, create_json_if_not_exist, read_json},
    types::{CheckpointRef, CommittedMarker, Epoch, EpochRecord, Generation},
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReadyProof {
    version: u32,
    checkpoint_ref: CheckpointRef,
    epoch_record: EpochRecord,
    needs_commit: bool,
    committed_marker: Option<CommittedMarker>,
}
fn path(checkpoint_ref: &CheckpointRef) -> Result<CheckpointRef, StoreError> {
    let Some((prefix, suffix)) = checkpoint_ref.as_str().rsplit_once("/checkpoints/") else {
        return Err(StoreError::InvalidProtobuf {
            path: checkpoint_ref.clone(),
            msg: "invalid ancestry checkpoint path".into(),
        });
    };
    let Some(epoch) = suffix
        .strip_prefix("checkpoint-")
        .and_then(|s| s.strip_suffix("/checkpoint-manifest.pb"))
    else {
        return Err(StoreError::InvalidProtobuf {
            path: checkpoint_ref.clone(),
            msg: "invalid ancestry checkpoint path".into(),
        });
    };
    let number: u64 = epoch.parse().map_err(|_| StoreError::InvalidProtobuf {
        path: checkpoint_ref.clone(),
        msg: "invalid ancestry epoch".into(),
    })?;
    Ok(CheckpointRef::new(format!(
        "{prefix}/ready-proofs/checkpoint-{number:07}.json"
    ))?)
}

pub async fn preserve<S: ProtocolStore + ?Sized>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint_ref: &CheckpointRef,
    checkpoint: &CheckpointManifest,
) -> Result<bool, StoreError> {
    let epoch_record: Option<EpochRecord> =
        read_json(store, &paths.epoch_record(Epoch(checkpoint.epoch))).await?;
    let marker: Option<CommittedMarker> = if checkpoint.needs_commit {
        read_json(
            store,
            &paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch)),
        )
        .await?
    } else {
        None
    };
    if derive_checkpoint_state(
        checkpoint_ref,
        Some(checkpoint),
        epoch_record.clone(),
        marker.as_ref(),
    )? != CheckpointState::Ready
    {
        return Ok(false);
    }
    let proof = ReadyProof {
        version: 1,
        checkpoint_ref: checkpoint_ref.clone(),
        epoch_record: epoch_record.expect("ready state requires epoch owner"),
        needs_commit: checkpoint.needs_commit,
        committed_marker: marker,
    };
    let proof_path = path(checkpoint_ref)?;
    match create_json_if_not_exist(store, &proof_path, &proof).await? {
        CreateResult::Created => Ok(true),
        CreateResult::AlreadyExists(existing) if existing == proof => Ok(true),
        CreateResult::AlreadyExists(_) => Err(StoreError::InvalidProtobuf {
            path: proof_path,
            msg: "ancestry proof ownership conflict".into(),
        }),
    }
}

pub async fn exists<S: ProtocolStore + ?Sized>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint_ref: &CheckpointRef,
) -> Result<bool, StoreError> {
    let proof_path = path(checkpoint_ref)?;
    let Some(proof): Option<ReadyProof> = read_json(store, &proof_path).await? else {
        return Ok(false);
    };
    let record = &proof.epoch_record;
    if proof.version != 1
        || record.version != crate::types::PROTOCOL_VERSION
        || proof.checkpoint_ref != *checkpoint_ref
        || record.checkpoint_ref != *checkpoint_ref
        || record.pipeline_id != *paths.pipeline_id()
        || record.job_id != *paths.job_id()
        || paths.checkpoint_manifest(record.generation, record.epoch) != *checkpoint_ref
    {
        return Err(StoreError::InvalidProtobuf {
            path: proof_path,
            msg: "invalid ancestry proof owner/version".into(),
        });
    }
    let current: Option<EpochRecord> = read_json(store, &paths.epoch_record(record.epoch)).await?;
    if current.as_ref() != Some(record) {
        return Err(StoreError::InvalidProtobuf {
            path: proof_path,
            msg: "ancestry proof no longer matches the canonical epoch claim".into(),
        });
    }
    if proof.needs_commit {
        let Some(marker) = proof.committed_marker else {
            return Err(StoreError::InvalidProtobuf {
                path: proof_path,
                msg: "ancestry proof missing connector commit completion".into(),
            });
        };
        if marker.version != crate::types::PROTOCOL_VERSION
            || marker.checkpoint_ref != *checkpoint_ref
            || marker.pipeline_id != record.pipeline_id
            || marker.job_id != record.job_id
            || marker.epoch != record.epoch
            || marker.checkpoint_generation != record.generation
        {
            return Err(StoreError::InvalidProtobuf {
                path: proof_path,
                msg: "invalid ancestry proof connector commit completion".into(),
            });
        }
    }
    Ok(true)
}
