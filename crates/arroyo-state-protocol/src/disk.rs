//! Validation shared by controller and leader disk-map checkpoint paths.
use arroyo_rpc::grpc::rpc::{
    DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
    DiskKeyedTableTaskCheckpointMetadata,
};
use std::collections::HashSet;

pub const DISK_CHECKPOINT_VERSION: u32 = 1;

pub fn validate_subtask(
    config: &DiskKeyedTableConfig,
    metadata: &DiskKeyedTableSubtaskCheckpointMetadata,
) -> Result<(), String> {
    if metadata.format_version != DISK_CHECKPOINT_VERSION
        || metadata.encoding_version != 1
        || config.encoding_version != 1
    {
        return Err("unsupported disk-map checkpoint or encoding version".into());
    }
    if config.table_name.contains(['/', '\\']) || matches!(config.table_name.as_str(), "." | "..") {
        return Err("disk-map table names must be a single safe checkpoint path component".into());
    }
    if config.table_name.is_empty()
        || config.schema_identity.is_empty()
        || config.schema_identity != metadata.schema_identity
        || metadata.namespace.is_empty()
    {
        return Err("disk-map checkpoint schema or namespace does not match configuration".into());
    }
    let table_len =
        u32::try_from(config.table_name.len()).map_err(|_| "disk-map table name too long")?;
    let mut expected_namespace = vec![1, 0];
    expected_namespace.extend_from_slice(&0u32.to_be_bytes());
    expected_namespace.extend_from_slice(&1u32.to_be_bytes());
    expected_namespace.extend_from_slice(&table_len.to_be_bytes());
    expected_namespace.extend_from_slice(config.table_name.as_bytes());
    if metadata.namespace != expected_namespace {
        return Err("disk-map namespace does not match singleton table ownership".into());
    }
    if metadata.subtask_index != 0 {
        return Err("disk maps currently require singleton ownership".into());
    }
    if metadata.empty != metadata.files.is_empty() {
        return Err("disk-map checkpoint must declare explicit empty state or a complete nonempty file list".into());
    }
    let mut seen = HashSet::new();
    for file in &metadata.files {
        crate::types::CheckpointRef::new(file.path.clone()).map_err(|e| e.to_string())?;
        if file.size_bytes == 0
            || file.row_count == 0
            || file.checksum.len() != 32
            || !seen.insert(&file.path)
        {
            return Err("invalid or duplicate disk-map checkpoint file".into());
        }
        if !file
            .path
            .contains(&format!("/checkpoints/checkpoint-{:07}/", metadata.epoch))
        {
            return Err("disk-map checkpoint file is owned by another epoch".into());
        }
        let table_directory = format!("/table-{}-000/disk-", config.table_name);
        if !file.path.contains(&table_directory) {
            return Err("disk-map checkpoint file is owned by another table".into());
        }
        if file.path.contains("/generations/") {
            if !file.path.contains(&format!(
                "/generations/{}/checkpoints/",
                metadata.generation
            )) {
                return Err("disk-map checkpoint file is owned by another generation".into());
            }
        } else if metadata.generation != 0 {
            return Err("legacy disk-map checkpoint must declare generation zero".into());
        }
    }
    Ok(())
}

pub fn validate_table(
    config: &DiskKeyedTableConfig,
    metadata: &DiskKeyedTableTaskCheckpointMetadata,
) -> Result<(), String> {
    if metadata.format_version != DISK_CHECKPOINT_VERSION
        || metadata.subtasks.len() != 1
        || !metadata.subtasks.contains_key(&0)
    {
        return Err(
            "disk maps require a versioned singleton checkpoint; rescaling is unsupported".into(),
        );
    }
    for (&index, subtask) in &metadata.subtasks {
        if index != subtask.subtask_index {
            return Err("disk-map subtask ownership mismatch".into());
        }
        validate_subtask(config, subtask)?;
    }
    Ok(())
}

/// Validate exclusive ownership before publishing or deleting checkpoint files.
pub fn validate_manifest(
    manifest: &arroyo_rpc::grpc::rpc::CheckpointManifest,
) -> Result<(), String> {
    use arroyo_rpc::grpc::rpc::{
        DiskKeyedTableConfig, DiskKeyedTableTaskCheckpointMetadata, TableEnum,
        TypedStateTableConfig, TypedStateTableTaskCheckpointMetadata,
    };
    use prost::Message;
    for operator in &manifest.operators {
        for (table_name, metadata) in &operator.table_checkpoint_metadata {
            if metadata.table_type() == TableEnum::TypedStateTable {
                let op = operator
                    .operator_metadata
                    .as_ref()
                    .ok_or("missing typed table operator ownership")?;
                if op.parallelism != 1
                    || u64::from(op.epoch) != manifest.epoch
                    || op.job_id != manifest.job_id
                {
                    return Err("typed state-table operator checkpoint ownership mismatch".into());
                }
                let config = operator
                    .table_configs
                    .get(table_name)
                    .ok_or("missing typed state-table configuration")?;
                if config.table_type() != TableEnum::TypedStateTable {
                    return Err("typed state-table configuration type mismatch".into());
                }
                let config = TypedStateTableConfig::decode(config.config.as_slice())
                    .map_err(|e| e.to_string())?;
                if config.transport_name != *table_name {
                    return Err("typed state-table transport name mismatch".into());
                }
                let metadata =
                    TypedStateTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                        .map_err(|e| e.to_string())?;
                crate::typed_checkpoint::validate_table(&config, &metadata)?;
                let prefix = format!(
                    "{}/{}/generations/{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000/",
                    manifest.pipeline_id,
                    manifest.job_id,
                    manifest.generation,
                    manifest.epoch,
                    op.operator_id,
                    table_name
                );
                for subtask in metadata.subtasks.values() {
                    if subtask.generation != manifest.generation
                        || u64::from(subtask.epoch) != manifest.epoch
                    {
                        return Err(
                            "typed state-table checkpoint generation or epoch mismatch".into()
                        );
                    }
                    for file in &subtask.files {
                        if !file.path.starts_with(&prefix) {
                            return Err("typed state-table file is outside exclusive checkpoint table directory".into());
                        }
                    }
                }
                continue;
            }
            if metadata.table_type() != TableEnum::DiskKeyedMap {
                continue;
            }
            let op = operator
                .operator_metadata
                .as_ref()
                .ok_or("missing disk-map operator ownership")?;
            if op.parallelism != 1
                || u64::from(op.epoch) != manifest.epoch
                || op.job_id != manifest.job_id
            {
                return Err("disk-map operator checkpoint ownership mismatch".into());
            }
            let config = operator
                .table_configs
                .get(table_name)
                .ok_or("missing disk-map configuration")?;
            if config.table_type() != TableEnum::DiskKeyedMap {
                return Err("disk-map configuration type mismatch".into());
            }
            let config = DiskKeyedTableConfig::decode(config.config.as_slice())
                .map_err(|e| e.to_string())?;
            if config.table_name != *table_name {
                return Err("disk-map table name mismatch".into());
            }
            let metadata = DiskKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .map_err(|e| e.to_string())?;
            validate_table(&config, &metadata)?;
            let prefix = format!(
                "{}/{}/generations/{}/checkpoints/checkpoint-{:07}/operator-{}/table-{}-000",
                manifest.pipeline_id,
                manifest.job_id,
                manifest.generation,
                manifest.epoch,
                op.operator_id,
                table_name
            );
            for subtask in metadata.subtasks.values() {
                if subtask.generation != manifest.generation
                    || u64::from(subtask.epoch) != manifest.epoch
                {
                    return Err("disk-map checkpoint generation or epoch mismatch".into());
                }
                for file in &subtask.files {
                    if !file.path.starts_with(&format!("{prefix}/"))
                        && !file.path.starts_with(&format!("{prefix}-"))
                    {
                        return Err(
                            "disk-map file is outside its exclusive checkpoint table directory"
                                .into(),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::grpc::rpc::DiskCheckpointFile;
    fn fixture() -> (
        DiskKeyedTableConfig,
        DiskKeyedTableSubtaskCheckpointMetadata,
    ) {
        let mut namespace = vec![1, 0];
        namespace.extend_from_slice(&0u32.to_be_bytes());
        namespace.extend_from_slice(&1u32.to_be_bytes());
        namespace.extend_from_slice(&1u32.to_be_bytes());
        namespace.push(b'm');
        (DiskKeyedTableConfig { table_name: "m".into(), encoding_version: 1, schema_identity: vec![1] }, DiskKeyedTableSubtaskCheckpointMetadata { subtask_index: 0, format_version: 1, encoding_version: 1, schema_identity: vec![1], namespace, generation: 2, epoch: 3, empty: false, files: vec![DiskCheckpointFile { path: "P/J/generations/2/checkpoints/checkpoint-0000003/operator-o/table-m-000/disk-0.bin".into(), size_bytes: 10, row_count: 1, checksum: vec![0;32] }] })
    }
    #[test]
    fn explicit_empty_and_complete_lists() {
        let (config, mut metadata) = fixture();
        assert!(validate_subtask(&config, &metadata).is_ok());
        metadata.files.clear();
        assert!(validate_subtask(&config, &metadata).is_err());
        metadata.empty = true;
        assert!(validate_subtask(&config, &metadata).is_ok());
    }
    #[test]
    fn reject_incompatible_or_ambiguous_ownership() {
        let (config, metadata) = fixture();
        let mut invalid = metadata.clone();
        invalid.format_version += 1;
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.schema_identity.push(2);
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.namespace[9] = 2;
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.files.push(invalid.files[0].clone());
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.files[0].checksum.clear();
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.generation += 1;
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.files[0].path = invalid.files[0]
            .path
            .replace("table-m-000", "table-other-000");
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata;
        invalid.files[0].path = "P/J/checkpoints/checkpoint-0000002/foreign".into();
        assert!(validate_subtask(&config, &invalid).is_err());
    }
}
