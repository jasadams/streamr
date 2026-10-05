//! Stable, path-safe transport names for typed live-state table snapshots.
//! The logical namespace remains the caller's opaque full table identity;
//! checkpoint paths use this digest only as an exclusive transport component.
use arroyo_rpc::grpc::rpc::{
    TableCheckpointMetadata, TableConfig, TableEnum, TypedStateTableConfig,
    TypedStateTableSubtaskCheckpointMetadata, TypedStateTableTaskCheckpointMetadata,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub const TYPED_CHECKPOINT_VERSION: u32 = 2;

pub fn validate_config(config: &TypedStateTableConfig) -> Result<(), String> {
    validate_transport_table_name(&config.table_identity, &config.transport_name)
        .map_err(|error| error.to_string())?;
    if config.encoding_version != 1
        || config.schema_identity.is_empty()
        || config.schema_json.is_empty()
        || config.primary_key.is_empty()
    {
        return Err("invalid typed state-table descriptor or encoding version".into());
    }
    let mut key = HashSet::new();
    if !config.primary_key.iter().all(|index| key.insert(*index)) {
        return Err("duplicate typed state-table primary-key column".into());
    }
    let mut partition = HashSet::new();
    if !config
        .partition_key
        .iter()
        .all(|index| key.contains(index) && partition.insert(*index))
    {
        return Err(
            "typed state-table partition key must be a distinct subset of primary key".into(),
        );
    }
    Ok(())
}

pub fn validate_subtask(
    config: &TypedStateTableConfig,
    metadata: &TypedStateTableSubtaskCheckpointMetadata,
) -> Result<(), String> {
    validate_config(config)?;
    if crate::disk::checkpoint_file_extension(metadata.format_version).is_err()
        || metadata.encoding_version != config.encoding_version
        || metadata.schema_identity != config.schema_identity
        || metadata.subtask_index != 0
        || metadata.empty != metadata.files.is_empty()
    {
        return Err(
            "typed state-table checkpoint descriptor, format, or singleton ownership mismatch"
                .into(),
        );
    }
    validate_singleton_namespace(&config.table_identity, &metadata.namespace)
        .map_err(|error| error.to_string())?;
    let extension = crate::disk::checkpoint_file_extension(metadata.format_version)?;
    let mut seen = HashSet::new();
    for file in &metadata.files {
        crate::types::CheckpointRef::new(file.path.clone()).map_err(|e| e.to_string())?;
        if file.size_bytes == 0
            || file.row_count == 0
            || file.checksum.len() != 32
            || !seen.insert(&file.path)
        {
            return Err("invalid or duplicate typed checkpoint file".into());
        }
        let basename = file.path.rsplit('/').next().unwrap_or_default();
        if !basename.starts_with("disk-")
            || !basename.ends_with(extension)
            || basename.contains('\\')
        {
            return Err("typed checkpoint file format differs from metadata".into());
        }
        if !file
            .path
            .contains(&format!("/checkpoints/checkpoint-{:07}/", metadata.epoch))
            || !file
                .path
                .contains(&format!("/table-{}-000/disk-", config.transport_name))
        {
            return Err("typed checkpoint file belongs to another epoch or table".into());
        }
        if file.path.contains("/generations/") {
            if !file.path.contains(&format!(
                "/generations/{}/checkpoints/",
                metadata.generation
            )) {
                return Err("typed checkpoint file belongs to another generation".into());
            }
        } else if metadata.generation != 0 {
            return Err("legacy typed checkpoint must declare generation zero".into());
        }
    }
    Ok(())
}

pub fn validate_table(
    config: &TypedStateTableConfig,
    metadata: &TypedStateTableTaskCheckpointMetadata,
) -> Result<(), String> {
    if crate::disk::checkpoint_file_extension(metadata.format_version).is_err()
        || metadata.subtasks.len() != 1
        || !metadata.subtasks.contains_key(&0)
    {
        return Err("typed state table requires one singleton checkpoint".into());
    }
    for (&index, subtask) in &metadata.subtasks {
        if index != subtask.subtask_index || subtask.format_version != metadata.format_version {
            return Err("typed state-table subtask ownership mismatch".into());
        }
        validate_subtask(config, subtask)?;
    }
    Ok(())
}

pub fn files_to_keep(
    config: TableConfig,
    checkpoint: TableCheckpointMetadata,
) -> Result<HashSet<String>, String> {
    if config.table_type() != TableEnum::TypedStateTable
        || checkpoint.table_type() != TableEnum::TypedStateTable
    {
        return Err("typed state-table type mismatch".into());
    }
    let config =
        TypedStateTableConfig::decode(config.config.as_slice()).map_err(|e| e.to_string())?;
    let checkpoint = TypedStateTableTaskCheckpointMetadata::decode(checkpoint.data.as_slice())
        .map_err(|e| e.to_string())?;
    validate_table(&config, &checkpoint)?;
    Ok(checkpoint
        .subtasks
        .into_values()
        .flat_map(|s| s.files.into_iter().map(|f| f.path))
        .collect())
}

pub fn transport_table_name(table_identity: &[u8]) -> Result<String, &'static str> {
    if table_identity.is_empty() {
        return Err("typed table identity must not be empty");
    }
    let digest = Sha256::digest(table_identity);
    let mut name = String::with_capacity(3 + digest.len() * 2);
    name.push_str("st_");
    for byte in digest {
        name.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        name.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    Ok(name)
}

pub fn validate_transport_table_name(
    table_identity: &[u8],
    transport_name: &str,
) -> Result<(), &'static str> {
    if transport_table_name(table_identity)? != transport_name {
        return Err("typed checkpoint transport name differs from table identity");
    }
    Ok(())
}

/// The live key encoding's version-1 partition-local singleton namespace.
/// This is checked by controller/leader before a checkpoint becomes runnable;
/// workers also compare it against their freshly constructed typed namespace.
pub fn singleton_namespace(table_identity: &[u8]) -> Result<Vec<u8>, &'static str> {
    let length =
        u32::try_from(table_identity.len()).map_err(|_| "table identity exceeds wire format")?;
    if length == 0 {
        return Err("typed table identity must not be empty");
    }
    let mut namespace = Vec::with_capacity(2 + 4 + 4 + 4 + table_identity.len());
    namespace.extend_from_slice(&[1, 0]);
    namespace.extend_from_slice(&0u32.to_be_bytes());
    namespace.extend_from_slice(&1u32.to_be_bytes());
    namespace.extend_from_slice(&length.to_be_bytes());
    namespace.extend_from_slice(table_identity);
    Ok(namespace)
}

pub fn validate_singleton_namespace(
    table_identity: &[u8],
    encoded_namespace: &[u8],
) -> Result<(), &'static str> {
    if singleton_namespace(table_identity)? != encoded_namespace {
        return Err("typed checkpoint namespace differs from singleton table ownership");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::grpc::rpc::DiskCheckpointFile;

    fn fixture() -> (
        TypedStateTableConfig,
        TypedStateTableSubtaskCheckpointMetadata,
    ) {
        let table_identity = br#"state-table-v1:["public","items"]"#.to_vec();
        let transport_name = transport_table_name(&table_identity).unwrap();
        let config = TypedStateTableConfig {
            transport_name: transport_name.clone(),
            table_identity: table_identity.clone(),
            schema_identity: b"schema-v1".to_vec(),
            schema_json: b"{}".to_vec(),
            primary_key: vec![0],
            partition_key: vec![0],
            encoding_version: 1,
        };
        let metadata = TypedStateTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            format_version: TYPED_CHECKPOINT_VERSION,
            encoding_version: 1,
            schema_identity: config.schema_identity.clone(),
            namespace: singleton_namespace(&table_identity).unwrap(),
            generation: 2,
            epoch: 3,
            empty: false,
            files: vec![DiskCheckpointFile {
                path: format!(
                    "P/J/generations/2/checkpoints/checkpoint-0000003/operator-o/table-{transport_name}-000/disk-0.parquet"
                ),
                size_bytes: 10,
                row_count: 1,
                checksum: vec![0; 32],
            }],
        };
        (config, metadata)
    }

    #[test]
    fn legacy_and_parquet_checkpoints_keep_distinct_file_formats() {
        let (config, mut subtask) = fixture();
        validate_subtask(&config, &subtask).unwrap();
        subtask.format_version = 1;
        assert!(validate_subtask(&config, &subtask).is_err());
        subtask.files[0].path = subtask.files[0].path.replace(".parquet", ".bin");
        validate_subtask(&config, &subtask).unwrap();
        let mut table = TypedStateTableTaskCheckpointMetadata {
            format_version: 1,
            subtasks: [(0, subtask)].into_iter().collect(),
        };
        validate_table(&config, &table).unwrap();
        table.format_version = TYPED_CHECKPOINT_VERSION;
        assert!(validate_table(&config, &table).is_err());
    }

    #[test]
    fn typed_checkpoint_binds_descriptor_and_exclusive_owner() {
        let (config, metadata) = fixture();
        validate_subtask(&config, &metadata).unwrap();
        let mut invalid = metadata.clone();
        invalid.namespace[13] ^= 1;
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = metadata.clone();
        invalid.files[0].path = invalid.files[0]
            .path
            .replace(&config.transport_name, "other");
        assert!(validate_subtask(&config, &invalid).is_err());
        let mut invalid = config.clone();
        invalid.table_identity.push(b'!');
        assert!(validate_subtask(&invalid, &metadata).is_err());
        let mut invalid = metadata.clone();
        invalid.files.clear();
        assert!(validate_subtask(&config, &invalid).is_err());
        invalid.empty = true;
        assert!(validate_subtask(&config, &invalid).is_ok());
    }

    #[test]
    fn transport_name_preserves_opaque_identity_without_using_it_as_a_path() {
        let dotted = transport_table_name(br#"state-table-v1:["public","items"]"#).unwrap();
        let quoted = transport_table_name(br#"state-table-v1:["public.items"]"#).unwrap();
        let slash = transport_table_name(br#"state-table-v1:["a/b"]"#).unwrap();
        assert_ne!(dotted, quoted);
        assert_ne!(dotted, slash);
        assert_eq!(dotted.len(), 67);
        assert!(
            slash
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        );
        assert_eq!(
            transport_table_name(br#"state-table-v1:["a/b"]"#).unwrap(),
            slash
        );
        validate_transport_table_name(br#"state-table-v1:["a/b"]"#, &slash).unwrap();
        assert!(validate_transport_table_name(br#"state-table-v1:["a/b"]"#, &dotted).is_err());
        let namespace = singleton_namespace(br#"state-table-v1:["a/b"]"#).unwrap();
        validate_singleton_namespace(br#"state-table-v1:["a/b"]"#, &namespace).unwrap();
        assert!(
            validate_singleton_namespace(br#"state-table-v1:["public","items"]"#, &namespace)
                .is_err()
        );
        assert!(namespace.ends_with(br#"state-table-v1:["a/b"]"#));
        assert_eq!(
            u32::from_be_bytes(namespace[10..14].try_into().unwrap()) as usize,
            namespace.len() - 14
        );
    }
}
