//! Complete native database inventories and immutable-file inheritance.
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, DiskKeyedTableConfig, NativeDatabaseCheckpointMetadata, TableConfig,
    TableEnum, TypedStateTableConfig,
};
use prost::Message;
use std::collections::{HashMap, HashSet};

pub const FORMAT_VERSION: u32 = 1;
pub const MAX_METADATA_BYTES: usize = 3 * 1024 * 1024;
pub const MAX_FILES: usize = 65_536;
pub const MAX_TABLES: usize = 32;
pub const MAX_PATH_BYTES: usize = 4096;

pub fn current_prefix(
    job: &str,
    operator: &str,
    pipeline: Option<&str>,
    generation: u64,
    epoch: u32,
) -> String {
    if pipeline.is_none() {
        format!("{job}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/")
    } else {
        format!(
            "{}/{job}/generations/{generation}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/",
            pipeline.unwrap_or_default()
        )
    }
}

fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

fn numbered(name: &str, prefix: &str, suffix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|n| n.strip_suffix(suffix))
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

pub fn validate(
    metadata: &NativeDatabaseCheckpointMetadata,
    configs: &HashMap<String, TableConfig>,
    job: &str,
    operator: &str,
    pipeline: Option<&str>,
) -> Result<(), String> {
    if metadata.format_version != FORMAT_VERSION
        || metadata.subtask_index != 0
        || metadata.database_id.is_empty()
        || metadata.database_id.len() > 128
        || metadata.compatibility_identity.is_empty()
        || metadata.compatibility_identity.len() > 1024
        || metadata.encoded_len() > MAX_METADATA_BYTES
        || metadata.files.len() > MAX_FILES
        || metadata.tables.is_empty()
        || metadata.tables.len() > MAX_TABLES
        || metadata.generation != 0 && pipeline.is_none()
        || metadata
            .completed_base_epoch
            .is_some_and(|e| e >= metadata.epoch)
    {
        return Err("invalid native checkpoint version, ownership or resource bounds".into());
    }
    let live = configs
        .iter()
        .filter(|(_, c)| {
            matches!(
                c.table_type(),
                TableEnum::DiskKeyedMap | TableEnum::TypedStateTable
            )
        })
        .map(|(name, config)| (name.as_str(), config))
        .collect::<HashMap<_, _>>();
    if live.len() != metadata.tables.len() {
        return Err("native checkpoint table roster differs from configurations".into());
    }
    let mut tables = HashSet::new();
    for table in &metadata.tables {
        let config = table
            .config
            .as_ref()
            .ok_or("native table missing configuration")?;
        if !tables.insert(&table.name) || live.get(table.name.as_str()).copied() != Some(config) {
            return Err("native table configuration differs from registered roster".into());
        }
        let identity = match config.table_type() {
            TableEnum::DiskKeyedMap => {
                let c = DiskKeyedTableConfig::decode(config.config.as_slice())
                    .map_err(|e| e.to_string())?;
                if c.table_name != table.name
                    || c.encoding_version != 1
                    || c.schema_identity.is_empty()
                {
                    return Err("invalid native disk table configuration".into());
                }
                c.table_name.into_bytes()
            }
            TableEnum::TypedStateTable => {
                let c = TypedStateTableConfig::decode(config.config.as_slice())
                    .map_err(|e| e.to_string())?;
                crate::typed_checkpoint::validate_config(&c)?;
                if c.transport_name != table.name {
                    return Err("native typed table transport mismatch".into());
                }
                c.table_identity
            }
            _ => return Err("native database contains non-live table".into()),
        };
        crate::typed_checkpoint::validate_singleton_namespace(&identity, &table.namespace)
            .map_err(str::to_string)?;
    }
    let mut names = HashSet::new();
    let mut paths = HashSet::new();
    let mut current = false;
    let mut manifest = false;
    for file in &metadata.files {
        let sst = numbered(&file.local_name, "", ".sst");
        let allowed = sst
            || numbered(&file.local_name, "MANIFEST-", "")
            || numbered(&file.local_name, "OPTIONS-", "")
            || numbered(&file.local_name, "", ".log")
            || file.local_name == "CURRENT"
            || file.local_name == "IDENTITY";
        if !safe_name(&file.local_name)
            || !allowed
            || file.path.len() > MAX_PATH_BYTES
            || file.size_bytes == 0 && !numbered(&file.local_name, "", ".log")
            || file.checksum.len() != 32
            || !names.insert(&file.local_name)
            || !paths.insert(&file.path)
            || file.shared && !sst
            || file.birth_epoch > metadata.epoch
            || file.birth_generation > metadata.generation
            || !file.shared
                && (file.birth_epoch != metadata.epoch
                    || file.birth_generation != metadata.generation)
        {
            return Err("invalid native checkpoint file or shared-file owner".into());
        }
        let prefix = current_prefix(
            job,
            operator,
            pipeline,
            file.birth_generation,
            file.birth_epoch,
        );
        let suffix = file
            .path
            .strip_prefix(&prefix)
            .ok_or("native file belongs to another job/operator/owner")?;
        let parts = suffix.split('/').collect::<Vec<_>>();
        if parts.len() != 2
            || !parts[0].starts_with("native-")
            || !safe_name(parts[0])
            || parts[1] != file.local_name
        {
            return Err("native file outside immutable upload directory".into());
        }
        crate::types::CheckpointRef::new(file.path.clone()).map_err(|e| e.to_string())?;
        current |= file.local_name == "CURRENT";
        manifest |= numbered(&file.local_name, "MANIFEST-", "");
    }
    if !current || !manifest {
        return Err("native checkpoint missing CURRENT or MANIFEST".into());
    }
    Ok(())
}

pub fn validate_reuse(
    metadata: &NativeDatabaseCheckpointMetadata,
    base: Option<&NativeDatabaseCheckpointMetadata>,
) -> Result<(), String> {
    let inherited = metadata
        .files
        .iter()
        .filter(|f| f.birth_epoch != metadata.epoch || f.birth_generation != metadata.generation)
        .collect::<Vec<_>>();
    if inherited.is_empty() {
        return Ok(());
    }
    let base = base.ok_or("native shared files require an authoritative completed base")?;
    if metadata.completed_base_epoch != Some(base.epoch)
        || metadata.database_id != base.database_id
        || metadata.compatibility_identity != base.compatibility_identity
        || metadata
            .tables
            .iter()
            .map(|t| (&t.name, t))
            .collect::<HashMap<_, _>>()
            != base
                .tables
                .iter()
                .map(|t| (&t.name, t))
                .collect::<HashMap<_, _>>()
        || base.epoch >= metadata.epoch
    {
        return Err("native shared-file base identity/epoch mismatch".into());
    }
    let files = base
        .files
        .iter()
        .map(|f| (&f.path, f))
        .collect::<HashMap<_, _>>();
    for file in inherited {
        if !file.shared || files.get(&file.path).copied() != Some(file) {
            return Err("native shared file is not in the authoritative completed base".into());
        }
    }
    Ok(())
}

pub fn validate_manifest(manifest: &CheckpointManifest) -> Result<(), String> {
    for operator in &manifest.operators {
        let Some(native) = &operator.native_database else {
            continue;
        };
        let owner = operator
            .operator_metadata
            .as_ref()
            .ok_or("native checkpoint missing operator ownership")?;
        if owner.job_id != manifest.job_id
            || u64::from(owner.epoch) != manifest.epoch
            || owner.parallelism != 1
            || native.generation != manifest.generation
            || u64::from(native.epoch) != manifest.epoch
            || operator
                .table_checkpoint_metadata
                .iter()
                .any(|(name, _)| native.tables.iter().any(|t| &t.name == name))
        {
            return Err("native checkpoint manifest ownership/representation mismatch".into());
        }
        validate(
            native,
            &operator.table_configs,
            &manifest.job_id,
            &owner.operator_id,
            Some(&manifest.pipeline_id),
        )?;
    }
    Ok(())
}

/// Checks inheritance against the canonical parent before publication creates a
/// reference that a concurrent retention pass would have to protect.
pub async fn validate_manifest_reuse<S: crate::store::ProtocolStore + ?Sized>(
    store: &S,
    manifest: &CheckpointManifest,
) -> Result<(), crate::store::StoreError> {
    use crate::store::{StoreError, read_protobuf};
    use crate::types::CheckpointRef;
    if !manifest
        .operators
        .iter()
        .any(|op| op.native_database.is_some())
    {
        return Ok(());
    }
    let parent = match manifest.parent_checkpoint_ref.as_ref() {
        Some(path) => {
            read_protobuf::<_, CheckpointManifest>(store, &CheckpointRef::new(path.clone())?)
                .await?
        }
        None => None,
    };
    for operator in &manifest.operators {
        let Some(native) = &operator.native_database else {
            continue;
        };
        let base = parent
            .as_ref()
            .and_then(|p| {
                p.operators.iter().find(|o| {
                    o.operator_metadata.as_ref().map(|m| &m.operator_id)
                        == operator.operator_metadata.as_ref().map(|m| &m.operator_id)
                })
            })
            .and_then(|o| o.native_database.as_ref());
        validate_reuse(native, base).map_err(|msg| StoreError::InvalidProtobuf {
            path: CheckpointRef::new(format!(
                "{}/{}/native-inheritance",
                manifest.pipeline_id, manifest.job_id
            ))
            .expect("validated manifest owner"),
            msg,
        })?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arroyo_rpc::grpc::rpc::{NativeCheckpointFile, NativeCheckpointTable};
    pub(crate) fn fixture(
        epoch: u32,
        generation: u64,
        pipeline: Option<&str>,
    ) -> (
        NativeDatabaseCheckpointMetadata,
        HashMap<String, TableConfig>,
    ) {
        let config = TableConfig {
            table_type: TableEnum::DiskKeyedMap.into(),
            config: DiskKeyedTableConfig {
                table_name: "m".into(),
                encoding_version: 1,
                schema_identity: vec![1],
            }
            .encode_to_vec(),
            state_version: 1,
        };
        let prefix = current_prefix("J", "o", pipeline, generation, epoch);
        let files = ["CURRENT", "MANIFEST-000001", "000001.sst"]
            .into_iter()
            .map(|name| NativeCheckpointFile {
                local_name: name.into(),
                path: format!("{prefix}native-attempt{epoch}/{name}"),
                size_bytes: 1,
                checksum: vec![1; 32],
                shared: name.ends_with(".sst"),
                birth_generation: generation,
                birth_epoch: epoch,
            })
            .collect();
        (
            NativeDatabaseCheckpointMetadata {
                format_version: FORMAT_VERSION,
                generation,
                epoch,
                subtask_index: 0,
                database_id: vec![7; 16],
                compatibility_identity: "test-native-v1".into(),
                tables: vec![NativeCheckpointTable {
                    name: "m".into(),
                    config: Some(config.clone()),
                    namespace: crate::typed_checkpoint::singleton_namespace(b"m").unwrap(),
                }],
                files,
                completed_base_epoch: None,
            },
            [("m".into(), config)].into(),
        )
    }
    #[test]
    fn native_inventory_rejects_foreign_incomplete_and_mixed_owners() {
        for generation in [0, 1] {
            let (metadata, configs) = fixture(2, generation, Some("P"));
            validate(&metadata, &configs, "J", "o", Some("P")).unwrap();
            assert!(validate(&metadata, &configs, "foreign", "o", Some("P")).is_err());
            let mut bad = metadata.clone();
            bad.files.retain(|f| f.local_name != "CURRENT");
            assert!(validate(&bad, &configs, "J", "o", Some("P")).is_err());
            let mut bad = metadata.clone();
            bad.tables[0].namespace.push(1);
            assert!(validate(&bad, &configs, "J", "o", Some("P")).is_err());
            let mut bad = metadata.clone();
            bad.files[0].shared = true;
            assert!(validate(&bad, &configs, "J", "o", Some("P")).is_err());
            let mut bad = metadata.clone();
            bad.files[0].local_name = "../CURRENT".into();
            assert!(validate(&bad, &configs, "J", "o", Some("P")).is_err());
        }
        let (legacy, configs) = fixture(2, 0, None);
        validate(&legacy, &configs, "J", "o", None).unwrap();
        assert!(validate(&legacy, &configs, "J", "o", Some("P")).is_err());
    }
    #[test]
    fn native_inheritance_requires_the_complete_confirmed_base_descriptor() {
        let (base, _) = fixture(1, 1, Some("P"));
        let (mut next, configs) = fixture(2, 1, Some("P"));
        next.completed_base_epoch = Some(1);
        next.files[2] = base.files[2].clone();
        validate(&next, &configs, "J", "o", Some("P")).unwrap();
        validate_reuse(&next, Some(&base)).unwrap();
        assert!(validate_reuse(&next, None).is_err());
        let mut bad = next.clone();
        bad.files[2].checksum[0] ^= 1;
        assert!(validate_reuse(&bad, Some(&base)).is_err());
        let mut bad = next.clone();
        bad.completed_base_epoch = Some(0);
        assert!(validate_reuse(&bad, Some(&base)).is_err());
        let mut bad = next.clone();
        bad.database_id.push(1);
        assert!(validate_reuse(&bad, Some(&base)).is_err());
    }
}
