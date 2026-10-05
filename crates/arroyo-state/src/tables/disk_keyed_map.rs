//! Metadata dispatch for full logical disk-map snapshots. Runtime data is owned
//! by the live table manager rather than a legacy Parquet checkpointer.
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    DiskKeyedTableConfig, DiskKeyedTableSubtaskCheckpointMetadata,
    DiskKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata,
};
use arroyo_state_protocol::disk::{DISK_CHECKPOINT_VERSION, validate_subtask, validate_table};
use prost::Message;
use std::collections::{HashMap, HashSet};

pub struct DiskKeyedTable;
fn error(error: impl ToString) -> StateError {
    StateError::Other {
        table: "disk-map".into(),
        error: error.to_string(),
    }
}
impl DiskKeyedTable {
    pub fn merge_checkpoint_metadata(
        config: TableConfig,
        subtasks: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        if config.table_type() != TableEnum::DiskKeyedMap {
            return Err(error("disk-map table type mismatch"));
        }
        let config = DiskKeyedTableConfig::decode(config.config.as_slice()).map_err(error)?;
        let mut result = DiskKeyedTableTaskCheckpointMetadata {
            format_version: DISK_CHECKPOINT_VERSION,
            subtasks: HashMap::new(),
        };
        for (index, metadata) in subtasks {
            if metadata.table_type() != TableEnum::DiskKeyedMap || index != metadata.subtask_index {
                return Err(error("disk-map subtask type or ownership mismatch"));
            }
            let metadata =
                DiskKeyedTableSubtaskCheckpointMetadata::decode(metadata.data.as_slice())
                    .map_err(error)?;
            if metadata.subtask_index != index {
                return Err(error("disk-map subtask ownership mismatch"));
            }
            validate_subtask(&config, &metadata).map_err(error)?;
            result.format_version = metadata.format_version;
            result.subtasks.insert(index, metadata);
        }
        validate_table(&config, &result).map_err(error)?;
        Ok(Some(TableCheckpointMetadata {
            table_type: TableEnum::DiskKeyedMap.into(),
            data: result.encode_to_vec(),
        }))
    }

    pub fn files_to_keep(
        config: TableConfig,
        checkpoint: TableCheckpointMetadata,
    ) -> Result<HashSet<String>, StateError> {
        if config.table_type() != TableEnum::DiskKeyedMap
            || checkpoint.table_type() != TableEnum::DiskKeyedMap
        {
            return Err(error("disk-map table type mismatch"));
        }
        let config = DiskKeyedTableConfig::decode(config.config.as_slice()).map_err(error)?;
        let checkpoint = DiskKeyedTableTaskCheckpointMetadata::decode(checkpoint.data.as_slice())
            .map_err(error)?;
        validate_table(&config, &checkpoint).map_err(error)?;
        Ok(checkpoint
            .subtasks
            .into_values()
            .flat_map(|s| s.files.into_iter().map(|f| f.path))
            .collect())
    }
}
