use super::*;
use arroyo_rpc::grpc::rpc::{
    DiskKeyedTableConfig, NativeCheckpointFile, NativeCheckpointTable,
    NativeDatabaseCheckpointMetadata, OperatorMetadata, TableConfig, TableEnum,
};

fn fixture(epoch: u32) -> OperatorCheckpointMetadata {
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
    let prefix = arroyo_state_protocol::native_checkpoint::current_prefix("J", "o", None, 0, epoch);
    OperatorCheckpointMetadata {
        operator_metadata: Some(OperatorMetadata {
            job_id: "J".into(),
            operator_id: "o".into(),
            epoch,
            parallelism: 1,
            ..Default::default()
        }),
        table_configs: [("m".into(), config.clone())].into(),
        native_database: Some(NativeDatabaseCheckpointMetadata {
            format_version: 1,
            epoch,
            generation: 0,
            subtask_index: 0,
            database_id: vec![7; 16],
            compatibility_identity: "native-test".into(),
            tables: vec![NativeCheckpointTable {
                name: "m".into(),
                config: Some(config),
                namespace: arroyo_state_protocol::typed_checkpoint::singleton_namespace(b"m")
                    .unwrap(),
            }],
            files: ["CURRENT", "MANIFEST-000001", "000001.sst"]
                .into_iter()
                .map(|name| NativeCheckpointFile {
                    local_name: name.into(),
                    path: format!("{prefix}native-{epoch}/{name}"),
                    size_bytes: 1,
                    checksum: vec![1; 32],
                    shared: name.ends_with(".sst"),
                    birth_generation: 0,
                    birth_epoch: epoch,
                })
                .collect(),
            completed_base_epoch: None,
        }),
        ..Default::default()
    }
}
async fn write(storage: &arroyo_storage::StorageProvider, metadata: &OperatorCheckpointMetadata) {
    let owner = metadata.operator_metadata.as_ref().unwrap();
    for file in &metadata.native_database.as_ref().unwrap().files {
        if !storage.exists(file.path.clone()).await.unwrap() {
            storage
                .put_if_not_exists(file.path.clone(), vec![1])
                .await
                .unwrap();
        }
    }
    storage
        .put(
            metadata_path(&operator_path("J", owner.epoch, "o")),
            metadata.encode_to_vec(),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn controller_native_gc_marks_all_retained_epochs_and_reclaims_final_shared_reference() {
    let directory = tempfile::tempdir().unwrap();
    let storage =
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", directory.path().display()))
            .await
            .unwrap();
    let first = fixture(1);
    let shared_a = first.native_database.as_ref().unwrap().files[2].clone();
    write(&storage, &first).await;
    let mut second = fixture(2);
    let native = second.native_database.as_mut().unwrap();
    native.files[2].local_name = "000002.sst".into();
    native.files[2].path = native.files[2].path.replace("000001.sst", "000002.sst");
    let shared_b = native.files[2].clone();
    native.files.push(shared_a.clone());
    native.completed_base_epoch = Some(1);
    write(&storage, &second).await;
    let mut third = fixture(3);
    third.native_database.as_mut().unwrap().files[2] = shared_b.clone();
    third.native_database.as_mut().unwrap().completed_base_epoch = Some(2);
    write(&storage, &third).await;
    let metadata = CheckpointMetadata {
        job_id: "J".into(),
        epoch: 3,
        min_epoch: 1,
        operator_ids: vec!["o".into()],
        ..Default::default()
    };
    cleanup_checkpoint_files(&storage, metadata.clone(), 1, 2)
        .await
        .unwrap();
    assert!(storage.exists(shared_a.path.clone()).await.unwrap());
    assert!(storage.exists(shared_b.path.clone()).await.unwrap());
    cleanup_checkpoint_files(&storage, metadata, 2, 3)
        .await
        .unwrap();
    assert!(!storage.exists(shared_a.path).await.unwrap());
    assert!(storage.exists(shared_b.path.clone()).await.unwrap());
    let fourth = fixture(4);
    write(&storage, &fourth).await;
    let metadata = CheckpointMetadata {
        job_id: "J".into(),
        epoch: 4,
        min_epoch: 3,
        operator_ids: vec!["o".into()],
        ..Default::default()
    };
    cleanup_checkpoint_files(&storage, metadata, 3, 4)
        .await
        .unwrap();
    assert!(!storage.exists(shared_b.path).await.unwrap());
}
#[tokio::test]
async fn missing_retained_native_root_fails_before_payload_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let storage =
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", directory.path().display()))
            .await
            .unwrap();
    let first = fixture(1);
    let path = first.native_database.as_ref().unwrap().files[2]
        .path
        .clone();
    write(&storage, &first).await;
    write(&storage, &fixture(3)).await;
    let metadata = CheckpointMetadata {
        job_id: "J".into(),
        epoch: 3,
        min_epoch: 1,
        operator_ids: vec!["o".into()],
        ..Default::default()
    };
    assert!(
        cleanup_checkpoint_files(&storage, metadata, 1, 2)
            .await
            .is_err()
    );
    assert!(storage.exists(path).await.unwrap());
}

#[tokio::test]
async fn missing_native_roots_cannot_be_mistaken_for_a_logical_only_history() {
    let directory = tempfile::tempdir().unwrap();
    let storage =
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", directory.path().display()))
            .await
            .unwrap();
    let first = fixture(1);
    let mut second = fixture(2);
    second.native_database.as_mut().unwrap().files[2] =
        first.native_database.as_ref().unwrap().files[2].clone();
    second
        .native_database
        .as_mut()
        .unwrap()
        .completed_base_epoch = Some(1);
    write(&storage, &first).await;
    write(&storage, &second).await;
    // A previous pass may already have removed the old descriptor. Losing the
    // retained descriptor must not make its inherited SST appear abandoned.
    for epoch in [1, 2] {
        storage
            .delete(&metadata_path(&operator_path("J", epoch, "o")))
            .await
            .unwrap();
    }
    let mut logical = fixture(3);
    logical.native_database = None;
    logical.table_checkpoint_metadata.insert(
        "m".into(),
        TableCheckpointMetadata {
            table_type: TableEnum::DiskKeyedMap.into(),
            data: rpc::DiskKeyedTableTaskCheckpointMetadata::default().encode_to_vec(),
        },
    );
    let logical_path = metadata_path(&operator_path("J", 3, "o"));
    storage
        .put(logical_path.clone(), logical.encode_to_vec())
        .await
        .unwrap();
    let metadata = CheckpointMetadata {
        job_id: "J".into(),
        epoch: 3,
        min_epoch: 1,
        operator_ids: vec!["o".into()],
        ..Default::default()
    };
    let error = cleanup_checkpoint_files(&storage, metadata, 1, 2)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing retained native controller root")
    );
    for file in &second.native_database.as_ref().unwrap().files {
        assert!(storage.exists(file.path.clone()).await.unwrap());
    }
    assert!(storage.exists(logical_path).await.unwrap());
}
