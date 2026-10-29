use super::*;

fn owner_paths(operator: &str, paths: &HashSet<String>) -> HashMap<String, HashSet<String>> {
    HashMap::from([(operator.to_owned(), paths.clone())])
}

#[derive(Clone, Copy, Default)]
enum CleanupFault {
    #[default]
    None,
    KnownFile,
    Pruning,
    SkipPruning,
    Listing,
    OrphanDelete,
    MetadataDelete,
    Publish,
}

#[derive(Default)]
struct CleanupTrace {
    listings: usize,
    orphan_deletes: usize,
    metadata_deletes: usize,
    deleted: Vec<String>,
}

struct ObservedCleanupStore {
    inner: arroyo_storage::StorageProvider,
    fault: CleanupFault,
    trace: std::sync::Mutex<CleanupTrace>,
}

fn injected_cleanup_error() -> StateError {
    StateError::Other {
        table: "cleanup-test".into(),
        error: "injected storage operation failure".into(),
    }
}

#[async_trait::async_trait]
impl CleanupStore for ObservedCleanupStore {
    async fn load_operator(
        &self,
        job: &str,
        operator: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError> {
        CleanupStore::load_operator(&self.inner, job, operator, epoch).await
    }
    async fn delete(&self, path: &str) -> Result<(), StateError> {
        let fail = {
            let trace = self.trace.lock().unwrap();
            match self.fault {
                CleanupFault::KnownFile => {
                    path.contains("checkpoint-0000002/operator-o/table-t-000/disk-")
                }
                CleanupFault::OrphanDelete => {
                    trace.listings > 0 && trace.orphan_deletes == 1 && path.contains("/disk-")
                }
                CleanupFault::MetadataDelete => {
                    trace.metadata_deletes == 1 && path.ends_with("/metadata")
                }
                _ => false,
            }
        };
        if fail {
            return Err(injected_cleanup_error());
        }
        CleanupStore::delete(&self.inner, path).await?;
        let mut trace = self.trace.lock().unwrap();
        if trace.listings > 0 && path.contains("/disk-") {
            trace.orphan_deletes += 1;
        }
        if path.ends_with("/metadata") {
            trace.metadata_deletes += 1;
        }
        trace.deleted.push(path.into());
        Ok(())
    }
    async fn prune_empty_directories(&self, prefix: &str) -> Result<(), StateError> {
        // Baseline control exists only in this private test adapter.
        if matches!(self.fault, CleanupFault::SkipPruning) {
            return Ok(());
        }
        if matches!(self.fault, CleanupFault::Pruning) {
            return Err(injected_cleanup_error());
        }
        CleanupStore::prune_empty_directories(&self.inner, prefix).await
    }
    async fn publish(&self, metadata: CheckpointMetadata) -> Result<(), StateError> {
        if matches!(self.fault, CleanupFault::Publish) {
            return Err(injected_cleanup_error());
        }
        CleanupStore::publish(&self.inner, metadata).await
    }
    fn qualified_prefix(&self, prefix: &str) -> String {
        CleanupStore::qualified_prefix(&self.inner, prefix)
    }
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<
        futures::stream::BoxStream<'_, Result<object_store::path::Path, object_store::Error>>,
        StateError,
    > {
        self.trace.lock().unwrap().listings += 1;
        let listing = CleanupStore::list_prefix(&self.inner, prefix).await?;
        if matches!(self.fault, CleanupFault::Listing) {
            // Fail the actual streaming operation after yielding its first object.
            let error = object_store::Error::Generic {
                store: "cleanup-test",
                source: std::io::Error::other("injected listing failure").into(),
            };
            Ok(listing
                .take(1)
                .chain(futures::stream::once(async move { Err(error) }))
                .boxed())
        } else {
            Ok(listing)
        }
    }
}

fn disk_operator_metadata(operator: &str, epoch: u32) -> (OperatorCheckpointMetadata, String) {
    let table = "t";
    let path = format!(
        "J/checkpoints/checkpoint-{epoch:07}/operator-{operator}/table-t-000/disk-{epoch}.parquet"
    );
    let mut namespace = vec![1, 0];
    namespace.extend_from_slice(&0u32.to_be_bytes());
    namespace.extend_from_slice(&1u32.to_be_bytes());
    namespace.extend_from_slice(&1u32.to_be_bytes());
    namespace.push(b't');
    let subtask = rpc::DiskKeyedTableSubtaskCheckpointMetadata {
        format_version: 2,
        encoding_version: 1,
        schema_identity: b"schema".to_vec(),
        namespace,
        epoch,
        files: vec![rpc::DiskCheckpointFile {
            path: path.clone(),
            size_bytes: 1,
            checksum: vec![0; 32],
            row_count: 1,
        }],
        ..Default::default()
    };
    let metadata = rpc::DiskKeyedTableTaskCheckpointMetadata {
        format_version: 2,
        subtasks: HashMap::from([(0, subtask)]),
    };
    let config = rpc::DiskKeyedTableConfig {
        table_name: table.into(),
        encoding_version: 1,
        schema_identity: b"schema".to_vec(),
    };
    (
        OperatorCheckpointMetadata {
            native_database: None,
            operator_metadata: Some(rpc::OperatorMetadata {
                job_id: "J".into(),
                operator_id: operator.into(),
                epoch,
                parallelism: 1,
                ..Default::default()
            }),
            table_configs: HashMap::from([(
                table.into(),
                rpc::TableConfig {
                    table_type: rpc::TableEnum::DiskKeyedMap.into(),
                    config: config.encode_to_vec(),
                    state_version: 1,
                },
            )]),
            table_checkpoint_metadata: HashMap::from([(
                table.into(),
                rpc::TableCheckpointMetadata {
                    table_type: rpc::TableEnum::DiskKeyedMap.into(),
                    data: metadata.encode_to_vec(),
                },
            )]),
            ..Default::default()
        },
        path,
    )
}

fn checkpoint_cleanup_fixture(root: &Path) -> (CheckpointMetadata, ObjectMap) {
    let mut objects = ObjectMap::new();
    let owners = vec!["o".to_owned(), "p".to_owned(), "empty".to_owned()];
    let global_keep = "J/checkpoints/checkpoint-0000001/operator-p/table-k-000";
    objects.insert(global_keep.into(), vec![7]);
    for epoch in 1..=3 {
        let checkpoint = CheckpointMetadata {
            job_id: "J".into(),
            epoch,
            min_epoch: 1,
            operator_ids: owners.clone(),
            ..Default::default()
        };
        objects.insert(
            metadata_path(&base_path("J", epoch)),
            checkpoint.encode_to_vec(),
        );
        let (disk, path) = disk_operator_metadata("o", epoch);
        objects.insert(path, vec![epoch as u8]);
        objects.insert(
            metadata_path(&operator_path("J", epoch, "o")),
            disk.encode_to_vec(),
        );
        let files = if epoch == 2 {
            vec!["J/checkpoints/checkpoint-0000002/operator-p/table-k-000".to_owned()]
        } else {
            vec![global_keep.to_owned()]
        };
        for path in &files {
            objects.entry(path.clone()).or_insert(vec![7]);
        }
        let global = OperatorCheckpointMetadata {
            native_database: None,
            operator_metadata: Some(rpc::OperatorMetadata {
                job_id: "J".into(),
                operator_id: "p".into(),
                epoch,
                parallelism: 1,
                ..Default::default()
            }),
            table_configs: HashMap::from([(
                "k".into(),
                rpc::TableConfig {
                    table_type: rpc::TableEnum::GlobalKeyValue.into(),
                    config: rpc::GlobalKeyedTableConfig {
                        table_name: "k".into(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                    state_version: 1,
                },
            )]),
            table_checkpoint_metadata: HashMap::from([(
                "k".into(),
                rpc::TableCheckpointMetadata {
                    table_type: rpc::TableEnum::GlobalKeyValue.into(),
                    data: rpc::GlobalKeyedTableTaskCheckpointMetadata {
                        files,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                },
            )]),
            ..Default::default()
        };
        objects.insert(
            metadata_path(&operator_path("J", epoch, "p")),
            global.encode_to_vec(),
        );
        let empty = OperatorCheckpointMetadata {
            native_database: None,
            operator_metadata: Some(rpc::OperatorMetadata {
                job_id: "J".into(),
                operator_id: "empty".into(),
                epoch,
                parallelism: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        objects.insert(
            metadata_path(&operator_path("J", epoch, "empty")),
            empty.encode_to_vec(),
        );
    }
    for path in [
        "J/checkpoints/checkpoint-0000001/operator-o/table-t-000/disk-orphan",
        "J/checkpoints/checkpoint-0000001/operator-empty/table-t-000/disk-orphan-a",
        "J/checkpoints/checkpoint-0000001/operator-empty/table-t-000/disk-orphan-b",
        "J/checkpoints/checkpoint-0000001/operator-unknown/table-t-000/disk-foreign",
        "J-other/checkpoints/checkpoint-0000001/operator-o/table-t-000/disk-foreign",
        "J/checkpoints-extra/checkpoint-0000001/operator-o/table-t-000/disk-neighbor",
        "J/checkpoints/checkpoint-0000001/operator-empty/table-t-000/ordinary",
        "J/checkpoints/checkpoint-invalid/operator-empty/table-t-000/disk-invalid",
        "J/checkpoints/checkpoint-0000003/operator-empty/table-t-000/disk-equal",
        "J/checkpoints/checkpoint-0000004/operator-empty/table-t-000/disk-newer",
    ] {
        objects.insert(path.into(), path.as_bytes().to_vec());
    }
    write_objects(root, &objects);
    let head =
        CheckpointMetadata::decode(objects[&metadata_path(&base_path("J", 3))].as_slice()).unwrap();
    (head, objects)
}

fn expected_checkpoint_cleanup(mut objects: ObjectMap, mut head: CheckpointMetadata) -> ObjectMap {
    for epoch in 1..3 {
        objects
            .remove(&format!(
                "J/checkpoints/checkpoint-{epoch:07}/operator-o/table-t-000/disk-{epoch}.parquet"
            ))
            .unwrap();
        objects
            .remove(&metadata_path(&base_path("J", epoch)))
            .unwrap();
        for owner in &head.operator_ids {
            objects
                .remove(&metadata_path(&operator_path("J", epoch, owner)))
                .unwrap();
        }
    }
    for path in [
        "J/checkpoints/checkpoint-0000002/operator-p/table-k-000",
        "J/checkpoints/checkpoint-0000001/operator-o/table-t-000/disk-orphan",
        "J/checkpoints/checkpoint-0000001/operator-empty/table-t-000/disk-orphan-a",
        "J/checkpoints/checkpoint-0000001/operator-empty/table-t-000/disk-orphan-b",
    ] {
        objects.remove(path).unwrap();
    }
    head.min_epoch = 3;
    objects.insert(metadata_path(&base_path("J", 3)), head.encode_to_vec());
    objects
}

#[cfg(target_os = "linux")]
fn watch_directory(path: &Path) -> std::os::fd::OwnedFd {
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: no pointer arguments; the successful descriptor is owned once.
    let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    assert!(raw >= 0);
    let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
    // Close events separate successive opens, preventing identical open events
    // from coalescing. Child-directory events have names; only self opens count.
    // SAFETY: path remains NUL-terminated through the synchronous call.
    assert!(
        unsafe {
            libc::inotify_add_watch(raw, path.as_ptr(), libc::IN_OPEN | libc::IN_CLOSE_NOWRITE)
        } >= 0
    );
    fd
}

#[cfg(target_os = "linux")]
fn observed_directory_opens(fd: &std::os::fd::OwnedFd) -> usize {
    use std::os::fd::AsRawFd;
    let mut buffer = [0u8; 16384];
    let mut opens = 0;
    loop {
        // SAFETY: the buffer is writable for exactly the provided length.
        let count = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count < 0 {
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            break;
        }
        assert!(count > 0);
        let mut offset = 0;
        while offset < count as usize {
            assert!(offset + std::mem::size_of::<libc::inotify_event>() <= count as usize);
            // SAFETY: the complete header is in bounds; its alignment is arbitrary.
            let event = unsafe {
                buffer
                    .as_ptr()
                    .add(offset)
                    .cast::<libc::inotify_event>()
                    .read_unaligned()
            };
            assert_eq!(
                event.mask & libc::IN_Q_OVERFLOW,
                0,
                "inotify overflow invalidates physical count"
            );
            if event.mask & libc::IN_OPEN != 0 && event.mask & libc::IN_ISDIR != 0 && event.len == 0
            {
                opens += 1;
            }
            offset += std::mem::size_of::<libc::inotify_event>() + event.len as usize;
            assert!(offset <= count as usize);
        }
    }
    opens
}

#[tokio::test]
async fn job_cleanup_has_one_listing_and_exact_multi_owner_file_oracle() {
    let root = tempfile::tempdir().unwrap();
    let (head, initial) = checkpoint_cleanup_fixture(root.path());
    let observed = ObservedCleanupStore {
        inner: provider(root.path()).await,
        fault: CleanupFault::None,
        trace: Default::default(),
    };
    #[cfg(target_os = "linux")]
    let watch = watch_directory(&root.path().join("J/checkpoints"));
    cleanup_checkpoint_files(&observed, head.clone(), 1, 3)
        .await
        .unwrap();
    assert_eq!(observed.trace.lock().unwrap().listings, 1);
    #[cfg(target_os = "linux")]
    assert_eq!(
        observed_directory_opens(&watch),
        2,
        "one pruning traversal and one object listing for three declared operators"
    );
    assert_eq!(
        read_objects(root.path()),
        expected_checkpoint_cleanup(initial, head)
    );
    assert!(
        !root
            .path()
            .join("J/checkpoints/checkpoint-0000002/operator-o/table-t-000")
            .exists(),
        "known-file deletion empties are pruned before listing"
    );
    // No history inventory survives: a fresh owner must discover a late old upload,
    // even for an explicitly declared owner with zero retained state descriptors.
    let late = "J/checkpoints/checkpoint-0000000/operator-empty/table-t-000/disk-late";
    observed.inner.put(late, vec![9]).await.unwrap();
    let fresh = provider(root.path()).await;
    let retained = CheckpointMetadata::decode(
        fresh
            .get(metadata_path(&base_path("J", 3)))
            .await
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    cleanup_checkpoint_files(&fresh, retained, 3, 3)
        .await
        .unwrap();
    assert!(!fresh.exists(late).await.unwrap());
}

#[tokio::test]
async fn job_cleanup_failures_preserve_retry_proof_and_commit_minimum_last() {
    for fault in [
        CleanupFault::KnownFile,
        CleanupFault::Pruning,
        CleanupFault::Listing,
        CleanupFault::OrphanDelete,
        CleanupFault::MetadataDelete,
        CleanupFault::Publish,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (head, initial) = checkpoint_cleanup_fixture(root.path());
        let observed = ObservedCleanupStore {
            inner: provider(root.path()).await,
            fault,
            trace: Default::default(),
        };
        assert!(
            cleanup_checkpoint_files(&observed, head.clone(), 1, 3)
                .await
                .is_err()
        );
        let current = read_objects(root.path());
        assert_eq!(
            current[&metadata_path(&base_path("J", 3))],
            initial[&metadata_path(&base_path("J", 3))],
            "frontier must not advance on any failed operation"
        );
        for owner in &head.operator_ids {
            assert_eq!(
                current[&metadata_path(&operator_path("J", 3, owner))],
                initial[&metadata_path(&operator_path("J", 3, owner))]
            );
        }
        if matches!(
            fault,
            CleanupFault::KnownFile
                | CleanupFault::Pruning
                | CleanupFault::Listing
                | CleanupFault::OrphanDelete
        ) {
            for epoch in 1..3 {
                assert_eq!(
                    current[&metadata_path(&base_path("J", epoch))],
                    initial[&metadata_path(&base_path("J", epoch))]
                );
                for owner in &head.operator_ids {
                    assert_eq!(
                        current[&metadata_path(&operator_path("J", epoch, owner))],
                        initial[&metadata_path(&operator_path("J", epoch, owner))]
                    );
                }
            }
        }
        {
            let trace = observed.trace.lock().unwrap();
            if matches!(fault, CleanupFault::KnownFile) {
                assert_eq!(trace.listings, 0);
                assert!(!trace.deleted.is_empty());
            }
            if matches!(fault, CleanupFault::Pruning) {
                assert_eq!(trace.listings, 0);
                assert_eq!(trace.metadata_deletes, 0);
                assert!(
                    !trace.deleted.is_empty(),
                    "known-file deletion may already complete"
                );
            }
            if matches!(fault, CleanupFault::OrphanDelete) {
                assert_eq!(trace.orphan_deletes, 1);
            }
            if matches!(fault, CleanupFault::MetadataDelete) {
                assert_eq!(trace.metadata_deletes, 1);
            }
        }
        let fresh = provider(root.path()).await;
        cleanup_checkpoint_files(&fresh, head.clone(), 1, 3)
            .await
            .unwrap();
        assert_eq!(
            read_objects(root.path()),
            expected_checkpoint_cleanup(initial, head)
        );
    }
}

#[tokio::test]
async fn cleans_abandoned_pages_only_below_retained_minimum_and_for_owned_operator() {
    let root = tempfile::tempdir().unwrap();
    let storage =
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.path().display()))
            .await
            .unwrap();
    let abandoned = "J/checkpoints/checkpoint-0000001/operator-o/table-m-000/disk-orphan.bin";
    let retained = "J/checkpoints/checkpoint-0000002/operator-o/table-m-000/disk-keep.bin";
    let active = "J/checkpoints/checkpoint-0000003/operator-o/table-m-000/disk-upload.bin";
    let foreign = "J/checkpoints/checkpoint-0000001/operator-other/table-m-000/disk-foreign.bin";
    let legacy = "J/checkpoints/checkpoint-0000001/operator-o/table-legacy-000";
    for path in [abandoned, retained, active, foreign, legacy] {
        storage.put(path, vec![1]).await.unwrap();
    }
    cleanup_abandoned_controller_job_disk_files(
        &storage,
        "J",
        2,
        &owner_paths("o", &HashSet::new()),
    )
    .await
    .unwrap();
    assert!(!storage.exists(abandoned).await.unwrap());
    for path in [retained, active, foreign, legacy] {
        assert!(storage.exists(path).await.unwrap());
    }
    cleanup_abandoned_controller_job_disk_files(
        &storage,
        "J",
        2,
        &owner_paths("o", &HashSet::new()),
    )
    .await
    .unwrap();
}

use std::collections::BTreeMap;
use std::path::Path;
use std::task::Poll;

type ObjectMap = BTreeMap<String, Vec<u8>>;

fn write_objects(root: &Path, objects: &ObjectMap) {
    for (key, bytes) in objects {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}

fn read_objects(root: &Path) -> ObjectMap {
    fn visit(root: &Path, directory: &Path, objects: &mut ObjectMap) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, objects);
            } else {
                objects.insert(
                    path.strip_prefix(root).unwrap().to_str().unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut objects = BTreeMap::new();
    visit(root, root, &mut objects);
    objects
}

async fn provider(root: &Path) -> arroyo_storage::StorageProvider {
    arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.display()))
        .await
        .unwrap()
}

#[tokio::test]
async fn exact_cleanup_oracle_retention_isolation_and_late_uploads() {
    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let mut expected = ObjectMap::new();
    let mut retained = HashSet::new();
    let mut orphans: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for job in ["J", "J-other"] {
        for operator in ["o", "p", "other"] {
            for epoch in 1..=4 {
                for table in ["a", "b"] {
                    let base = format!(
                        "{job}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/table-{table}"
                    );
                    for name in ["disk-orphan.bin", "disk-reference.bin", "ordinary.bin"] {
                        let key = format!("{base}/{name}");
                        expected.insert(key.clone(), key.as_bytes().to_vec());
                        if job == "J" && epoch < 3 {
                            if name == "disk-reference.bin" {
                                retained.insert(key.clone());
                            } else if name == "disk-orphan.bin" {
                                orphans.entry(operator).or_default().push(key);
                            }
                        }
                    }
                }
            }
        }
    }
    for key in [
        "unrelated/object",
        "J/checkpoints-extra/checkpoint-1/operator-o/table-a/disk-neighbor",
        "J/checkpoints/checkpoint-invalid/operator-o/table-a/disk-invalid",
        "J/checkpoints/checkpoint-4294967296/operator-o/table-a/disk-overflow",
        "J/checkpoints/checkpoint-1/operator-o/table-a/disk-extra/child",
        "J/checkpoints/checkpoint-1/operator-o/table-file",
        "J/checkpoints/checkpoint-1/operator-o/not-table/disk-not-table",
        "J/checkpoints/checkpoint-1/operator-other-prefix/table-a/disk-other",
    ] {
        expected.insert(key.into(), key.as_bytes().to_vec());
    }
    write_objects(root.path(), &expected);
    std::fs::create_dir_all(root.path().join("J/checkpoints/checkpoint-0/empty-table")).unwrap();
    for operator in ["o", "p"] {
        cleanup_abandoned_controller_job_disk_files(
            &storage,
            "J",
            3,
            &owner_paths(operator, &retained),
        )
        .await
        .unwrap();
        for orphan in &orphans[operator] {
            expected.remove(orphan).unwrap();
        }
        assert_eq!(read_objects(root.path()), expected);
    }
    cleanup_abandoned_controller_job_disk_files(&storage, "J", 3, &owner_paths("o", &retained))
        .await
        .unwrap();
    assert_eq!(read_objects(root.path()), expected);

    let old = "J/checkpoints/checkpoint-1/operator-o/table-a/disk-late";
    let active = "J/checkpoints/checkpoint-4/operator-o/table-a/disk-active";
    for key in [old, active] {
        storage.put(key, vec![9]).await.unwrap();
    }
    // A fresh owner must find late uploads without any cached inventory.
    let fresh = provider(root.path()).await;
    cleanup_abandoned_controller_job_disk_files(&fresh, "J", 3, &owner_paths("o", &retained))
        .await
        .unwrap();
    expected.insert(active.into(), vec![9]);
    assert_eq!(read_objects(root.path()), expected);
    for missing in ["absent", "empty"] {
        if missing == "empty" {
            std::fs::create_dir_all(root.path().join("empty/checkpoints")).unwrap();
        }
        cleanup_abandoned_controller_job_disk_files(
            &fresh,
            missing,
            3,
            &owner_paths("o", &retained),
        )
        .await
        .unwrap();
        assert_eq!(read_objects(root.path()), expected);
    }
}

#[tokio::test]
async fn observed_partial_cleanup_retries_with_fresh_storage_owner() {
    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let orphan_keys: Vec<_> = (0..256)
        .map(|i| format!("J/checkpoints/checkpoint-1/operator-o/table-a/disk-{i:04}"))
        .collect();
    let kept = "J/checkpoints/checkpoint-3/operator-o/table-a/disk-keep";
    let mut objects: ObjectMap = orphan_keys
        .iter()
        .map(|key| (key.clone(), vec![3]))
        .collect();
    objects.insert(kept.into(), vec![7]);
    write_objects(root.path(), &objects);
    let retained = HashSet::new();
    let owners = owner_paths("o", &retained);
    let mut cleanup = Box::pin(cleanup_abandoned_controller_job_disk_files(
        &storage, "J", 3, &owners,
    ));
    let partial = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let poll = futures::poll!(cleanup.as_mut());
            let deleted = orphan_keys
                .iter()
                .filter(|key| !root.path().join(key).exists())
                .count();
            if deleted > 0 && deleted < orphan_keys.len() {
                break deleted;
            }
            assert!(
                matches!(poll, Poll::Pending),
                "cleanup finished before interruption was observed"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must observe real partial deletion before aborting");
    drop(cleanup);
    // A pending local delete may finish after dropping the future. Retrying
    // remains safe because delete_if_present treats NotFound as success.
    let fresh = provider(root.path()).await;
    cleanup_abandoned_controller_job_disk_files(&fresh, "J", 3, &owner_paths("o", &HashSet::new()))
        .await
        .unwrap();
    assert_eq!(
        read_objects(root.path()),
        ObjectMap::from([(kept.into(), vec![7])])
    );
    eprintln!("STR156_INTERRUPTION observed_deleted={partial} total=256 final_deleted=256");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn prefix_listing_physically_excludes_unrelated_directories() {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let watched = [
        "J/checkpoints",
        "J/checkpoints/checkpoint-empty",
        "J-other/checkpoints",
        "J-other/checkpoints/checkpoint-empty",
        "J/checkpoints-extra",
    ];
    for path in watched {
        std::fs::create_dir_all(root.path().join(path)).unwrap();
    }
    storage
        .put(
            "J/checkpoints/checkpoint-1/operator-o/table-a/disk-old",
            vec![1],
        )
        .await
        .unwrap();
    storage
        .put(
            "J-other/checkpoints/checkpoint-1/operator-o/table-a/disk-other",
            vec![2],
        )
        .await
        .unwrap();
    // SAFETY: inotify_init1 has no pointer arguments; ownership is transferred
    // exactly once to OwnedFd after validating the descriptor.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    assert!(fd >= 0, "{}", std::io::Error::last_os_error());
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut descriptors = BTreeMap::new();
    for directory in watched {
        let path = CString::new(root.path().join(directory).as_os_str().as_bytes()).unwrap();
        // SAFETY: path is NUL terminated and remains valid through this call.
        let wd = unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), libc::IN_OPEN) };
        assert!(wd >= 0, "{}", std::io::Error::last_os_error());
        descriptors.insert(wd, directory);
    }
    use std::io::Write;
    eprintln!("STR156_CLEANUP_START");
    std::io::stderr().flush().unwrap();
    cleanup_abandoned_controller_job_disk_files(
        &storage,
        "J",
        3,
        &owner_paths("o", &HashSet::new()),
    )
    .await
    .unwrap();
    eprintln!("STR156_CLEANUP_END");
    std::io::stderr().flush().unwrap();
    let mut opened = HashSet::new();
    let mut buffer = [0_u8; 16384];
    loop {
        // SAFETY: buffer is writable for exactly its supplied length.
        let count = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count < 0 {
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            break;
        }
        assert!(count > 0);
        let mut offset = 0;
        while offset < count as usize {
            assert!(offset + std::mem::size_of::<libc::inotify_event>() <= count as usize);
            // SAFETY: the bounds above cover the fixed header; read_unaligned
            // avoids imposing alignment on a byte buffer returned by read.
            let event = unsafe {
                std::ptr::read_unaligned(buffer.as_ptr().add(offset).cast::<libc::inotify_event>())
            };
            assert_eq!(
                event.mask & libc::IN_Q_OVERFLOW,
                0,
                "inotify overflow invalidates evidence"
            );
            if event.mask & libc::IN_OPEN != 0 && event.mask & libc::IN_ISDIR != 0 {
                opened.insert(*descriptors.get(&event.wd).unwrap());
            }
            offset += std::mem::size_of::<libc::inotify_event>() + event.len as usize;
            assert!(offset <= count as usize);
        }
    }
    assert!(opened.contains("J/checkpoints"));
    assert!(opened.contains("J/checkpoints/checkpoint-empty"));
    let expect_root_scan = std::env::var_os("STR156_EXPECT_ROOT_SCAN").is_some();
    for unrelated in &watched[2..] {
        assert_eq!(
            opened.contains(unrelated),
            expect_root_scan,
            "directory {unrelated}: {opened:?}"
        );
    }
    // Inotify can coalesce events: this is intentionally zero/nonzero evidence,
    // never a claim about exact directory-open or returned-object counts.
    eprintln!("STR156_PHYSICAL root_scan={expect_root_scan} opened={opened:?}");
}

#[cfg(target_os = "linux")]
fn process_usage() -> libc::rusage {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied writable rusage on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    unsafe { usage.assume_init() }
}

#[cfg(target_os = "linux")]
fn io_counters() -> BTreeMap<String, u64> {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_owned(), value.trim().parse().unwrap())
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn host_counters() -> BTreeMap<String, serde_json::Value> {
    [
        "/proc/stat",
        "/proc/loadavg",
        "/proc/pressure/cpu",
        "/proc/pressure/io",
        "/proc/pressure/memory",
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/memory.current",
    ]
    .into_iter()
    .map(|path| {
        let value = match std::fs::read_to_string(path) {
            Ok(value) => serde_json::json!({"value": value}),
            Err(error) => serde_json::json!({"unavailable": error.to_string()}),
        };
        (path.to_owned(), value)
    })
    .collect()
}

/// One arm only. The external, queue-owning runner supplies ABBA order,
/// watchdogs, immutable binaries, contention samples and separate strace phases.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in finite STR-156 release comparison; requires evidence paths"]
async fn cleanup_prefix_benchmark() {
    use std::io::Write;
    use std::time::{Duration, Instant};

    let evidence_root = std::env::var("STR156_BENCH_ROOT").expect("STR156_BENCH_ROOT required");
    let receipt_path = std::env::var("STR156_RECEIPT").expect("STR156_RECEIPT required");
    assert!(Path::new(&evidence_root).is_absolute());
    assert!(Path::new(&receipt_path).is_absolute());
    assert!(
        !Path::new(&receipt_path).exists(),
        "do not overwrite a previous arm receipt"
    );
    let root = tempfile::Builder::new()
        .prefix("str156-arm-")
        .tempdir_in(evidence_root)
        .unwrap();
    let storage = provider(root.path()).await;
    let mut objects = ObjectMap::new();
    let mut expected_deleted = HashSet::new();
    let mut retained = HashSet::new();
    for job_index in 0..32 {
        let job = if job_index == 0 {
            "J".to_owned()
        } else {
            format!("unrelated-{job_index:02}")
        };
        for epoch in 0..16 {
            for operator in 0..4 {
                for table in 0..2 {
                    for name in ["disk-orphan", "disk-reference", "ordinary"] {
                        let key = format!(
                            "{job}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/table-{table}/{name}"
                        );
                        objects.insert(key.clone(), vec![42; 32]);
                        if job_index == 0 && epoch < 8 {
                            if name == "disk-orphan" {
                                expected_deleted.insert(key);
                            } else if name == "disk-reference" {
                                retained.insert(key);
                            }
                        }
                    }
                }
            }
        }
        // These literal names cannot alias the sixteen populated epoch paths.
        for empty in 0..16 {
            std::fs::create_dir_all(
                root.path()
                    .join(format!("{job}/checkpoints/checkpoint-empty-{empty:02}")),
            )
            .unwrap();
        }
    }
    assert_eq!(objects.len(), 12_288);
    assert_eq!(expected_deleted.len(), 64);
    assert_eq!(retained.len(), 64);
    write_objects(root.path(), &objects);
    assert_eq!(read_objects(root.path()), objects);
    let before_files = objects.len();
    let host_before = host_counters();
    let io_before = io_counters();
    let cpu_before = process_usage();
    eprintln!("STR156_CLEANUP_START");
    std::io::stderr().flush().unwrap();
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let owners: HashMap<_, _> = (0..4)
            .map(|operator| {
                let operator = operator.to_string();
                let paths = retained
                    .iter()
                    .filter(|path| path.contains(&format!("/operator-{operator}/")))
                    .cloned()
                    .collect();
                (operator, paths)
            })
            .collect();
        cleanup_abandoned_controller_job_disk_files(&storage, "J", 8, &owners).await
    })
    .await;
    let elapsed_seconds = start.elapsed().as_secs_f64();
    eprintln!("STR156_CLEANUP_END");
    std::io::stderr().flush().unwrap();
    let cpu_after = process_usage();
    let io_after = io_counters();
    let host_after = host_counters();
    result
        .expect("120-second processing bound exceeded")
        .unwrap();
    for deleted in &expected_deleted {
        objects.remove(deleted).unwrap();
    }
    let actual = read_objects(root.path());
    assert_eq!(actual, objects, "full relative-key and byte oracle");
    let io_delta: BTreeMap<_, _> = io_after
        .into_iter()
        .map(|(key, value)| {
            let previous = io_before[&key];
            (key, value.checked_sub(previous).unwrap())
        })
        .collect();
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    let receipt = serde_json::json!({
        "status": "pass",
        "arm": std::env::var("STR156_ARM").unwrap_or_else(|_| "unspecified".into()),
        "completed_jobs": 1,
        "completed_operators": 4,
        "deleted_files": before_files - actual.len(),
        "expected_deletions": expected_deleted.len(),
        "root_files": before_files,
        "before_files": before_files,
        "after_files": actual.len(),
        "oracle_pass": true,
        "elapsed_seconds": elapsed_seconds,
        "user_cpu_seconds": seconds(cpu_after.ru_utime) - seconds(cpu_before.ru_utime),
        "system_cpu_seconds": seconds(cpu_after.ru_stime) - seconds(cpu_before.ru_stime),
        "lifetime_maxrss_kib": cpu_after.ru_maxrss,
        "maxrss_scope": "process lifetime high-water mark, includes fixture setup; not interval peak",
        "io_delta": io_delta,
        "host_before": host_before,
        "host_after": host_after,
    });
    // Fixture cleanup is outside measured processing and must succeed before
    // writing the successful receipt. Raw failures remain in the runner log.
    root.close().unwrap();
    std::fs::write(receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    eprintln!("STR156_RECEIPT {}", receipt);
}

#[tokio::test]
async fn historical_empty_cleanup_cancellation_retries_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let (head, initial) = checkpoint_cleanup_fixture(root.path());
    let empty: Vec<_> = (0..256)
        .map(|epoch| {
            root.path().join(format!(
                "J/checkpoints/historical-{epoch:04}/operator/table"
            ))
        })
        .collect();
    for directory in &empty {
        std::fs::create_dir_all(directory).unwrap();
    }
    let observed = ObservedCleanupStore {
        inner: provider(root.path()).await,
        fault: CleanupFault::None,
        trace: Default::default(),
    };
    let mut cleanup = Box::pin(cleanup_checkpoint_files(&observed, head.clone(), 1, 3));
    let removed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let poll = futures::poll!(cleanup.as_mut());
            let removed = empty.iter().filter(|directory| !directory.exists()).count();
            if removed > 0 && removed < empty.len() {
                assert_eq!(observed.trace.lock().unwrap().listings, 0);
                break removed;
            }
            assert!(
                matches!(poll, Poll::Pending),
                "must interrupt the prune, not file cleanup"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observe partial directory pruning");
    drop(cleanup);
    let current = read_objects(root.path());
    for epoch in 1..=3 {
        let metadata = metadata_path(&base_path("J", epoch));
        assert_eq!(
            current[&metadata], initial[&metadata],
            "prune cancellation cannot advance metadata"
        );
    }
    let fresh = provider(root.path()).await;
    cleanup_checkpoint_files(&fresh, head.clone(), 1, 3)
        .await
        .unwrap();
    let mut expected = expected_checkpoint_cleanup(initial, head.clone());
    assert_eq!(read_objects(root.path()), expected);
    assert!(empty.iter().all(|directory| !directory.exists()));
    let late = "J/checkpoints/checkpoint-0000000/operator-empty/table-t/disk-late";
    fresh.put(late, vec![9]).await.unwrap();
    let active = "J/checkpoints/checkpoint-0000004/operator-empty/table-t/disk-active";
    fresh.put(active, vec![8]).await.unwrap();
    let retained = CheckpointMetadata::decode(
        fresh
            .get(metadata_path(&base_path("J", 3)))
            .await
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    let retry = provider(root.path()).await;
    cleanup_checkpoint_files(&retry, retained.clone(), 3, 3)
        .await
        .unwrap();
    expected.insert(active.into(), vec![8]);
    assert_eq!(read_objects(root.path()), expected);
    // Files deleted after the prepass can leave directories until the next pass.
    assert!(
        root.path()
            .join("J/checkpoints/checkpoint-0000000")
            .exists()
    );
    cleanup_checkpoint_files(&retry, retained, 3, 3)
        .await
        .unwrap();
    assert!(
        !root
            .path()
            .join("J/checkpoints/checkpoint-0000000")
            .exists()
    );
    assert_eq!(read_objects(root.path()), expected);
    eprintln!("STR161_PRUNE_INTERRUPTION observed_removed={removed} total=256");
}

/// Finite filesystem-work comparison, not an engine throughput benchmark.
/// The baseline changes only the private test adapter's pruning operation.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "opt-in finite STR-161 local directory comparison"]
async fn historical_empty_directory_work_comparison() {
    fn directory_count(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    1 + directory_count(&entry.path())
                } else {
                    0
                }
            })
            .sum()
    }
    let mut results = Vec::new();
    for history in [0, 2_000] {
        let baseline_root = tempfile::tempdir().unwrap();
        let candidate_root = tempfile::tempdir().unwrap();
        let roots = [baseline_root.path(), candidate_root.path()];
        let mut expected = ObjectMap::new();
        for epoch in 2_001..=2_004 {
            for table in ["a", "b"] {
                let key = format!(
                    "J/checkpoints/checkpoint-{epoch:07}/operator-o/table-{table}/disk-live"
                );
                expected.insert(key.clone(), key.into_bytes());
            }
        }
        let reference = "J/checkpoints/checkpoint-0000000/operator-o/table-a/disk-reference";
        expected.insert(reference.into(), vec![7]);
        expected.insert("other/checkpoints/untouched/data".into(), vec![8]);
        let owners = owner_paths("o", &HashSet::from([reference.to_owned()]));
        let mut watches = Vec::new();
        for root in roots {
            for epoch in 1..=history {
                for table in ["a", "b"] {
                    std::fs::create_dir_all(root.join(format!(
                        "J/checkpoints/checkpoint-{epoch:07}/operator-o/table-{table}"
                    )))
                    .unwrap();
                }
            }
            std::fs::create_dir_all(root.join("other/checkpoints/empty/descendant")).unwrap();
            write_objects(root, &expected);
            assert_eq!(read_objects(root), expected);
            if history > 0 {
                watches.push(watch_directory(
                    &root.join("J/checkpoints/checkpoint-0001000"),
                ));
            }
        }
        let baseline = ObservedCleanupStore {
            inner: provider(roots[0]).await,
            fault: CleanupFault::SkipPruning,
            trace: Default::default(),
        };
        let candidate = ObservedCleanupStore {
            inner: provider(roots[1]).await,
            fault: CleanupFault::None,
            trace: Default::default(),
        };
        let stores = [&baseline, &candidate];
        let before_directories = roots.map(directory_count);
        // Initial inventory opens watched directories; exclude setup events.
        for watch in &watches {
            let _ = observed_directory_opens(watch);
        }
        let mut first_directories = [0; 2];
        for pass in 0..=8 {
            let order = if pass % 2 == 0 { [0, 1] } else { [1, 0] };
            for arm in order {
                let before = process_usage();
                let start = std::time::Instant::now();
                cleanup_abandoned_controller_job_disk_files(stores[arm], "J", 2_001, &owners)
                    .await
                    .unwrap();
                let elapsed = start.elapsed().as_secs_f64();
                let after = process_usage();
                // Drain instrumentation before untimed oracle enumeration.
                let historical_opens = if history > 0 {
                    Some(observed_directory_opens(&watches[arm]))
                } else {
                    None
                };
                if let Some(opens) = historical_opens {
                    assert_eq!(opens, usize::from(arm == 0 || pass == 0));
                }
                assert_eq!(read_objects(roots[arm]), expected);
                if history > 0 {
                    // Oracle traversal also opens the baseline watched directory.
                    let _ = observed_directory_opens(&watches[arm]);
                    assert_eq!(
                        roots[arm].join("J/checkpoints/checkpoint-0001000").exists(),
                        arm == 0
                    );
                }
                assert!(
                    roots[arm]
                        .join("other/checkpoints/empty/descendant")
                        .is_dir()
                );
                if pass == 0 {
                    first_directories[arm] = directory_count(roots[arm]);
                    if history > 0 {
                        let _ = observed_directory_opens(&watches[arm]);
                    }
                }
                let seconds =
                    |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
                results.push(serde_json::json!({
                    "history_epochs": history,
                    "arm": if arm == 0 { "baseline" } else { "candidate" },
                    "pass": pass,
                    "phase": if pass == 0 { "first" } else { "steady" },
                    "wall_seconds": elapsed,
                    "user_cpu_seconds": seconds(after.ru_utime) - seconds(before.ru_utime),
                    "system_cpu_seconds": seconds(after.ru_stime) - seconds(before.ru_stime),
                    "selected_historical_directory_opens": historical_opens,
                    "oracle_pass": true,
                }));
            }
        }
        let after_directories = roots.map(directory_count);
        assert_eq!(before_directories[0], before_directories[1]);
        assert_eq!(before_directories[0], after_directories[0]);
        assert_eq!(after_directories, first_directories);
        assert_eq!(
            after_directories[0] - after_directories[1],
            history as usize * 4
        );
        results.push(serde_json::json!({
            "history_epochs": history,
            "directories_before": before_directories,
            "directories_after_first": first_directories,
            "directories_after_all": after_directories,
            "immutable_objects": expected.len(),
        }));
    }
    let receipt = serde_json::json!({
        "status": "pass", "profile": "test", "results": results,
        "scope": "local filesystem cleanup only; selected opens are not whole-tree syscalls",
        "timing_excludes": "fixture construction, inventories, inotify setup/drain and byte oracle",
    });
    if let Some(path) = std::env::var_os("STR161_RECEIPT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        writeln!(file, "{}", serde_json::to_string_pretty(&receipt).unwrap()).unwrap();
    }
    eprintln!(
        "STR161_COMPARISON {}",
        serde_json::to_string(&receipt).unwrap()
    );
}
