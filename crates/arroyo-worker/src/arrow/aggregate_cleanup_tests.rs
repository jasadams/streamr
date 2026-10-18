use super::*;

/// Exact cleanup and fresh-backend recovery, independent of TTL discovery.
#[tokio::test]
async fn cleanup_generations_restore_mid_page_on_both_backends() {
    use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
    use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
    for rocks in [false, true] {
        for page_entries in [1, 4] {
            for checkpoint_phase in [b'M', b'U', b'T'] {
                let root =
                    std::env::temp_dir().join(format!("cleanup-recovery-{}", uuid::Uuid::new_v4()));
                let remote = root.join("checkpoint");
                std::fs::create_dir_all(&remote).unwrap();
                let resources = WorkerStateResources::new(ResourceConfig {
                    max_open_databases: 2,
                    scan_page_bytes: 16 * 1024 * 1024,
                    queued_write_bytes: 16 * 1024 * 1024,
                    decoded_value_bytes: 32 * 1024 * 1024,
                    ..native_test_store().resources().config().clone()
                })
                .unwrap();
                let mut native = Vec::new();
                let mut backends: Vec<Arc<dyn LiveStateBackend>> = Vec::new();
                for generation in [0, 1] {
                    if rocks {
                        let backend = Arc::new(
                            RocksLiveState::open(
                                RocksStateConfig {
                                    root: root.join("live"),
                                    job_id: "cleanup-recovery".into(),
                                    operator_id: "aggregate".into(),
                                    subtask: 0,
                                    generation,
                                    attempt: 0,
                                },
                                resources.clone(),
                            )
                            .await
                            .unwrap(),
                        );
                        backends.push(backend.clone());
                        native.push(backend);
                    } else {
                        backends.push(Arc::new(
                            MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap(),
                        ));
                    }
                }
                let ownership = Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                };
                let mut manager =
                    LiveTableManager::new(backends[0].clone(), ownership.clone()).unwrap();
                let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
                let namespace = table.namespace().clone();
                let limits = AggregateStoreLimits {
                    page_entries,
                    ..native_test_store().limits()
                };
                let mut operator = native_calendar_operator();
                operator.native_store = Some(
                    AggregateStore::new(backends[0].clone(), table, resources.clone(), limits)
                        .unwrap(),
                );
                calendar_test_event(&mut operator, 200, 200, Some(true), false, &[1; 16]).await;
                calendar_test_event(&mut operator, 199, 200, Some(true), false, &[2; 16]).await;
                let expected_calendar = calendar_test_values(&operator).await;
                assert_eq!(
                    expected_calendar,
                    vec![
                        ScalarValue::Int64(Some(2)),
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(2))
                    ]
                );
                let member_key = |family, group: &[u8], generation, row: u64| {
                    let mut key = native_generation_prefix(family, group, generation).unwrap();
                    key.extend_from_slice(&row.to_be_bytes());
                    key
                };
                let mut obsolete = Vec::new();
                let mut retained = Vec::new();
                let unkeyed_schema = operator.schema_without_metadata.clone();
                if checkpoint_phase == b'T' {
                    let mut fields = operator.schema_without_metadata.fields().to_vec();
                    fields.insert(
                        0,
                        Arc::new(Field::new("generic_group", DataType::Utf8, false)),
                    );
                    operator.schema_without_metadata = Arc::new(Schema::new(fields));
                }
                let store = operator.native_store.as_ref().unwrap();
                for group_id in 0u64..3 {
                    let group = group_id.to_be_bytes();
                    let tombstone = checkpoint_phase == b'T' && group_id == 0;
                    for generation in 0u64..4 {
                        if tombstone && generation == 3 {
                            continue;
                        }
                        let mut scope = store.begin().await.unwrap();
                        if generation < 3 {
                            let marker = native_cleanup_key(&group, generation).unwrap();
                            scope.put(&marker, b"M").unwrap();
                            obsolete.push(marker);
                        }
                        for family in b"MRBJU" {
                            for row in 0u64..9 {
                                let key = member_key(*family, &group, generation, row);
                                // Two U rows refer to obsolete-owned pointers; the
                                // other rows refer to pointers replaced by generation3.
                                let deadline = if row < 2 {
                                    (100 + generation * 10 + row) as i64
                                } else {
                                    (1000 + row) as i64
                                };
                                let value = if *family == b'U' {
                                    deadline.to_be_bytes().to_vec()
                                } else {
                                    vec![*family, generation as u8, row as u8]
                                };
                                scope.put(&key, &value).unwrap();
                                if generation < 3 {
                                    obsolete.push(key);
                                } else {
                                    retained.push((key, value));
                                }
                                if *family == b'U' {
                                    let due = calendar_native::calendar_due_key(deadline, &group)
                                        .unwrap();
                                    let due_value = generation.to_be_bytes().to_vec();
                                    scope.put(&due, &due_value).unwrap();
                                    if (row < 2 && generation < 3)
                                        || (tombstone && generation == 2 && row >= 2)
                                    {
                                        obsolete.push(due);
                                    } else if generation == 3 {
                                        retained.push((due, due_value));
                                    }
                                }
                            }
                        }
                        scope.commit().await.unwrap();
                    }
                    if !tombstone {
                        let totals = native_collection_totals_key(&group, 0).unwrap();
                        let value = CollectionStats {
                            members: 9,
                            encoded_args_bytes: 123,
                            scalar_bytes: 456,
                            max_member_workspace: 789,
                        }
                        .encode()
                        .to_vec();
                        let mut scope = store.begin().await.unwrap();
                        scope.put(&totals, &value).unwrap();
                        scope.commit().await.unwrap();
                        retained.push((totals, value));
                    }
                    if checkpoint_phase == b'T' {
                        let mut scope = store.begin().await.unwrap();
                        let encoded_group = scope
                            .get(&native_group_key(b'G', &GLOBAL_KEY).unwrap())
                            .await
                            .unwrap()
                            .unwrap();
                        let mut decoded = operator.native_group_codec().unwrap()
                            .decode(&encoded_group, limits.value_bytes).unwrap();
                        decoded.generation = 3;
                        decoded.live_rows = if tombstone { 0 } else { 2 };
                        let encoded_group = operator.native_group_codec().unwrap().encode(&decoded, limits.value_bytes).unwrap();
                        let group_key = native_group_key(b'G', &group).unwrap();
                        scope.put(&group_key, &encoded_group).unwrap();
                        scope.commit().await.unwrap();
                        if tombstone {
                            obsolete.push(group_key);
                        } else {
                            retained.push((group_key, encoded_group));
                        }
                    }
                }
                let first_group = 0u64.to_be_bytes();
                let target_generation = if checkpoint_phase == b'T' { 2 } else { 0 };
                let target_marker = native_cleanup_key(&first_group, target_generation).unwrap();
                let mut progress = NativeCleanupProgress::new(store);
                if checkpoint_phase == b'M' {
                    for _ in 0..2 {
                        assert!(
                            operator
                                .cleanup_native_with_progress(&mut progress)
                                .await
                                .unwrap()
                        );
                    }
                } else {
                    let mut reached = false;
                    for _ in 0..2000 {
                        let scope = store.begin().await.unwrap();
                        let phase = scope.get(&target_marker).await.unwrap();
                        drop(scope);
                        if phase == Some(vec![checkpoint_phase]) {
                            reached = true;
                            break;
                        }
                        assert!(
                            operator
                                .cleanup_native_with_progress(&mut progress)
                                .await
                                .unwrap()
                        );
                    }
                    assert!(reached, "target checkpoint phase was not reached");
                    assert!(
                        operator
                            .cleanup_native_with_progress(&mut progress)
                            .await
                            .unwrap()
                    );
                }
                let scope = store.begin().await.unwrap();
                assert!(scope.get(&target_marker).await.unwrap().is_some());
                if checkpoint_phase == b'T' {
                    assert!(
                        scope
                            .get(&native_group_key(b'G', &first_group).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    );
                } else {
                    assert!(
                        scope
                            .get(&member_key(checkpoint_phase, &first_group, 0, 0))
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert!(
                        scope
                            .get(&member_key(checkpoint_phase, &first_group, 0, 8))
                            .await
                            .unwrap()
                            .is_some()
                    );
                }
                if checkpoint_phase == b'U' {
                    assert!(
                        scope
                            .get(&calendar_native::calendar_due_key(100, &first_group).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert_eq!(
                        scope
                            .get(&calendar_native::calendar_due_key(1002, &first_group).unwrap())
                            .await
                            .unwrap(),
                        Some(3u64.to_be_bytes().to_vec())
                    );
                }
                drop(scope);
                drop(progress);
                let snapshot = backends[0].snapshot().await.unwrap();
                let storage = arroyo_state::get_storage_provider(
                    &arroyo_state::StorageProviderFor::Controller {
                        storage_url: Some(format!("file://{}", remote.display())),
                    },
                )
                .await
                .unwrap();
                let config = DiskKeyedTableConfig {
                    table_name: NATIVE_AGGREGATE_TABLE.into(),
                    encoding_version: 1,
                    schema_identity: operator.native_schema_identity.clone(),
                };
                let receipt = checkpoint::export(
                    &snapshot,
                    &namespace,
                    &config,
                    &storage,
                    "checkpoint-1/aggregate",
                    1,
                    0,
                    0,
                )
                .await
                .unwrap();
                checkpoint::restore(
                    backends[1].as_ref(),
                    &namespace,
                    &config,
                    &receipt,
                    &storage,
                )
                .await
                .unwrap();
                let mut restored_manager =
                    LiveTableManager::new(backends[1].clone(), ownership).unwrap();
                let restored_table = restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
                let mut fresh = native_calendar_operator();
                if checkpoint_phase == b'T' {
                    fresh.schema_without_metadata = operator.schema_without_metadata.clone();
                }
                fresh.native_store = Some(
                    AggregateStore::new(
                        backends[1].clone(),
                        restored_table,
                        resources.clone(),
                        limits,
                    )
                    .unwrap(),
                );
                // A fresh owner reselects the persisted marker after interruption.
                let mut progress = NativeCleanupProgress::new(fresh.native_store.as_ref().unwrap());
                assert!(
                    fresh
                        .cleanup_native_with_progress(&mut progress)
                        .await
                        .unwrap()
                );
                drop(progress);
                let mut progress = NativeCleanupProgress::new(fresh.native_store.as_ref().unwrap());
                let mut finished = false;
                for _ in 0..2000 {
                    if !fresh
                        .cleanup_native_with_progress(&mut progress)
                        .await
                        .unwrap()
                    {
                        finished = true;
                        break;
                    }
                }
                assert!(
                    finished,
                    "finite cleanup did not finish rocks={rocks} page={page_entries}"
                );
                drop(progress);
                let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
                assert!(scope.first(b"C").await.unwrap().is_none());
                for key in &obsolete {
                    assert!(
                        scope.get(key).await.unwrap().is_none(),
                        "obsolete key {key:?}"
                    );
                }
                for (key, value) in &retained {
                    assert_eq!(
                        scope.get(key).await.unwrap().as_ref(),
                        Some(value),
                        "retained key {key:?}"
                    );
                }
                drop(scope);
                assert_eq!(calendar_test_values(&fresh).await, expected_calendar);
                // Calendar helper is a global owner; the keyed T fixture above
                // exercised real synthetic groups without changing its schema.
                fresh.schema_without_metadata = unkeyed_schema;
                // Correction still uses the original surviving calendar history.
                calendar_test_event(&mut fresh, 200, 200, Some(false), true, &[1; 16]).await;
                assert_eq!(
                    calendar_test_values(&fresh).await,
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(Some(1))
                    ]
                );
                drop(snapshot);
                drop(operator);
                drop(fresh);
                drop(manager);
                drop(restored_manager);
                drop(backends);
                for backend in native {
                    Arc::try_unwrap(backend)
                        .unwrap_or_else(|_| panic!("retained cleanup backend"))
                        .close_and_remove()
                        .await
                        .unwrap();
                }
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}

/// Pauses only a real cleanup write; all reads, snapshots and admissions are
/// forwarded to the bounded backend unchanged.
pub(super) struct CleanupCommitGate {
    pub(super) inner: Arc<dyn LiveStateBackend>,
    // 1: before commit; 2: after commit; 0: ordinary forwarding.
    pub(super) gate: std::sync::atomic::AtomicU8,
    pub(super) release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl LiveStateBackend for CleanupCommitGate {
    async fn get(
        &self,
        key: &arroyo_state::live::StateKey,
        options: arroyo_state::live::ReadOptions,
    ) -> arroyo_state::live::Result<Option<Vec<u8>>> {
        self.inner.get(key, options).await
    }
    async fn try_get(
        &self,
        key: &arroyo_state::live::StateKey,
        options: arroyo_state::live::ReadOptions,
    ) -> arroyo_state::live::Result<Option<Vec<u8>>> {
        self.inner.try_get(key, options).await
    }
    async fn multi_get(
        &self,
        keys: &[arroyo_state::live::StateKey],
        options: arroyo_state::live::ReadOptions,
    ) -> arroyo_state::live::Result<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys, options).await
    }
    fn try_admit_write(
        &self,
        bytes: usize,
        operations: usize,
    ) -> arroyo_state::live::Result<AdmittedWriteBatch> {
        self.inner.try_admit_write(bytes, operations)
    }
    async fn admit_write(
        &self,
        bytes: usize,
        operations: usize,
    ) -> arroyo_state::live::Result<AdmittedWriteBatch> {
        self.inner.admit_write(bytes, operations).await
    }
    async fn write_batch(
        &self,
        batch: arroyo_state::live::WriteBatch,
    ) -> arroyo_state::live::Result<()> {
        self.inner.write_batch(batch).await
    }
    async fn snapshot(&self) -> arroyo_state::live::Result<arroyo_state::live::StateSnapshot> {
        self.inner.snapshot().await
    }
    async fn write_admitted(&self, batch: AdmittedWriteBatch) -> arroyo_state::live::Result<()> {
        use std::sync::atomic::Ordering;
        let gate = self.gate.load(Ordering::SeqCst);
        if gate == 1 {
            self.release.notified().await;
        }
        self.inner.write_admitted(batch).await?;
        if gate == 2 {
            self.release.notified().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn cleanup_cancellation_preserves_atomic_page_and_retries() {
    use std::sync::atomic::{AtomicU8, Ordering};
    for rows in [5u64, 9] {
        for gate_at in [1, 2] {
            let template = native_test_store();
            let resources = template.resources().clone();
            let limits = template.limits();
            drop(template); // Release its sole database admission before replacement.
            let backend = Arc::new(CleanupCommitGate {
                inner: Arc::new(
                    MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap(),
                ),
                gate: AtomicU8::new(0),
                release: tokio::sync::Notify::new(),
            });
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
            let mut operator = native_operator();
            operator.native_store = Some(
                AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap(),
            );
            let store = operator.native_store.as_ref().unwrap();
            let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
            let keys: Vec<_> = (0u64..rows)
                .map(|row| {
                    let mut key = native_generation_prefix(b'M', &GLOBAL_KEY, 0).unwrap();
                    key.extend_from_slice(&row.to_be_bytes());
                    key
                })
                .collect();
            let retained = native_generation_prefix(b'M', &GLOBAL_KEY, 1).unwrap();
            let r_member = native_generation_prefix(b'R', &GLOBAL_KEY, 0).unwrap();
            let mut scope = store.begin().await.unwrap();
            scope.put(&marker, b"M").unwrap();
            for key in &keys {
                scope.put(key, b"old").unwrap();
            }
            scope.put(&retained, b"current").unwrap();
            if rows == 5 {
                scope.put(&r_member, b"old-R").unwrap();
            }
            scope.commit().await.unwrap();
            let mut progress = NativeCleanupProgress::new(store);
            assert!(progress._permit.is_some());
            assert!(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .is_err()
            );
            assert!(
                operator
                    .cleanup_native_with_progress(&mut progress)
                    .await
                    .unwrap()
            );
            let saved = (
                progress.marker.clone(),
                progress.member_after.clone(),
                progress.phase,
                progress.completed,
            );
            assert!(saved.0.is_some());
            assert!(saved.1.is_some());
            backend.gate.store(gate_at, Ordering::SeqCst);
            let mut cleanup = Box::pin(operator.cleanup_native_with_progress(&mut progress));
            assert!(futures::poll!(&mut cleanup).is_pending());
            drop(cleanup);
            // Both gates cancel before the async write returns to its caller.
            assert_eq!(
                (
                    progress.marker.clone(),
                    progress.member_after.clone(),
                    progress.phase,
                    progress.completed
                ),
                saved
            );
            backend.gate.store(0, Ordering::SeqCst);
            let scope = store.begin().await.unwrap();
            assert_eq!(
                scope.get(&marker).await.unwrap(),
                Some(if rows == 5 && gate_at == 2 {
                    b"R".to_vec()
                } else {
                    b"M".to_vec()
                })
            );
            for (i, key) in keys.iter().enumerate() {
                let retained_old = i >= if gate_at == 1 { 4 } else { 8 };
                assert_eq!(
                    scope.get(key).await.unwrap(),
                    retained_old.then(|| b"old".to_vec())
                );
            }
            assert_eq!(
                scope.get(&retained).await.unwrap(),
                Some(b"current".to_vec())
            );
            drop(scope);
            assert!(
                operator
                    .cleanup_native_with_progress(&mut progress)
                    .await
                    .unwrap()
            );
            if rows == 5 && gate_at == 2 {
                let scope = store.begin().await.unwrap();
                assert!(scope.get(&r_member).await.unwrap().is_none());
                drop(scope);
            }
            drop(progress);
            let full = resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .unwrap();
            drop(full);
            let mut fresh = NativeCleanupProgress::new(store);
            let mut complete = false;
            for _ in 0..20 {
                if !operator
                    .cleanup_native_with_progress(&mut fresh)
                    .await
                    .unwrap()
                {
                    complete = true;
                    break;
                }
            }
            assert!(complete);
            drop(fresh);
            let scope = store.begin().await.unwrap();
            assert!(scope.get(&marker).await.unwrap().is_none());
            assert!(scope.get(&r_member).await.unwrap().is_none());
            for key in &keys {
                assert!(scope.get(key).await.unwrap().is_none());
            }
            assert_eq!(
                scope.get(&retained).await.unwrap(),
                Some(b"current".to_vec())
            );
            drop(scope);
            assert!(
                resources
                    .try_queued_write(resources.config().queued_write_bytes)
                    .is_ok()
            );
            assert!(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .is_ok()
            );
        }
    }
}

#[tokio::test]
async fn cleanup_write_refusal_preserves_progress_and_releases_permit() {
    let mut operator = native_operator();
    operator.native_store = Some(native_test_store());
    let store = operator.native_store.as_ref().unwrap();
    let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
    let members: Vec<_> = (0u64..9)
        .map(|row| {
            let mut key = native_generation_prefix(b'M', &GLOBAL_KEY, 0).unwrap();
            key.extend_from_slice(&row.to_be_bytes());
            key
        })
        .collect();
    let mut scope = store.begin().await.unwrap();
    scope.put(&marker, b"M").unwrap();
    for member in &members {
        scope.put(member, b"old").unwrap();
    }
    scope.commit().await.unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    assert!(progress._permit.is_some());
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    let saved = (
        progress.marker.clone(),
        progress.member_after.clone(),
        progress.phase,
        progress.completed,
    );
    assert!(saved.0.is_some());
    assert!(saved.1.is_some());
    let held = store
        .resources()
        .try_queued_write(store.resources().config().queued_write_bytes)
        .unwrap();
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .is_err()
    );
    assert_eq!(
        (
            progress.marker.clone(),
            progress.member_after.clone(),
            progress.phase,
            progress.completed
        ),
        saved
    );
    drop(held);
    let scope = store.begin().await.unwrap();
    assert_eq!(scope.get(&marker).await.unwrap(), Some(b"M".to_vec()));
    for (i, member) in members.iter().enumerate() {
        assert_eq!(
            scope.get(member).await.unwrap(),
            (i >= 4).then(|| b"old".to_vec())
        );
    }
    drop(scope);
    let mut complete = false;
    for _ in 0..20 {
        if !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    drop(progress);
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&marker).await.unwrap().is_none());
    for member in &members {
        assert!(scope.get(member).await.unwrap().is_none());
    }
    drop(scope);
    assert!(
        store
            .resources()
            .try_queued_write(store.resources().config().queued_write_bytes)
            .is_ok()
    );
    assert!(
        store
            .resources()
            .try_decoded_value(store.resources().config().decoded_value_bytes)
            .is_ok()
    );
}

#[tokio::test]
async fn cleanup_cancelled_snapshot_wait_releases_uncommitted_scope() {
    let template = native_test_store();
    let resources = template.resources().clone();
    let limits = template.limits();
    drop(template);
    let backend = Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
    let mut manager = LiveTableManager::new(
        backend.clone(),
        Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
    )
    .unwrap();
    let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
    let mut operator = native_operator();
    operator.native_store =
        Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
    let store = operator.native_store.as_ref().unwrap();
    let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
    let member = native_generation_prefix(b'M', &GLOBAL_KEY, 0).unwrap();
    let mut seed = store.begin().await.unwrap();
    seed.put(&marker, b"M").unwrap();
    seed.put(&member, b"old").unwrap();
    seed.commit().await.unwrap();
    // Exhaust real snapshot admission without retaining two write scopes:
    // the unchanged 4 MiB queue admits one 2.6 MiB cleanup scope, not three.
    let first = backend.snapshot().await.unwrap();
    let second = backend.snapshot().await.unwrap();
    let spare = store.resources().config().queued_write_bytes;
    assert!(store.resources().try_queued_write(spare).is_ok());
    let mut progress = NativeCleanupProgress::new(store);
    let mut cleanup = Box::pin(operator.cleanup_native_with_progress(&mut progress));
    assert!(futures::poll!(&mut cleanup).is_pending());
    assert!(store.resources().try_queued_write(spare).is_err());
    drop(cleanup);
    assert!(store.resources().try_queued_write(spare).is_ok());
    assert!(progress.marker.is_none());
    assert!(progress.member_after.is_none());
    assert!(!progress.completed);
    drop(first);
    drop(second);
    let scope = store.begin().await.unwrap();
    assert_eq!(scope.get(&marker).await.unwrap(), Some(b"M".to_vec()));
    assert_eq!(scope.get(&member).await.unwrap(), Some(b"old".to_vec()));
    drop(scope);
    let mut complete = false;
    for _ in 0..20 {
        if !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    drop(progress);
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&marker).await.unwrap().is_none());
    assert!(scope.get(&member).await.unwrap().is_none());
    drop(scope);
    assert!(
        store
            .resources()
            .try_decoded_value(store.resources().config().decoded_value_bytes)
            .is_ok()
    );
    assert!(
        store
            .resources()
            .try_queued_write(store.resources().config().queued_write_bytes)
            .is_ok()
    );
}

#[tokio::test]
async fn cleanup_terminal_commit_cancellation_resumes_same_active_cursor() {
    use std::sync::atomic::{AtomicU8, Ordering};
    let template = native_test_store();
    let resources = template.resources().clone();
    let limits = template.limits();
    drop(template); // Release its sole database admission before replacement.
    let backend = Arc::new(CleanupCommitGate {
        inner: Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap()),
        gate: AtomicU8::new(0),
        release: tokio::sync::Notify::new(),
    });
    let mut manager = LiveTableManager::new(
        backend.clone(),
        Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
    )
    .unwrap();
    let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
    let mut operator = native_operator();
    operator.native_store =
        Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
    let store = operator.native_store.as_ref().unwrap();
    let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
    let old_member = native_generation_prefix(b'M', &GLOBAL_KEY, 0).unwrap();
    let current_member = native_generation_prefix(b'M', &GLOBAL_KEY, 1).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.put(&marker, b"M").unwrap();
    scope.put(&old_member, b"old").unwrap();
    scope.put(&current_member, b"current").unwrap();
    scope.commit().await.unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    let mut reached = false;
    for _ in 0..20 {
        let scope = store.begin().await.unwrap();
        let phase = scope.get(&marker).await.unwrap();
        drop(scope);
        if phase == Some(b"T".to_vec()) {
            reached = true;
            break;
        }
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
    }
    assert!(reached);
    let saved = (
        progress.marker.clone(),
        progress.member_after.clone(),
        progress.phase,
        progress.completed,
    );
    assert!(saved.0.is_some());
    backend.gate.store(2, Ordering::SeqCst);
    let mut cleanup = Box::pin(operator.cleanup_native_with_progress(&mut progress));
    assert!(futures::poll!(&mut cleanup).is_pending());
    drop(cleanup);
    assert_eq!(
        (
            progress.marker.clone(),
            progress.member_after.clone(),
            progress.phase,
            progress.completed
        ),
        saved
    );
    backend.gate.store(0, Ordering::SeqCst);
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&marker).await.unwrap().is_none());
    assert!(scope.get(&old_member).await.unwrap().is_none());
    assert_eq!(
        scope.get(&current_member).await.unwrap(),
        Some(b"current".to_vec())
    );
    drop(scope);
    // Retry the active cursor after its marker was atomically removed while
    // cancellation prevented acknowledgment; absence means reselection/end.
    let mut complete = false;
    for _ in 0..20 {
        if !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    drop(progress);
    let scope = store.begin().await.unwrap();
    assert!(scope.first(b"C").await.unwrap().is_none());
    assert_eq!(
        scope.get(&current_member).await.unwrap(),
        Some(b"current".to_vec())
    );
    drop(scope);
    assert!(
        resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .is_ok()
    );
    assert!(
        resources
            .try_queued_write(resources.config().queued_write_bytes)
            .is_ok()
    );
}
