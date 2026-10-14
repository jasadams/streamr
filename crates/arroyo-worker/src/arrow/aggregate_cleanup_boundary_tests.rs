use super::*;

fn cleanup_store_with_decoded_budget(
    limits: AggregateStoreLimits,
    budget: usize,
) -> AggregateStore {
    let resources = WorkerStateResources::new(ResourceConfig {
        decoded_value_bytes: budget,
        ..native_test_store().resources().config().clone()
    })
    .unwrap();
    let backend: Arc<dyn LiveStateBackend> =
        Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
    let mut manager = LiveTableManager::new(
        backend.clone(),
        Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
    )
    .unwrap();
    let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
    AggregateStore::new(backend, table, resources, limits).unwrap()
}

#[tokio::test]
async fn cleanup_progress_exact_capacity_and_optional_refusal_fallback() {
    let reference = native_test_store();
    let limits = reference.limits();
    let encoded_key = reference.max_encoded_entry_bytes() - limits.value_bytes - 1;
    // Match the real scope plus Rocks read_many one-key reservation exactly.
    let required = limits.overlay_bytes
        + 3 * limits.value_bytes
        + limits.value_bytes
        + 2 * encoded_key
        + mem::size_of::<Vec<u8>>()
        + mem::size_of::<Option<Vec<u8>>>();
    let charge = 2 * limits.key_bytes;
    for extra in [0, charge - 1, charge] {
        let store = cleanup_store_with_decoded_budget(limits, required + extra);
        let progress = NativeCleanupProgress::new(&store);
        assert_eq!(progress._permit.is_some(), extra == charge);
        // Optional progress leaves the entire mandatory scope/read peak free.
        let peak = store.resources().try_decoded_value(required).unwrap();
        drop(peak);
        drop(progress);
        assert!(
            store
                .resources()
                .try_decoded_value(required + extra)
                .is_ok()
        );
    }
    let mut operator = native_calendar_operator();
    operator.native_store = Some(cleanup_store_with_decoded_budget(limits, required + charge));
    let store = operator.native_store.as_ref().unwrap();
    let held = store.resources().try_decoded_value(required + 1).unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    assert!(progress._permit.is_none());
    drop(held);
    let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
    let old = native_generation_prefix(b'M', &GLOBAL_KEY, 0).unwrap();
    let live = native_generation_prefix(b'M', &GLOBAL_KEY, 1).unwrap();
    let mut old_due_pointer = native_generation_prefix(b'U', &GLOBAL_KEY, 0).unwrap();
    old_due_pointer.extend_from_slice(&0_u32.to_be_bytes());
    let mut replaced_due_pointer = native_generation_prefix(b'U', &GLOBAL_KEY, 0).unwrap();
    replaced_due_pointer.extend_from_slice(&1_u32.to_be_bytes());
    let due_old = calendar_native::calendar_due_key(17, &GLOBAL_KEY).unwrap();
    let due_current = calendar_native::calendar_due_key(18, &GLOBAL_KEY).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.put(&marker, b"M").unwrap();
    scope.put(&old, b"obsolete").unwrap();
    scope.put(&live, b"current").unwrap();
    scope.put(&old_due_pointer, &17_i64.to_be_bytes()).unwrap();
    scope
        .put(&replaced_due_pointer, &18_i64.to_be_bytes())
        .unwrap();
    scope.put(&due_old, &0_u64.to_be_bytes()).unwrap();
    scope.put(&due_current, &1_u64.to_be_bytes()).unwrap();
    scope.commit().await.unwrap();
    for _ in 0..16 {
        if !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
        {
            break;
        }
    }
    assert!(
        store
            .begin()
            .await
            .unwrap()
            .get(&marker)
            .await
            .unwrap()
            .is_none()
    );
    assert!(progress.marker.is_none());
    assert!(progress.member_after.is_none());
    let scope = store.begin().await.unwrap();
    for key in [&old, &old_due_pointer, &replaced_due_pointer, &due_old] {
        assert!(scope.get(key).await.unwrap().is_none());
    }
    assert_eq!(scope.get(&live).await.unwrap(), Some(b"current".to_vec()));
    assert_eq!(
        scope.get(&due_current).await.unwrap(),
        Some(1_u64.to_be_bytes().to_vec())
    );
}

#[tokio::test]
async fn cleanup_next_drain_sees_lower_marker_and_new_live_generation() {
    let mut operator = native_calendar_operator();
    operator.native_store = Some(native_test_store());
    let store = operator.native_store.as_ref().unwrap();
    let late = native_cleanup_key(b"z", 0).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.put(&late, b"T").unwrap();
    scope.commit().await.unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    assert!(
        !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    drop(progress);
    // New input is processed between serial drains, so even a key below the
    // previous drain's completed minimum must be discovered on the next flush.
    let early = native_cleanup_key(b"a", 0).unwrap();
    let old = native_generation_prefix(b'M', b"a", 0).unwrap();
    let live = native_generation_prefix(b'M', b"a", 1).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.put(&early, b"M").unwrap();
    scope.put(&old, b"obsolete").unwrap();
    scope.put(&live, b"current").unwrap();
    scope.commit().await.unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    let mut finished = false;
    for _ in 0..16 {
        if !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
        {
            finished = true;
            break;
        }
    }
    assert!(finished);
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&early).await.unwrap().is_none());
    assert!(scope.get(&old).await.unwrap().is_none());
    assert_eq!(scope.get(&live).await.unwrap(), Some(b"current".to_vec()));
}

#[tokio::test]
async fn cleanup_terminal_preserves_later_generation_and_retires_pair_atomically() {
    let mut operator = native_operator();
    let mut fields = operator.schema_without_metadata.fields().to_vec();
    fields.insert(0, Arc::new(Field::new("group_key", DataType::Utf8, false)));
    operator.schema_without_metadata = Arc::new(Schema::new(fields));
    operator.native_store = Some(native_test_store());
    let store = operator.native_store.as_ref().unwrap();
    let group = b"keyed";
    let storage = native_group_key(b'G', group).unwrap();
    let rows = native_live_rows_key(group).unwrap();
    let earlier = native_cleanup_key(group, 0).unwrap();
    let later = native_cleanup_key(group, 1).unwrap();
    let state = EncodedGroup {
        last_update_nanos: 0,
        generation: 2,
        next_ordinal: 0,
        accumulator_state: operator
            .native_state_values(&mut operator.native_accumulators(None).unwrap())
            .unwrap(),
        last_emitted: None,
    };
    let value = operator
        .native_group_codec()
        .unwrap()
        .encode(&state, store.limits().value_bytes)
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.put(&storage, &value).unwrap();
    scope.put(&rows, &0_u64.to_be_bytes()).unwrap();
    scope.put(&earlier, b"T").unwrap();
    scope.put(&later, b"T").unwrap();
    scope.commit().await.unwrap();
    let mut progress = NativeCleanupProgress::new(store);
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&earlier).await.unwrap().is_none());
    assert_eq!(scope.get(&storage).await.unwrap(), Some(value.clone()));
    assert!(scope.get(&rows).await.unwrap().is_some());
    drop(scope);
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    let scope = store.begin().await.unwrap();
    assert!(scope.get(&storage).await.unwrap().is_none());
    assert!(scope.get(&rows).await.unwrap().is_none());
    assert_eq!(scope.get(&later).await.unwrap(), Some(b"T".to_vec()));
    drop(scope);
    assert!(!progress.completed);
    assert!(
        operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    assert!(progress.completed);
    assert!(
        !operator
            .cleanup_native_with_progress(&mut progress)
            .await
            .unwrap()
    );
    let scope = store.begin().await.unwrap();
    assert!(scope.first(b"C").await.unwrap().is_none());
}

#[tokio::test]
async fn cleanup_due_refuses_insufficient_operation_byte_and_overlay_capacities() {
    let reference = native_test_store();
    let ordinary = reference.limits();
    let maximum_entry = reference.max_encoded_entry_bytes();
    for limits in [
        AggregateStoreLimits {
            page_entries: 1,
            write_operations: 2,
            ..ordinary
        },
        AggregateStoreLimits {
            page_entries: 1,
            write_bytes: maximum_entry * 2,
            ..ordinary
        },
        AggregateStoreLimits {
            page_entries: 1,
            overlay_bytes: (ordinary.key_bytes + ordinary.value_bytes) * 2,
            ..ordinary
        },
    ] {
        let mut operator = native_calendar_operator();
        operator.native_store = Some(native_test_store_with_limits(limits));
        let store = operator.native_store.as_ref().unwrap();
        let marker = native_cleanup_key(&GLOBAL_KEY, 0).unwrap();
        let pointer = native_generation_prefix(b'U', &GLOBAL_KEY, 0).unwrap();
        let due = calendar_native::calendar_due_key(17, &GLOBAL_KEY).unwrap();
        for (key, value) in [
            (&marker, b"U".to_vec()),
            (&pointer, 17_i64.to_be_bytes().to_vec()),
            (&due, 0_u64.to_be_bytes().to_vec()),
        ] {
            let mut scope = store.begin().await.unwrap();
            scope.put(key, &value).unwrap();
            scope.commit().await.unwrap();
        }
        let mut progress = NativeCleanupProgress::new(store);
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap_err()
                .to_string()
                .contains("calendar cleanup cannot admit a due pointer")
        );
        assert!(progress.marker.is_none());
        assert!(progress.member_after.is_none());
        let scope = store.begin().await.unwrap();
        assert_eq!(scope.get(&marker).await.unwrap(), Some(b"U".to_vec()));
        assert_eq!(
            scope.get(&pointer).await.unwrap(),
            Some(17_i64.to_be_bytes().to_vec())
        );
        assert_eq!(
            scope.get(&due).await.unwrap(),
            Some(0_u64.to_be_bytes().to_vec())
        );
    }
}

#[tokio::test]
async fn cleanup_progress_leaves_exact_rocks_scope_and_point_read_peak() {
    use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
    let reference = native_test_store();
    let limits = reference.limits();
    let encoded_key = reference.max_encoded_entry_bytes() - limits.value_bytes - 1;
    let required = limits.overlay_bytes
        + 4 * limits.value_bytes
        + 2 * encoded_key
        + mem::size_of::<Vec<u8>>()
        + mem::size_of::<Option<Vec<u8>>>();
    let charge = 2 * limits.key_bytes;
    for extra in [0, charge] {
        let root =
            std::env::temp_dir().join(format!("cleanup-cursor-peak-{}", uuid::Uuid::new_v4()));
        let resources = WorkerStateResources::new(ResourceConfig {
            decoded_value_bytes: required + extra,
            ..reference.resources().config().clone()
        })
        .unwrap();
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "cleanup-peak".into(),
                    operator_id: "aggregate".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let store = AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap();
        let key = vec![b'M'; limits.key_bytes];
        let value = vec![7; limits.value_bytes];
        let mut scope = store.begin().await.unwrap();
        scope.put(&key, &value).unwrap();
        scope.commit().await.unwrap();
        let progress = NativeCleanupProgress::new(&store);
        assert_eq!(progress._permit.is_some(), extra == charge);
        let scope = store.begin().await.unwrap();
        assert_eq!(scope.get(&key).await.unwrap(), Some(value));
        drop(scope);
        drop(progress);
        assert!(resources.try_decoded_value(required + extra).is_ok());
        drop(store);
        drop(manager);
        Arc::try_unwrap(backend)
            .unwrap_or_else(|_| panic!("retained cursor peak backend"))
            .close_and_remove()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn cleanup_shared_decoded_pressure_falls_back_before_and_after_cursor_creation() {
    use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
    let reference = native_test_store();
    let limits = reference.limits();
    let encoded_key = reference.max_encoded_entry_bytes() - limits.value_bytes - 1;
    let mandatory = limits.overlay_bytes
        + 4 * limits.value_bytes
        + 2 * encoded_key
        + mem::size_of::<Vec<u8>>()
        + mem::size_of::<Option<Vec<u8>>>();
    let charge = 2 * limits.key_bytes;
    for pressure_after_creation in [false, true] {
        let root = std::env::temp_dir().join(format!("cleanup-pressure-{}", uuid::Uuid::new_v4()));
        let resources = WorkerStateResources::new(ResourceConfig {
            decoded_value_bytes: mandatory + charge,
            ..reference.resources().config().clone()
        })
        .unwrap();
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.clone(),
                    job_id: "cleanup-pressure".into(),
                    operator_id: "aggregate".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                },
                resources.clone(),
            )
            .await
            .unwrap(),
        );
        let mut manager = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_calendar_operator();
        operator.native_store =
            Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
        let store = operator.native_store.as_ref().unwrap();
        // Force the real Rocks point-read reservation close to its exact
        // maximum: U/group/generation/storage-index fills the logical key cap.
        let group = vec![0; limits.key_bytes - 17];
        let marker = native_cleanup_key(&group, 0).unwrap();
        let mut pointer = native_generation_prefix(b'U', &group, 0).unwrap();
        pointer.extend_from_slice(&0_u32.to_be_bytes());
        assert_eq!(pointer.len(), limits.key_bytes);
        let mut replaced = native_generation_prefix(b'U', &group, 0).unwrap();
        replaced.extend_from_slice(&1_u32.to_be_bytes());
        let mut live = native_generation_prefix(b'U', &group, 1).unwrap();
        live.extend_from_slice(&0_u32.to_be_bytes());
        let owned_due = calendar_native::calendar_due_key(0, &group).unwrap();
        let current_due = calendar_native::calendar_due_key(1, &group).unwrap();
        let mut scope = store.begin().await.unwrap();
        scope.put(&marker, b"U").unwrap();
        scope.put(&pointer, &0_i64.to_be_bytes()).unwrap();
        scope.put(&replaced, &1_i64.to_be_bytes()).unwrap();
        scope.put(&live, &1_i64.to_be_bytes()).unwrap();
        scope.put(&owned_due, &0_u64.to_be_bytes()).unwrap();
        scope.put(&current_due, &1_u64.to_be_bytes()).unwrap();
        scope.commit().await.unwrap();
        let (mut progress, held) = if pressure_after_creation {
            let progress = NativeCleanupProgress::new(store);
            assert!(progress._permit.is_some());
            let held = resources.try_decoded_value(charge).unwrap();
            (progress, held)
        } else {
            let held = resources.try_decoded_value(charge).unwrap();
            let progress = NativeCleanupProgress::new(store);
            assert!(progress._permit.is_none());
            (progress, held)
        };
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
        // Existing pressure denies optional admission; later pressure releases
        // the admitted cursor and retries the entirely uncommitted page cold.
        assert!(progress._permit.is_none());
        assert!(progress.marker.is_none());
        assert!(progress.member_after.is_none());
        assert!(resources.try_decoded_value(mandatory).is_ok());
        let scope = store.begin().await.unwrap();
        for key in [&pointer, &replaced, &owned_due] {
            assert!(scope.get(key).await.unwrap().is_none());
        }
        assert_eq!(
            scope.get(&live).await.unwrap(),
            Some(1_i64.to_be_bytes().to_vec())
        );
        assert_eq!(
            scope.get(&current_due).await.unwrap(),
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert_eq!(scope.get(&marker).await.unwrap(), Some(b"T".to_vec()));
        drop(scope);
        assert!(
            operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
        assert!(
            !operator
                .cleanup_native_with_progress(&mut progress)
                .await
                .unwrap()
        );
        drop(progress);
        drop(held);
        assert!(resources.try_decoded_value(mandatory + charge).is_ok());
        drop(operator);
        drop(manager);
        Arc::try_unwrap(backend)
            .unwrap_or_else(|_| panic!("retained pressure backend"))
            .close_and_remove()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
