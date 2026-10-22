use super::*;

fn duplicate_changelog(base: &RecordBatch, retract: bool, ids: &[u64]) -> RecordBatch {
    let ids = ids.iter().map(|number| {
        let mut id = [0_u8; 16]; id[..8].copy_from_slice(&number.to_be_bytes()); id
    }).collect::<Vec<_>>();
    native_test_changelog_with_ids(base, &vec![retract; base.num_rows()], &ids)
}

async fn duplicate_delta(
    operator: &IncrementalAggregatingFunc,
    scope: &mut AggregateScope<'_>,
    group: &[u8],
    generation: u64,
    values: &[ArrayRef],
    ordinal: u64,
    retract: bool,
) -> Result<()> {
    operator.native_member_delta(scope, NativeMemberChange {
        group, generation, aggregate_index: 0, values, ordinal, retract, row_id: None,
    }).await
}

#[tokio::test]
async fn duplicate_progress_sparse_nulls_prefixes_insertion_and_rollback() {
    let mut operator = native_array_operator(false, false, false);
    operator.native_store = Some(native_test_store_with_limits(AggregateStoreLimits {
        page_entries: 1, ..native_test_store().limits()
    }));
    let store = operator.native_store.as_ref().unwrap();
    let input = batch(&[Some("equal"), None, Some("missing")], &[1; 3], &[Some(true); 3]);
    let inputs = operator.compute_inputs(&input).unwrap();
    let values = (0..3).map(|row| inputs[0].selected_values(Some(row)).unwrap().unwrap()).collect::<Vec<_>>();
    // Intentionally sparse occurrences, complete tuple-prefix isolation across
    // group, generation and NULL. Every actual pair has the same ownership path.
    for group in [b"A", b"B"] {
        for generation in [0, 1] {
            for ordinal in [1, 4, 9, 12] {
                for (row, value) in values[..2].iter().enumerate() {
                    let ordinal = ordinal + row as u64;
                    let mut scope = store.begin().await.unwrap();
                    duplicate_delta(&operator, &mut scope, group, generation, value, ordinal, false).await.unwrap();
                    scope.commit().await.unwrap();
                }
            }
        }
    }
    let mut progress = store.try_member_progress(native_test_config().max_pending_output_bytes);
    assert!(progress.is_some());
    for (group, generation, row, ordinal) in [
        (b"A", 0, 0, 1), (b"A", 0, 0, 4), (b"B", 0, 0, 1),
        (b"B", 0, 1, 2), (b"A", 1, 0, 1), (b"A", 0, 0, 9),
    ] {
        let mut scope = store.begin().await.unwrap();
        scope.set_member_progress(progress.take());
        duplicate_delta(&operator, &mut scope, group, generation, &values[row], 0, true).await.unwrap();
        let (primary, secondary, _) = operator.inspect_member_keys(group, generation, 0, &values[row], ordinal).unwrap();
        assert!(scope.get(&primary).await.unwrap().is_none());
        assert!(scope.get(&secondary).await.unwrap().is_none());
        progress = scope.commit_member_progress().await.unwrap();
    }
    // Even an earlier helper ordinal insertion invalidates the staged bound
    // before its writes; the next retract must remove it rather than ordinal12.
    let mut scope = store.begin().await.unwrap();
    scope.set_member_progress(progress.take());
    duplicate_delta(&operator, &mut scope, b"A", 0, &values[0], 0, false).await.unwrap();
    duplicate_delta(&operator, &mut scope, b"A", 0, &values[0], 0, true).await.unwrap();
    let (primary, secondary, _) = operator.inspect_member_keys(b"A", 0, 0, &values[0], 12).unwrap();
    assert!(scope.get(&primary).await.unwrap().is_some());
    assert!(scope.get(&secondary).await.unwrap().is_some());
    progress = scope.commit_member_progress().await.unwrap();
    // One successful pending retract followed by an unmatched tuple rolls back
    // the pair and collection accounting together; the cursor dies with scope.
    let totals_key = native_collection_totals_key(b"A", 0).unwrap();
    let before = store.begin().await.unwrap().get(&totals_key).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.set_member_progress(progress.take());
    duplicate_delta(&operator, &mut scope, b"A", 0, &values[0], 0, true).await.unwrap();
    assert!(duplicate_delta(&operator, &mut scope, b"A", 0, &values[2], 0, true).await.unwrap_err().to_string().contains("nonmatching indexed member"));
    drop(scope);
    let scope = store.begin().await.unwrap();
    assert_eq!(scope.get(&totals_key).await.unwrap(), before);
    assert!(scope.get(&primary).await.unwrap().is_some());
    assert!(scope.get(&secondary).await.unwrap().is_some());
    drop(scope);
    assert!(store.resources().try_decoded_value(store.resources().config().decoded_value_bytes).is_ok());
}

#[tokio::test]
async fn duplicate_progress_optional_admission_leaves_mandatory_work_available() {
    let store = native_test_store();
    let limits = store.limits();
    let output = native_test_config().max_pending_output_bytes;
    let mandatory = limits.overlay_bytes + 3 * limits.value_bytes + 3 * output + 2 * store.max_encoded_entry_bytes() + 128;
    let charge = 4 * limits.key_bytes + std::mem::size_of::<crate::arrow::aggregate_store::AggregateMemberProgress>() + 128;
    let progress = store.try_member_progress(output).unwrap();
    let free = store.resources().config().decoded_value_bytes - charge;
    assert!(store.resources().try_decoded_value(free).is_ok());
    assert!(store.resources().try_decoded_value(free + 1).is_err());
    drop(progress);
    let held = store.resources().try_decoded_value(store.resources().config().decoded_value_bytes - mandatory).unwrap();
    // Only the mandatory peak is free: optional reservation is refused.
    let additional = store.resources().try_decoded_value(mandatory - charge + 1).unwrap();
    assert!(store.try_member_progress(output).is_none());
    drop(additional); drop(held);
    assert!(store.try_member_progress(usize::MAX).is_none());
    let progress = store.try_member_progress(output).unwrap();
    let mut scope = store.begin().await.unwrap(); scope.set_member_progress(Some(progress));
    scope.put(b"rollback", b"discard").unwrap(); drop(scope);
    assert!(store.begin().await.unwrap().get(b"rollback").await.unwrap().is_none());
    assert!(store.resources().try_decoded_value(store.resources().config().decoded_value_bytes).is_ok());
}

#[tokio::test]
async fn duplicate_progress_cancel_before_and_after_atomic_commit_is_cold_on_retry() {
    use super::aggregate_cleanup_tests::CleanupCommitGate;
    use std::sync::atomic::{AtomicU8, Ordering};
    for gate_at in [1, 2] {
        let template = native_test_store();
        let resources = template.resources().clone(); let limits = template.limits(); drop(template);
        let backend = Arc::new(CleanupCommitGate {
            inner: Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap()),
            gate: AtomicU8::new(0), release: tokio::sync::Notify::new(),
        });
        let mut manager = LiveTableManager::new(backend.clone(), Ownership::PartitionLocal {subtask: 0, parallelism: 1}).unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_array_operator(false, false, false);
        operator.native_store = Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
        let store = operator.native_store.as_ref().unwrap();
        let input = batch(&[Some("equal")], &[1], &[Some(true)]);
        let values = operator.compute_inputs(&input).unwrap()[0].selected_values(Some(0)).unwrap().unwrap();
        for ordinal in [1, 4, 9] {
            let mut scope = store.begin().await.unwrap();
            duplicate_delta(&operator, &mut scope, &GLOBAL_KEY, 0, &values, ordinal, false).await.unwrap();
            scope.commit().await.unwrap();
        }
        let mut scope = store.begin().await.unwrap();
        scope.set_member_progress(store.try_member_progress(native_test_config().max_pending_output_bytes));
        duplicate_delta(&operator, &mut scope, &GLOBAL_KEY, 0, &values, 0, true).await.unwrap();
        let progress = scope.commit_member_progress().await.unwrap();
        let mut scope = store.begin().await.unwrap(); scope.set_member_progress(progress);
        duplicate_delta(&operator, &mut scope, &GLOBAL_KEY, 0, &values, 0, true).await.unwrap();
        backend.gate.store(gate_at, Ordering::SeqCst);
        let mut committing = Box::pin(scope.commit_member_progress());
        assert!(futures::poll!(&mut committing).is_pending());
        drop(committing);
        backend.gate.store(0, Ordering::SeqCst);
        assert!(resources.try_decoded_value(resources.config().decoded_value_bytes).is_ok());
        let expected = if gate_at == 1 {2} else {1};
        assert_eq!(array_members(store, &operator).await.len(), expected);
        // A fresh optional progress starts at the true current minimum after
        // either rollback or committed-but-unacknowledged cancellation.
        let mut scope = store.begin().await.unwrap();
        scope.set_member_progress(store.try_member_progress(native_test_config().max_pending_output_bytes));
        duplicate_delta(&operator, &mut scope, &GLOBAL_KEY, 0, &values, 0, true).await.unwrap();
        scope.commit_member_progress().await.unwrap();
        assert_eq!(array_members(store, &operator).await.len(), expected - 1);
        drop(operator); drop(manager); drop(backend);
    }
}

#[tokio::test]
async fn duplicate_native_batches_exact_values_and_fresh_checkpoint_recovery() {
    use arroyo_state::live::{checkpoint, lifecycle::RocksStateConfig, rocks::RocksLiveState};
    for rocks in [false, true] {
        let root = std::env::temp_dir().join(format!("duplicate-recovery-{}", uuid::Uuid::new_v4()));
        let remote = root.join("checkpoint"); std::fs::create_dir_all(&remote).unwrap();
        let resources = WorkerStateResources::new(ResourceConfig {
            max_open_databases: 2, scan_page_bytes: 16 * 1024 * 1024,
            queued_write_bytes: 16 * 1024 * 1024, decoded_value_bytes: 32 * 1024 * 1024,
            ..native_test_store().resources().config().clone()
        }).unwrap();
        let mut native = Vec::new(); let mut backends: Vec<Arc<dyn LiveStateBackend>> = Vec::new();
        for generation in [0, 1] {
            if rocks {
                let backend = Arc::new(RocksLiveState::open(RocksStateConfig {
                    root: root.join("live"), job_id: "duplicate-recovery".into(), operator_id: "aggregate".into(),
                    subtask: 0, generation, attempt: 0,
                }, resources.clone()).await.unwrap());
                backends.push(backend.clone()); native.push(backend);
            } else { backends.push(Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap())); }
        }
        let ownership = Ownership::PartitionLocal {subtask: 0, parallelism: 1};
        let mut manager = LiveTableManager::new(backends[0].clone(), ownership.clone()).unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap(); let namespace = table.namespace().clone();
        let limits = AggregateStoreLimits {page_entries: 1, ..native_test_store().limits()};
        let mut operator = native_array_operator(false, false, true);
        operator.native_store = Some(AggregateStore::new(backends[0].clone(), table, resources.clone(), limits).unwrap());
        let values = [vec![Some("a"); 6], vec![None; 3], vec![Some("z"); 3]].concat();
        let sequences = [vec![1; 6], vec![2; 3], vec![3; 3]].concat();
        let raw = batch(&values, &sequences, &[Some(true); 12]);
        let seed = duplicate_changelog(&raw, false, &(0..12).collect::<Vec<_>>());
        let mut ctx = native_input_context(seed.schema()).await;
        operator.process_native_batch(&seed, &mut ctx).await.unwrap();
        let deleted = [vec![Some("a"); 4], vec![None; 1], vec![Some("z"); 1]].concat();
        let sequences = [vec![1; 4], vec![2; 1], vec![3; 1]].concat();
        let raw = batch(&deleted, &sequences, &[Some(true); 6]);
        operator.process_native_batch(&duplicate_changelog(&raw, true, &[(0..4).collect::<Vec<_>>(), (6..7).collect(), (9..10).collect()].concat()), &mut ctx).await.unwrap();
        let expected = [vec![ScalarValue::Utf8(Some("a".into())); 2], vec![ScalarValue::Utf8(None); 2], vec![ScalarValue::Utf8(Some("z".into())); 2]].concat();
        assert_eq!(array_members(operator.native_store.as_ref().unwrap(), &operator).await, expected);
        // Matched deletes followed by an unmatched tuple fit one actual input
        // chunk and roll back its staged progress, primary/secondary and totals.
        let bad = batch(&[Some("a"), Some("a"), Some("missing")], &[1, 1, 1], &[Some(true); 3]);
        assert!(operator.process_native_batch(&duplicate_changelog(&bad, true, &[4, 5, 999]), &mut ctx).await.unwrap_err().to_string().contains("nonmatching indexed member"));
        assert_eq!(array_members(operator.native_store.as_ref().unwrap(), &operator).await, expected);
        assert!(resources.try_decoded_value(resources.config().decoded_value_bytes).is_ok());
        let snapshot = backends[0].snapshot().await.unwrap();
        let storage = arroyo_state::get_storage_provider(&arroyo_state::StorageProviderFor::Controller {storage_url: Some(format!("file://{}", remote.display()))}).await.unwrap();
        let config = DiskKeyedTableConfig {table_name: NATIVE_AGGREGATE_TABLE.into(), encoding_version: 1, schema_identity: operator.native_schema_identity.clone()};
        let receipt = checkpoint::export(&snapshot, &namespace, &config, &storage, "checkpoint-1/aggregate", 1, 0, 0).await.unwrap();
        checkpoint::restore(backends[1].as_ref(), &namespace, &config, &receipt, &storage).await.unwrap();
        let mut restored_manager = LiveTableManager::new(backends[1].clone(), ownership).unwrap();
        let table = restored_manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut fresh = native_array_operator(false, false, true);
        fresh.native_store = Some(AggregateStore::new(backends[1].clone(), table, resources.clone(), limits).unwrap());
        assert_eq!(array_members(fresh.native_store.as_ref().unwrap(), &fresh).await, expected);
        let raw = batch(&[vec![Some("a"); 1], vec![None; 1]].concat(), &[vec![1; 1], vec![2; 1]].concat(), &[Some(true); 2]);
        fresh.process_native_batch(&duplicate_changelog(&raw, true, &[(4..5).collect::<Vec<_>>(), (7..8).collect()].concat()), &mut ctx).await.unwrap();
        let expected = [vec![ScalarValue::Utf8(Some("a".into())); 1], vec![ScalarValue::Utf8(None); 1], vec![ScalarValue::Utf8(Some("z".into())); 2]].concat();
        assert_eq!(array_members(fresh.native_store.as_ref().unwrap(), &fresh).await, expected);
        let scope = fresh.native_store.as_ref().unwrap().begin().await.unwrap();
        assert_eq!(CollectionStats::decode(scope.get(&native_collection_totals_key(&GLOBAL_KEY, 0).unwrap()).await.unwrap()).unwrap().members, 4);
        drop(scope); drop(snapshot); drop(operator); drop(fresh); drop(manager); drop(restored_manager); drop(backends);
        for backend in native {
            Arc::try_unwrap(backend).unwrap_or_else(|_| panic!("retained duplicate backend")).close_and_remove().await.unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Adds shared-owner decoded pressure at the second real scan scope, after the
/// first input chunk committed. All state and resource admissions are native.
struct DuplicateReadPressure {
    inner: Arc<dyn LiveStateBackend>,
    resources: WorkerStateResources,
    held_bytes: usize,
    held: std::sync::Mutex<Option<arroyo_state::live::resources::ResourcePermit>>,
    snapshots: std::sync::atomic::AtomicUsize,
    writes: std::sync::atomic::AtomicUsize,
    non_resource_error: bool,
}

#[async_trait::async_trait]
impl LiveStateBackend for DuplicateReadPressure {
    async fn get(&self, key: &arroyo_state::live::StateKey, options: arroyo_state::live::ReadOptions) -> arroyo_state::live::Result<Option<Vec<u8>>> { self.inner.get(key, options).await }
    async fn try_get(&self, key: &arroyo_state::live::StateKey, options: arroyo_state::live::ReadOptions) -> arroyo_state::live::Result<Option<Vec<u8>>> { self.inner.try_get(key, options).await }
    async fn multi_get(&self, keys: &[arroyo_state::live::StateKey], options: arroyo_state::live::ReadOptions) -> arroyo_state::live::Result<Vec<Option<Vec<u8>>>> { self.inner.multi_get(keys, options).await }
    async fn write_batch(&self, batch: arroyo_state::live::WriteBatch) -> arroyo_state::live::Result<()> { self.inner.write_batch(batch).await }
    async fn write_admitted(&self, batch: AdmittedWriteBatch) -> arroyo_state::live::Result<()> {
        self.inner.write_admitted(batch).await?;
        self.writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn snapshot(&self) -> arroyo_state::live::Result<arroyo_state::live::StateSnapshot> {
        if self.snapshots.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            if self.non_resource_error {
                return Err(arroyo_state::live::LiveStateError::Backend("duplicate fixture non-resource failure".into()));
            }
            *self.held.lock().unwrap() = Some(self.resources.try_decoded_value(self.held_bytes)?);
        }
        self.inner.snapshot().await
    }
}

#[tokio::test]
async fn duplicate_native_batch_mid_chunk_pressure_retries_only_uncommitted_preparation() {
    use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for non_resource_error in [false, true] {
        let template = native_test_store();
        let resources = template.resources().clone(); let limits = template.limits();
        let encoded_key = template.max_encoded_entry_bytes() - limits.value_bytes - 1;
        let cold_scope = limits.overlay_bytes + 3 * limits.value_bytes;
        let read_peak = limits.value_bytes + 2 * encoded_key + mem::size_of::<Vec<u8>>() + mem::size_of::<Option<Vec<u8>>>();
        drop(template);
        let root = std::env::temp_dir().join(format!("duplicate-pressure-{}", uuid::Uuid::new_v4()));
        let rocks = Arc::new(RocksLiveState::open(RocksStateConfig {root: root.clone(), job_id: "duplicate-pressure".into(), operator_id: "aggregate".into(), subtask: 0, generation: 0, attempt: 0}, resources.clone()).await.unwrap());
        let backend = Arc::new(DuplicateReadPressure {
            inner: rocks.clone(), resources: resources.clone(), held_bytes: resources.config().decoded_value_bytes - cold_scope - read_peak,
            held: std::sync::Mutex::new(None), snapshots: AtomicUsize::new(0), writes: AtomicUsize::new(0), non_resource_error,
        });
        let mut manager = LiveTableManager::new(backend.clone(), Ownership::PartitionLocal {subtask: 0, parallelism: 1}).unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = native_array_operator(false, false, false);
        operator.native_store = Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
        let raw = batch(&[Some("equal"); 6], &[1; 6], &[Some(true); 6]);
        let seed = duplicate_changelog(&raw, false, &[0, 1, 2, 3, 4, 5]);
        let mut ctx = native_input_context(seed.schema()).await;
        operator.process_native_batch(&seed, &mut ctx).await.unwrap();
        let seed_writes = backend.writes.load(Ordering::SeqCst);
        assert_eq!(seed_writes, 2, "fixture needs two original three-row chunks");
        let result = operator.process_native_batch(&duplicate_changelog(&raw, true, &[0, 1, 2, 3, 4, 5]), &mut ctx).await;
        if non_resource_error {
            assert!(result.unwrap_err().to_string().contains("duplicate fixture non-resource failure"));
            assert_eq!(backend.snapshots.load(Ordering::SeqCst), 2, "non-resource failure must not retry");
            assert_eq!(backend.writes.load(Ordering::SeqCst), seed_writes + 1, "only first chunk acknowledged");
        } else {
            result.unwrap();
            assert_eq!(backend.snapshots.load(Ordering::SeqCst), 3, "exactly one cold preparation retry");
            assert_eq!(backend.writes.load(Ordering::SeqCst), seed_writes + 2, "no acknowledged chunk replay");
            assert!(backend.held.lock().unwrap().is_some(), "shared pressure remains live through cold retry");
        }
        backend.held.lock().unwrap().take();
        assert_eq!(array_members(operator.native_store.as_ref().unwrap(), &operator).await.len(), if non_resource_error {3} else {0});
        assert!(resources.try_decoded_value(resources.config().decoded_value_bytes).is_ok());
        drop(operator); drop(manager); drop(backend);
        Arc::try_unwrap(rocks).unwrap_or_else(|_| panic!("retained pressure backend")).close_and_remove().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn duplicate_native_batch_exact_original_decoded_budget_uses_cold_fallback() {
    let reference = native_test_store(); let limits = reference.limits();
    let output = native_test_config().max_pending_output_bytes;
    let budget = limits.overlay_bytes + 3 * limits.value_bytes + 3 * output;
    let resources = WorkerStateResources::new(ResourceConfig {decoded_value_bytes: budget, ..reference.resources().config().clone()}).unwrap();
    let backend: Arc<dyn LiveStateBackend> = Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
    let mut manager = LiveTableManager::new(backend.clone(), Ownership::PartitionLocal {subtask: 0, parallelism: 1}).unwrap();
    let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
    let mut operator = native_array_operator(false, false, false);
    operator.native_store = Some(AggregateStore::new(backend, table, resources.clone(), limits).unwrap());
    assert!(operator.native_store.as_ref().unwrap().try_member_progress(output).is_none());
    let raw = batch(&[Some("equal"); 6], &[1; 6], &[Some(true); 6]);
    let mut ctx = native_input_context(duplicate_changelog(&raw, false, &[0,1,2,3,4,5]).schema()).await;
    operator.process_native_batch(&duplicate_changelog(&raw, false, &[0,1,2,3,4,5]), &mut ctx).await.unwrap();
    operator.process_native_batch(&duplicate_changelog(&raw, true, &[0,1,2,3,4,5]), &mut ctx).await.unwrap();
    assert!(array_members(operator.native_store.as_ref().unwrap(), &operator).await.is_empty());
    assert!(resources.try_decoded_value(budget).is_ok());
}

#[tokio::test]
async fn duplicate_progress_maximum_logical_key_rocks_scans_and_exact_pair_ownership() {
    use arroyo_state::live::{lifecycle::RocksStateConfig, rocks::RocksLiveState};
    let template = native_test_store(); let resources = template.resources().clone();
    let limits = AggregateStoreLimits {page_entries: 1, ..template.limits()}; drop(template);
    let root = std::env::temp_dir().join(format!("duplicate-max-key-{}", uuid::Uuid::new_v4()));
    let backend = Arc::new(RocksLiveState::open(RocksStateConfig {root: root.clone(), job_id: "duplicate-max-key".into(), operator_id: "aggregate".into(), subtask: 0, generation: 0, attempt: 0}, resources.clone()).await.unwrap());
    let mut manager = LiveTableManager::new(backend.clone(), Ownership::PartitionLocal {subtask: 0, parallelism: 1}).unwrap();
    let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
    let mut operator = native_array_operator(false, false, false);
    operator.native_store = Some(AggregateStore::new(backend.clone(), table, resources.clone(), limits).unwrap());
    let store = operator.native_store.as_ref().unwrap();
    let raw = batch(&[Some("equal")], &[1], &[Some(true)]);
    let values = operator.compute_inputs(&raw).unwrap()[0].selected_values(Some(0)).unwrap().unwrap();
    let (_, secondary, _) = operator.inspect_member_keys(&[], 0, 0, &values, 1).unwrap();
    let group = vec![0; limits.key_bytes - secondary.len()];
    let mut progress = store.try_member_progress(native_test_config().max_pending_output_bytes);
    assert!(progress.is_some());
    for ordinal in [1, 4, 9] {
        let (primary, secondary, _) = operator.inspect_member_keys(&group, 0, 0, &values, ordinal).unwrap();
        assert_eq!(secondary.len(), limits.key_bytes); assert!(primary.len() <= limits.key_bytes);
        let mut scope = store.begin().await.unwrap();
        duplicate_delta(&operator, &mut scope, &group, 0, &values, ordinal, false).await.unwrap();
        scope.commit().await.unwrap();
    }
    for ordinal in [1, 4] {
        let mut scope = store.begin().await.unwrap(); scope.set_member_progress(progress.take());
        duplicate_delta(&operator, &mut scope, &group, 0, &values, 0, true).await.unwrap();
        let (primary, secondary, _) = operator.inspect_member_keys(&group, 0, 0, &values, ordinal).unwrap();
        assert!(scope.get(&primary).await.unwrap().is_none()); assert!(scope.get(&secondary).await.unwrap().is_none());
        progress = scope.commit_member_progress().await.unwrap();
    }
    drop(progress);
    let scope = store.begin().await.unwrap();
    let (primary, secondary, _) = operator.inspect_member_keys(&group, 0, 0, &values, 9).unwrap();
    assert!(scope.get(&primary).await.unwrap().is_some()); assert!(scope.get(&secondary).await.unwrap().is_some());
    assert_eq!(CollectionStats::decode(scope.get(&native_collection_totals_key(&group, 0).unwrap()).await.unwrap()).unwrap().members, 1);
    drop(scope);
    assert!(resources.try_decoded_value(resources.config().decoded_value_bytes).is_ok());
    drop(operator); drop(manager);
    Arc::try_unwrap(backend).unwrap_or_else(|_| panic!("retained max-key backend")).close_and_remove().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
