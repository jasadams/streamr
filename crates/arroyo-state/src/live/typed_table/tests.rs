use super::*;
use crate::live::{
    WriteBatch, lifecycle::RocksStateConfig, memory::MemoryLiveState, resources::ResourceConfig,
    rocks::RocksLiveState,
};
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use async_trait::async_trait;

fn test_resources() -> WorkerStateResources {
    WorkerStateResources::new(ResourceConfig {
        block_cache_bytes: 1024 * 1024,
        memtable_bytes: 512 * 1024,
        queued_write_bytes: 2 * 1024 * 1024,
        decoded_value_bytes: 2 * 1024 * 1024,
        scan_page_bytes: 1024 * 1024,
        max_blocking_operations: 2,
        max_snapshots: 4,
        max_open_databases: 1,
        disk_reserve_bytes: 0,
    })
    .unwrap()
}
fn limits() -> TableLimits {
    TableLimits {
        key_bytes: 1024,
        row_bytes: 8192,
        decoded_bytes: 8192,
        scope_bytes: 32768,
        scope_operations: 16,
        page_bytes: 32768,
        page_entries: 2,
    }
}
fn descriptor() -> TableDescriptor {
    TableDescriptor {
        table_identity: b"state-table-v1:test".to_vec(),
        schema_identity: b"state-schema-v1:test".to_vec(),
        schema: Arc::new(Schema::new(vec![
            Field::new("first", DataType::Utf8, false),
            Field::new("second", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        primary_key: vec![0, 1],
    }
}
fn namespace() -> StateNamespace {
    StateNamespace {
        ownership: Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
        table: descriptor().table_identity,
    }
}
fn row(first: &str, second: &str, value: Option<i64>) -> RecordBatch {
    RecordBatch::try_new(
        descriptor().schema,
        vec![
            Arc::new(StringArray::from(vec![first])),
            Arc::new(StringArray::from(vec![second])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap()
}
fn key(first: &str, second: &str) -> RecordBatch {
    row(first, second, None).project(&[0, 1]).unwrap()
}
fn value(row: TableRow) -> Option<i64> {
    let values = row
        .batch()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    use arrow_array::Array;
    (!values.is_null(0)).then(|| values.value(0))
}

/// Third adapter changes only adaptation, forwarding to a bounded reference
/// backend. The generic row kernel never learns its concrete type.
struct TestAdapter(Arc<dyn LiveStateBackend>);
#[async_trait]
impl LiveStateBackend for TestAdapter {
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.0.try_get(key, options).await
    }
    fn try_admit_write(&self, bytes: usize, operations: usize) -> Result<AdmittedWriteBatch> {
        self.0.try_admit_write(bytes, operations)
    }
    async fn get(&self, key: &StateKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.0.get(key, options).await
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.0.multi_get(keys, options).await
    }
    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.0.write_batch(batch).await
    }
    async fn snapshot(&self) -> Result<StateSnapshot> {
        self.0.snapshot().await
    }
    async fn admit_write(&self, bytes: usize, operations: usize) -> Result<AdmittedWriteBatch> {
        self.0.admit_write(bytes, operations).await
    }
    async fn write_admitted(&self, batch: AdmittedWriteBatch) -> Result<()> {
        self.0.write_admitted(batch).await
    }
    async fn close(self: Arc<Self>) -> Result<()> {
        let this = Arc::try_unwrap(self).map_err(|_| invalid("test adapter active handles"))?;
        this.0.close().await
    }
}

async fn conformance(backend: Arc<dyn LiveStateBackend>, resources: WorkerStateResources) {
    let mut manager =
        crate::live::table::LiveTableManager::new(backend.clone(), namespace().ownership).unwrap();
    let table = manager
        .register_typed(descriptor(), limits(), resources.clone())
        .unwrap();
    let mut second_descriptor = descriptor();
    second_descriptor
        .table_identity
        .extend_from_slice(b"-second");
    let second = manager
        .register_typed(second_descriptor, limits(), resources.clone())
        .unwrap();
    assert_eq!(manager.namespaces().count(), 2);
    assert_eq!(manager.typed_descriptors().count(), 2);
    assert!(
        manager
            .register_typed(descriptor(), limits(), resources.clone())
            .is_err()
    );
    assert!(table.get(&key("a", "bc")).await.unwrap().is_none());
    let mut scope = table.begin().await.unwrap();
    scope.put(&row("a", "bc", Some(1))).unwrap();
    scope.put(&row("ab", "c", Some(2))).unwrap();
    scope.put_into(&second, &row("a", "bc", Some(200))).unwrap();
    assert_eq!(
        value(
            scope
                .get_from(&second, &key("a", "bc"))
                .await
                .unwrap()
                .unwrap()
        ),
        Some(200)
    );
    scope.put(&row("🦀", "é", None)).unwrap();
    assert_eq!(
        value(scope.get(&key("a", "bc")).await.unwrap().unwrap()),
        Some(1)
    );
    scope.delete(&key("a", "bc")).unwrap();
    assert!(scope.get(&key("a", "bc")).await.unwrap().is_none());
    scope.put(&row("a", "bc", Some(3))).unwrap();
    scope.commit().await.unwrap();
    assert_eq!(
        value(table.get(&key("a", "bc")).await.unwrap().unwrap()),
        Some(3)
    );
    assert_eq!(
        value(table.get(&key("ab", "c")).await.unwrap().unwrap()),
        Some(2)
    );
    assert_eq!(
        value(table.get(&key("🦀", "é")).await.unwrap().unwrap()),
        None
    );
    assert_eq!(
        value(second.get(&key("a", "bc")).await.unwrap().unwrap()),
        Some(200)
    );
    let snapshot = table.snapshot().await.unwrap();
    let mut scope = table.begin().await.unwrap();
    scope.delete(&key("a", "bc")).unwrap();
    scope.commit().await.unwrap();
    assert!(table.get(&key("a", "bc")).await.unwrap().is_none());
    assert_eq!(
        value(snapshot.get(&key("a", "bc")).await.unwrap().unwrap()),
        Some(3)
    );
    let page = snapshot.scan(None).await.unwrap();
    assert_eq!(page.rows.len(), 2);
    let cursor = page.next_cursor.clone().unwrap();
    drop(page);
    let page = snapshot.scan(Some(cursor)).await.unwrap();
    assert_eq!(page.rows.len(), 1);
    drop(page);
    let mut abandoned = table.begin().await.unwrap();
    abandoned.put(&row("discard", "me", Some(99))).unwrap();
    drop(abandoned);
    assert!(table.get(&key("discard", "me")).await.unwrap().is_none());
    // Different catalog identity cannot silently reinterpret persisted values.
    let mut changed = descriptor();
    changed.schema_identity.push(0);
    let incompatible =
        TypedTable::new(backend.clone(), namespace(), changed, limits(), resources).unwrap();
    assert!(incompatible.get(&key("ab", "c")).await.is_err());
    drop(incompatible);
    assert!(table.get(&row("a", "b", Some(3))).await.is_err());
    let mut scope = table.begin().await.unwrap();
    assert!(scope.put(&row(&"x".repeat(9000), "b", Some(3))).is_err());
    scope
        .put(&row("valid", "after-rejection", Some(7)))
        .unwrap();
    scope.commit().await.unwrap();
    assert_eq!(
        value(
            table
                .get(&key("valid", "after-rejection"))
                .await
                .unwrap()
                .unwrap()
        ),
        Some(7)
    );
    drop(snapshot);
    drop(table);
    drop(second);
    drop(manager);
    backend.close().await.unwrap();
}

#[tokio::test]
async fn typed_table_backend_conformance() {
    let resources = test_resources();
    conformance(
        Arc::new(MemoryLiveState::bounded(resources.clone(), 256 * 1024).unwrap()),
        resources,
    )
    .await;
    let resources = test_resources();
    conformance(
        Arc::new(TestAdapter(Arc::new(
            MemoryLiveState::bounded(resources.clone(), 256 * 1024).unwrap(),
        ))),
        resources,
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let resources = test_resources();
    let backend = RocksLiveState::open_worker_with_resources(
        RocksStateConfig {
            root: root.path().to_path_buf(),
            job_id: "test".into(),
            operator_id: "owner".into(),
            subtask: 0,
            generation: 0,
            attempt: 0,
        },
        resources.clone(),
    )
    .await
    .unwrap();
    backend.remove_on_drop();
    conformance(Arc::new(backend), resources).await;
}

#[tokio::test]
async fn limits_and_schema_fail_before_mutation() {
    let resources = test_resources();
    let backend = Arc::new(MemoryLiveState::bounded(resources.clone(), 128).unwrap());
    let table = TypedTable::new(
        backend.clone(),
        namespace(),
        descriptor(),
        limits(),
        resources.clone(),
    )
    .unwrap();
    let mut scope = table.begin().await.unwrap();
    scope.put(&row("a", "b", Some(1))).unwrap();
    assert!(scope.commit().await.is_err());
    assert!(table.get(&key("a", "b")).await.unwrap().is_none());
    let mut nullable = descriptor();
    nullable.primary_key = vec![2];
    assert!(
        TypedTable::new(
            backend.clone(),
            namespace(),
            nullable,
            limits(),
            resources.clone()
        )
        .is_err()
    );
    let mut unsupported = descriptor();
    unsupported.schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::Null,
        false,
    )]));
    unsupported.primary_key = vec![0];
    assert!(
        TypedTable::new(
            backend.clone(),
            namespace(),
            unsupported,
            limits(),
            resources.clone()
        )
        .is_err()
    );
    let wrong_owner = StateNamespace {
        ownership: Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 2,
        },
        ..namespace()
    };
    assert!(
        TypedTable::new(
            backend.clone(),
            wrong_owner,
            descriptor(),
            limits(),
            resources.clone()
        )
        .is_err()
    );
    // Legacy unaccounted reference adapters explicitly reject producer admission.
    assert!(MemoryLiveState::new().admit_write(100, 1).await.is_err());
    let other_resources = test_resources();
    let batch = MemoryLiveState::bounded(other_resources, 1024)
        .unwrap()
        .admit_write(100, 1)
        .await
        .unwrap();
    assert!(backend.write_admitted(batch).await.is_err());
}

fn tight_resources(decoded: usize, scans: usize) -> WorkerStateResources {
    let mut config = test_resources().config().clone();
    config.decoded_value_bytes = decoded;
    config.scan_page_bytes = scans;
    WorkerStateResources::new(config).unwrap()
}
fn decoded_headroom() -> usize {
    decoded_headroom_for(&namespace())
}
fn decoded_headroom_for(namespace: &StateNamespace) -> usize {
    let limits = limits();
    scope_reservation(limits).unwrap()
        + key_workspace(limits.key_bytes, descriptor().primary_key.len(), namespace).unwrap()
        + limits.decoded_bytes * 3
        + limits.row_bytes
        + limits.key_bytes * 2
        + std::mem::size_of::<Vec<u8>>()
        + std::mem::size_of::<Option<Vec<u8>>>()
}

#[tokio::test]
async fn nested_admission_rejects_impossible_limits_and_never_waits_for_itself() {
    use std::time::Duration;
    let small = tight_resources(scope_reservation(limits()).unwrap(), 1024 * 1024);
    let backend = Arc::new(MemoryLiveState::bounded(small.clone(), 65536).unwrap());
    assert!(TypedTable::new(backend, namespace(), descriptor(), limits(), small).is_err());
    let small_scan = tight_resources(decoded_headroom(), limits().page_bytes * 4);
    let backend = Arc::new(MemoryLiveState::bounded(small_scan.clone(), 65536).unwrap());
    assert!(TypedTable::new(backend, namespace(), descriptor(), limits(), small_scan).is_err());
    for rocks in [false, true] {
        let resources = tight_resources(decoded_headroom(), 1024 * 1024);
        let root = tempfile::tempdir().unwrap();
        let backend: Arc<dyn LiveStateBackend> = if rocks {
            crate::live::worker::construct_backend(
                crate::live::worker::BackendConstruction::Rocksdb(RocksStateConfig {
                    root: root.path().to_path_buf(),
                    job_id: "test".into(),
                    operator_id: "tight".into(),
                    subtask: 0,
                    generation: 0,
                    attempt: 0,
                }),
                resources.clone(),
            )
            .await
            .unwrap()
        } else {
            Arc::new(MemoryLiveState::bounded(resources.clone(), 65536).unwrap())
        };
        let table = TypedTable::new(
            backend.clone(),
            namespace(),
            descriptor(),
            limits(),
            resources.clone(),
        )
        .unwrap();
        let mut scope = tokio::time::timeout(Duration::from_secs(2), table.begin())
            .await
            .unwrap()
            .unwrap();
        // Exact combined headroom supports pending and backend reads in scope.
        assert!(
            tokio::time::timeout(Duration::from_secs(2), scope.get(&key("missing", "row")))
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        scope.put(&row("a", "b", Some(1))).unwrap();
        scope.commit().await.unwrap();
        // Exhaust the remainder while leaving the typed read's own reservation
        // available. A nested RocksDB reservation fails instead of self-waiting.
        let held = resources
            .try_decoded_value(
                resources.config().decoded_value_bytes
                    - limits().decoded_bytes * 3
                    - key_workspace(
                        limits().key_bytes,
                        descriptor().primary_key.len(),
                        &namespace(),
                    )
                    .unwrap(),
            )
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), table.get(&key("a", "b")))
            .await
            .unwrap();
        if rocks {
            assert!(result.is_err());
        } else {
            assert!(result.is_ok());
        }
        drop(result);
        drop(held);
        let snapshot = table.snapshot().await.unwrap();
        let held = resources
            .try_decoded_value(
                resources.config().decoded_value_bytes
                    - limits().decoded_bytes * 3
                    - key_workspace(
                        limits().key_bytes,
                        descriptor().primary_key.len(),
                        &namespace(),
                    )
                    .unwrap(),
            )
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), snapshot.get(&key("a", "b")))
            .await
            .unwrap();
        if rocks {
            assert!(result.is_err());
        }
        drop(result);
        drop(held);
        let held = resources
            .try_scan_page(resources.config().scan_page_bytes - limits().page_bytes * 2)
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), snapshot.scan(None))
            .await
            .unwrap();
        if rocks {
            assert!(result.is_err());
        }
        drop(result);
        drop(held);
        drop(snapshot);
        drop(table);
        backend.close().await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_owners_release_partial_admission_and_retained_rows_keep_their_charge() {
    use std::time::Duration;
    let mut next = descriptor();
    next.table_identity.extend_from_slice(b"-next");
    let next_namespace = StateNamespace {
        table: next.table_identity.clone(),
        ..namespace()
    };
    // Both owners share the pool; admit the larger namespace's exact headroom.
    let resources = tight_resources(decoded_headroom_for(&next_namespace), 1024 * 1024);
    let backend: Arc<dyn LiveStateBackend> =
        Arc::new(MemoryLiveState::bounded(resources.clone(), 65536).unwrap());
    let first = TypedTable::new(
        backend.clone(),
        namespace(),
        descriptor(),
        limits(),
        resources.clone(),
    )
    .unwrap();
    let second = TypedTable::new(
        backend.clone(),
        next_namespace,
        next,
        limits(),
        resources.clone(),
    )
    .unwrap();
    let queue = resources
        .try_queued_write(resources.config().queued_write_bytes)
        .unwrap();
    let (left, right) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(first.begin(), second.begin())
    })
    .await
    .unwrap();
    assert!(left.is_err() && right.is_err());
    drop(left);
    drop(right);
    drop(queue);
    let mut scope = first.begin().await.unwrap();
    scope.put(&row("a", "b", Some(1))).unwrap();
    scope.commit().await.unwrap();
    let held = first.get(&key("a", "b")).await.unwrap().unwrap();
    let borrowed = held.batch();
    let rest = resources
        .try_decoded_value(resources.config().decoded_value_bytes - limits().decoded_bytes * 3)
        .unwrap();
    assert!(resources.try_decoded_value(1).is_err());
    assert_eq!(borrowed.num_rows(), 1);
    drop(rest);
    drop(held);
    assert!(
        resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .is_ok()
    );
}

/// Return metadata framing offsets for this writer's trusted fixture.
fn ipc_message_offsets(bytes: &[u8]) -> Vec<(usize, usize, usize)> {
    let mut offsets = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let frame = offset;
        let mut length = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if length == -1 {
            length = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
        }
        if length == 0 {
            break;
        }
        let metadata = offset;
        let end = metadata + length as usize;
        let message = arrow::ipc::root_as_message(&bytes[metadata..end]).unwrap();
        offsets.push((frame, metadata, end));
        offset = end + message.bodyLength() as usize;
    }
    offsets
}
fn flatbuffer_slot(bytes: &[u8], table: usize, field: usize) -> usize {
    let vtable = (table as isize
        - i32::from_le_bytes(bytes[table..table + 4].try_into().unwrap()) as isize)
        as usize;
    let offset = u16::from_le_bytes(
        bytes[vtable + field..vtable + field + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    assert_ne!(offset, 0);
    table + offset
}

#[tokio::test]
async fn ipc_preflight_rejects_lengths_truncation_extra_batches_and_compression() {
    let descriptor = descriptor();
    let limits = limits();
    let encoded = encode(&row("a", "b", Some(1)), &descriptor, limits.row_bytes).unwrap();
    let start = 12 + descriptor.schema_identity.len();
    let ipc = &encoded[start..];
    assert!(preflight_ipc(ipc, &descriptor, limits).is_ok());
    let messages = ipc_message_offsets(ipc);
    assert_eq!(messages.len(), 2);
    for length in [i32::MAX, -2, i32::MIN] {
        let mut malformed = ipc.to_vec();
        malformed[4..8].copy_from_slice(&length.to_le_bytes());
        assert!(preflight_ipc(&malformed, &descriptor, limits).is_err());
    }
    for length in [i64::MAX, -1, limits.decoded_bytes as i64] {
        let mut malformed = ipc.to_vec();
        let metadata = messages[1].1;
        let table = metadata
            + u32::from_le_bytes(malformed[metadata..metadata + 4].try_into().unwrap()) as usize;
        let slot = flatbuffer_slot(
            &malformed,
            table,
            arrow::ipc::Message::VT_BODYLENGTH as usize,
        );
        malformed[slot..slot + 8].copy_from_slice(&length.to_le_bytes());
        assert!(preflight_ipc(&malformed, &descriptor, limits).is_err());
    }
    assert!(preflight_ipc(&ipc[..messages[1].2 + 1], &descriptor, limits).is_err());
    let mut extra = ipc[..ipc.len() - 8].to_vec();
    extra.extend_from_slice(&ipc[messages[1].0..]);
    assert!(preflight_ipc(&extra, &descriptor, limits).is_err());
    // Valid flatbuffer metadata declaring compression, without running a codec.
    // The decoder must reject it before consulting uncompressed buffer sizes.
    let mut compressed = vec![0u8; 112];
    compressed[0..4].copy_from_slice(&24u32.to_le_bytes());
    for (offset, value) in [
        (4, 14u16),
        (6, 24),
        (8, 4),
        (10, 6),
        (12, 8),
        (14, 16),
        (48, 12),
        (50, 32),
        (52, 8),
        (58, 24),
        (96, 8),
        (98, 8),
        (100, 4),
        (102, 5),
    ] {
        compressed[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
    compressed[24..28].copy_from_slice(&20i32.to_le_bytes());
    compressed[28..30].copy_from_slice(&4i16.to_le_bytes());
    compressed[30] = 3;
    compressed[32..36].copy_from_slice(&32u32.to_le_bytes());
    compressed[64..68].copy_from_slice(&16i32.to_le_bytes());
    compressed[72..80].copy_from_slice(&1i64.to_le_bytes());
    compressed[88..92].copy_from_slice(&16u32.to_le_bytes());
    compressed[104..108].copy_from_slice(&8i32.to_le_bytes());
    let verified = arrow::ipc::root_as_message(&compressed).unwrap();
    assert!(
        verified
            .header_as_record_batch()
            .unwrap()
            .compression()
            .is_some()
    );
    let mut expanded = ipc[..messages[1].0].to_vec();
    expanded.extend_from_slice(&[255; 4]);
    expanded.extend_from_slice(&(compressed.len() as i32).to_le_bytes());
    expanded.extend_from_slice(&compressed);
    expanded.extend_from_slice(&[255; 4]);
    expanded.extend_from_slice(&0i32.to_le_bytes());
    assert!(preflight_ipc(&expanded, &descriptor, limits).is_err());
    // Exercise the public read decoder path, not only the framing helper.
    let resources = test_resources();
    let permit = Arc::new(
        resources
            .try_decoded_value(limits.decoded_bytes * 3)
            .unwrap(),
    );
    let mut malformed = encoded[..start].to_vec();
    malformed.extend_from_slice(&[255; 4]);
    malformed.extend_from_slice(&i32::MAX.to_le_bytes());
    assert!(decode(&malformed, &descriptor, limits, permit).is_err());
}

#[tokio::test]
async fn oversized_persisted_values_are_rejected_before_memory_read_copy() {
    let resources = test_resources();
    let mut limits = limits();
    limits.row_bytes = 1024 * 1024;
    let backend: Arc<dyn LiveStateBackend> =
        Arc::new(MemoryLiveState::bounded(resources.clone(), 2 * 1024 * 1024).unwrap());
    let table = TypedTable::new(
        backend.clone(),
        namespace(),
        descriptor(),
        limits,
        resources.clone(),
    )
    .unwrap();
    let encoded_key = table.key(&key("a", "b")).unwrap();
    backend
        .put(
            encoded_key.key.clone(),
            vec![0; limits.row_bytes],
            limits.row_bytes * 2,
        )
        .await
        .unwrap();
    drop(encoded_key);
    let snapshot = table.snapshot().await.unwrap();
    for error in [
        table.get(&key("a", "b")).await.err().unwrap(),
        snapshot.get(&key("a", "b")).await.err().unwrap(),
    ] {
        assert!(
            matches!(error, LiveStateError::ReadLimitExceeded { required, limit } if required == limits.row_bytes && limit == limits.decoded_bytes)
        );
    }
    let scope = table.begin().await.unwrap();
    assert!(
        matches!(scope.get(&key("a", "b")).await, Err(LiveStateError::ReadLimitExceeded { required, limit }) if required == limits.row_bytes && limit == limits.decoded_bytes)
    );
    drop(scope);
    // All failed-read/key workspace reservations were returned.
    assert!(
        resources
            .try_decoded_value(resources.config().decoded_value_bytes)
            .is_ok()
    );
}

#[tokio::test]
async fn variable_keys_admit_converter_buffers_and_namespace_before_allocation() {
    for binary in [false, true] {
        let resources = test_resources();
        let mut limits = limits();
        limits.key_bytes = 16384;
        let mut descriptor = descriptor();
        descriptor.schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            if binary {
                DataType::Binary
            } else {
                DataType::Utf8
            },
            false,
        )]));
        descriptor.primary_key = vec![0];
        let table = TypedTable::new(
            Arc::new(MemoryLiveState::bounded(resources.clone(), 65536).unwrap()),
            namespace(),
            descriptor.clone(),
            limits,
            resources.clone(),
        )
        .unwrap();
        let payload = vec![b'a'; 7000];
        let column: arrow_array::ArrayRef = if binary {
            Arc::new(arrow_array::BinaryArray::from(vec![payload.as_slice()]))
        } else {
            Arc::new(StringArray::from(vec![
                std::str::from_utf8(&payload).unwrap(),
            ]))
        };
        let input = RecordBatch::try_new(descriptor.schema.clone(), vec![column]).unwrap();
        let encoded_bound = key_encoded_bound(&input).unwrap();
        let admitted_bytes = key_workspace(encoded_bound, 1, &namespace()).unwrap();
        assert!(admitted_bytes > limits.decoded_bytes * 3);
        let encoded = table.key(&input).unwrap();
        let rest = resources
            .try_decoded_value(resources.config().decoded_value_bytes - admitted_bytes)
            .unwrap();
        assert!(resources.try_decoded_value(1).is_err());
        assert!(encoded.key.key.len() <= encoded_bound + 1);
        drop(rest);
        drop(encoded);
        // A sufficient but occupied pool fails before building converter buffers.
        let held = resources
            .try_decoded_value(resources.config().decoded_value_bytes - admitted_bytes + 1)
            .unwrap();
        assert!(matches!(
            table.key(&input),
            Err(LiveStateError::Resource(_))
        ));
        drop(held);
    }
    let resources = test_resources();
    let mut descriptor = descriptor();
    descriptor.table_identity = vec![b'x'; 32768];
    let namespace = StateNamespace {
        table: descriptor.table_identity.clone(),
        ..namespace()
    };
    let mut limits = limits();
    limits.key_bytes = 65536;
    let backend = Arc::new(MemoryLiveState::bounded(resources.clone(), 65536).unwrap());
    assert!(TypedTable::new(backend, namespace, descriptor, limits, resources).is_err());
}

fn indirect(bytes: &[u8], slot: usize) -> usize {
    slot + u32::from_le_bytes(bytes[slot..slot + 4].try_into().unwrap()) as usize
}

#[test]
fn ipc_preflight_rejects_aliased_schema_and_field_metadata_before_expansion() {
    use std::collections::HashMap;
    for field_metadata in [false, true] {
        let mut descriptor = descriptor();
        let metadata: HashMap<String, String> = (0..32)
            .map(|index| {
                (
                    format!("key-{index}"),
                    if index == 0 {
                        "v".repeat(1024)
                    } else {
                        "x".into()
                    },
                )
            })
            .collect();
        descriptor.schema = if field_metadata {
            let mut fields = descriptor
                .schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[0] = fields[0].clone().with_metadata(metadata);
            Arc::new(Schema::new(fields))
        } else {
            Arc::new(descriptor.schema.as_ref().clone().with_metadata(metadata))
        };
        assert!(schema_owned_bytes(&descriptor.schema).unwrap() < limits().decoded_bytes);
        let source = row("a", "b", Some(1));
        let row =
            RecordBatch::try_new(descriptor.schema.clone(), source.columns().to_vec()).unwrap();
        let encoded = encode(&row, &descriptor, limits().row_bytes).unwrap();
        let start = 12 + descriptor.schema_identity.len();
        let ipc = &encoded[start..];
        assert!(preflight_ipc(ipc, &descriptor, limits()).is_ok());
        let messages = ipc_message_offsets(ipc);
        let mut metadata = ipc[messages[0].1..messages[0].2].to_vec();
        let message = u32::from_le_bytes(metadata[..4].try_into().unwrap()) as usize;
        let schema = indirect(
            &metadata,
            flatbuffer_slot(&metadata, message, arrow::ipc::Message::VT_HEADER as usize),
        );
        let metadata_slot = if field_metadata {
            let fields = indirect(
                &metadata,
                flatbuffer_slot(&metadata, schema, arrow::ipc::Schema::VT_FIELDS as usize),
            );
            let first = indirect(&metadata, fields + 4);
            flatbuffer_slot(
                &metadata,
                first,
                arrow::ipc::Field::VT_CUSTOM_METADATA as usize,
            )
        } else {
            flatbuffer_slot(
                &metadata,
                schema,
                arrow::ipc::Schema::VT_CUSTOM_METADATA as usize,
            )
        };
        let vector = indirect(&metadata, metadata_slot);
        let count = u32::from_le_bytes(metadata[vector..vector + 4].try_into().unwrap()) as usize;
        assert_eq!(count, 32);
        // All key/value entries legally alias one appended string. Flatbuffer
        // verification succeeds while owned conversion would copy it 32 times.
        let shared = metadata.len();
        metadata.extend_from_slice(&1024u32.to_le_bytes());
        metadata.extend_from_slice(&vec![b'v'; 1024]);
        metadata.push(0);
        while !metadata.len().is_multiple_of(8) {
            metadata.push(0);
        }
        for index in 0..count {
            let entry = indirect(&metadata, vector + 4 + index * 4);
            let value = flatbuffer_slot(&metadata, entry, arrow::ipc::KeyValue::VT_VALUE as usize);
            metadata[value..value + 4].copy_from_slice(&((shared - value) as u32).to_le_bytes());
        }
        let verified = arrow::ipc::root_as_message(&metadata).unwrap();
        let schema = verified.header_as_schema().unwrap();
        let entries = if field_metadata {
            schema.fields().unwrap().get(0).custom_metadata().unwrap()
        } else {
            schema.custom_metadata().unwrap()
        };
        assert!(
            entries
                .iter()
                .map(|entry| entry.value().unwrap().len())
                .sum::<usize>()
                > limits().decoded_bytes * 3
        );
        let mut malformed = vec![255; 4];
        malformed.extend_from_slice(&(metadata.len() as i32).to_le_bytes());
        malformed.extend_from_slice(&metadata);
        malformed.extend_from_slice(&ipc[messages[0].2..]);
        assert!(malformed.len() < limits().decoded_bytes);
        assert!(preflight_ipc(&malformed, &descriptor, limits()).is_err());
    }
}

fn numbered_row(index: usize, payload: &str, generation: i64) -> RecordBatch {
    row(
        &format!("{index:010}"),
        payload,
        (!index.is_multiple_of(3)).then_some(index as i64 + generation),
    )
}

async fn write_numbered(table: &TypedTable, count: usize, payload: &str, churn: bool) {
    for start in (0..count).step_by(8) {
        let mut scope = table.begin().await.unwrap();
        for index in start..(start + 8).min(count) {
            if churn && index.is_multiple_of(2) {
                scope
                    .delete(&key(&format!("{index:010}"), payload))
                    .unwrap();
            } else {
                scope
                    .put(&numbered_row(index, payload, if churn { 100 } else { 0 }))
                    .unwrap();
            }
        }
        scope.commit().await.unwrap();
    }
}

fn process_work() -> [u64; 5] {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    let io = std::fs::read_to_string("/proc/self/io").unwrap();
    let bytes = |name: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap()
        * 1024;
    [
        fields[11].parse().unwrap(),
        fields[12].parse().unwrap(),
        bytes("read_bytes:"),
        bytes("write_bytes:"),
        rss,
    ]
}

/// Actual public API, with finite retained growth and pinned pre-churn history.
/// Timings are dev-profile diagnostics, not engine throughput qualification.
#[tokio::test]
#[ignore = "opt-in STR-125 finite typed-table pagination diagnostic"]
async fn typed_scan_growth_churn_diagnostic() {
    for (payload_bytes, page_entries, page_bytes) in
        [(16, 1, 32768), (16, 16, 32768), (256, 16, 1900)]
    {
        for retained in [192usize, 768, 3072] {
            let root = tempfile::tempdir().unwrap();
            let resources = test_resources();
            let backend = Arc::new(
                RocksLiveState::open_worker_with_resources(
                    RocksStateConfig {
                        root: root.path().to_path_buf(),
                        job_id: "str125".into(),
                        operator_id: "enumeration".into(),
                        subtask: 0,
                        generation: 0,
                        attempt: 0,
                    },
                    resources.clone(),
                )
                .await
                .unwrap(),
            );
            backend.remove_on_drop();
            let table_limits = TableLimits {
                page_entries,
                page_bytes,
                ..limits()
            };
            let table = TypedTable::new(
                backend.clone(),
                namespace(),
                descriptor(),
                table_limits,
                resources.clone(),
            )
            .unwrap();
            let payload = "p".repeat(payload_bytes);
            write_numbered(&table, retained * 2, &payload, false).await;
            let history = table.snapshot().await.unwrap();
            for _ in 0..3 {
                write_numbered(&table, retained * 2, &payload, true).await;
            }
            let snapshot = table.snapshot().await.unwrap();
            let mut completed = 0;
            let mut next_report = retained / 3;
            let mut cursor = None;
            let mut pages = 0;
            let mut scan_us = 0;
            let mut started = std::time::Instant::now();
            let mut work = process_work();
            loop {
                let scan_started = std::time::Instant::now();
                let page = snapshot.scan(cursor).await.unwrap();
                scan_us += scan_started.elapsed().as_micros();
                assert!(!page.rows.is_empty());
                for actual in &page.rows {
                    let index = (completed * 2) + 1;
                    assert_eq!(actual.batch(), &numbered_row(index, &payload, 100));
                    completed += 1;
                }
                pages += 1;
                cursor = page.next_cursor.clone();
                drop(page);
                if completed >= next_report || cursor.is_none() {
                    next_report += retained / 3;
                    let next = process_work();
                    println!(
                        "STR125_API retained={retained} history_keys={} churn_rounds=3 payload={payload_bytes} page_entries={page_entries} page_bytes={page_bytes} completed={completed} pages={pages} scan_us={scan_us} elapsed_us={} cpu_user_ticks={} cpu_system_ticks={} read_bytes={} write_bytes={} rss_bytes={}",
                        retained * 2,
                        started.elapsed().as_micros(),
                        next[0] - work[0],
                        next[1] - work[1],
                        next[2] - work[2],
                        next[3] - work[3],
                        next[4]
                    );
                    started = std::time::Instant::now();
                    work = next;
                    pages = 0;
                    scan_us = 0;
                }
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(completed, retained);
            drop(snapshot);
            drop(history);
            drop(table);
            Arc::try_unwrap(backend)
                .ok()
                .unwrap()
                .close()
                .await
                .unwrap();
            assert!(
                resources
                    .try_scan_page(resources.config().scan_page_bytes)
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
async fn typed_scan_cursor_values_isolation_and_resource_lifetimes() {
    for rocks in [false, true] {
        for (page_entries, page_bytes) in [(1, 32768), (16, 32768), (16, 1900)] {
            let root = tempfile::tempdir().unwrap();
            let resources = test_resources();
            let backend: Arc<dyn LiveStateBackend> = crate::live::worker::construct_backend(
                if rocks {
                    crate::live::worker::BackendConstruction::Rocksdb(RocksStateConfig {
                        root: root.path().to_path_buf(),
                        job_id: "str125".into(),
                        operator_id: "exact".into(),
                        subtask: 0,
                        generation: 0,
                        attempt: 0,
                    })
                } else {
                    crate::live::worker::BackendConstruction::Memory {
                        max_resident_bytes: 1024 * 1024,
                    }
                },
                resources.clone(),
            )
            .await
            .unwrap();
            let table = TypedTable::new(
                backend.clone(),
                namespace(),
                descriptor(),
                TableLimits {
                    page_entries,
                    page_bytes,
                    ..limits()
                },
                resources.clone(),
            )
            .unwrap();
            let empty = table.snapshot().await.unwrap();
            let page = empty.scan(None).await.unwrap();
            assert!(page.rows.is_empty() && page.next_cursor.is_none());
            drop(page);
            drop(empty);
            write_numbered(&table, 48, "payload", false).await;
            let history = table.snapshot().await.unwrap();
            write_numbered(&table, 48, "payload", true).await;
            let snapshot = table.snapshot().await.unwrap();
            let mut other_descriptor = descriptor();
            other_descriptor.table_identity.push(b'2');
            let other = TypedTable::new(
                backend.clone(),
                StateNamespace {
                    table: other_descriptor.table_identity.clone(),
                    ..namespace()
                },
                other_descriptor,
                limits(),
                resources.clone(),
            )
            .unwrap();
            let mut scope = other.begin().await.unwrap();
            scope.put(&row("foreign", "payload", Some(999))).unwrap();
            scope.commit().await.unwrap();
            let other_view = other.snapshot().await.unwrap();
            let first = snapshot.scan(None).await.unwrap();
            let cursor = first.next_cursor.clone().unwrap();
            assert!(matches!(
                history.scan(Some(cursor.clone())).await,
                Err(LiveStateError::InvalidCursor)
            ));
            assert!(matches!(
                other_view.scan(Some(cursor.clone())).await,
                Err(LiveStateError::InvalidCursor)
            ));
            // Mutations before and after the continuation position cannot leak
            // into this stable view, even when already returned keys are deleted.
            let mut scope = table.begin().await.unwrap();
            scope.delete(&key("0000000001", "payload")).unwrap();
            scope.put(&row("0000000000", "payload", Some(999))).unwrap();
            scope.put(&row("zzzz", "payload", None)).unwrap();
            scope.commit().await.unwrap();
            let mut completed = 0;
            let mut page = first;
            loop {
                for actual in &page.rows {
                    assert_eq!(
                        actual.batch(),
                        &numbered_row(completed * 2 + 1, "payload", 100)
                    );
                    completed += 1;
                }
                let cursor = page.next_cursor.clone();
                drop(page);
                let Some(cursor) = cursor else {
                    break;
                };
                page = snapshot.scan(Some(cursor)).await.unwrap();
            }
            assert_eq!(completed, 24);
            // Retaining a row keeps the shared decoded-page charge after the
            // page drops. Encoded-page capacity is released independently.
            let mut page = snapshot.scan(None).await.unwrap();
            let held = page.rows.pop().unwrap();
            drop(page);
            assert!(
                resources
                    .try_scan_page(resources.config().scan_page_bytes)
                    .is_ok()
            );
            assert!(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .is_err()
            );
            drop(held);
            assert!(
                resources
                    .try_decoded_value(resources.config().decoded_value_bytes)
                    .is_ok()
            );
            // Cancel after polling into a blocked native-operation admission.
            // The other operations own the slots; the abandoned scan must
            // release its page reservations without waiting for their completion.
            if rocks {
                let mut blockers = Vec::new();
                for _ in 0..resources.config().max_blocking_operations {
                    let worker = resources.clone();
                    let (started, ready) = tokio::sync::oneshot::channel();
                    let (release, released) = std::sync::mpsc::channel();
                    let task = tokio::spawn(async move {
                        worker
                            .run_blocking(move || {
                                let _ = started.send(());
                                let _ = released.recv();
                            })
                            .await
                            .unwrap();
                    });
                    ready.await.unwrap();
                    blockers.push((release, task));
                }
                {
                    let pending = snapshot.scan(Some(cursor));
                    tokio::pin!(pending);
                    assert!(futures::poll!(&mut pending).is_pending());
                }
                assert!(
                    resources
                        .try_scan_page(resources.config().scan_page_bytes)
                        .is_ok()
                );
                for (release, task) in blockers {
                    release.send(()).unwrap();
                    task.await.unwrap();
                }
            }
            let tiny = TypedTable::new(
                backend.clone(),
                namespace(),
                descriptor(),
                TableLimits {
                    page_bytes: 1,
                    ..limits()
                },
                resources.clone(),
            )
            .unwrap();
            let tiny_view = tiny.snapshot().await.unwrap();
            assert!(matches!(
                tiny_view.scan(None).await,
                Err(LiveStateError::ReadLimitExceeded { .. })
            ));
            drop(tiny_view);
            drop(tiny);
            drop(other_view);
            drop(other);
            drop(snapshot);
            drop(history);
            drop(table);
            backend.close().await.unwrap();
            assert!(
                resources
                    .try_scan_page(resources.config().scan_page_bytes)
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
async fn typed_scan_after_checkpoint_restore_restarts_with_new_cursor() {
    use crate::live::checkpoint::{export_typed, restore_typed};
    use arroyo_rpc::grpc::rpc::TypedStateTableConfig;
    for target_rocks in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let resources = test_resources();
        let source: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 1024 * 1024).unwrap());
        let table = TypedTable::new(
            source.clone(),
            namespace(),
            descriptor(),
            limits(),
            resources,
        )
        .unwrap();
        write_numbered(&table, 48, "restore", false).await;
        let history = source.snapshot().await.unwrap();
        write_numbered(&table, 48, "restore", true).await;
        let typed_snapshot = table.snapshot().await.unwrap();
        let page = typed_snapshot.scan(None).await.unwrap();
        let old_cursor = page.next_cursor.clone().unwrap();
        drop(page);
        let raw = source.snapshot().await.unwrap();
        let transport = arroyo_state_protocol::typed_checkpoint::transport_table_name(
            &descriptor().table_identity,
        )
        .unwrap();
        let config = TypedStateTableConfig {
            transport_name: transport.clone(),
            table_identity: descriptor().table_identity,
            schema_identity: descriptor().schema_identity,
            schema_json: b"{}".to_vec(),
            primary_key: vec![0, 1],
            partition_key: vec![0, 1],
            encoding_version: ENCODING_VERSION,
        };
        let storage = Arc::new(
            arroyo_storage::StorageProvider::for_url(&format!(
                "file://{}/checkpoint",
                root.path().display()
            ))
            .await
            .unwrap(),
        );
        let metadata = export_typed(
            &raw,
            &namespace(),
            &config,
            &storage,
            &format!(
                "P/J/generations/1/checkpoints/checkpoint-0000001/operator-o/table-{transport}-000"
            ),
            1,
            1,
            0,
            65536,
        )
        .await
        .unwrap();
        let mut config_resources = test_resources().config().clone();
        // The existing restore adapter reserves a full backend page. These
        // fixture budgets are identical for the two restored backends.
        config_resources.scan_page_bytes = 8 * 1024 * 1024;
        config_resources.queued_write_bytes = 8 * 1024 * 1024;
        let resources = WorkerStateResources::new(config_resources).unwrap();
        let target = crate::live::worker::construct_backend(
            if target_rocks {
                crate::live::worker::BackendConstruction::Rocksdb(RocksStateConfig {
                    root: root.path().join("target"),
                    job_id: "str125".into(),
                    operator_id: "restore".into(),
                    subtask: 0,
                    generation: 1,
                    attempt: 1,
                })
            } else {
                crate::live::worker::BackendConstruction::Memory {
                    max_resident_bytes: 1024 * 1024,
                }
            },
            resources.clone(),
        )
        .await
        .unwrap();
        restore_typed(target.as_ref(), &namespace(), &config, &metadata, &storage)
            .await
            .unwrap();
        let restored = TypedTable::new(
            target.clone(),
            namespace(),
            descriptor(),
            limits(),
            resources,
        )
        .unwrap();
        let view = restored.snapshot().await.unwrap();
        assert!(matches!(
            view.scan(Some(old_cursor)).await,
            Err(LiveStateError::InvalidCursor)
        ));
        let mut cursor = None;
        let mut completed = 0;
        loop {
            let page = view.scan(cursor).await.unwrap();
            for actual in &page.rows {
                assert_eq!(
                    actual.batch(),
                    &numbered_row(completed * 2 + 1, "restore", 100)
                );
                completed += 1;
            }
            cursor = page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(completed, 24);
        drop(view);
        drop(restored);
        target.close().await.unwrap();
        drop(raw);
        drop(typed_snapshot);
        drop(history);
        drop(table);
        source.close().await.unwrap();
    }
}
