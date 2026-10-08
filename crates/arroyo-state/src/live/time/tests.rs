use super::*;
use crate::live::{
    Ownership,
    lifecycle::RocksStateConfig,
    resources::{ResourceConfig, WorkerStateResources},
    rocks::RocksLiveState,
};
use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

fn resources() -> WorkerStateResources {
    WorkerStateResources::new(ResourceConfig {
        block_cache_bytes: 8 << 20,
        memtable_bytes: 4 << 20,
        queued_write_bytes: 4 << 20,
        decoded_value_bytes: 4 << 20,
        scan_page_bytes: 4 << 20,
        max_blocking_operations: 2,
        max_snapshots: 8,
        max_open_databases: 4,
        disk_reserve_bytes: 0,
    })
    .unwrap()
}
fn config(root: &std::path::Path) -> RocksStateConfig {
    RocksStateConfig {
        root: root.to_path_buf(),
        job_id: "history-job".into(),
        operator_id: "history-op".into(),
        subtask: 0,
        generation: 7,
        attempt: 1,
    }
}
fn namespace() -> StateNamespace {
    StateNamespace {
        ownership: Ownership::PartitionLocal {
            subtask: 0,
            parallelism: 1,
        },
        table: b"history".to_vec(),
    }
}
fn limits() -> HistoryLimits {
    HistoryLimits {
        chunk_bytes: 16 << 10,
        chunk_rows: 2,
        page_bytes: 128 << 10,
        page_entries: 3,
        batch_bytes: 64 << 10,
    }
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("value", DataType::Utf8, true),
    ]))
}
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-7),
                None,
                Some(2_345_678),
                Some(0),
                Some(i64::MAX),
            ])),
            Arc::new(StringArray::from(vec![
                Some("short"),
                Some(""),
                None,
                Some("variable string: \0 and unicode 🎉"),
                Some("tail"),
            ])),
        ],
    )
    .unwrap()
}
async fn all_chunks(
    snapshot: &HistorySnapshot,
    key: &[u8],
    start: Option<i64>,
    end: Option<i64>,
) -> Vec<HistoryChunk> {
    let mut cursor = None;
    let mut chunks = Vec::new();
    loop {
        let page = snapshot.scan_key(key, start, end, cursor).await.unwrap();
        assert!(page.chunks.len() <= limits().page_entries);
        chunks.extend(page.chunks);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    chunks
}

#[tokio::test]
async fn rocks_hot_key_pages_preserve_arrow_values_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let resources = resources();
    let backend = Arc::new(
        RocksLiveState::open(config.clone(), resources.clone())
            .await
            .unwrap(),
    );
    let history = ArrowHistory::new(backend.clone(), namespace(), schema(), limits()).unwrap();
    let input = batch();
    let hot = b"hot\0key";
    for time in 0..24 {
        assert_eq!(history.append(hot, time, 0, &input).await.unwrap(), 3);
    }
    let stable = history.snapshot().await.unwrap();
    history.append(hot, 24, 0, &input).await.unwrap();
    let chunks = all_chunks(&stable, hot, None, None).await;
    assert_eq!(chunks.len(), 72);
    for (index, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.timestamp, (index / 3) as i64);
        assert_eq!(chunk.sequence, (index % 3) as u64);
        let offset = (index % 3) * 2;
        assert_eq!(
            chunk.batch,
            input.slice(offset, (input.num_rows() - offset).min(2))
        );
    }
    assert!(chunks[0].batch.column(0).is_null(1));
    assert!(chunks[1].batch.column(1).is_null(0));
    drop(stable);
    drop(history);
    Arc::try_unwrap(backend)
        .ok()
        .expect("history released backend")
        .close()
        .await
        .unwrap();
    let reopened = Arc::new(RocksLiveState::reopen(config, resources).await.unwrap());
    let history = ArrowHistory::new(reopened.clone(), namespace(), schema(), limits()).unwrap();
    let snapshot = history.snapshot().await.unwrap();
    let chunks = all_chunks(&snapshot, hot, Some(23), Some(25)).await;
    assert_eq!(chunks.len(), 6);
    assert_eq!(
        chunks.iter().map(|c| c.timestamp).collect::<Vec<_>>(),
        vec![23, 23, 23, 24, 24, 24]
    );
    drop(snapshot);
    drop(history);
    Arc::try_unwrap(reopened)
        .ok()
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn rocks_expiry_keeps_exact_cutoff_and_escaped_keys_are_distinct() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(
        RocksLiveState::open(config(root.path()), resources())
            .await
            .unwrap(),
    );
    let history = ArrowHistory::new(backend.clone(), namespace(), schema(), limits()).unwrap();
    let row = batch().slice(0, 1);
    let keys: &[&[u8]] = &[b"", b"a", b"a\0", b"a\0\0", b"aa"];
    for key in keys {
        for time in [-2, 0, 2, 3] {
            history.append(key, time, 0, &row).await.unwrap();
        }
    }
    let before = history.snapshot().await.unwrap();
    let mut removed = 0;
    loop {
        let count = history.expire_page(2).await.unwrap();
        removed += count;
        if count == 0 {
            break;
        }
    }
    assert_eq!(removed, 10);
    let after = history.snapshot().await.unwrap();
    for key in keys {
        assert_eq!(all_chunks(&before, key, None, None).await.len(), 4);
        let chunks = all_chunks(&after, key, None, None).await;
        assert_eq!(
            chunks.iter().map(|c| c.timestamp).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(
            all_chunks(&after, key, Some(2), Some(3))
                .await
                .iter()
                .all(|c| c.timestamp == 2)
        );
    }
    drop(before);
    drop(after);
    drop(history);
    Arc::try_unwrap(backend)
        .ok()
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn rocks_rejected_arrow_and_batch_limits_leave_both_indexes_empty() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(
        RocksLiveState::open(config(root.path()), resources())
            .await
            .unwrap(),
    );
    let input = batch();
    let mut tiny = limits();
    tiny.batch_bytes = 1;
    let rejected = ArrowHistory::new(backend.clone(), namespace(), schema(), tiny).unwrap();
    assert!(matches!(
        rejected.append(b"key", 1, 0, &input).await,
        Err(LiveStateError::BatchLimitExceeded { .. })
    ));
    let normal = ArrowHistory::new(backend.clone(), namespace(), schema(), limits()).unwrap();
    assert_eq!(normal.expire_page(2).await.unwrap(), 0);
    let snapshot = normal.snapshot().await.unwrap();
    assert!(all_chunks(&snapshot, b"key", None, None).await.is_empty());
    drop(snapshot);
    let mismatch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["data"]))],
    )
    .unwrap();
    assert!(matches!(
        normal.append(b"key", 1, 0, &mismatch).await,
        Err(LiveStateError::InvalidEncoding(_))
    ));
    tiny = limits();
    tiny.chunk_bytes = 64;
    let oversized = ArrowHistory::new(backend.clone(), namespace(), schema(), tiny).unwrap();
    assert!(oversized.append(b"key", 1, 0, &input).await.is_err());
    assert_eq!(normal.expire_page(2).await.unwrap(), 0);
    assert!(
        all_chunks(&normal.snapshot().await.unwrap(), b"key", None, None)
            .await
            .is_empty()
    );
    drop(rejected);
    drop(normal);
    drop(oversized);
    Arc::try_unwrap(backend)
        .ok()
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn history_cursors_bind_snapshot_key_and_time_range_and_replacements_are_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(
        RocksLiveState::open(config(root.path()), resources())
            .await
            .unwrap(),
    );
    let history = ArrowHistory::new(backend.clone(), namespace(), schema(), limits()).unwrap();
    let input = batch();
    for timestamp in [i64::MIN, -1, 0, 1, i64::MAX] {
        history.append(b"key", timestamp, 0, &input).await.unwrap();
        history.append(b"key", timestamp, 0, &input).await.unwrap();
    }
    let snapshot = history.snapshot().await.unwrap();
    let chunks = all_chunks(&snapshot, b"key", None, None).await;
    assert_eq!(chunks.len(), 15);
    assert_eq!(chunks.first().unwrap().timestamp, i64::MIN);
    assert_eq!(chunks.last().unwrap().timestamp, i64::MAX);
    let page = snapshot.scan_key(b"key", None, None, None).await.unwrap();
    let cursor = page.next_cursor.unwrap();
    assert!(matches!(
        snapshot
            .scan_key(b"other", None, None, Some(cursor.clone()))
            .await,
        Err(LiveStateError::InvalidCursor)
    ));
    assert!(matches!(
        snapshot
            .scan_key(b"key", Some(0), None, Some(cursor.clone()))
            .await,
        Err(LiveStateError::InvalidCursor)
    ));
    let other_snapshot = history.snapshot().await.unwrap();
    assert!(matches!(
        other_snapshot
            .scan_key(b"key", None, None, Some(cursor))
            .await,
        Err(LiveStateError::InvalidCursor)
    ));
    assert!(matches!(
        snapshot.scan_key(b"key", Some(1), Some(0), None).await,
        Err(LiveStateError::InvalidCursor)
    ));
    assert!(
        snapshot
            .scan_key(b"key", Some(0), Some(0), None)
            .await
            .unwrap()
            .chunks
            .is_empty()
    );
    drop(other_snapshot);
    drop(snapshot);
    drop(history);
    Arc::try_unwrap(backend)
        .ok()
        .unwrap()
        .close()
        .await
        .unwrap();
}

async fn expiry_respects_delete_batch_cap(backend: Arc<dyn LiveStateBackend>) {
    let mut bounded = limits();
    bounded.page_bytes = 64 << 10;
    bounded.batch_bytes = 64 << 10;
    bounded.page_entries = 4096;
    let history = ArrowHistory::new(backend.clone(), namespace(), schema(), bounded).unwrap();
    let row = batch().slice(0, 1);
    for sequence in 0..600 {
        history.append(b"k", 0, sequence, &row).await.unwrap();
    }
    history.append(b"k", 1, 0, &row).await.unwrap();
    let mut removed = 0;
    let mut pages = 0;
    loop {
        let count = history.expire_page(1).await.unwrap();
        if count == 0 {
            break;
        }
        assert!(count < 600, "delete batches must split the scan page");
        removed += count;
        pages += 1;
        assert!(removed <= 600);
        let snapshot = history.snapshot().await.unwrap();
        let mut cursor = None;
        let mut remaining = 0;
        loop {
            let page = snapshot.scan_key(b"k", None, None, cursor).await.unwrap();
            for chunk in &page.chunks {
                assert_eq!(chunk.batch, row);
                assert!(chunk.timestamp == 0 || chunk.timestamp == 1);
            }
            remaining += page.chunks.len();
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(remaining, 601 - removed);
        // Both indexes must reflect exactly the committed prefix after each page.
        let raw = backend.snapshot().await.unwrap();
        let index = raw
            .scan(ScanRequest {
                range: ScanRange {
                    namespace: index_namespace(&namespace(), 1),
                    prefix: None,
                    start: None,
                    end: None,
                },
                max_entries: bounded.page_entries,
                max_bytes: bounded.page_bytes,
                cursor: None,
            })
            .await
            .unwrap();
        assert!(index.next_cursor.is_none());
        assert_eq!(index.entries.len(), remaining);
    }
    assert_eq!(removed, 600);
    assert!(pages > 1);
    let snapshot = history.snapshot().await.unwrap();
    let page = snapshot.scan_key(b"k", None, None, None).await.unwrap();
    assert_eq!(page.chunks.len(), 1);
    assert_eq!(page.chunks[0].timestamp, 1);
    assert_eq!(page.chunks[0].batch, row);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn memory_expiry_splits_expanded_delete_batches() {
    expiry_respects_delete_batch_cap(Arc::new(crate::live::memory::MemoryLiveState::new())).await;
}

#[tokio::test]
async fn rocks_expiry_splits_expanded_delete_batches() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(
        RocksLiveState::open(config(root.path()), resources())
            .await
            .unwrap(),
    );
    expiry_respects_delete_batch_cap(backend.clone()).await;
    Arc::try_unwrap(backend)
        .ok()
        .unwrap()
        .close_and_remove()
        .await
        .unwrap();
}

#[tokio::test]
async fn oversized_borrowed_history_keys_reject_before_owned_key_assembly() {
    let backend = Arc::new(crate::live::memory::MemoryLiveState::new());
    let pool = resources();
    let history = ArrowHistory::new(backend, namespace(), schema(), limits())
        .unwrap()
        .with_resources(pool.clone());
    // The Arrow source is small and valid; only the borrowed key is oversized.
    let key = vec![0; 1 << 20];
    let row = batch().slice(0, 1);
    let occupied = pool
        .decoded_value(pool.config().decoded_value_bytes)
        .await
        .unwrap();
    {
        // Rejection must precede both admission and internal key assembly.
        let append = history.append(&key, 0, 0, &row);
        tokio::pin!(append);
        assert!(matches!(
            futures::poll!(&mut append),
            std::task::Poll::Ready(Err(LiveStateError::BatchLimitExceeded { .. }))
        ));
    }
    let snapshot = history.snapshot().await.unwrap();
    assert!(matches!(
        snapshot.scan_key(&key, Some(0), Some(1), None).await,
        Err(LiveStateError::ReadLimitExceeded { .. })
    ));
    drop(occupied);
    assert!(
        snapshot
            .scan_key(b"k", None, None, None)
            .await
            .unwrap()
            .chunks
            .is_empty()
    );
}

async fn scan_key_checks_page_bound_before_copies(
    backend: Arc<dyn LiveStateBackend>,
    pool: WorkerStateResources,
) {
    for attached in [false, true] {
        let mut bounded = limits();
        bounded.page_bytes = 512;
        let history = ArrowHistory::new(backend.clone(), namespace(), schema(), bounded).unwrap();
        let history = if attached {
            history.with_resources(pool.clone())
        } else {
            history
        };
        let snapshot = history.snapshot().await.unwrap();
        let oversized_key = vec![0; 2048];
        assert!(matches!(
            snapshot.scan_key(&oversized_key, Some(-1), Some(1), None).await,
            Err(LiveStateError::ReadLimitExceeded { required, limit: 512 }) if required > 512
        ));
        drop(snapshot);

        // A page that fits a real encoded record exactly must still work,
        // including nested zero escaping and half-open signed time bounds.
        let key = b"k\0x";
        let row = batch().slice(0, 1);
        let primary = primary_key(key, -1, 255).unwrap();
        bounded.page_bytes = crate::live::encoding::encoded_key_size(&state_key(
            &index_namespace(&namespace(), 0),
            primary,
        ))
        .unwrap()
            + encode_chunk(&row, bounded.chunk_bytes).unwrap().len()
            + 1;
        let history = ArrowHistory::new(backend.clone(), namespace(), schema(), bounded).unwrap();
        let history = if attached {
            history.with_resources(pool.clone())
        } else {
            history
        };
        history.append(key, -1, 255, &row).await.unwrap();
        let snapshot = history.snapshot().await.unwrap();
        let page = snapshot
            .scan_key(key, Some(-1), Some(0), None)
            .await
            .unwrap();
        assert_eq!(page.chunks.len(), 1);
        assert_eq!(page.chunks[0].timestamp, -1);
        assert_eq!(page.chunks[0].sequence, 255);
        assert_eq!(page.chunks[0].batch, row);
        assert!(page.next_cursor.is_none());
        assert!(
            snapshot
                .scan_key(key, Some(0), Some(1), None)
                .await
                .unwrap()
                .chunks
                .is_empty()
        );
    }
}

#[tokio::test]
async fn memory_history_scan_bounds_keys_without_optional_resources() {
    scan_key_checks_page_bound_before_copies(
        Arc::new(crate::live::memory::MemoryLiveState::new()),
        resources(),
    )
    .await;
}

#[tokio::test]
async fn rocks_history_scan_bounds_keys_without_optional_resources() {
    let root = tempfile::tempdir().unwrap();
    let pool = resources();
    let backend = Arc::new(
        RocksLiveState::open(config(root.path()), pool.clone())
            .await
            .unwrap(),
    );
    scan_key_checks_page_bound_before_copies(backend.clone(), pool).await;
    Arc::try_unwrap(backend)
        .ok()
        .unwrap()
        .close_and_remove()
        .await
        .unwrap();
}

#[test]
fn history_key_measurements_match_nested_encoding_and_check_overflow() {
    let ns = namespace();
    for key in [b"".as_slice(), b"k", b"\0\0", b"a\0b"] {
        for timestamp in [i64::MIN, -1, 0, i64::MAX] {
            for sequence in [0, 255, u64::MAX] {
                let (bytes, zeros) = history_key_size(key, timestamp, sequence).unwrap();
                for logical in [
                    primary_key(key, timestamp, sequence).unwrap(),
                    expiry_key(key, timestamp, sequence).unwrap(),
                ] {
                    assert_eq!(logical.len(), bytes);
                    assert_eq!(logical.iter().filter(|byte| **byte == 0).count(), zeros);
                    assert_eq!(
                        encoded_history_key_size(&ns, bytes, zeros).unwrap(),
                        crate::live::encoding::encode_key(&state_key(&ns, logical))
                            .unwrap()
                            .len()
                    );
                }
            }
        }
    }
    assert!(encoded_history_key_size(&ns, usize::MAX, 0).is_err());
    assert!(encoded_history_key_size(&ns, 0, usize::MAX).is_err());
}

#[test]
fn bounded_ipc_preflight_rejects_truncated_frames_and_excessive_rows() {
    let bytes = encode_chunk(&batch(), limits().chunk_bytes).unwrap();
    validate_ipc(&bytes, 5).unwrap();
    assert!(validate_ipc(&bytes, 4).is_err());
    for length in [0, 1, 4, bytes.len() - 1] {
        assert!(validate_ipc(&bytes[..length], 5).is_err());
    }
    let mut oversized_metadata = bytes.clone();
    oversized_metadata[4..8].copy_from_slice(&i32::MAX.to_le_bytes());
    assert!(validate_ipc(&oversized_metadata, 5).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(validate_ipc(&trailing, 5).is_err());
}

#[test]
fn bounded_ipc_preflight_rejects_body_length_before_decoder_allocation() {
    let mut bytes = encode_chunk(&batch(), limits().chunk_bytes).unwrap();
    let mut offset = 0usize;
    loop {
        let marker = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let length = if marker == u32::MAX {
            let length = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            length
        } else {
            marker
        } as usize;
        assert_ne!(length, 0, "expected a record batch frame");
        let message = arrow::ipc::root_as_message(&bytes[offset..offset + length]).unwrap();
        let body = message.bodyLength() as usize;
        if message.header_as_record_batch().is_some() {
            // Modify the generated flatbuffer's bodyLength slot, preserving all
            // other valid metadata. An unchecked reader would allocate this body.
            let table =
                offset + u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let vtable = (table as isize
                - i32::from_le_bytes(bytes[table..table + 4].try_into().unwrap()) as isize)
                as usize;
            // Message.bodyLength is flatbuffer field 3 (vtable offset 10).
            let field =
                u16::from_le_bytes(bytes[vtable + 10..vtable + 12].try_into().unwrap()) as usize;
            assert_ne!(field, 0);
            bytes[table + field..table + field + 8].copy_from_slice(&i64::MAX.to_le_bytes());
            assert!(validate_ipc(&bytes, 5).is_err());
            break;
        }
        offset += length + body;
    }
}
