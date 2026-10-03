use super::*;

fn key(bytes: &[u8]) -> StateKey {
    StateKey {
        namespace: StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"test".to_vec(),
        },
        key: bytes.to_vec(),
        routing_hash: None,
    }
}
fn options() -> ReadOptions {
    ReadOptions { max_bytes: 1024 }
}
fn request(namespace: StateNamespace) -> ScanRequest {
    ScanRequest {
        range: ScanRange {
            namespace,
            prefix: None,
            start: None,
            end: None,
        },
        max_entries: 1,
        max_bytes: 1024,
        cursor: None,
    }
}

/// Shared behavioral contract; persistent backends can invoke this with a fresh
/// backend before running their own reopen/lifecycle tests.
pub(crate) async fn backend_contract(backend: &dyn LiveStateBackend) {
    let a = key(b"a");
    let b = key(b"b");
    backend
        .write_batch(WriteBatch {
            operations: vec![
                WriteOperation::Put {
                    key: a.clone(),
                    value: b"first".to_vec(),
                },
                WriteOperation::Delete { key: a.clone() },
                WriteOperation::Put {
                    key: a.clone(),
                    value: b"last".to_vec(),
                },
                WriteOperation::Put {
                    key: b.clone(),
                    value: b"bee".to_vec(),
                },
            ],
            max_bytes: 1024,
        })
        .await
        .unwrap();
    assert_eq!(
        backend.get(&a, options()).await.unwrap(),
        Some(b"last".to_vec())
    );
    assert_eq!(
        backend
            .multi_get(
                &[b.clone(), key(b"missing"), a.clone(), a.clone()],
                options()
            )
            .await
            .unwrap(),
        vec![
            Some(b"bee".to_vec()),
            None,
            Some(b"last".to_vec()),
            Some(b"last".to_vec())
        ]
    );
    assert!(matches!(
        backend
            .multi_get(&[a.clone(), a.clone()], ReadOptions { max_bytes: 7 })
            .await,
        Err(LiveStateError::ReadLimitExceeded { .. })
    ));
    let stable = backend.snapshot().await.unwrap();
    let first = stable.scan(request(a.namespace.clone())).await.unwrap();
    assert_eq!(first.entries[0].key, a);
    let cursor = first.next_cursor.unwrap();
    let mut next = request(a.namespace.clone());
    next.cursor = Some(cursor.clone());
    assert_eq!(stable.scan(next.clone()).await.unwrap().entries[0].key, b);
    backend
        .put(a.clone(), b"changed".to_vec(), 1024)
        .await
        .unwrap();
    assert_eq!(
        stable.get(&a, options()).await.unwrap(),
        Some(b"last".to_vec())
    );
    assert!(matches!(
        backend.snapshot().await.unwrap().scan(next.clone()).await,
        Err(LiveStateError::InvalidCursor)
    ));
    next.range.start = Some(b"a".to_vec());
    assert!(matches!(
        stable.scan(next).await,
        Err(LiveStateError::InvalidCursor)
    ));
    let mut tiny = request(a.namespace.clone());
    tiny.max_bytes = 1;
    assert!(matches!(
        stable.scan(tiny).await,
        Err(LiveStateError::ReadLimitExceeded { .. })
    ));
    let failed = backend
        .write_batch(WriteBatch {
            operations: vec![
                WriteOperation::Delete { key: a.clone() },
                WriteOperation::Put {
                    key: b.clone(),
                    value: vec![0; 2048],
                },
            ],
            max_bytes: 32,
        })
        .await;
    assert!(matches!(
        failed,
        Err(LiveStateError::BatchLimitExceeded { .. })
    ));
    assert_eq!(
        backend.get(&a, options()).await.unwrap(),
        Some(b"changed".to_vec())
    );
    assert_eq!(
        backend.get(&b, options()).await.unwrap(),
        Some(b"bee".to_vec())
    );
    let mut filtered = request(a.namespace.clone());
    filtered.range.prefix = Some(b"b".to_vec());
    let page = stable.scan(filtered).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, b);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn memory_conforms() {
    backend_contract(&memory::MemoryLiveState::new()).await;
}

#[test]
fn encoding_preserves_ownership_and_full_keys() {
    let owners = vec![
        Ownership::PartitionLocal {
            subtask: 2,
            parallelism: 4,
        },
        Ownership::Routed {
            range_start: 10,
            range_end: 100,
        },
        Ownership::Replicated {
            id: b"broadcast".to_vec(),
        },
        Ownership::Connector {
            connector: b"kafka".to_vec(),
            partition: b"topic/2".to_vec(),
        },
    ];
    for ownership in owners {
        let routing_hash = if matches!(ownership, Ownership::Routed { .. }) {
            Some(77)
        } else {
            None
        };
        let key = StateKey {
            namespace: StateNamespace {
                ownership,
                table: b"table\0name".to_vec(),
            },
            key: b"full\0key".to_vec(),
            routing_hash,
        };
        assert_eq!(
            encoding::decode_key(&encoding::encode_key(&key).unwrap()).unwrap(),
            key
        );
    }
    let keys = [b"".as_slice(), b"\0", b"a", b"a\0", b"aa", b"b"];
    let encoded = keys
        .iter()
        .map(|k| encoding::encode_key(&key(k)).unwrap())
        .collect::<Vec<_>>();
    assert!(encoded.windows(2).all(|v| v[0] < v[1]));
    assert!(encoding::decode_key(&[2]).is_err());
    assert!(encoding::decode_value(&[2]).is_err());
}

#[tokio::test]
async fn namespace_isolation_and_atomic_encoding_failure() {
    let backend = memory::MemoryLiveState::new();
    let a = key(b"a");
    backend.put(a.clone(), vec![1], 1024).await.unwrap();
    let mut other = a.clone();
    other.namespace.table = b"other".to_vec();
    assert!(backend.get(&other, options()).await.unwrap().is_none());
    other.namespace.ownership = Ownership::PartitionLocal {
        subtask: 1,
        parallelism: 1,
    };
    assert!(
        backend
            .write_batch(WriteBatch {
                operations: vec![
                    WriteOperation::Delete { key: a.clone() },
                    WriteOperation::Put {
                        key: other,
                        value: vec![2]
                    }
                ],
                max_bytes: 1024
            })
            .await
            .is_err()
    );
    assert_eq!(backend.get(&a, options()).await.unwrap(), Some(vec![1]));
}
