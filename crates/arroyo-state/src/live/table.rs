//! Operator-scoped logical handles. Legacy checkpoint tables remain separate;
//! callers must checkpoint these handles through the live-state snapshot API.
use super::{
    LiveStateBackend, LiveStateError, Ownership, ReadOptions, Result, StateKey, StateNamespace,
    StateSnapshot, WriteBatch,
};
use std::{collections::HashMap, sync::Arc};

/// One backend per operator attempt; all its logical tables share its snapshot
/// boundary. The worker must share resources when constructing native backends.
pub struct LiveTableManager {
    backend: Arc<dyn LiveStateBackend>,
    ownership: Ownership,
    tables: HashMap<String, LiveTable>,
    typed_owner: Arc<tokio::sync::Mutex<()>>,
    typed_descriptors: HashMap<String, super::typed_table::TableDescriptor>,
}
impl LiveTableManager {
    pub fn new(backend: Arc<dyn LiveStateBackend>, ownership: Ownership) -> Result<Self> {
        // Validate ownership eagerly, including unchanged parallelism semantics.
        super::encoding::encode_namespace(&StateNamespace {
            ownership: ownership.clone(),
            table: vec![],
        })?;
        Ok(Self {
            backend,
            ownership,
            tables: HashMap::new(),
            typed_owner: Arc::new(tokio::sync::Mutex::new(())),
            typed_descriptors: HashMap::new(),
        })
    }
    pub fn register(&mut self, name: impl Into<String>) -> Result<LiveTable> {
        let name = name.into();
        if name.is_empty() || self.tables.contains_key(&name) {
            return Err(LiveStateError::InvalidEncoding(
                "empty or duplicate live table name".into(),
            ));
        }
        let table = LiveTable {
            backend: self.backend.clone(),
            namespace: StateNamespace {
                ownership: self.ownership.clone(),
                table: name.as_bytes().to_vec(),
            },
        };
        self.tables.insert(name, table.clone());
        Ok(table)
    }
    /// All registered typed tables share one serialized input-event owner and
    /// backend snapshot boundary. Durable namespace metadata remains available
    /// to checkpoint integration; registration never writes hidden namespaces.
    pub fn register_typed(
        &mut self,
        descriptor: super::typed_table::TableDescriptor,
        limits: super::typed_table::TableLimits,
        resources: super::resources::WorkerStateResources,
    ) -> Result<super::typed_table::TypedTable> {
        let name = String::from_utf8(descriptor.table_identity.clone())
            .map_err(|_| LiveStateError::InvalidEncoding("table identity must be UTF-8".into()))?;
        if self.tables.contains_key(&name) {
            return Err(LiveStateError::InvalidEncoding(
                "duplicate live table name".into(),
            ));
        }
        let namespace = StateNamespace {
            ownership: self.ownership.clone(),
            table: descriptor.table_identity.clone(),
        };
        let table = super::typed_table::TypedTable::new(
            self.backend.clone(),
            namespace,
            descriptor.clone(),
            limits,
            resources,
        )?
        .with_owner(self.typed_owner.clone());
        self.register(name.clone())?;
        self.typed_descriptors.insert(name, descriptor);
        Ok(table)
    }
    pub fn namespaces(&self) -> impl Iterator<Item = &StateNamespace> {
        self.tables.values().map(|table| &table.namespace)
    }
    pub fn typed_descriptors(&self) -> impl Iterator<Item = &super::typed_table::TableDescriptor> {
        self.typed_descriptors.values()
    }
    pub fn table(&self, name: &str) -> Result<LiveTable> {
        self.tables.get(name).cloned().ok_or_else(|| {
            LiveStateError::InvalidEncoding(format!("unregistered live table {name}"))
        })
    }
    pub async fn snapshot(&self) -> Result<StateSnapshot> {
        let _owner = self.typed_owner.lock().await;
        self.backend.snapshot().await
    }
}
#[derive(Clone)]
pub struct LiveTable {
    backend: Arc<dyn LiveStateBackend>,
    namespace: StateNamespace,
}
impl LiveTable {
    pub fn ranked_counts(
        &self,
        limits: super::collections::CollectionLimits,
    ) -> Result<super::collections::RankedCounts> {
        super::collections::RankedCounts::new(self.backend.clone(), self.namespace.clone(), limits)
    }

    /// Construct the single serial timer owner for this registered table. Both
    /// timer indexes remain inside its checkpointed namespace.
    pub fn timers(
        &self,
        limits: super::timers::TimerLimits,
    ) -> Result<super::timers::DurableTimers> {
        super::timers::DurableTimers::new(self.backend.clone(), self.namespace.clone(), limits)
    }

    pub fn history(
        &self,
        schema: arrow_schema::SchemaRef,
        limits: super::time::HistoryLimits,
    ) -> Result<super::time::ArrowHistory> {
        super::time::ArrowHistory::new(self.backend.clone(), self.namespace.clone(), schema, limits)
    }

    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }
    pub fn key(&self, key: Vec<u8>, routing_hash: Option<u64>) -> StateKey {
        StateKey {
            namespace: self.namespace.clone(),
            key,
            routing_hash,
        }
    }
    pub async fn get(
        &self,
        key: Vec<u8>,
        routing_hash: Option<u64>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.backend
            .get(&self.key(key, routing_hash), options)
            .await
    }
    pub async fn put(
        &self,
        key: Vec<u8>,
        routing_hash: Option<u64>,
        value: Vec<u8>,
        max_bytes: usize,
    ) -> Result<()> {
        self.backend
            .put(self.key(key, routing_hash), value, max_bytes)
            .await
    }
    pub async fn delete(
        &self,
        key: Vec<u8>,
        routing_hash: Option<u64>,
        max_bytes: usize,
    ) -> Result<()> {
        self.backend
            .delete(self.key(key, routing_hash), max_bytes)
            .await
    }
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for operation in &batch.operations {
            let key = match operation {
                super::WriteOperation::Put { key, .. } | super::WriteOperation::Delete { key } => {
                    key
                }
            };
            if key.namespace != self.namespace {
                return Err(LiveStateError::InvalidEncoding(
                    "batch crosses a logical table handle".into(),
                ));
            }
        }
        self.backend.write_batch(batch).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{WriteOperation, memory::MemoryLiveState};
    #[tokio::test]
    async fn handles_validate_namespaces_and_share_a_snapshot_boundary() {
        let mut manager = LiveTableManager::new(
            Arc::new(MemoryLiveState::new()),
            Ownership::PartitionLocal {
                subtask: 1,
                parallelism: 2,
            },
        )
        .unwrap();
        let left = manager.register("left").unwrap();
        let right = manager.register("right").unwrap();
        left.put(vec![0], None, vec![1], 1024).await.unwrap();
        right.put(vec![0], None, vec![2], 1024).await.unwrap();
        let stable = manager.snapshot().await.unwrap();
        assert!(
            left.write_batch(WriteBatch {
                operations: vec![WriteOperation::Delete {
                    key: right.key(vec![0], None)
                }],
                max_bytes: 1024
            })
            .await
            .is_err()
        );
        left.delete(vec![0], None, 1024).await.unwrap();
        assert_eq!(
            stable
                .get(&left.key(vec![0], None), ReadOptions { max_bytes: 1 })
                .await
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            right
                .get(vec![0], None, ReadOptions { max_bytes: 1 })
                .await
                .unwrap(),
            Some(vec![2])
        );
        assert!(manager.register("right").is_err());
        assert!(manager.table("unknown").is_err());
    }
}
