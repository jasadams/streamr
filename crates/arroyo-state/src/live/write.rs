//! Producer admission before owned write buffers are assembled. Existing owned
//! `WriteBatch` inputs remain caller allocations while waiting for admission;
//! this optional builder bounds the copies it creates and retains its reservation
//! through native completion. Producers must still bound their source buffers.
use super::{
    LiveStateError, Result, StateKey, WriteBatch, WriteOperation, encoding,
    resources::{ResourcePermit, WorkerStateResources},
};

/// A batch assembled only after worker-wide queued-write admission. Operations
/// accept borrowed inputs and reject size/count overflow before cloning them.
/// Dropping an unfinished batch releases its buffers and its reservation.
pub struct AdmittedWriteBatch {
    batch: WriteBatch,
    permit: ResourcePermit,
    resources: WorkerStateResources,
    max_operations: usize,
    encoded_bytes: usize,
}
impl AdmittedWriteBatch {
    pub fn reservation_bytes(max_encoded_bytes: usize, max_operations: usize) -> Result<usize> {
        if max_encoded_bytes == 0 || max_operations == 0 {
            return Err(LiveStateError::InvalidLimit);
        }
        max_encoded_bytes
            .checked_mul(5)
            .and_then(|bytes| {
                max_operations
                    .checked_mul(std::mem::size_of::<WriteOperation>() + 32)
                    .and_then(|operations| bytes.checked_add(operations))
            })
            .ok_or(LiveStateError::InvalidLimit)
    }
    pub async fn reserve(
        resources: WorkerStateResources,
        max_encoded_bytes: usize,
        max_operations: usize,
    ) -> Result<Self> {
        let bytes = Self::reservation_bytes(max_encoded_bytes, max_operations)?;
        let permit = resources.queued_write(bytes).await?;
        Ok(Self::with_permit(
            resources,
            permit,
            max_encoded_bytes,
            max_operations,
        ))
    }
    /// Fail fast when a producer already holds another pool's admission.
    pub fn try_reserve(
        resources: WorkerStateResources,
        max_encoded_bytes: usize,
        max_operations: usize,
    ) -> Result<Self> {
        let bytes = Self::reservation_bytes(max_encoded_bytes, max_operations)?;
        let permit = resources.try_queued_write(bytes)?;
        Ok(Self::with_permit(
            resources,
            permit,
            max_encoded_bytes,
            max_operations,
        ))
    }
    fn with_permit(
        resources: WorkerStateResources,
        permit: ResourcePermit,
        max_encoded_bytes: usize,
        max_operations: usize,
    ) -> Self {
        Self {
            batch: WriteBatch {
                operations: Vec::with_capacity(max_operations),
                max_bytes: max_encoded_bytes,
            },
            permit,
            resources,
            max_operations,
            encoded_bytes: 0,
        }
    }

    pub fn put(&mut self, key: &StateKey, value: &[u8]) -> Result<()> {
        let bytes = encoding::encoded_key_size(key)?
            .checked_add(encoding::encoded_value_size(value)?)
            .ok_or(LiveStateError::InvalidLimit)?;
        self.check_operation(bytes)?;
        self.batch.operations.push(WriteOperation::Put {
            key: key.clone(),
            value: value.to_vec(),
        });
        self.encoded_bytes += bytes;
        Ok(())
    }

    pub fn delete(&mut self, key: &StateKey) -> Result<()> {
        let bytes = encoding::encoded_key_size(key)?;
        self.check_operation(bytes)?;
        self.batch
            .operations
            .push(WriteOperation::Delete { key: key.clone() });
        self.encoded_bytes += bytes;
        Ok(())
    }

    fn check_operation(&self, bytes: usize) -> Result<()> {
        if self.batch.operations.len() == self.max_operations {
            return Err(LiveStateError::BatchLimitExceeded {
                required: self.max_operations.saturating_add(1),
                limit: self.max_operations,
            });
        }
        let required = self.encoded_bytes.saturating_add(bytes);
        if required > self.batch.max_bytes {
            return Err(LiveStateError::BatchLimitExceeded {
                required,
                limit: self.batch.max_bytes,
            });
        }
        Ok(())
    }

    /// The backend must verify resource-pool identity and retain this permit in
    /// its native write closure; reacquiring queued admission would deadlock.
    pub fn into_parts(self) -> (WriteBatch, ResourcePermit, WorkerStateResources) {
        (self.batch, self.permit, self.resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{Ownership, StateNamespace, resources::ResourceConfig};

    fn resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 512 * 1024,
            queued_write_bytes: 1400,
            decoded_value_bytes: 16,
            scan_page_bytes: 16,
            max_blocking_operations: 1,
            max_snapshots: 1,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn producer_admission_precedes_assembly_and_releases_on_cancellation() {
        let resources = resources();
        let first = AdmittedWriteBatch::reserve(resources.clone(), 200, 1)
            .await
            .unwrap();
        {
            let waiting = AdmittedWriteBatch::reserve(resources.clone(), 200, 1);
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
        }
        drop(first);
        assert!(AdmittedWriteBatch::reserve(resources, 200, 1).await.is_ok());
    }

    #[tokio::test]
    async fn oversized_operations_do_not_consume_builder_capacity() {
        let key = StateKey {
            namespace: StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: b"table".to_vec(),
            },
            key: b"key".to_vec(),
            routing_hash: None,
        };
        let mut builder = AdmittedWriteBatch::reserve(resources(), 100, 1)
            .await
            .unwrap();
        assert!(builder.put(&key, &[0; 101]).is_err());
        builder.delete(&key).unwrap();
        assert!(builder.delete(&key).is_err());
        assert_eq!(builder.batch.operations.len(), 1);
    }
}
