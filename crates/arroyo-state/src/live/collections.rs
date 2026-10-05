//! Exact per-entity counters and a count-descending/member-ascending rank index.
//! Both indexes stay in one registered checkpoint namespace. No operation loads
//! the complete collection. Equal-count byte ordering is an explicit policy,
//! not a claim of parity with Java HashMap iteration order.
use super::{
    LiveStateBackend, LiveStateError, ReadOptions, Result, ScanCursor, ScanRange, ScanRequest,
    StateKey, StateNamespace, StateSnapshot, WriteBatch, WriteOperation,
    resources::{ResourcePermit, WorkerStateResources},
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug)]
pub struct CollectionLimits {
    pub max_entity_bytes: usize,
    pub max_member_bytes: usize,
    pub max_top_k: usize,
    pub page_entries: usize,
    pub page_bytes: usize,
    pub batch_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountedMember {
    /// UTF-8 callers may validate/interpret these bytes without changing ranking.
    pub member: Vec<u8>,
    pub count: u64,
}

pub struct CollectionPage {
    entries: Vec<CountedMember>,
    pub next_cursor: Option<ScanCursor>,
    _reservation: Option<ResourcePermit>,
}

pub struct TopMembers {
    entries: Vec<CountedMember>,
    _reservation: Option<ResourcePermit>,
}
impl CollectionPage {
    /// Borrow entries while the page retains its admission reservation.
    pub fn entries(&self) -> &[CountedMember] {
        &self.entries
    }
}
impl TopMembers {
    /// Borrow exact ranked output while its admission reservation remains live.
    pub fn entries(&self) -> &[CountedMember] {
        &self.entries
    }
}

/// Keep this object alive until the combined batch is committed. Preparation
/// reads current state; exactly one serial execution owner must exclude other
/// mutations from that read through commit. Do not prepare two updates of the
/// same member against unchanged state and combine them. There is no CAS here.
pub struct PreparedCountUpdate {
    operations: Vec<WriteOperation>,
    pub count: u64,
    _reservation: Option<ResourcePermit>,
}
impl PreparedCountUpdate {
    /// Moves owned operations without copying their buffers. Validation occurs
    /// before altering the destination. Retain self until backend commit so the
    /// reservation continues accounting for moved buffers.
    pub fn append_to(&mut self, batch: &mut WriteBatch) -> Result<()> {
        let additional = operation_size(&self.operations, batch.max_bytes)?;
        let total = batch.encoded_size()?.checked_add(additional).ok_or(
            LiveStateError::BatchLimitExceeded {
                required: usize::MAX,
                limit: batch.max_bytes,
            },
        )?;
        if total > batch.max_bytes {
            return Err(LiveStateError::BatchLimitExceeded {
                required: total,
                limit: batch.max_bytes,
            });
        }
        batch.operations.append(&mut self.operations);
        Ok(())
    }
}

/// Share one view with Arc. The direct wrappers serialize only this view; they
/// cannot protect independently constructed writers or prepared transactions.
/// RC01-prefixed logical keys are reserved for this view; related metadata and
/// timers must not write those keys. Entity bytes must include the caller's
/// tenant and incarnation when needed.
/// clear_page is incremental deletion, not an atomic logical reset: retire the
/// incarnation in application metadata before clearing its collection pages.
pub struct RankedCounts {
    backend: Arc<dyn LiveStateBackend>,
    namespace: StateNamespace,
    limits: CollectionLimits,
    resources: Option<WorkerStateResources>,
    mutation: Mutex<()>,
}

pub struct CollectionSnapshot {
    snapshot: StateSnapshot,
    namespace: StateNamespace,
    limits: CollectionLimits,
    resources: Option<WorkerStateResources>,
}

impl RankedCounts {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        namespace: StateNamespace,
        limits: CollectionLimits,
    ) -> Result<Self> {
        if [
            limits.max_entity_bytes,
            limits.max_member_bytes,
            limits.max_top_k,
            limits.page_entries,
            limits.page_bytes,
            limits.batch_bytes,
        ]
        .contains(&0)
            || limits.max_entity_bytes > u32::MAX as usize
        {
            return Err(LiveStateError::InvalidLimit);
        }
        super::encoding::encode_namespace(&namespace)?;
        if matches!(namespace.ownership, super::Ownership::Routed { .. }) {
            return Err(invalid(
                "routed collections require an operator routing adapter",
            ));
        }
        // Validate worst-case admission arithmetic before any caller allocation.
        page_reservation(limits, super::encoding::encoded_namespace_size(&namespace)?)?;
        prepare_reservation(limits)?;
        clear_reservation(limits, super::encoding::encoded_namespace_size(&namespace)?)?;
        top_reservation(limits, limits.max_top_k)?;
        Ok(Self {
            backend,
            namespace,
            limits,
            resources: None,
            mutation: Mutex::new(()),
        })
    }

    /// Reserve assembly/result buffers independently of native write/scan pools.
    /// Leave decoded-read headroom for composing metadata/timer mutations. Higher
    /// level owners must additionally account for their combined transaction.
    pub fn with_resources(mut self, resources: WorkerStateResources) -> Result<Self> {
        let namespace_bytes = super::encoding::encoded_namespace_size(&self.namespace)?;
        let page = page_reservation(self.limits, namespace_bytes)?;
        let top = top_reservation(self.limits, self.limits.max_top_k)?;
        let read = native_read_reservation(self.limits, namespace_bytes)?;
        let simultaneous = page
            .checked_add(top)
            .and_then(|n| {
                n.checked_add(
                    prepare_reservation(self.limits)
                        .ok()?
                        .max(clear_reservation(self.limits, namespace_bytes).ok()?),
                )
            })
            .and_then(|n| n.checked_add(read))
            .ok_or(LiveStateError::InvalidLimit)?;
        if simultaneous > resources.config().decoded_value_bytes {
            return Err(invalid(
                "collection page/top-k/prepared buffers and native read headroom exceed decoded-value budget",
            ));
        }
        if native_scan_reservation(self.limits, namespace_bytes)?
            > resources.config().scan_page_bytes
        {
            return Err(invalid("collection native scan exceeds scan-page budget"));
        }
        self.resources = Some(resources);
        Ok(self)
    }

    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    pub async fn count(&self, entity: &[u8], member: &[u8]) -> Result<u64> {
        validate_input(self.limits, entity, Some(member))?;
        let value = self
            .backend
            .get(
                &state_key(&self.namespace, member_key(entity, member)),
                ReadOptions { max_bytes: 8 },
            )
            .await?;
        decode_count(value.as_deref())
    }

    pub async fn prepare_set(
        &self,
        entity: &[u8],
        member: &[u8],
        count: u64,
    ) -> Result<PreparedCountUpdate> {
        let previous = self.count(entity, member).await?;
        self.prepare_known(entity, member, previous, count).await
    }

    pub async fn prepare_add(
        &self,
        entity: &[u8],
        member: &[u8],
        delta: i64,
    ) -> Result<PreparedCountUpdate> {
        let previous = self.count(entity, member).await?;
        let count = previous
            .checked_add_signed(delta)
            .ok_or_else(|| invalid("collection counter overflow or underflow"))?;
        self.prepare_known(entity, member, previous, count).await
    }

    async fn prepare_known(
        &self,
        entity: &[u8],
        member: &[u8],
        previous: u64,
        count: u64,
    ) -> Result<PreparedCountUpdate> {
        if previous != count {
            // Reject before assembling copied keys. The conservative bound
            // includes worst-case escaping for all three update operations.
            let namespace_bytes = super::encoding::encoded_namespace_size(&self.namespace)?;
            let required = entity
                .len()
                .checked_add(member.len())
                .and_then(|n| n.checked_add(17))
                .and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_add(namespace_bytes + 11))
                .and_then(|n| n.checked_mul(3))
                .ok_or(LiveStateError::InvalidLimit)?;
            if required > self.limits.batch_bytes {
                return Err(LiveStateError::BatchLimitExceeded {
                    required,
                    limit: self.limits.batch_bytes,
                });
            }
        }
        let reservation = if let Some(resources) = &self.resources {
            Some(
                resources
                    .decoded_value(prepare_reservation(self.limits)?)
                    .await?,
            )
        } else {
            None
        };
        let mut operations = Vec::with_capacity(3);
        if previous != count {
            if previous != 0 {
                operations.push(WriteOperation::Delete {
                    key: state_key(&self.namespace, rank_key(entity, member, previous)),
                });
            }
            if count == 0 {
                if previous != 0 {
                    operations.push(WriteOperation::Delete {
                        key: state_key(&self.namespace, member_key(entity, member)),
                    });
                }
            } else {
                operations.push(WriteOperation::Put {
                    key: state_key(&self.namespace, member_key(entity, member)),
                    value: count.to_be_bytes().to_vec(),
                });
                operations.push(WriteOperation::Put {
                    key: state_key(&self.namespace, rank_key(entity, member, count)),
                    value: count.to_be_bytes().to_vec(),
                });
            }
        }
        operation_size(&operations, self.limits.batch_bytes)?;
        Ok(PreparedCountUpdate {
            operations,
            count,
            _reservation: reservation,
        })
    }

    pub async fn set(&self, entity: &[u8], member: &[u8], count: u64) -> Result<()> {
        let _guard = self.mutation.lock().await;
        self.commit(self.prepare_set(entity, member, count).await?)
            .await
    }

    pub async fn add(&self, entity: &[u8], member: &[u8], delta: i64) -> Result<u64> {
        let _guard = self.mutation.lock().await;
        let update = self.prepare_add(entity, member, delta).await?;
        let count = update.count;
        self.commit(update).await?;
        Ok(count)
    }

    async fn commit(&self, mut update: PreparedCountUpdate) -> Result<()> {
        let batch = WriteBatch {
            operations: std::mem::take(&mut update.operations),
            max_bytes: self.limits.batch_bytes,
        };
        self.backend.write_batch(batch).await
    }

    pub async fn snapshot(&self) -> Result<CollectionSnapshot> {
        let _guard = self.mutation.lock().await;
        Ok(CollectionSnapshot {
            snapshot: self.backend.snapshot().await?,
            namespace: self.namespace.clone(),
            limits: self.limits,
            resources: self.resources.clone(),
        })
    }

    /// Delete a bounded prefix of one scan page of a retired entity/incarnation.
    /// A smaller write budget still makes progress. Fresh snapshot each call;
    /// repeat until zero. Both indexes disappear in one atomic batch. Never apply
    /// this to an incarnation still accepting input.
    pub async fn clear_page(&self, entity: &[u8]) -> Result<usize> {
        let _guard = self.mutation.lock().await;
        let snapshot = CollectionSnapshot {
            snapshot: self.backend.snapshot().await?,
            namespace: self.namespace.clone(),
            limits: self.limits,
            resources: self.resources.clone(),
        };
        let page = snapshot.members(entity, None).await?;
        let namespace_bytes = super::encoding::encoded_namespace_size(&self.namespace)?;
        let _assembly = if let Some(resources) = &self.resources {
            Some(
                resources
                    .decoded_value(clear_reservation(self.limits, namespace_bytes)?)
                    .await?,
            )
        } else {
            None
        };
        // Bound container capacity by admitted encoded delete cost rather than
        // the potentially much larger scan page. No Vec growth is needed.
        let pair_capacity = clear_pair_capacity(self.limits, namespace_bytes)?;
        let mut operations = Vec::with_capacity(page.entries.len().min(pair_capacity) * 2);
        let mut bytes = 0usize;
        let mut removed = 0usize;
        for entry in &page.entries {
            let required = delete_pair_size(namespace_bytes, entity, &entry.member, entry.count)?;
            let next = bytes
                .checked_add(required)
                .ok_or(LiveStateError::InvalidLimit)?;
            if next > self.limits.batch_bytes {
                if removed == 0 {
                    return Err(LiveStateError::BatchLimitExceeded {
                        required,
                        limit: self.limits.batch_bytes,
                    });
                }
                break;
            }
            operations.push(WriteOperation::Delete {
                key: state_key(&self.namespace, member_key(entity, &entry.member)),
            });
            operations.push(WriteOperation::Delete {
                key: state_key(
                    &self.namespace,
                    rank_key(entity, &entry.member, entry.count),
                ),
            });
            bytes = next;
            removed += 1;
        }
        operation_size(&operations, self.limits.batch_bytes)?;
        self.backend
            .write_batch(WriteBatch {
                operations,
                max_bytes: self.limits.batch_bytes,
            })
            .await?;
        Ok(removed)
    }
}

impl CollectionSnapshot {
    pub async fn count(&self, entity: &[u8], member: &[u8]) -> Result<u64> {
        validate_input(self.limits, entity, Some(member))?;
        let value = self
            .snapshot
            .get(
                &state_key(&self.namespace, member_key(entity, member)),
                ReadOptions { max_bytes: 8 },
            )
            .await?;
        decode_count(value.as_deref())
    }

    pub async fn members(
        &self,
        entity: &[u8],
        cursor: Option<ScanCursor>,
    ) -> Result<CollectionPage> {
        self.page(entity, false, cursor).await
    }

    pub async fn ranked(
        &self,
        entity: &[u8],
        cursor: Option<ScanCursor>,
    ) -> Result<CollectionPage> {
        self.page(entity, true, cursor).await
    }

    async fn page(
        &self,
        entity: &[u8],
        ranked: bool,
        cursor: Option<ScanCursor>,
    ) -> Result<CollectionPage> {
        validate_input(self.limits, entity, None)?;
        let reservation = if let Some(resources) = &self.resources {
            Some(
                resources
                    .decoded_value(page_reservation(
                        self.limits,
                        super::encoding::encoded_namespace_size(&self.namespace)?,
                    )?)
                    .await?,
            )
        } else {
            None
        };
        let prefix = entity_prefix(entity, ranked);
        let page = self
            .snapshot
            .scan(ScanRequest {
                range: ScanRange {
                    namespace: self.namespace.clone(),
                    prefix: Some(prefix.clone()),
                    start: None,
                    end: None,
                },
                max_entries: self.limits.page_entries,
                max_bytes: self.limits.page_bytes,
                cursor,
            })
            .await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let count = decode_count(Some(&entry.value))?;
            let suffix = entry
                .key
                .key
                .strip_prefix(prefix.as_slice())
                .ok_or_else(|| invalid("collection entry outside entity prefix"))?;
            let member = if ranked {
                let bytes: [u8; 8] = suffix
                    .get(..8)
                    .ok_or_else(|| invalid("invalid rank key"))?
                    .try_into()
                    .map_err(|_| invalid("invalid rank key"))?;
                if u64::MAX - u64::from_be_bytes(bytes) != count {
                    return Err(invalid("collection rank/count disagreement"));
                }
                &suffix[8..]
            } else {
                suffix
            };
            if member.len() > self.limits.max_member_bytes {
                return Err(invalid("stored member exceeds max_member_bytes"));
            }
            entries.push(CountedMember {
                member: member.to_vec(),
                count,
            });
        }
        Ok(CollectionPage {
            entries,
            next_cursor: page.next_cursor,
            _reservation: reservation,
        })
    }

    /// Exact first k entries, bounded by configured k/member/page budgets. Count
    /// descending, byte key ascending. Pages are consumed individually; retained
    /// top-k output keeps its own reservation until dropped.
    pub async fn top_k(&self, entity: &[u8], k: usize) -> Result<TopMembers> {
        validate_input(self.limits, entity, None)?;
        if k > self.limits.max_top_k {
            return Err(invalid("top-k exceeds max_top_k"));
        }
        let reservation = if let Some(resources) = &self.resources {
            Some(
                resources
                    .decoded_value(top_reservation(self.limits, k)?)
                    .await?,
            )
        } else {
            None
        };
        let mut entries = Vec::with_capacity(k);
        let mut cursor = None;
        while entries.len() < k {
            let page = self.ranked(entity, cursor).await?;
            let remaining = k - entries.len();
            entries.extend(page.entries.into_iter().take(remaining));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(TopMembers {
            entries,
            _reservation: reservation,
        })
    }
}

fn entity_prefix(entity: &[u8], ranked: bool) -> Vec<u8> {
    let mut key = b"RC01".to_vec();
    key.push(u8::from(ranked));
    key.extend_from_slice(&(entity.len() as u32).to_be_bytes());
    key.extend_from_slice(entity);
    key
}
fn member_key(entity: &[u8], member: &[u8]) -> Vec<u8> {
    let mut key = entity_prefix(entity, false);
    key.extend_from_slice(member);
    key
}
fn rank_key(entity: &[u8], member: &[u8], count: u64) -> Vec<u8> {
    let mut key = entity_prefix(entity, true);
    key.extend_from_slice(&(u64::MAX - count).to_be_bytes());
    key.extend_from_slice(member);
    key
}
fn state_key(namespace: &StateNamespace, key: Vec<u8>) -> StateKey {
    StateKey {
        namespace: namespace.clone(),
        key,
        routing_hash: None,
    }
}
fn validate_input(limits: CollectionLimits, entity: &[u8], member: Option<&[u8]>) -> Result<()> {
    if entity.is_empty() || entity.len() > limits.max_entity_bytes {
        return Err(invalid(
            "collection entity is empty or exceeds max_entity_bytes",
        ));
    }
    if member.is_some_and(|m| m.len() > limits.max_member_bytes) {
        return Err(invalid("collection member exceeds max_member_bytes"));
    }
    Ok(())
}
fn decode_count(value: Option<&[u8]>) -> Result<u64> {
    let Some(value) = value else { return Ok(0) };
    let count = u64::from_be_bytes(
        value
            .try_into()
            .map_err(|_| invalid("invalid collection counter"))?,
    );
    if count == 0 {
        return Err(invalid("zero collection counter must be absent"));
    }
    Ok(count)
}
fn operation_size(operations: &[WriteOperation], limit: usize) -> Result<usize> {
    let mut size = 0usize;
    for operation in operations {
        let (key, value) = match operation {
            WriteOperation::Put { key, value } => {
                (key, super::encoding::encoded_value_size(value)?)
            }
            WriteOperation::Delete { key } => (key, 0),
        };
        size = size
            .checked_add(super::encoding::encoded_key_size(key)?)
            .and_then(|s| s.checked_add(value))
            .ok_or(LiveStateError::InvalidLimit)?;
    }
    if size > limit {
        return Err(LiveStateError::BatchLimitExceeded {
            required: size,
            limit,
        });
    }
    Ok(size)
}
// These bounds mirror Rocks read/scan admission, including worst-case NUL
// escaping and the encoded cursor. They allocate no oversized trial keys.
fn maximum_encoded_key(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    limits
        .max_entity_bytes
        .checked_add(limits.max_member_bytes)
        .and_then(|n| n.checked_add(17))
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(namespace_bytes))
        .and_then(|n| n.checked_add(2))
        .ok_or(LiveStateError::InvalidLimit)
}
fn native_read_reservation(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    maximum_encoded_key(limits, namespace_bytes)?
        .checked_mul(2)
        .and_then(|n| {
            n.checked_add(
                8 + std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>(),
            )
        })
        .ok_or(LiveStateError::InvalidLimit)
}
fn native_scan_reservation(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    let request = namespace_bytes
        .checked_mul(5)
        .and_then(|n| n.checked_add(limits.max_entity_bytes.checked_add(9)?))
        .and_then(|n| n.checked_add(maximum_encoded_key(limits, namespace_bytes).ok()?))
        .ok_or(LiveStateError::InvalidLimit)?;
    let containers = limits
        .page_entries
        .min(limits.page_bytes / namespace_bytes.saturating_add(3))
        .checked_mul(std::mem::size_of::<super::ScanEntry>() * 2)
        .ok_or(LiveStateError::InvalidLimit)?;
    limits
        .page_bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(request.checked_mul(4)?))
        .and_then(|n| n.checked_add(containers))
        .ok_or(LiveStateError::InvalidLimit)
}
fn prepare_reservation(limits: CollectionLimits) -> Result<usize> {
    limits
        .batch_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(1024))
        .ok_or(LiveStateError::InvalidLimit)
}
fn clear_pair_capacity(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    // Two namespace headers, two nine-byte logical prefixes, an eight-byte rank
    // and two key terminators are a lower bound even before entity/member bytes.
    let minimum = namespace_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(30))
        .ok_or(LiveStateError::InvalidLimit)?;
    Ok(limits.page_entries.min(limits.batch_bytes / minimum))
}
fn clear_reservation(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    let containers = clear_pair_capacity(limits, namespace_bytes)?
        .checked_mul(2)
        .and_then(|n| n.checked_mul(std::mem::size_of::<WriteOperation>()))
        .ok_or(LiveStateError::InvalidLimit)?;
    prepare_reservation(limits)?
        .checked_add(containers)
        .ok_or(LiveStateError::InvalidLimit)
}
fn delete_pair_size(
    namespace_bytes: usize,
    entity: &[u8],
    member: &[u8],
    count: u64,
) -> Result<usize> {
    let length_zeros = (entity.len() as u32)
        .to_be_bytes()
        .iter()
        .filter(|b| **b == 0)
        .count();
    let zeros = length_zeros
        .checked_add(entity.iter().filter(|b| **b == 0).count())
        .and_then(|n| n.checked_add(member.iter().filter(|b| **b == 0).count()))
        .ok_or(LiveStateError::InvalidLimit)?;
    let common = namespace_bytes
        .checked_add(9)
        .and_then(|n| n.checked_add(entity.len()))
        .and_then(|n| n.checked_add(member.len()))
        .and_then(|n| n.checked_add(zeros))
        .and_then(|n| n.checked_add(2))
        .ok_or(LiveStateError::InvalidLimit)?;
    // Primary kind zero escapes once; rank kind one does not. Rank timestamp
    // bytes can independently escape. Values are absent in delete operations.
    let primary = common.checked_add(1).ok_or(LiveStateError::InvalidLimit)?;
    let rank = common
        .checked_add(8)
        .and_then(|n| {
            n.checked_add(
                (u64::MAX - count)
                    .to_be_bytes()
                    .iter()
                    .filter(|b| **b == 0)
                    .count(),
            )
        })
        .ok_or(LiveStateError::InvalidLimit)?;
    primary
        .checked_add(rank)
        .ok_or(LiveStateError::InvalidLimit)
}
fn page_reservation(limits: CollectionLimits, namespace_bytes: usize) -> Result<usize> {
    // Producer-owned range/prefix/cursor clones exist before native admission,
    // even for an empty scan. Charge them independently of returned entry bytes.
    let request = namespace_bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(limits.max_entity_bytes.checked_add(9)?.checked_mul(4)?))
        .and_then(|n| {
            n.checked_add(
                maximum_encoded_key(limits, namespace_bytes)
                    .ok()?
                    .checked_mul(2)?,
            )
        })
        .ok_or(LiveStateError::InvalidLimit)?;
    limits
        .page_bytes
        .checked_mul(4)
        .and_then(|n| {
            limits
                .page_entries
                .checked_mul(256)
                .and_then(|e| n.checked_add(e))
        })
        .and_then(|n| n.checked_add(request))
        .ok_or(LiveStateError::InvalidLimit)
}

fn top_reservation(limits: CollectionLimits, k: usize) -> Result<usize> {
    k.checked_mul(
        limits
            .max_member_bytes
            .checked_add(64)
            .ok_or(LiveStateError::InvalidLimit)?,
    )
    .ok_or(LiveStateError::InvalidLimit)
}
fn invalid(message: &str) -> LiveStateError {
    LiveStateError::InvalidEncoding(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{
        Ownership, checkpoint, lifecycle::RocksStateConfig, memory::MemoryLiveState,
        resources::ResourceConfig, rocks::RocksLiveState,
    };
    use arroyo_rpc::grpc::rpc::DiskKeyedTableConfig;
    use arroyo_storage::{StorageProvider, StorageProviderRef};
    use std::time::Duration;

    fn namespace() -> StateNamespace {
        StateNamespace {
            ownership: Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
            table: b"collections".to_vec(),
        }
    }
    fn limits() -> CollectionLimits {
        CollectionLimits {
            max_entity_bytes: 64,
            max_member_bytes: 128,
            max_top_k: 20,
            page_entries: 3,
            page_bytes: 2048,
            batch_bytes: 8192,
        }
    }
    fn resources() -> WorkerStateResources {
        WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 8 << 20,
            memtable_bytes: 4 << 20,
            queued_write_bytes: 4 << 20,
            decoded_value_bytes: 4 << 20,
            scan_page_bytes: 8 << 20,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 4,
            disk_reserve_bytes: 0,
        })
        .unwrap()
    }
    async fn rocks(
        root: &std::path::Path,
        generation: u64,
    ) -> (Arc<RocksLiveState>, WorkerStateResources) {
        let pool = resources();
        let backend = Arc::new(
            RocksLiveState::open(
                RocksStateConfig {
                    root: root.to_path_buf(),
                    job_id: "collections-job".into(),
                    operator_id: "collections-op".into(),
                    subtask: 0,
                    generation,
                    attempt: 1,
                },
                pool.clone(),
            )
            .await
            .unwrap(),
        );
        (backend, pool)
    }
    async fn close_native(backend: Arc<RocksLiveState>, resources: &WorkerStateResources) {
        // Drop schedules native destruction on the resource pool's dedicated
        // cleanup thread. Nextest exits this process immediately after one test;
        // await live completion after all snapshots/views have been released.
        // Their earlier FIFO cleanup jobs finish before this completion signal.
        Arc::try_unwrap(backend)
            .unwrap_or_else(|_| panic!("collection fixture retained a native backend alias"))
            .close_and_remove()
            .await
            .unwrap();
        let (finished, completion) = tokio::sync::oneshot::channel();
        resources.cleanup().await.unwrap().submit(move || {
            let _ = finished.send(());
        });
        completion.await.unwrap();
    }
    fn view(backend: Arc<dyn LiveStateBackend>) -> RankedCounts {
        RankedCounts::new(backend, namespace(), limits()).unwrap()
    }
    async fn contract(backend: Arc<dyn LiveStateBackend>) {
        let counts = view(backend);
        counts.set(b"a", b"z", 5).await.unwrap();
        counts.set(b"a", b"b", 5).await.unwrap();
        counts.set(b"a", b"a", 2).await.unwrap();
        counts.set(b"a", b"\0member", 5).await.unwrap();
        counts.set(b"a\0", b"b", 99).await.unwrap();
        counts.set(b"ab", b"b", 98).await.unwrap();
        let stable = counts.snapshot().await.unwrap();
        assert_eq!(
            stable.top_k(b"a", 3).await.unwrap().entries,
            vec![
                CountedMember {
                    member: b"\0member".to_vec(),
                    count: 5
                },
                CountedMember {
                    member: b"b".to_vec(),
                    count: 5
                },
                CountedMember {
                    member: b"z".to_vec(),
                    count: 5
                }
            ]
        );
        assert_eq!(counts.add(b"a", b"a", 10).await.unwrap(), 12);
        counts.set(b"a", b"z", 0).await.unwrap();
        assert_eq!(counts.count(b"a", b"z").await.unwrap(), 0);
        assert_eq!(stable.count(b"a", b"z").await.unwrap(), 5);
        let current = counts.snapshot().await.unwrap();
        assert_eq!(
            current.top_k(b"a", 1).await.unwrap().entries[0],
            CountedMember {
                member: b"a".to_vec(),
                count: 12
            }
        );
        assert_eq!(current.top_k(b"a\0", 20).await.unwrap().entries.len(), 1);
        assert_eq!(current.top_k(b"ab", 20).await.unwrap().entries[0].count, 98);
        assert!(current.top_k(b"a", 21).await.is_err());
        counts.set(b"overflow", b"m", u64::MAX).await.unwrap();
        assert!(matches!(counts.add(b"overflow", b"m", 1).await,
            Err(LiveStateError::InvalidEncoding(message)) if message.contains("overflow")));
        assert_eq!(counts.count(b"overflow", b"m").await.unwrap(), u64::MAX);
        assert!(counts.add(b"missing", b"m", -1).await.is_err());
        assert!(counts.set(b"a", &[0; 129], 1).await.is_err());
        assert!(counts.set(&[0; 65], b"m", 1).await.is_err());
        drop(current);
        drop(stable);
        drop(counts);
    }
    #[tokio::test]
    async fn exact_counts_ranks_prefixes_overflow_and_stable_snapshots_on_both_backends() {
        contract(Arc::new(MemoryLiveState::new())).await;
        let root = tempfile::tempdir().unwrap();
        let (backend, native_resources) = rocks(root.path(), 0).await;
        contract(backend.clone()).await;
        close_native(backend, &native_resources).await;
    }

    async fn hot_collection(backend: Arc<dyn LiveStateBackend>) {
        let counts = view(backend);
        for i in 0u32..200 {
            counts
                .set(b"hot", &i.to_be_bytes(), 1 + u64::from(i % 10))
                .await
                .unwrap();
        }
        counts
            .set(b"hot\0next-incarnation", b"keep", 9)
            .await
            .unwrap();
        let stable = counts.snapshot().await.unwrap();
        let first = stable.members(b"hot", None).await.unwrap();
        assert_eq!(first.entries.len(), 3);
        let cursor = first.next_cursor.unwrap();
        assert!(stable.ranked(b"hot", Some(cursor.clone())).await.is_err());
        assert!(
            stable
                .members(b"hot\0next-incarnation", Some(cursor.clone()))
                .await
                .is_err()
        );
        assert!(
            counts
                .snapshot()
                .await
                .unwrap()
                .members(b"hot", Some(cursor))
                .await
                .is_err()
        );
        let mut cursor = None;
        let mut seen = 0;
        loop {
            let page = stable.ranked(b"hot", cursor).await.unwrap();
            assert!(page.entries.len() <= 3);
            seen += page.entries.len();
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, 200);
        let best = stable.top_k(b"hot", 20).await.unwrap();
        assert!(best.entries.iter().all(|e| e.count == 10));
        assert!(best.entries.windows(2).all(|e| e[0].member < e[1].member));
        let mut removed = 0;
        loop {
            let n = counts.clear_page(b"hot").await.unwrap();
            assert!(n <= 3);
            removed += n;
            if n == 0 {
                break;
            }
        }
        assert_eq!(removed, 200);
        assert!(
            counts
                .snapshot()
                .await
                .unwrap()
                .top_k(b"hot", 20)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        assert_eq!(
            counts
                .count(b"hot\0next-incarnation", b"keep")
                .await
                .unwrap(),
            9
        );
        assert_eq!(stable.top_k(b"hot", 20).await.unwrap().entries.len(), 20);
        drop(best);
        drop(stable);
        drop(counts);
    }
    #[tokio::test]
    async fn hot_collection_pages_and_retired_incarnation_clear_on_both_backends() {
        hot_collection(Arc::new(MemoryLiveState::new())).await;
        let root = tempfile::tempdir().unwrap();
        let (backend, native_resources) = rocks(root.path(), 0).await;
        hot_collection(backend.clone()).await;
        close_native(backend, &native_resources).await;
    }

    #[tokio::test]
    async fn prepared_update_composes_atomically_with_metadata_and_retains_admission() {
        let backend: Arc<dyn LiveStateBackend> = Arc::new(MemoryLiveState::new());
        let resources = resources();
        let counts = view(backend.clone())
            .with_resources(resources.clone())
            .unwrap();
        let mut prepared = counts.prepare_set(b"profile", b"page", 3).await.unwrap();
        let free = resources.config().decoded_value_bytes - prepare_reservation(limits()).unwrap();
        let occupied = resources.decoded_value(free - 1024).await.unwrap();
        let mut too_small = WriteBatch {
            operations: vec![],
            max_bytes: 1,
        };
        assert!(prepared.append_to(&mut too_small).is_err());
        assert!(too_small.operations.is_empty());
        assert_eq!(counts.count(b"profile", b"page").await.unwrap(), 0);
        let meta = state_key(&namespace(), b"metadata".to_vec());
        let mut combined = WriteBatch {
            operations: vec![WriteOperation::Put {
                key: meta.clone(),
                value: b"timer-generation-1".to_vec(),
            }],
            max_bytes: 8192,
        };
        prepared.append_to(&mut combined).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), resources.decoded_value(1025))
                .await
                .is_err()
        );
        backend.write_batch(combined).await.unwrap();
        assert_eq!(counts.count(b"profile", b"page").await.unwrap(), 3);
        assert_eq!(
            backend
                .get(&meta, ReadOptions { max_bytes: 64 })
                .await
                .unwrap(),
            Some(b"timer-generation-1".to_vec())
        );
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(1), resources.decoded_value(1025))
            .await
            .unwrap()
            .unwrap();
        drop(occupied);
    }

    #[tokio::test]
    async fn retained_scan_and_top_k_outputs_keep_bounded_reservations() {
        let resources = resources();
        let counts = view(Arc::new(MemoryLiveState::new()))
            .with_resources(resources.clone())
            .unwrap();
        counts.set(b"profile", b"a", 1).await.unwrap();
        let snapshot = counts.snapshot().await.unwrap();
        let occupied = resources
            .decoded_value(
                resources.config().decoded_value_bytes
                    - page_reservation(
                        limits(),
                        super::super::encoding::encoded_namespace_size(&namespace()).unwrap(),
                    )
                    .unwrap()
                    - 1024,
            )
            .await
            .unwrap();
        let page = snapshot.members(b"profile", None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), resources.decoded_value(1025))
                .await
                .is_err()
        );
        drop(page);
        tokio::time::timeout(Duration::from_secs(1), resources.decoded_value(1025))
            .await
            .unwrap()
            .unwrap();
        drop(occupied);
        let top = snapshot.top_k(b"profile", 20).await.unwrap();
        let occupied = resources
            .decoded_value(
                resources.config().decoded_value_bytes
                    - top_reservation(limits(), 20).unwrap()
                    - 1024,
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), resources.decoded_value(1025))
                .await
                .is_err()
        );
        drop(top);
        tokio::time::timeout(Duration::from_secs(1), resources.decoded_value(1025))
            .await
            .unwrap()
            .unwrap();
        drop(occupied);
    }

    #[test]
    fn resource_limits_include_escaped_keys_namespace_cursors_and_simultaneous_buffers() {
        let mut ns = namespace();
        ns.table = vec![0; 256];
        let mut resources_config = resources().config().clone();
        let namespace_bytes = super::super::encoding::encoded_namespace_size(&ns).unwrap();
        resources_config.scan_page_bytes =
            native_scan_reservation(limits(), namespace_bytes).unwrap() - 1;
        assert!(
            RankedCounts::new(Arc::new(MemoryLiveState::new()), ns.clone(), limits())
                .unwrap()
                .with_resources(WorkerStateResources::new(resources_config).unwrap())
                .is_err()
        );
        let mut resources_config = resources().config().clone();
        resources_config.decoded_value_bytes = page_reservation(limits(), namespace_bytes).unwrap()
            + top_reservation(limits(), limits().max_top_k).unwrap()
            + prepare_reservation(limits())
                .unwrap()
                .max(clear_reservation(limits(), namespace_bytes).unwrap())
            + native_read_reservation(limits(), namespace_bytes).unwrap()
            - 1;
        assert!(
            RankedCounts::new(Arc::new(MemoryLiveState::new()), ns, limits())
                .unwrap()
                .with_resources(WorkerStateResources::new(resources_config).unwrap())
                .is_err()
        );
    }

    #[tokio::test]
    async fn empty_large_entity_prefix_keeps_producer_buffers_admitted() {
        let mut configured = limits();
        configured.max_entity_bytes = 8192;
        configured.page_bytes = 64;
        let resources = resources();
        let counts = RankedCounts::new(Arc::new(MemoryLiveState::new()), namespace(), configured)
            .unwrap()
            .with_resources(resources.clone())
            .unwrap();
        let reserved = page_reservation(
            configured,
            super::super::encoding::encoded_namespace_size(&namespace()).unwrap(),
        )
        .unwrap();
        // An empty scan still constructs a large escaped range/prefix/cursor
        // request. Its producer reservation cannot depend only on output bytes.
        assert!(reserved > configured.max_entity_bytes * 4);
        let occupied = resources
            .decoded_value(resources.config().decoded_value_bytes - reserved - 1024)
            .await
            .unwrap();
        let page = counts
            .snapshot()
            .await
            .unwrap()
            .members(&vec![0; 8192], None)
            .await
            .unwrap();
        assert!(page.entries().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), resources.decoded_value(1025))
                .await
                .is_err()
        );
        drop(page);
        tokio::time::timeout(Duration::from_secs(1), resources.decoded_value(1025))
            .await
            .unwrap()
            .unwrap();
        drop(occupied);
    }

    #[tokio::test]
    async fn clear_progresses_when_full_scan_exceeds_write_budget_and_preserves_snapshot() {
        let backend: Arc<dyn LiveStateBackend> = Arc::new(MemoryLiveState::new());
        let mut configured = limits();
        configured.page_entries = 100;
        configured.page_bytes = 8192;
        configured.batch_bytes = 512;
        let counts = RankedCounts::new(backend.clone(), namespace(), configured)
            .unwrap()
            .with_resources(resources())
            .unwrap();
        for i in 0u32..50 {
            counts
                .set(b"h", &i.to_be_bytes(), u64::from(i) + 1)
                .await
                .unwrap();
        }
        counts.set(b"h\0next", b"keep", 1).await.unwrap();
        let stable = counts.snapshot().await.unwrap();
        let page = stable.members(b"h", None).await.unwrap();
        assert_eq!(page.entries().len(), 50);
        let namespace_bytes = super::super::encoding::encoded_namespace_size(&namespace()).unwrap();
        let full_cost: usize = page
            .entries()
            .iter()
            .map(|entry| {
                delete_pair_size(namespace_bytes, b"h", &entry.member, entry.count).unwrap()
            })
            .sum();
        assert!(full_cost > configured.batch_bytes);
        drop(page);
        let mut removed = 0;
        loop {
            let n = counts.clear_page(b"h").await.unwrap();
            if n == 0 {
                break;
            }
            assert!(n < 50);
            removed += n;
        }
        assert_eq!(removed, 50);
        assert!(
            counts
                .snapshot()
                .await
                .unwrap()
                .members(b"h", None)
                .await
                .unwrap()
                .entries()
                .is_empty()
        );
        assert!(
            counts
                .snapshot()
                .await
                .unwrap()
                .ranked(b"h", None)
                .await
                .unwrap()
                .entries()
                .is_empty()
        );
        assert_eq!(counts.count(b"h\0next", b"keep").await.unwrap(), 1);
        assert_eq!(stable.count(b"h", &0u32.to_be_bytes()).await.unwrap(), 1);
        assert_eq!(stable.top_k(b"h", 1).await.unwrap().entries()[0].count, 50);
        // Check exact preflight matches the authoritative encoding for NULs and
        // both extreme rank scores, rather than depending on optimistic sizing.
        for count in [1, 256, u64::MAX] {
            let entity = b"e\0";
            let member = b"m\0";
            let pair = [
                WriteOperation::Delete {
                    key: state_key(&namespace(), member_key(entity, member)),
                },
                WriteOperation::Delete {
                    key: state_key(&namespace(), rank_key(entity, member, count)),
                },
            ];
            assert_eq!(
                delete_pair_size(namespace_bytes, entity, member, count).unwrap(),
                operation_size(&pair, usize::MAX).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn clear_assembly_is_admitted_before_mutation_and_cancellation_releases_page() {
        let resources = resources();
        let counts = view(Arc::new(MemoryLiveState::new()))
            .with_resources(resources.clone())
            .unwrap();
        counts.set(b"retired", b"member", 1).await.unwrap();
        let namespace_bytes = super::super::encoding::encoded_namespace_size(&namespace()).unwrap();
        let page = page_reservation(limits(), namespace_bytes).unwrap();
        assert!(clear_reservation(limits(), namespace_bytes).unwrap() > 1024);
        let occupied = resources
            .decoded_value(resources.config().decoded_value_bytes - page - 1024)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), counts.clear_page(b"retired"))
                .await
                .is_err()
        );
        assert_eq!(counts.count(b"retired", b"member").await.unwrap(), 1);
        // The cancelled clear releases its held page reservation too.
        tokio::time::timeout(Duration::from_secs(1), resources.decoded_value(page + 1024))
            .await
            .unwrap()
            .unwrap();
        drop(occupied);
        assert_eq!(counts.clear_page(b"retired").await.unwrap(), 1);
        assert_eq!(counts.clear_page(b"retired").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn complete_registered_namespace_export_restores_counts_and_ranks() {
        let root = tempfile::tempdir().unwrap();
        let (backend, native_resources) = rocks(root.path(), 0).await;
        let counts = view(backend.clone());
        for i in 0u32..140 {
            counts
                .set(b"hot", &i.to_be_bytes(), u64::from(i) + 1)
                .await
                .unwrap();
        }
        let barrier = backend.snapshot().await.unwrap();
        counts.set(b"hot", &139u32.to_be_bytes(), 0).await.unwrap();
        counts.set(b"hot", b"uncommitted", 999).await.unwrap();
        let remote = tempfile::tempdir().unwrap();
        let storage: StorageProviderRef = Arc::new(
            StorageProvider::for_url(&format!("file://{}", remote.path().display()))
                .await
                .unwrap(),
        );
        let config = DiskKeyedTableConfig {
            table_name: "collections".into(),
            encoding_version: 1,
            schema_identity: b"streamr.ranked-counts.v1".to_vec(),
        };
        let metadata = checkpoint::export(
            &barrier,
            &namespace(),
            &config,
            &storage,
            "checkpoints/checkpoint-0000001/collections",
            1,
            0,
            0,
        )
        .await
        .unwrap();
        assert!(!metadata.empty && !metadata.files.is_empty());
        assert!(
            metadata
                .files
                .iter()
                .map(|file| file.row_count)
                .sum::<u64>()
                > 1
        );
        let (restored, restored_resources) = rocks(root.path(), 1).await;
        checkpoint::restore(
            restored.as_ref(),
            &namespace(),
            &config,
            &metadata,
            &storage,
        )
        .await
        .unwrap();
        let recovered = view(restored.clone());
        assert_eq!(recovered.count(b"hot", b"uncommitted").await.unwrap(), 0);
        assert_eq!(
            recovered
                .count(b"hot", &139u32.to_be_bytes())
                .await
                .unwrap(),
            140
        );
        assert_eq!(
            recovered
                .snapshot()
                .await
                .unwrap()
                .top_k(b"hot", 1)
                .await
                .unwrap()
                .entries[0],
            CountedMember {
                member: 139u32.to_be_bytes().to_vec(),
                count: 140
            }
        );
        let mut removed = 0;
        loop {
            let n = recovered.clear_page(b"hot").await.unwrap();
            removed += n;
            if n == 0 {
                break;
            }
        }
        assert_eq!(removed, 140);
        assert!(
            recovered
                .snapshot()
                .await
                .unwrap()
                .ranked(b"hot", None)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        drop(recovered);
        drop(barrier);
        drop(counts);
        close_native(restored, &restored_resources).await;
        close_native(backend, &native_resources).await;
    }
}
