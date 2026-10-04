//! Paged, backend-neutral state for SQL SESSION windows.
//!
//! Rows are retained as bounded Arrow IPC records. Session and deadline indexes
//! are metadata only; merging sessions never copies their histories.
use anyhow::{Context, Result, ensure};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arroyo_rpc::config::WindowStateConfig;
use arroyo_state::live::{
    LiveStateBackend, ReadOptions, ScanRange, ScanRequest, StateSnapshot, encoding,
    resources::{ResourcePermit, WorkerStateResources},
    table::LiveTable,
    write::AdmittedWriteBatch,
};
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

const ROW: u8 = b'R';
const SESSION: u8 = b'S';
const DEADLINE: u8 = b'D';
const COUNTER: u8 = b'C';
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionMeta {
    pub start: i64,
    pub end: i64,
}

pub(crate) struct SessionRow {
    pub key: Vec<u8>,
    pub batch: RecordBatch,
    _permit: ResourcePermit,
}

pub(crate) struct SessionStore {
    backend: Arc<dyn LiveStateBackend>,
    table: LiveTable,
    resources: WorkerStateResources,
    schema: SchemaRef,
    limits: WindowStateConfig,
    gap: i64,
}

struct CappedWriter {
    bytes: Vec<u8>,
    max: usize,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|len| len > self.max)
        {
            return Err(std::io::Error::other(
                "native SESSION row exceeds configured value limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn ordered_time(time: i64) -> [u8; 8] {
    ((time as u64) ^ (1 << 63)).to_be_bytes()
}

fn decode_time(bytes: &[u8]) -> Result<i64> {
    ensure!(bytes.len() == 8, "native SESSION time key is malformed");
    Ok((u64::from_be_bytes(bytes.try_into()?) ^ (1 << 63)) as i64)
}

fn prefix(kind: u8, group: &[u8]) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(5 + group.len());
    result.push(kind);
    result.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    result.extend_from_slice(group);
    Ok(result)
}

fn session_key(group: &[u8], start: i64) -> Result<Vec<u8>> {
    let mut key = prefix(SESSION, group)?;
    key.extend_from_slice(&ordered_time(start));
    Ok(key)
}

fn row_key(group: &[u8], time: i64, ordinal: u64) -> Result<Vec<u8>> {
    let mut key = prefix(ROW, group)?;
    key.extend_from_slice(&ordered_time(time));
    key.extend_from_slice(&ordinal.to_be_bytes());
    Ok(key)
}

fn deadline_key(group: &[u8], start: i64, deadline: i64) -> Result<Vec<u8>> {
    let mut key = vec![DEADLINE];
    key.extend_from_slice(&ordered_time(deadline));
    key.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    key.extend_from_slice(group);
    key.extend_from_slice(&ordered_time(start));
    Ok(key)
}

impl SessionStore {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        table: LiveTable,
        resources: WorkerStateResources,
        schema: SchemaRef,
        limits: WindowStateConfig,
        gap: i64,
    ) -> Result<Self> {
        limits.validate()?;
        ensure!(gap > 0, "native SESSION gap must be positive");
        ensure!(
            limits.key_bytes >= 21,
            "native SESSION key-bytes must fit the 21-byte unkeyed row and deadline indexes"
        );
        let max_key = table.key(vec![0; limits.key_bytes], None);
        let encoded_key = encoding::encoded_key_size(&max_key)?;
        let max_entry = encoded_key.saturating_add(limits.partial_bytes.saturating_add(1));
        ensure!(
            max_entry <= limits.page_bytes,
            "native SESSION scan page cannot fit one row"
        );
        let namespace_bytes = encoding::encoded_namespace_size(table.namespace())?;
        let request_bytes = namespace_bytes
            .saturating_mul(5)
            .saturating_add(limits.key_bytes.saturating_mul(3))
            .saturating_add(encoded_key);
        let containers = limits
            .page_entries
            .min(limits.page_bytes / namespace_bytes.saturating_add(3))
            .saturating_mul(std::mem::size_of::<arroyo_state::live::ScanEntry>())
            .saturating_mul(2);
        ensure!(
            limits
                .page_bytes
                .saturating_mul(4)
                .saturating_add(request_bytes.saturating_mul(4))
                .saturating_add(containers)
                <= resources.config().scan_page_bytes,
            "native SESSION scan pool cannot admit one page"
        );
        ensure!(
            limits.partial_bytes.saturating_mul(9) <= resources.config().decoded_value_bytes,
            "native SESSION decoded pool cannot admit one row and bounded final input"
        );
        ensure!(
            AdmittedWriteBatch::reservation_bytes(limits.write_bytes, limits.write_operations)?
                <= resources.config().queued_write_bytes,
            "native SESSION queued-write pool cannot admit one write batch"
        );
        Ok(Self {
            backend,
            table,
            resources,
            schema,
            limits,
            gap,
        })
    }

    pub fn reserve_final_input_queue(&self) -> Result<ResourcePermit> {
        Ok(self
            .resources
            .try_decoded_value(self.limits.partial_bytes.saturating_mul(6))?)
    }

    pub async fn snapshot(&self) -> Result<StateSnapshot> {
        Ok(self.backend.snapshot().await?)
    }

    async fn write_pair(
        &self,
        first: (&[u8], Option<&[u8]>),
        second: (&[u8], Option<&[u8]>),
    ) -> Result<()> {
        let mut batch = AdmittedWriteBatch::try_reserve(
            self.resources.clone(),
            self.limits.write_bytes,
            self.limits.write_operations,
        )?;
        for (key, value) in [first, second] {
            ensure!(
                key.len() <= self.limits.key_bytes,
                "native SESSION key exceeds configured limit"
            );
            let key = self.table.key(key.to_vec(), None);
            match value {
                Some(value) => {
                    ensure!(
                        value.len() <= self.limits.partial_bytes,
                        "native SESSION value exceeds configured limit"
                    );
                    batch.put(&key, value)?;
                }
                None => batch.delete(&key)?,
            }
        }
        self.backend.write_admitted(batch).await?;
        Ok(())
    }

    async fn next_session(&self, group: &[u8], after: Option<i64>) -> Result<Option<SessionMeta>> {
        let snapshot = self.snapshot().await?;
        let prefix = prefix(SESSION, group)?;
        let start = after.map(|time| session_key(group, time)).transpose()?;
        let mut cursor = None;
        loop {
            let page = snapshot
                .try_scan(ScanRequest {
                    range: ScanRange {
                        namespace: self.table.namespace().clone(),
                        prefix: Some(prefix.clone()),
                        start: start.clone(),
                        end: None,
                    },
                    max_entries: self.limits.page_entries.min(2),
                    max_bytes: self.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in page.entries {
                if start.as_ref().is_some_and(|key| entry.key.key <= *key) {
                    continue;
                }
                ensure!(
                    entry.key.key.len() == prefix.len() + 8
                        && entry.value.len() == 9
                        && entry.value[0] == VERSION,
                    "native SESSION metadata is malformed"
                );
                let start = decode_time(&entry.key.key[prefix.len()..])?;
                let end = i64::from_be_bytes(entry.value[1..9].try_into()?);
                ensure!(
                    start <= end,
                    "native SESSION metadata has reversed interval"
                );
                return Ok(Some(SessionMeta { start, end }));
            }
            let Some(next) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next);
        }
    }

    async fn add_session(&self, group: &[u8], session: SessionMeta) -> Result<()> {
        let key = session_key(group, session.start)?;
        let deadline = session
            .end
            .checked_add(self.gap)
            .context("native SESSION deadline overflow")?;
        let expiry = deadline_key(group, session.start, deadline)?;
        let mut value = vec![VERSION];
        value.extend_from_slice(&session.end.to_be_bytes());
        self.write_pair((&key, Some(&value)), (&expiry, Some(&[])))
            .await
    }

    async fn remove_session(&self, group: &[u8], session: SessionMeta) -> Result<()> {
        let key = session_key(group, session.start)?;
        let deadline = session
            .end
            .checked_add(self.gap)
            .context("native SESSION deadline overflow")?;
        let expiry = deadline_key(group, session.start, deadline)?;
        self.write_pair((&key, None), (&expiry, None)).await
    }

    /// Each source row is processed by the sole operator owner. A bounded
    /// backend write batch may contain one row; it has no per-row sync/fsync.
    pub async fn insert(&self, group: &[u8], time: i64, row: &RecordBatch) -> Result<()> {
        ensure!(
            row.schema() == self.schema && row.num_rows() == 1,
            "native SESSION retained row schema or cardinality changed"
        );
        ensure!(
            row.get_array_memory_size() <= self.limits.partial_bytes,
            "native SESSION retained row exceeds configured value limit"
        );
        let _decoded = self
            .resources
            .try_decoded_value(self.limits.partial_bytes.saturating_mul(3))?;
        let mut output = CappedWriter {
            bytes: Vec::new(),
            max: self.limits.partial_bytes,
        };
        {
            let mut writer = StreamWriter::try_new(&mut output, &self.schema)?;
            writer.write(row)?;
            writer.finish()?;
        }
        let counter = prefix(COUNTER, group)?;
        let ordinal = match self
            .table
            .get(counter.clone(), None, ReadOptions { max_bytes: 8 })
            .await?
        {
            Some(bytes) => {
                ensure!(
                    bytes.len() == 8,
                    "native SESSION arrival counter is malformed"
                );
                u64::from_be_bytes(bytes.as_slice().try_into()?)
            }
            None => 0,
        };
        let next = ordinal
            .checked_add(1)
            .context("native SESSION arrival counter overflow")?;
        let raw = row_key(group, time, ordinal)?;
        self.write_pair(
            (&raw, Some(&output.bytes)),
            (&counter, Some(&next.to_be_bytes())),
        )
        .await?;

        // Session intervals are disjoint and ordered. Remove one overlapping
        // interval at a time, then rescan: a newly bridged neighbor may become
        // reachable after expanding the interval. No history is loaded.
        let mut merged = SessionMeta {
            start: time,
            end: time,
        };
        loop {
            let mut after = None;
            let mut found = None;
            while let Some(existing) = self.next_session(group, after).await? {
                if existing.start
                    > merged
                        .end
                        .checked_add(self.gap)
                        .context("native SESSION interval overflow")?
                {
                    break;
                }
                if merged.start
                    <= existing
                        .end
                        .checked_add(self.gap)
                        .context("native SESSION interval overflow")?
                {
                    found = Some(existing);
                    break;
                }
                after = Some(existing.start);
            }
            let Some(existing) = found else {
                break;
            };
            self.remove_session(group, existing).await?;
            merged.start = merged.start.min(existing.start);
            merged.end = merged.end.max(existing.end);
        }
        self.add_session(group, merged).await
    }

    pub async fn first_due(
        &self,
        watermark: Option<i64>,
    ) -> Result<Option<(Vec<u8>, SessionMeta)>> {
        let snapshot = self.snapshot().await?;
        let page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: self.table.namespace().clone(),
                    prefix: Some(vec![DEADLINE]),
                    start: None,
                    end: watermark.map(|time| {
                        let mut end = vec![DEADLINE];
                        end.extend_from_slice(&ordered_time(time));
                        end
                    }),
                },
                max_entries: 1,
                max_bytes: self.limits.page_bytes,
                cursor: None,
            })
            .await?;
        let Some(entry) = page.entries.into_iter().next() else {
            return Ok(None);
        };
        ensure!(
            entry.key.key.len() >= 21 && entry.key.key[0] == DEADLINE && entry.value.is_empty(),
            "native SESSION deadline index is malformed"
        );
        let deadline = decode_time(&entry.key.key[1..9])?;
        let group_len = u32::from_be_bytes(entry.key.key[9..13].try_into()?) as usize;
        ensure!(
            entry.key.key.len() == 21 + group_len,
            "native SESSION deadline key length changed"
        );
        let group = entry.key.key[13..13 + group_len].to_vec();
        let start = decode_time(&entry.key.key[13 + group_len..])?;
        let metadata = self
            .table
            .get(
                session_key(&group, start)?,
                None,
                ReadOptions { max_bytes: 9 },
            )
            .await?
            .context("native SESSION deadline points to missing session")?;
        ensure!(
            metadata.len() == 9 && metadata[0] == VERSION,
            "native SESSION metadata version changed"
        );
        let end = i64::from_be_bytes(metadata[1..9].try_into()?);
        ensure!(
            end.checked_add(self.gap) == Some(deadline),
            "native SESSION deadline disagrees with metadata"
        );
        Ok(Some((group, SessionMeta { start, end })))
    }

    pub async fn next_row(
        &self,
        snapshot: &StateSnapshot,
        group: &[u8],
        session: SessionMeta,
        after: Option<&[u8]>,
    ) -> Result<Option<SessionRow>> {
        let prefix = prefix(ROW, group)?;
        let mut start = prefix.clone();
        start.extend_from_slice(&ordered_time(session.start));
        let lower = after.map(<[u8]>::to_vec).unwrap_or(start);
        ensure!(
            lower.starts_with(&prefix),
            "native SESSION row cursor left group"
        );
        let mut cursor = None;
        loop {
            let page = snapshot
                .try_scan(ScanRequest {
                    range: ScanRange {
                        namespace: self.table.namespace().clone(),
                        prefix: Some(prefix.clone()),
                        start: Some(lower.clone()),
                        end: None,
                    },
                    max_entries: self.limits.page_entries.min(2),
                    max_bytes: self.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in page.entries {
                if after.is_some_and(|key| entry.key.key.as_slice() <= key) {
                    continue;
                }
                ensure!(
                    entry.key.key.len() == prefix.len() + 16
                        && entry.value.len() <= self.limits.partial_bytes,
                    "native SESSION retained row key/value is malformed"
                );
                let time = decode_time(&entry.key.key[prefix.len()..prefix.len() + 8])?;
                if time > session.end {
                    return Ok(None);
                }
                let permit = self
                    .resources
                    .try_decoded_value(self.limits.partial_bytes.saturating_mul(3))?;
                let mut reader = StreamReader::try_new(Cursor::new(&entry.value), None)?;
                ensure!(
                    reader.schema() == self.schema,
                    "native SESSION retained row schema changed"
                );
                let batch = reader
                    .next()
                    .transpose()?
                    .context("native SESSION retained row is empty")?;
                ensure!(
                    batch.num_rows() == 1 && reader.next().transpose()?.is_none(),
                    "native SESSION retained value must contain one Arrow row"
                );
                return Ok(Some(SessionRow {
                    key: entry.key.key,
                    batch,
                    _permit: permit,
                }));
            }
            let Some(next) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next);
        }
    }

    /// Delete one indexed row per admitted batch, retaining no result history.
    /// The metadata and counter are dropped only after all rows are gone.
    pub async fn retire(&self, group: &[u8], session: SessionMeta) -> Result<()> {
        loop {
            let snapshot = self.snapshot().await?;
            let Some(row) = self.next_row(&snapshot, group, session, None).await? else {
                break;
            };
            let key = self.table.key(row.key, None);
            let mut writes = AdmittedWriteBatch::try_reserve(
                self.resources.clone(),
                self.limits.write_bytes,
                self.limits.write_operations,
            )?;
            writes.delete(&key)?;
            self.backend.write_admitted(writes).await?;
            tokio::task::yield_now().await;
        }
        self.remove_session(group, session).await?;
        if self.next_session(group, None).await?.is_none() {
            let counter = prefix(COUNTER, group)?;
            let mut writes = AdmittedWriteBatch::try_reserve(
                self.resources.clone(),
                self.limits.write_bytes,
                self.limits.write_operations,
            )?;
            writes.delete(&self.table.key(counter, None))?;
            self.backend.write_admitted(writes).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use arroyo_state::live::{
        Ownership, memory::MemoryLiveState, resources::ResourceConfig, table::LiveTableManager,
    };

    fn store() -> SessionStore {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> =
            Arc::new(MemoryLiveState::bounded(resources.clone(), 8 * 1024 * 1024).unwrap());
        let mut tables = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = tables.register("session").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        SessionStore::new(
            backend,
            table,
            resources,
            schema,
            WindowStateConfig {
                key_bytes: 128,
                partial_bytes: 1024,
                page_bytes: 8192,
                page_entries: 8,
                write_bytes: 32768,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            },
            10,
        )
        .unwrap()
    }

    fn row(schema: SchemaRef, value: i64) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
    }

    #[tokio::test]
    async fn equality_and_out_of_order_bridge_merge_metadata_without_loading_history() {
        let store = store();
        for time in [0, 12] {
            store
                .insert(b"a", time, &row(store.schema.clone(), time))
                .await
                .unwrap();
        }
        assert_eq!(
            store.next_session(b"a", None).await.unwrap(),
            Some(SessionMeta { start: 0, end: 0 })
        );
        assert_eq!(
            store.next_session(b"a", Some(0)).await.unwrap(),
            Some(SessionMeta { start: 12, end: 12 })
        );
        store
            .insert(b"a", 10, &row(store.schema.clone(), 10))
            .await
            .unwrap();
        assert_eq!(
            store.next_session(b"a", None).await.unwrap(),
            Some(SessionMeta { start: 0, end: 12 })
        );
        assert_eq!(store.next_session(b"a", Some(0)).await.unwrap(), None);
        assert_eq!(store.first_due(Some(22)).await.unwrap(), None);
        let (group, session) = store.first_due(Some(23)).await.unwrap().unwrap();
        assert_eq!(group, b"a");
        assert_eq!(session, SessionMeta { start: 0, end: 12 });
        let snapshot = store.snapshot().await.unwrap();
        let mut after = None;
        let mut values = Vec::new();
        while let Some(next) = store
            .next_row(&snapshot, b"a", session, after.as_deref())
            .await
            .unwrap()
        {
            let column = next
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.push(column.value(0));
            after = Some(next.key);
        }
        assert_eq!(values, vec![0, 10, 12]);
        store.retire(b"a", session).await.unwrap();
        assert_eq!(store.first_due(None).await.unwrap(), None);
        store
            .insert(b"a", 40, &row(store.schema.clone(), 40))
            .await
            .unwrap();
        assert_eq!(
            store.first_due(Some(51)).await.unwrap().unwrap().1,
            SessionMeta { start: 40, end: 40 }
        );
    }
}
