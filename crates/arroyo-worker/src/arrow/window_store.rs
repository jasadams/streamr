//! Backend-neutral, paged partial state for native fixed-width windows.
//!
//! One registered live table owns all three indexes, so the operator checkpoint
//! captures the partials, group catalogue, and expiry cursor at one boundary.
//! Keys are versioned and application-independent. No scan returns a whole
//! pane or a whole group's history.

use anyhow::{Context, Result, ensure};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use arroyo_state::live::{
    LiveStateBackend, ReadOptions, ScanEntry, ScanRange, ScanRequest, StateSnapshot, encoding,
    resources::{ResourcePermit, WorkerStateResources},
    table::LiveTable,
    write::AdmittedWriteBatch,
};
use std::{
    collections::BTreeMap,
    io::{Cursor, Write},
    sync::Arc,
};

const GROUP: u8 = b'G';
const PARTIAL: u8 = b'P';
const EXPIRY: u8 = b'E';
const PROGRESS: u8 = b'M';
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowStoreLimits {
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub page_bytes: usize,
    pub page_entries: usize,
    pub write_bytes: usize,
    pub write_operations: usize,
    pub max_resident_bytes: usize,
}

pub(crate) struct WindowStore {
    backend: Arc<dyn LiveStateBackend>,
    table: LiveTable,
    resources: WorkerStateResources,
    schema: SchemaRef,
    limits: WindowStoreLimits,
}

pub(crate) struct WindowSnapshot {
    table: LiveTable,
    resources: WorkerStateResources,
    schema: SchemaRef,
    limits: WindowStoreLimits,
    snapshot: StateSnapshot,
}

pub(crate) struct WindowPartial {
    pub key: Vec<u8>,
    pub batch: RecordBatch,
    pub encoded_bytes: usize,
    _permit: ResourcePermit,
}

struct CappedWriter {
    data: Vec<u8>,
    limit: usize,
}

impl Write for CappedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self
            .data
            .len()
            .checked_add(data.len())
            .is_none_or(|size| size > self.limit)
        {
            return Err(std::io::Error::other(
                "native window partial exceeds value limit",
            ));
        }
        self.data.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn time_key(time: i64) -> [u8; 8] {
    ((time as u64) ^ (1 << 63)).to_be_bytes()
}

fn group_prefix(group: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(group.len())?;
    let mut key = Vec::with_capacity(1 + 4 + group.len());
    key.push(PARTIAL);
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

fn group_index_key(group: &[u8]) -> Result<Vec<u8>> {
    let mut key = group_prefix(group)?;
    key[0] = GROUP;
    Ok(key)
}

fn partial_key(group: &[u8], time: i64, sequence: [u8; 16]) -> Result<Vec<u8>> {
    let mut key = group_prefix(group)?;
    key.extend_from_slice(&time_key(time));
    key.extend_from_slice(&sequence);
    Ok(key)
}

fn expiry_key(group: &[u8], time: i64, sequence: [u8; 16]) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(1 + 8 + 4 + group.len() + 16);
    key.push(EXPIRY);
    key.extend_from_slice(&time_key(time));
    key.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    key.extend_from_slice(group);
    key.extend_from_slice(&sequence);
    Ok(key)
}

fn expiry_group(entry: &ScanEntry) -> Result<Vec<u8>> {
    ensure!(
        entry.key.key.len() >= 13 && entry.value.first() == Some(&PARTIAL),
        "native window expiry points outside partial index"
    );
    let group_len = u32::from_be_bytes(entry.key.key[9..13].try_into()?) as usize;
    ensure!(
        entry.key.key.len() == 13 + group_len + 16,
        "native window expiry key is malformed"
    );
    let group = entry.key.key[13..13 + group_len].to_vec();
    let expected = partial_key(
        &group,
        (u64::from_be_bytes(entry.key.key[1..9].try_into()?) ^ (1 << 63)) as i64,
        entry.key.key[13 + group_len..].try_into()?,
    )?;
    ensure!(
        entry.value == expected,
        "native window expiry index is inconsistent"
    );
    Ok(group)
}

impl WindowStore {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        table: LiveTable,
        resources: WorkerStateResources,
        schema: SchemaRef,
        limits: WindowStoreLimits,
    ) -> Result<Self> {
        ensure!(
            limits.key_bytes > 0
                && limits.value_bytes > 0
                && limits.page_bytes > 0
                && limits.page_entries > 0
                && limits.write_bytes > 0
                && limits.write_operations >= 4
                && limits.max_resident_bytes > 0,
            "native window store limits must be positive and admit four index writes"
        );
        let max_key = table.key(vec![0; limits.key_bytes], None);
        let encoded_key = encoding::encoded_key_size(&max_key)?;
        let max_entry = encoded_key
            .checked_add(limits.value_bytes.max(limits.key_bytes).saturating_add(1))
            .context("window entry size overflow")?;
        ensure!(
            max_entry <= limits.page_bytes,
            "native window scan page cannot fit one maximum partial"
        );
        ensure!(
            max_entry.saturating_mul(4) <= limits.write_bytes,
            "native window write batch cannot fit one partial and its indexes"
        );
        let namespace_bytes = encoding::encoded_namespace_size(table.namespace())?;
        // The Rocks adapter admits 4×page/request plus entry containers.
        // Our range scans use a prefix, start, end, and a full cursor key.
        let request_bytes = namespace_bytes
            .saturating_mul(5)
            .saturating_add(limits.key_bytes.saturating_mul(3))
            .saturating_add(encoded_key);
        let containers = limits
            .page_entries
            .min(limits.page_bytes / namespace_bytes.saturating_add(3))
            .saturating_mul(std::mem::size_of::<arroyo_state::live::ScanEntry>())
            .saturating_mul(2);
        let scan_probe = limits
            .page_bytes
            .saturating_mul(4)
            .saturating_add(request_bytes.saturating_mul(4))
            .saturating_add(containers);
        ensure!(
            scan_probe <= resources.config().scan_page_bytes,
            "native window scan pool cannot admit one page"
        );
        ensure!(
            limits.value_bytes.saturating_mul(9) <= resources.config().decoded_value_bytes,
            "native window decoded pool cannot admit the bounded final input and one decoded partial"
        );
        ensure!(
            AdmittedWriteBatch::reservation_bytes(limits.write_bytes, limits.write_operations)?
                <= resources.config().queued_write_bytes,
            "native window queued-write pool cannot admit one write batch"
        );
        Ok(Self {
            backend,
            table,
            resources,
            schema,
            limits,
        })
    }

    pub async fn snapshot(&self) -> Result<WindowSnapshot> {
        Ok(WindowSnapshot {
            table: self.table.clone(),
            resources: self.resources.clone(),
            schema: self.schema.clone(),
            limits: self.limits,
            snapshot: self.backend.snapshot().await?,
        })
    }

    /// A one-slot channel can retain one batch while the final aggregate is
    /// consuming another. The producer may be decoding the next page at the
    /// same time. Hold the first two batches' charge until both tasks finish;
    /// `WindowPartial` charges the currently decoded page separately.
    pub fn reserve_final_input_queue(&self) -> Result<ResourcePermit> {
        Ok(self
            .resources
            .try_decoded_value(self.limits.value_bytes.saturating_mul(6))?)
    }

    /// Hold a separate budget for cardinality-growing final accumulators. The
    /// caller counts the persisted IPC payload in a first, paged snapshot pass.
    /// Leave room for the two queued partials and one decoded partial in the
    /// same pool while the final reader and collector are active.
    pub fn reserve_collection_final(&self, bytes: usize) -> Result<ResourcePermit> {
        let concurrent = self
            .limits
            .value_bytes
            .checked_mul(9)
            .context("native window collection concurrent budget overflow")?;
        ensure!(
            bytes
                .checked_add(concurrent)
                .is_some_and(|total| { total <= self.resources.config().decoded_value_bytes }),
            "native window collection exceeds decoded-state budget"
        );
        Ok(self.resources.try_decoded_value(bytes)?)
    }

    pub async fn progress(&self) -> Result<Option<i64>> {
        match self
            .table
            .get(vec![PROGRESS], None, ReadOptions { max_bytes: 9 })
            .await?
        {
            None => Ok(None),
            Some(bytes) => {
                ensure!(
                    bytes.len() == 9 && bytes[0] == VERSION,
                    "native window progress version changed"
                );
                Ok(Some(i64::from_be_bytes(bytes[1..9].try_into()?)))
            }
        }
    }

    pub async fn set_progress(&self, end: i64) -> Result<()> {
        let mut value = vec![VERSION];
        value.extend_from_slice(&end.to_be_bytes());
        let mut writes = AdmittedWriteBatch::try_reserve(
            self.resources.clone(),
            self.limits.write_bytes,
            self.limits.write_operations,
        )?;
        writes.put(&self.table.key(vec![PROGRESS], None), &value)?;
        self.backend.write_admitted(writes).await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn earliest_time(&self) -> Result<Option<i64>> {
        let snapshot = self.backend.snapshot().await?;
        let page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: self.table.namespace().clone(),
                    prefix: Some(vec![EXPIRY]),
                    start: None,
                    end: None,
                },
                max_entries: 1,
                max_bytes: self.limits.page_bytes,
                cursor: None,
            })
            .await?;
        page.entries
            .first()
            .map(|entry| {
                ensure!(
                    entry.key.key.len() >= 9,
                    "native window expiry key is malformed"
                );
                let sorted = u64::from_be_bytes(entry.key.key[1..9].try_into()?);
                Ok((sorted ^ (1 << 63)) as i64)
            })
            .transpose()
    }

    /// Appends one partial batch and its expiry reference atomically. The
    /// caller serializes input to this owner, so the catalogue's maximum pane
    /// time is monotonic even when event times arrive out of order.
    #[cfg(test)]
    pub async fn append(&self, group: &[u8], time: i64, batch: &RecordBatch) -> Result<()> {
        ensure!(
            batch.num_rows() == 1,
            "native window append expects one partial row"
        );
        self.append_rows(time, batch, &[group.to_vec()]).await
    }

    /// A bounded input chunk is committed in admitted multi-row write batches.
    /// If a chunk cannot fit in one backend batch, its committed prefix remains
    /// attempt-local until the next aligned checkpoint; replay restores the
    /// prior checkpoint, not this disposable local database.
    pub async fn append_rows(
        &self,
        time: i64,
        batch: &RecordBatch,
        groups: &[Vec<u8>],
    ) -> Result<()> {
        ensure!(
            batch.schema() == self.schema && batch.num_rows() == groups.len(),
            "native window partial schema or row count changed"
        );
        let mut writes = AdmittedWriteBatch::try_reserve(
            self.resources.clone(),
            self.limits.write_bytes,
            self.limits.write_operations,
        )?;
        let mut pending_bytes = 0usize;
        let mut pending_rows = 0usize;
        let mut catalogue_times = BTreeMap::<Vec<u8>, i64>::new();
        for (row, group) in groups.iter().enumerate() {
            let sequence = *uuid::Uuid::new_v4().as_bytes();
            let primary = partial_key(group, time, sequence)?;
            let expiry = expiry_key(group, time, sequence)?;
            let catalogue = group_index_key(group)?;
            ensure!(
                primary.len() <= self.limits.key_bytes
                    && expiry.len() <= self.limits.key_bytes
                    && catalogue.len() <= self.limits.key_bytes,
                "native window group key exceeds configured limit"
            );
            let mut latest = if let Some(time) = catalogue_times.get(&catalogue) {
                *time
            } else {
                match self
                    .table
                    .get(catalogue.clone(), None, ReadOptions { max_bytes: 9 })
                    .await?
                {
                    Some(bytes) => {
                        ensure!(
                            bytes.len() == 9 && bytes[0] == VERSION,
                            "native window group index version changed"
                        );
                        i64::from_be_bytes(bytes[1..9].try_into()?)
                    }
                    None => time,
                }
            };
            latest = latest.max(time);
            let one = UInt32Array::from(vec![u32::try_from(row)?]);
            let columns = batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column, &one, None))
                .collect::<arrow::error::Result<Vec<_>>>()?;
            let one = RecordBatch::try_new(batch.schema(), columns)?;
            ensure!(
                one.get_array_memory_size() <= self.limits.value_bytes,
                "native window partial source exceeds value limit"
            );
            let _decoded = self
                .resources
                .try_decoded_value(self.limits.value_bytes.saturating_mul(3))?;
            let mut output = CappedWriter {
                data: Vec::new(),
                limit: self.limits.value_bytes,
            };
            {
                let mut writer = StreamWriter::try_new(&mut output, &self.schema)?;
                writer.write(&one)?;
                writer.finish()?;
            }
            let mut index_value = vec![VERSION];
            index_value.extend_from_slice(&latest.to_be_bytes());
            let primary_key = self.table.key(primary.clone(), None);
            let expiry_key = self.table.key(expiry, None);
            let catalogue_key = self.table.key(catalogue.clone(), None);
            let row_bytes = encoding::encoded_key_size(&primary_key)?
                .saturating_add(encoding::encoded_value_size(&output.data)?)
                .saturating_add(encoding::encoded_key_size(&expiry_key)?)
                .saturating_add(encoding::encoded_value_size(&primary)?)
                .saturating_add(encoding::encoded_key_size(&catalogue_key)?)
                .saturating_add(encoding::encoded_value_size(&index_value)?);
            ensure!(
                row_bytes <= self.limits.write_bytes,
                "native window one partial cannot fit write budget"
            );
            if pending_rows > 0
                && (pending_rows * 3 + 3 > self.limits.write_operations
                    || pending_bytes.saturating_add(row_bytes) > self.limits.write_bytes)
            {
                self.backend.write_admitted(writes).await?;
                writes = AdmittedWriteBatch::try_reserve(
                    self.resources.clone(),
                    self.limits.write_bytes,
                    self.limits.write_operations,
                )?;
                pending_rows = 0;
                pending_bytes = 0;
                catalogue_times.clear();
            }
            writes.put(&primary_key, &output.data)?;
            writes.put(&expiry_key, &primary)?;
            writes.put(&catalogue_key, &index_value)?;
            catalogue_times.insert(catalogue, latest);
            pending_rows += 1;
            pending_bytes += row_bytes;
        }
        if pending_rows > 0 {
            self.backend.write_admitted(writes).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn expire_page(&self, before: i64) -> Result<usize> {
        let snapshot = self.backend.snapshot().await?;
        let mut end = vec![EXPIRY];
        end.extend_from_slice(&time_key(before));
        let page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: self.table.namespace().clone(),
                    prefix: Some(vec![EXPIRY]),
                    start: None,
                    end: Some(end),
                },
                max_entries: 1,
                max_bytes: self.limits.page_bytes,
                cursor: None,
            })
            .await?;
        let count = page.entries.len();
        if count == 0 {
            return Ok(0);
        }
        let mut writes = AdmittedWriteBatch::try_reserve(
            self.resources.clone(),
            self.limits.write_bytes,
            self.limits.write_operations,
        )?;
        let mut expired_groups = Vec::with_capacity(count);
        for entry in page.entries {
            let group = expiry_group(&entry)?;
            writes.delete(&self.table.key(entry.value, None))?;
            writes.delete(&entry.key)?;
            expired_groups.push(group);
        }
        self.backend.write_admitted(writes).await?;
        for group in expired_groups {
            self.delete_group_if_older(&group, before).await?;
        }
        Ok(count)
    }

    /// Retire due partials through a caller-owned stable view. `after` is the
    /// last committed expiry key in that view; later watermark intervals skip
    /// deleted keys without creating another physical Rocks checkpoint.
    pub async fn expire_before_snapshot(
        &self,
        snapshot: &WindowSnapshot,
        before: i64,
        after: &mut Option<Vec<u8>>,
    ) -> Result<usize> {
        let max_key = self.table.key(vec![0; self.limits.key_bytes], None);
        let max_delete_bytes = encoding::encoded_key_size(&max_key)?;
        let per_entry = max_delete_bytes
            .checked_mul(3)
            .context("native window expiry write bound overflow")?;
        let max_entries = self
            .limits
            .page_entries
            .min(self.limits.write_operations / 3)
            .min(self.limits.write_bytes / per_entry);
        ensure!(
            max_entries > 0,
            "native window expiry cannot admit one indexed row"
        );
        let mut end = vec![EXPIRY];
        end.extend_from_slice(&time_key(before));
        let range = ScanRange {
            namespace: self.table.namespace().clone(),
            prefix: Some(vec![EXPIRY]),
            start: after.clone(),
            end: Some(end),
        };
        let mut cursor = None;
        let mut total = 0usize;
        loop {
            let page = snapshot
                .snapshot
                .try_scan(ScanRequest {
                    range: range.clone(),
                    max_entries,
                    max_bytes: self.limits.page_bytes,
                    cursor,
                })
                .await?;
            if page.entries.is_empty() {
                ensure!(
                    page.next_cursor.is_none(),
                    "native window expiry cursor made no progress"
                );
                break;
            }
            let next_cursor = page.next_cursor;
            let mut writes = AdmittedWriteBatch::try_reserve(
                self.resources.clone(),
                self.limits.write_bytes,
                self.limits.write_operations,
            )?;
            let mut count = 0usize;
            let mut last_key = None;
            for entry in page.entries {
                if after.as_ref().is_some_and(|key| entry.key.key <= *key) {
                    continue;
                }
                let group = expiry_group(&entry)?;
                last_key = Some(entry.key.key.clone());
                writes.delete(&self.table.key(entry.value, None))?;
                writes.delete(&entry.key)?;
                let catalogue = group_index_key(&group)?;
                if let Some(value) = self
                    .table
                    .get(catalogue.clone(), None, ReadOptions { max_bytes: 9 })
                    .await?
                {
                    ensure!(
                        value.len() == 9 && value[0] == VERSION,
                        "native window group index version changed"
                    );
                    if i64::from_be_bytes(value[1..9].try_into()?) < before {
                        writes.delete(&self.table.key(catalogue, None))?;
                    }
                }
                count += 1;
            }
            if count > 0 {
                self.backend.write_admitted(writes).await?;
                *after = last_key;
                total = total
                    .checked_add(count)
                    .context("native window expired row count overflow")?;
            }
            let Some(next) = next_cursor else {
                break;
            };
            cursor = Some(next);
            tokio::task::yield_now().await;
        }
        Ok(total)
    }

    #[cfg(test)]
    pub async fn expire_before(&self, before: i64) -> Result<usize> {
        let snapshot = self.snapshot().await?;
        self.expire_before_snapshot(&snapshot, before, &mut None)
            .await
    }

    #[cfg(test)]
    pub async fn delete_group_if_older(&self, group: &[u8], before: i64) -> Result<()> {
        let key = group_index_key(group)?;
        let Some(value) = self
            .table
            .get(key.clone(), None, ReadOptions { max_bytes: 9 })
            .await?
        else {
            return Ok(());
        };
        ensure!(
            value.len() == 9 && value[0] == VERSION,
            "native window group index version changed"
        );
        if i64::from_be_bytes(value[1..9].try_into()?) < before {
            let mut writes = AdmittedWriteBatch::try_reserve(
                self.resources.clone(),
                self.limits.write_bytes,
                self.limits.write_operations,
            )?;
            writes.delete(&self.table.key(key, None))?;
            self.backend.write_admitted(writes).await?;
        }
        Ok(())
    }
}

impl WindowSnapshot {
    /// First expiry remaining after the last successfully retired key in this
    /// stable view. A one-entry page can contain only the inclusive start;
    /// follow its cursor until the next key or EOF.
    pub async fn next_expiry_time(&self, after: Option<&[u8]>) -> Result<Option<i64>> {
        let range = ScanRange {
            namespace: self.table.namespace().clone(),
            prefix: Some(vec![EXPIRY]),
            start: after.map(<[u8]>::to_vec),
            end: None,
        };
        let mut cursor = None;
        loop {
            let page = self
                .snapshot
                .try_scan(ScanRequest {
                    range: range.clone(),
                    max_entries: self.limits.page_entries.min(2),
                    max_bytes: self.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in page.entries {
                if after.is_some_and(|key| entry.key.key.as_slice() <= key) {
                    continue;
                }
                expiry_group(&entry)?;
                let sorted = u64::from_be_bytes(entry.key.key[1..9].try_into()?);
                return Ok(Some((sorted ^ (1 << 63)) as i64));
            }
            let Some(next) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next);
        }
    }
    /// Returns the next catalogue key after `after`, without materializing the
    /// catalogue. The returned group is a caller-defined opaque Arrow row key.
    pub async fn next_group(&self, after: Option<&[u8]>) -> Result<Option<(Vec<u8>, i64)>> {
        let key = after.map(group_index_key).transpose()?;
        let range = ScanRange {
            namespace: self.table.namespace().clone(),
            prefix: Some(vec![GROUP]),
            start: key.clone(),
            end: None,
        };
        let mut cursor = None;
        loop {
            let page = self
                .snapshot
                .try_scan(ScanRequest {
                    range: range.clone(),
                    max_entries: self.limits.page_entries.min(2),
                    max_bytes: self.limits.page_bytes,
                    cursor,
                })
                .await?;
            for entry in page.entries {
                if key.as_ref().is_some_and(|key| entry.key.key <= *key) {
                    continue;
                }
                ensure!(
                    entry.key.key.len() >= 5 && entry.value.len() == 9 && entry.value[0] == VERSION,
                    "native window group index is malformed"
                );
                let length = u32::from_be_bytes(entry.key.key[1..5].try_into()?) as usize;
                ensure!(
                    entry.key.key.len() == 5 + length,
                    "native window group key is malformed"
                );
                let time = i64::from_be_bytes(entry.value[1..9].try_into()?);
                return Ok(Some((entry.key.key[5..].to_vec(), time)));
            }
            let Some(next) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next);
        }
    }

    /// Fetches one bounded partial in `[start, end)` for a single group.
    pub async fn next_partial(
        &self,
        group: &[u8],
        start: i64,
        end: i64,
        after: Option<&[u8]>,
    ) -> Result<Option<WindowPartial>> {
        ensure!(start <= end, "native window scan range is reversed");
        let prefix = group_prefix(group)?;
        let mut lower = prefix.clone();
        lower.extend_from_slice(&time_key(start));
        let mut upper = prefix.clone();
        upper.extend_from_slice(&time_key(end));
        let scan_start = after.map(<[u8]>::to_vec).unwrap_or(lower);
        ensure!(
            scan_start.starts_with(&prefix),
            "native window cursor is outside group"
        );
        let range = ScanRange {
            namespace: self.table.namespace().clone(),
            prefix: Some(prefix),
            start: Some(scan_start),
            end: Some(upper),
        };
        let mut cursor = None;
        loop {
            let page = self
                .snapshot
                .try_scan(ScanRequest {
                    range: range.clone(),
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
                    entry.key.key.len() <= self.limits.key_bytes,
                    "native window partial key exceeds configured limit"
                );
                ensure!(
                    entry.value.len() <= self.limits.value_bytes,
                    "native window partial value exceeds configured limit"
                );
                let permit = self
                    .resources
                    .try_decoded_value(self.limits.value_bytes.saturating_mul(3))?;
                let mut reader = StreamReader::try_new(Cursor::new(&entry.value), None)?;
                ensure!(
                    reader.schema() == self.schema,
                    "native window partial schema changed"
                );
                let batch = reader
                    .next()
                    .transpose()?
                    .context("native window partial is empty")?;
                ensure!(
                    batch.num_rows() == 1,
                    "native window partial must contain one group row"
                );
                ensure!(
                    reader.next().transpose()?.is_none(),
                    "native window partial has multiple Arrow batches"
                );
                return Ok(Some(WindowPartial {
                    key: entry.key.key,
                    batch,
                    encoded_bytes: entry.value.len(),
                    _permit: permit,
                }));
            }
            let Some(next) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next);
        }
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

    fn store_with_page_entries(page_entries: usize) -> WindowStore {
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
        let table = tables.register("window").unwrap();
        WindowStore::new(
            backend,
            table,
            resources,
            Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )])),
            WindowStoreLimits {
                key_bytes: 128,
                value_bytes: 8192,
                page_bytes: 32768,
                page_entries,
                write_bytes: 65536,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn store() -> WindowStore {
        store_with_page_entries(1)
    }

    fn partial(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn partials_are_paged_by_group_and_expire_without_loading_all_panes() {
        let store = store();
        store.append(b"a", -10, &partial(1)).await.unwrap();
        let grouped = RecordBatch::try_new(
            partial(2).schema(),
            vec![Arc::new(Int64Array::from(vec![2, 3]))],
        )
        .unwrap();
        store
            .append_rows(0, &grouped, &[b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(store.earliest_time().await.unwrap(), Some(-10));
        let snapshot = store.snapshot().await.unwrap();
        let (first, _) = snapshot.next_group(None).await.unwrap().unwrap();
        let (second, _) = snapshot.next_group(Some(&first)).await.unwrap().unwrap();
        assert_ne!(first, second);
        assert!(snapshot.next_group(Some(&second)).await.unwrap().is_none());
        let old = snapshot
            .next_partial(b"a", -10, 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            old.batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(
            snapshot
                .next_partial(b"a", -10, 0, Some(&old.key))
                .await
                .unwrap()
                .is_none()
        );
        drop(snapshot);
        assert_eq!(store.expire_page(0).await.unwrap(), 1);
        assert_eq!(store.earliest_time().await.unwrap(), Some(0));
        store.set_progress(5).await.unwrap();
        assert_eq!(store.progress().await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn one_snapshot_expiry_walks_pages_and_preserves_future_group_index() {
        let store = store_with_page_entries(8);
        for item in 0..14 {
            store
                .append(format!("group-{item}").as_bytes(), 0, &partial(item))
                .await
                .unwrap();
        }
        store.append(b"group-0", 20, &partial(20)).await.unwrap();
        assert_eq!(store.expire_before(10).await.unwrap(), 14);
        assert_eq!(store.expire_before(10).await.unwrap(), 0);
        assert_eq!(store.earliest_time().await.unwrap(), Some(20));
        let snapshot = store.snapshot().await.unwrap();
        assert!(
            snapshot
                .next_partial(b"group-0", 0, 10, None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            snapshot
                .next_partial(b"group-0", 20, 21, None)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            snapshot.next_group(None).await.unwrap().unwrap().0,
            b"group-0".to_vec()
        );
        assert!(
            snapshot
                .next_group(Some(&b"group-0"[..]))
                .await
                .unwrap()
                .is_none()
        );
        drop(snapshot);
        assert_eq!(store.expire_before(21).await.unwrap(), 1);
        assert_eq!(store.earliest_time().await.unwrap(), None);
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .next_group(None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn one_watermark_view_skips_retired_prefix_with_single_entry_pages() {
        let store = store_with_page_entries(1);
        store.append(b"a", 0, &partial(1)).await.unwrap();
        store.append(b"b", 10, &partial(2)).await.unwrap();
        store.append(b"c", 20, &partial(3)).await.unwrap();
        let view = store.snapshot().await.unwrap();
        let mut retired = None;
        assert_eq!(view.next_expiry_time(None).await.unwrap(), Some(0));
        assert_eq!(
            store
                .expire_before_snapshot(&view, 10, &mut retired)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            view.next_expiry_time(retired.as_deref()).await.unwrap(),
            Some(10)
        );
        assert_eq!(
            store
                .expire_before_snapshot(&view, 20, &mut retired)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            view.next_expiry_time(retired.as_deref()).await.unwrap(),
            Some(20)
        );
        // The stable view still has the old rows, but the monotone bound
        // prevents their emission after live deletion.
        assert!(
            view.next_partial(b"a", 0, 10, None)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(store.earliest_time().await.unwrap(), Some(20));
        assert_eq!(
            store
                .expire_before_snapshot(&view, 30, &mut retired)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            view.next_expiry_time(retired.as_deref()).await.unwrap(),
            None
        );
        assert_eq!(store.earliest_time().await.unwrap(), None);
    }

    #[test]
    fn collection_reservation_keeps_room_for_final_queue_and_decoder() {
        let store = store();
        let available = store.resources.config().decoded_value_bytes - 9 * store.limits.value_bytes;
        assert!(store.reserve_collection_final(available).is_ok());
        assert!(store.reserve_collection_final(available + 1).is_err());
    }
}
