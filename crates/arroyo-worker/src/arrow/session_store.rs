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
    LiveStateBackend, ReadOptions, ScanEntry, ScanRange, ScanRequest, StateSnapshot, encoding,
    resources::{ResourcePermit, WorkerStateResources},
    table::LiveTable,
    write::AdmittedWriteBatch,
};
use std::{
    io::{Cursor, Write},
    sync::{Arc, Mutex},
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
    /// One admitted group only. A proof is never serialized and starts cold
    /// after restore; all mutations pass through this serial owner.
    last_group: Mutex<Option<CachedGroup>>,
    /// A negative expiry proof only; never persisted or shared with another owner.
    deadline: Mutex<Option<CachedDeadline>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GroupProof {
    Empty,
    Sole(SessionMeta),
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeadlineProof {
    Unknown,
    Empty,
    /// Every deadline is at least this value; deletion need not advance it.
    LowerBound(i64),
}

struct CachedDeadline {
    proof: DeadlineProof,
    _permit: ResourcePermit,
}

struct CachedGroup {
    group: Vec<u8>,
    proof: GroupProof,
    _permit: ResourcePermit,
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
        // Optional headroom only: a valid nine-row reader configuration must
        // retain its existing fallback rather than fail to construct a cache.
        let charge = std::mem::size_of::<CachedDeadline>() + 128;
        let deadline = limits
            .partial_bytes
            .checked_mul(9)
            .and_then(|required| required.checked_add(charge))
            .filter(|required| *required <= resources.config().decoded_value_bytes)
            .and_then(|_| resources.try_decoded_value(charge).ok())
            .map(|permit| CachedDeadline {
                proof: DeadlineProof::Unknown,
                _permit: permit,
            });
        Ok(Self {
            backend,
            table,
            resources,
            schema,
            limits,
            gap,
            last_group: Mutex::new(None),
            deadline: Mutex::new(deadline),
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

    fn cached_proof(&self, group: &[u8]) -> Result<Option<GroupProof>> {
        let cache = self
            .last_group
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION group cache lock poisoned"))?;
        Ok(cache
            .as_ref()
            .filter(|entry| entry.group == group)
            .map(|entry| entry.proof))
    }

    fn remember_group(&self, group: &[u8], proof: GroupProof) -> Result<()> {
        let mut cache = self
            .last_group
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION group cache lock poisoned"))?;
        if let Some(entry) = cache.as_mut()
            && entry.group == group
        {
            entry.proof = proof;
            return Ok(());
        }
        // The cache is an optimization. Release the previous group's permit
        // before trying another; a tight pool falls back to a backend read.
        *cache = None;
        if group.len() > self.limits.key_bytes {
            return Ok(());
        }
        let Some(charge) = group
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CachedGroup>() + 128))
        else {
            return Ok(());
        };
        // The final reader can concurrently hold a two-slot queue (6 rows)
        // and decode one more row (3 rows). A cache may use only headroom above
        // the constructor's mandatory nine-row decoded reservation.
        let deadline_charge = if self
            .deadline
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION deadline cache lock poisoned"))?
            .is_some()
        {
            std::mem::size_of::<CachedDeadline>() + 128
        } else {
            0
        };
        if self
            .limits
            .partial_bytes
            .checked_mul(9)
            .and_then(|required| required.checked_add(deadline_charge))
            .and_then(|required| required.checked_add(charge))
            .is_none_or(|required| required > self.resources.config().decoded_value_bytes)
        {
            return Ok(());
        }
        if let Ok(permit) = self.resources.try_decoded_value(charge) {
            *cache = Some(CachedGroup {
                group: group.to_vec(),
                proof,
                _permit: permit,
            });
        }
        Ok(())
    }

    /// No cache proof survives a write whose completion is still ambiguous.
    /// On success the caller may publish a proof derived from this prior state.
    fn invalidate_group(&self, group: &[u8]) -> Result<Option<GroupProof>> {
        let mut cache = self
            .last_group
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION group cache lock poisoned"))?;
        Ok(cache
            .as_mut()
            .filter(|entry| entry.group == group)
            .map(|entry| {
                let prior = entry.proof;
                entry.proof = GroupProof::Unknown;
                prior
            }))
    }

    fn decode_session(entry: &ScanEntry, prefix: &[u8]) -> Result<SessionMeta> {
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
        Ok(SessionMeta { start, end })
    }

    async fn next_session(&self, group: &[u8], after: Option<i64>) -> Result<Option<SessionMeta>> {
        if let Some(proof) = self.cached_proof(group)? {
            match proof {
                GroupProof::Empty => return Ok(None),
                GroupProof::Sole(session) => {
                    return Ok((after.is_none_or(|after| session.start > after)).then_some(session));
                }
                GroupProof::Unknown => {}
            }
        }
        let snapshot = self.snapshot().await?;
        let prefix = prefix(SESSION, group)?;
        // A full-prefix page proves empty or sole only if no continuation
        // exists. A page-byte cutoff can return one entry plus a cursor.
        let proof_page = snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: self.table.namespace().clone(),
                    prefix: Some(prefix.clone()),
                    start: None,
                    end: None,
                },
                max_entries: self.limits.page_entries.min(2),
                max_bytes: self.limits.page_bytes,
                cursor: None,
            })
            .await?;
        let proof = if proof_page.next_cursor.is_none() {
            match proof_page.entries.as_slice() {
                [] => GroupProof::Empty,
                [entry] => GroupProof::Sole(Self::decode_session(entry, &prefix)?),
                _ => GroupProof::Unknown,
            }
        } else {
            GroupProof::Unknown
        };
        self.remember_group(group, proof)?;
        match proof {
            GroupProof::Empty => return Ok(None),
            GroupProof::Sole(session) => {
                return Ok((after.is_none_or(|after| session.start > after)).then_some(session));
            }
            GroupProof::Unknown => {}
        }
        self.next_session_from_snapshot(&snapshot, group, after, prefix)
            .await
    }

    async fn next_session_from_snapshot(
        &self,
        snapshot: &StateSnapshot,
        group: &[u8],
        after: Option<i64>,
        prefix: Vec<u8>,
    ) -> Result<Option<SessionMeta>> {
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
                return Ok(Some(Self::decode_session(&entry, &prefix)?));
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
        // Lower before awaiting: cancellation or an ambiguous write failure
        // must never preserve Empty or a bound above the possible new deadline.
        self.update_deadline(|proof| match proof {
            DeadlineProof::Unknown => DeadlineProof::Unknown,
            DeadlineProof::Empty => DeadlineProof::LowerBound(deadline),
            DeadlineProof::LowerBound(bound) => DeadlineProof::LowerBound(bound.min(deadline)),
        })?;
        let prior = self.invalidate_group(group)?;
        self.write_pair((&key, Some(&value)), (&expiry, Some(&[])))
            .await?;
        if let Some(prior) = prior {
            let proof = match prior {
                GroupProof::Empty => GroupProof::Sole(session),
                GroupProof::Sole(_) | GroupProof::Unknown => GroupProof::Unknown,
            };
            self.remember_group(group, proof)?;
        }
        Ok(())
    }

    async fn remove_session(&self, group: &[u8], session: SessionMeta) -> Result<()> {
        let key = session_key(group, session.start)?;
        let deadline = session
            .end
            .checked_add(self.gap)
            .context("native SESSION deadline overflow")?;
        let expiry = deadline_key(group, session.start, deadline)?;
        let prior = self.invalidate_group(group)?;
        self.write_pair((&key, None), (&expiry, None)).await?;
        if let Some(prior) = prior {
            let proof = match prior {
                GroupProof::Sole(sole) if sole == session => GroupProof::Empty,
                GroupProof::Empty | GroupProof::Sole(_) | GroupProof::Unknown => {
                    GroupProof::Unknown
                }
            };
            self.remember_group(group, proof)?;
        }
        Ok(())
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

    fn update_deadline(&self, update: impl FnOnce(DeadlineProof) -> DeadlineProof) -> Result<()> {
        let mut cache = self
            .deadline
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION deadline cache lock poisoned"))?;
        if let Some(cache) = cache.as_mut() {
            cache.proof = update(cache.proof);
        }
        Ok(())
    }

    fn no_deadline_due(&self, watermark: Option<i64>) -> Result<bool> {
        let cache = self
            .deadline
            .lock()
            .map_err(|_| anyhow::anyhow!("native SESSION deadline cache lock poisoned"))?;
        Ok(cache.as_ref().is_some_and(|cache| match cache.proof {
            DeadlineProof::Empty => true,
            DeadlineProof::LowerBound(bound) => watermark.is_some_and(|time| time <= bound),
            DeadlineProof::Unknown => false,
        }))
    }

    pub async fn first_due(
        &self,
        watermark: Option<i64>,
    ) -> Result<Option<(Vec<u8>, SessionMeta)>> {
        if self.no_deadline_due(watermark)? {
            return Ok(None);
        }
        let snapshot = self.snapshot().await?;
        let request = ScanRequest {
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
        };
        let page = snapshot.try_scan(request.clone()).await?;
        let Some(entry) = page.entries.into_iter().next() else {
            // A watermark-limited empty range is not globally Empty. Peek at
            // the global minimum using the same snapshot, but do not surface
            // future malformed metadata or optional read errors before due.
            if watermark.is_none() && page.next_cursor.is_none() {
                self.update_deadline(|_| DeadlineProof::Empty)?;
                return Ok(None);
            }
            let mut global = request;
            global.range.end = None;
            if let Ok(page) = snapshot.try_scan(global).await {
                let proof = match page.entries.first() {
                    None if page.next_cursor.is_none() => Some(DeadlineProof::Empty),
                    Some(entry)
                        if entry.key.key.len() >= 21
                            && entry.key.key[0] == DEADLINE
                            && entry.value.is_empty()
                            && entry.key.key.len()
                                == 21
                                    + u32::from_be_bytes(
                                        entry.key.key[9..13]
                                            .try_into()
                                            .expect("checked key length"),
                                    ) as usize =>
                    {
                        decode_time(&entry.key.key[1..9])
                            .ok()
                            .map(DeadlineProof::LowerBound)
                    }
                    _ => None,
                };
                if let Some(proof) = proof {
                    self.update_deadline(|_| proof)?;
                }
            }
            return Ok(None);
        };
        ensure!(
            entry.key.key.len() >= 21 && entry.key.key[0] == DEADLINE && entry.value.is_empty(),
            "native SESSION deadline index is malformed"
        );
        let deadline = decode_time(&entry.key.key[1..9])?;
        self.update_deadline(|_| DeadlineProof::LowerBound(deadline))?;
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

    async fn retire_single_rows(
        &self,
        snapshot: &StateSnapshot,
        group: &[u8],
        session: SessionMeta,
    ) -> Result<()> {
        let mut after = None;
        while let Some(row) = self
            .next_row(snapshot, group, session, after.as_deref())
            .await?
        {
            after = Some(row.key.clone());
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
        Ok(())
    }

    async fn retire_pages(
        &self,
        snapshot: &StateSnapshot,
        group: &[u8],
        session: SessionMeta,
        max_entries: usize,
    ) -> Result<()> {
        let prefix = prefix(ROW, group)?;
        let mut start = prefix.clone();
        start.extend_from_slice(&ordered_time(session.start));
        let range = ScanRange {
            namespace: self.table.namespace().clone(),
            prefix: Some(prefix.clone()),
            start: Some(start),
            end: None,
        };
        let mut cursor = None;
        loop {
            let page = snapshot
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
                    "native SESSION retirement cursor made no progress"
                );
                break;
            }
            let next_cursor = page.next_cursor;
            let mut writes = AdmittedWriteBatch::try_reserve(
                self.resources.clone(),
                self.limits.write_bytes,
                self.limits.write_operations,
            )?;
            let mut count = 0;
            let mut past_end = false;
            for entry in page.entries {
                ensure!(
                    entry.key.key.len() == prefix.len() + 16
                        && entry.value.len() <= self.limits.partial_bytes,
                    "native SESSION retained row key/value is malformed"
                );
                let time = decode_time(&entry.key.key[prefix.len()..prefix.len() + 8])?;
                if time > session.end {
                    past_end = true;
                    break;
                }
                writes.delete(&entry.key)?;
                count += 1;
            }
            if count != 0 {
                self.backend.write_admitted(writes).await?;
                tokio::task::yield_now().await;
            }
            if past_end {
                break;
            }
            let Some(next) = next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        Ok(())
    }

    /// One stable snapshot drives bounded row-key pages while admitted batches
    /// delete from live state. A retry starts with a fresh snapshot and sees
    /// only rows still present; no raw result history is retained here.
    pub async fn retire(&self, group: &[u8], session: SessionMeta) -> Result<()> {
        let snapshot = self.snapshot().await?;
        let max_key = self.table.key(vec![0; self.limits.key_bytes], None);
        let max_delete_bytes = encoding::encoded_key_size(&max_key)?;
        let max_entries = self
            .limits
            .page_entries
            .min(self.limits.write_operations)
            .min(self.limits.write_bytes / max_delete_bytes);
        let page_charge = self.limits.page_bytes.checked_mul(3).and_then(|bytes| {
            max_entries
                .checked_mul(std::mem::size_of::<ScanEntry>() * 2)
                .and_then(|containers| bytes.checked_add(containers))
        });
        if let Some(_page_permit) = page_charge
            .filter(|_| max_entries > 0)
            .and_then(|bytes| self.resources.try_decoded_value(bytes).ok())
        {
            self.retire_pages(&snapshot, group, session, max_entries)
                .await?;
        } else {
            // A conservative page reservation must not make a configuration
            // that admitted one row lose its existing retirement path.
            self.retire_single_rows(&snapshot, group, session).await?;
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

    fn store_with_decoded(decoded_value_bytes: usize) -> SessionStore {
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes,
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

    fn store() -> SessionStore {
        store_with_decoded(1024 * 1024)
    }

    struct ObservedBackend {
        inner: Arc<dyn LiveStateBackend>,
        snapshots: std::sync::atomic::AtomicUsize,
        pause_write: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl LiveStateBackend for ObservedBackend {
        async fn get(
            &self,
            key: &arroyo_state::live::StateKey,
            options: ReadOptions,
        ) -> arroyo_state::live::Result<Option<Vec<u8>>> {
            self.inner.get(key, options).await
        }
        async fn multi_get(
            &self,
            keys: &[arroyo_state::live::StateKey],
            options: ReadOptions,
        ) -> arroyo_state::live::Result<Vec<Option<Vec<u8>>>> {
            self.inner.multi_get(keys, options).await
        }
        async fn write_batch(
            &self,
            batch: arroyo_state::live::WriteBatch,
        ) -> arroyo_state::live::Result<()> {
            self.inner.write_batch(batch).await
        }
        async fn write_admitted(
            &self,
            batch: AdmittedWriteBatch,
        ) -> arroyo_state::live::Result<()> {
            if self.pause_write.load(std::sync::atomic::Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            self.inner.write_admitted(batch).await
        }
        async fn snapshot(&self) -> arroyo_state::live::Result<StateSnapshot> {
            self.snapshots
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.snapshot().await
        }
    }

    fn observed_store() -> (SessionStore, Arc<ObservedBackend>) {
        let mut store = store();
        let observed = Arc::new(ObservedBackend {
            inner: store.backend.clone(),
            snapshots: std::sync::atomic::AtomicUsize::new(0),
            pause_write: std::sync::atomic::AtomicBool::new(false),
        });
        store.backend = observed.clone();
        (store, observed)
    }

    fn snapshots(backend: &ObservedBackend) -> usize {
        backend.snapshots.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn deadline_proof_skips_snapshots_through_hot_extensions_but_not_expiry_or_eof() {
        let (store, backend) = observed_store();
        store
            .insert(b"a", 100, &row(store.schema.clone(), 100))
            .await
            .unwrap();
        assert_eq!(store.first_due(Some(100)).await.unwrap(), None);
        let before = snapshots(&backend);
        for time in 101..110 {
            store
                .insert(b"a", time, &row(store.schema.clone(), time))
                .await
                .unwrap();
            assert_eq!(store.first_due(Some(110)).await.unwrap(), None);
        }
        assert_eq!(snapshots(&backend), before);
        // The old conservative bound is 110; a later check refreshes it to119.
        assert_eq!(store.first_due(Some(119)).await.unwrap(), None);
        let refreshed = snapshots(&backend);
        assert_eq!(store.first_due(Some(119)).await.unwrap(), None);
        assert_eq!(snapshots(&backend), refreshed);
        assert_eq!(
            store.first_due(Some(120)).await.unwrap().unwrap().1.end,
            109
        );
        assert_eq!(store.first_due(None).await.unwrap().unwrap().1.end, 109);
        assert_eq!(snapshots(&backend), refreshed + 2);
    }

    #[tokio::test]
    async fn deadline_empty_proof_out_of_order_insert_and_earliest_delete_remain_conservative() {
        let (store, backend) = observed_store();
        assert_eq!(store.first_due(Some(0)).await.unwrap(), None);
        let empty = snapshots(&backend);
        assert_eq!(store.first_due(None).await.unwrap(), None);
        assert_eq!(snapshots(&backend), empty);
        store
            .insert(b"a", 100, &row(store.schema.clone(), 100))
            .await
            .unwrap();
        assert_eq!(store.first_due(Some(110)).await.unwrap(), None);
        store
            .insert(b"b", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        let (group, session) = store.first_due(Some(11)).await.unwrap().unwrap();
        assert_eq!(group, b"b");
        store.retire(&group, session).await.unwrap();
        assert_eq!(store.first_due(Some(110)).await.unwrap(), None);
        let (group, session) = store.first_due(None).await.unwrap().unwrap();
        assert_eq!(group, b"a");
        store.retire(&group, session).await.unwrap();
        assert_eq!(store.first_due(None).await.unwrap(), None);
        let empty = snapshots(&backend);
        assert_eq!(store.first_due(Some(i64::MAX)).await.unwrap(), None);
        assert_eq!(snapshots(&backend), empty);
    }

    #[tokio::test]
    async fn deadline_add_failure_and_cancellation_downgrade_before_write() {
        let (mut store, backend) = observed_store();
        assert_eq!(store.first_due(None).await.unwrap(), None);
        let limit = store.limits.write_bytes;
        store.limits.write_bytes = 1;
        let session = SessionMeta { start: 0, end: 0 };
        assert!(store.add_session(b"a", session).await.is_err());
        assert!(!store.no_deadline_due(Some(11)).unwrap());
        store.limits.write_bytes = limit;
        assert_eq!(store.first_due(None).await.unwrap(), None);
        backend
            .pause_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut pending = Box::pin(store.add_session(b"a", session));
        assert!(futures::poll!(&mut pending).is_pending());
        assert!(!store.no_deadline_due(Some(11)).unwrap());
        drop(pending);
        backend
            .pause_write
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(store.first_due(Some(11)).await.unwrap(), None);
        store.add_session(b"a", session).await.unwrap();
        assert_eq!(store.first_due(Some(11)).await.unwrap().unwrap().1, session);
    }

    #[tokio::test]
    async fn deadline_future_malformed_entry_and_scan_limit_error_wait_until_due() {
        for oversized in [false, true] {
            let store = store();
            let key = deadline_key(b"a", 100, 110).unwrap();
            let value = if oversized {
                vec![0; store.limits.page_bytes + 1]
            } else {
                vec![1]
            };
            store
                .backend
                .put(
                    store.table.key(key, None),
                    value,
                    store.limits.page_bytes * 2,
                )
                .await
                .unwrap();
            assert_eq!(store.first_due(Some(110)).await.unwrap(), None);
            assert!(store.first_due(Some(111)).await.is_err());
        }
    }

    #[tokio::test]
    async fn deadline_recovered_store_starts_unknown_and_tight_reader_budget_falls_back() {
        let store = store();
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(store.first_due(Some(10)).await.unwrap(), None);
        let recovered = SessionStore::new(
            store.backend.clone(),
            store.table.clone(),
            store.resources.clone(),
            store.schema.clone(),
            store.limits,
            store.gap,
        )
        .unwrap();
        assert!(!recovered.no_deadline_due(Some(10)).unwrap());
        assert_eq!(
            recovered.first_due(Some(11)).await.unwrap().unwrap().1.end,
            0
        );
        let tight = store_with_decoded(9 * 1024);
        assert!(tight.deadline.lock().unwrap().is_none());
        tight
            .insert(b"a", 0, &row(tight.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(tight.first_due(Some(11)).await.unwrap().unwrap().1.end, 0);
    }

    #[tokio::test]
    async fn deadline_and_group_caches_share_only_headroom_above_nine_row_reader() {
        let deadline_charge = std::mem::size_of::<CachedDeadline>() + 128;
        let group_charge = 2 + std::mem::size_of::<CachedGroup>() + 128;
        let budget = 9 * 1024 + deadline_charge + group_charge - 1;
        let store = store_with_decoded(budget);
        assert!(store.deadline.lock().unwrap().is_some());
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(store.cached_proof(b"a").unwrap(), None);
        let resources = store.resources.clone();
        let reader = resources.try_decoded_value(9 * 1024).unwrap();
        drop(reader);
        assert!(resources.try_decoded_value(budget).is_err());
        drop(store);
        assert!(resources.try_decoded_value(budget).is_ok());
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

    #[tokio::test]
    async fn one_group_proof_handles_empty_sole_unknown_and_after_bounds() {
        let store = store();
        assert_eq!(store.next_session(b"a", None).await.unwrap(), None);
        assert_eq!(store.cached_proof(b"a").unwrap(), Some(GroupProof::Empty));
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(
            store.cached_proof(b"a").unwrap(),
            Some(GroupProof::Sole(SessionMeta { start: 0, end: 0 }))
        );
        assert_eq!(
            store
                .next_session(b"a", Some(-1))
                .await
                .unwrap()
                .unwrap()
                .start,
            0
        );
        assert_eq!(store.next_session(b"a", Some(0)).await.unwrap(), None);
        store
            .insert(b"a", 30, &row(store.schema.clone(), 30))
            .await
            .unwrap();
        assert_eq!(store.cached_proof(b"a").unwrap(), Some(GroupProof::Unknown));
        assert_eq!(
            store
                .next_session(b"a", Some(0))
                .await
                .unwrap()
                .unwrap()
                .start,
            30
        );
        // An out-of-order bridge collapses the unproven catalogue back to one
        // session; the next full-prefix proof can then be cached.
        for time in [10, 20] {
            store
                .insert(b"a", time, &row(store.schema.clone(), time))
                .await
                .unwrap();
        }
        assert_eq!(
            store.next_session(b"a", None).await.unwrap(),
            Some(SessionMeta { start: 0, end: 30 })
        );
        assert_eq!(
            store.cached_proof(b"a").unwrap(),
            Some(GroupProof::Sole(SessionMeta { start: 0, end: 30 }))
        );
        store
            .insert(b"b", 5, &row(store.schema.clone(), 5))
            .await
            .unwrap();
        assert_eq!(store.cached_proof(b"a").unwrap(), None);
        assert_eq!(
            store.next_session(b"a", None).await.unwrap().unwrap().end,
            30
        );
    }

    #[tokio::test]
    async fn cache_skips_when_nine_row_reader_budget_has_no_headroom() {
        let store = store_with_decoded(9 * 1024);
        assert_eq!(store.next_session(b"a", None).await.unwrap(), None);
        assert_eq!(store.cached_proof(b"a").unwrap(), None);
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(store.cached_proof(b"a").unwrap(), None);
        assert_eq!(
            store.next_session(b"a", None).await.unwrap(),
            Some(SessionMeta { start: 0, end: 0 })
        );
    }

    #[tokio::test]
    async fn one_entry_scan_limit_still_proves_sole_only_without_cursor() {
        let mut store = store();
        store.limits.page_entries = 1;
        assert_eq!(store.next_session(b"a", None).await.unwrap(), None);
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        assert_eq!(
            store.cached_proof(b"a").unwrap(),
            Some(GroupProof::Sole(SessionMeta { start: 0, end: 0 }))
        );
        store
            .insert(b"a", 30, &row(store.schema.clone(), 30))
            .await
            .unwrap();
        assert_eq!(store.cached_proof(b"a").unwrap(), Some(GroupProof::Unknown));
        assert_eq!(
            store.next_session(b"a", Some(0)).await.unwrap(),
            Some(SessionMeta { start: 30, end: 30 })
        );
    }

    #[tokio::test]
    async fn failed_session_write_invalidates_proof_and_new_store_starts_cold() {
        let mut store = store();
        store
            .insert(b"a", 0, &row(store.schema.clone(), 0))
            .await
            .unwrap();
        let current = SessionMeta { start: 0, end: 0 };
        assert_eq!(
            store.cached_proof(b"a").unwrap(),
            Some(GroupProof::Sole(current))
        );
        let old_limit = store.limits.write_bytes;
        store.limits.write_bytes = 1;
        assert!(store.remove_session(b"a", current).await.is_err());
        assert_eq!(store.cached_proof(b"a").unwrap(), Some(GroupProof::Unknown));
        store.limits.write_bytes = old_limit;
        assert_eq!(store.next_session(b"a", None).await.unwrap(), Some(current));
        let recovered = SessionStore::new(
            store.backend.clone(),
            store.table.clone(),
            store.resources.clone(),
            store.schema.clone(),
            store.limits,
            store.gap,
        )
        .unwrap();
        assert_eq!(recovered.cached_proof(b"a").unwrap(), None);
        assert_eq!(
            recovered.next_session(b"a", None).await.unwrap(),
            Some(current)
        );
    }

    #[tokio::test]
    async fn paged_retirement_retries_remaining_repeated_timestamp_rows() {
        let mut store = store();
        for value in 0..11 {
            store
                .insert(b"a", 7, &row(store.schema.clone(), value))
                .await
                .unwrap();
        }
        store.limits.page_entries = 2;
        let session = SessionMeta { start: 7, end: 7 };
        let snapshot = store.snapshot().await.unwrap();
        let first = store
            .next_row(&snapshot, b"a", session, None)
            .await
            .unwrap()
            .unwrap();
        let first_key = first.key.clone();
        drop(first);
        let mut interrupted = AdmittedWriteBatch::try_reserve(
            store.resources.clone(),
            store.limits.write_bytes,
            store.limits.write_operations,
        )
        .unwrap();
        interrupted
            .delete(&store.table.key(first_key, None))
            .unwrap();
        store.backend.write_admitted(interrupted).await.unwrap();
        drop(snapshot);
        // A failed attempt can leave a committed prefix of row deletes. The
        // next attempt scans live state and finishes the remaining pages.
        store.retire(b"a", session).await.unwrap();
        let remaining = store.snapshot().await.unwrap();
        assert!(
            store
                .next_row(&remaining, b"a", session, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(store.first_due(None).await.unwrap(), None);
        store
            .insert(b"a", 8, &row(store.schema.clone(), 99))
            .await
            .unwrap();
        assert_eq!(
            store.next_session(b"a", None).await.unwrap().unwrap().start,
            8
        );
    }

    #[tokio::test]
    async fn paged_retirement_stops_before_next_session_in_same_group() {
        let mut store = store();
        for time in [0, 0, 0, 30] {
            store
                .insert(b"a", time, &row(store.schema.clone(), time))
                .await
                .unwrap();
        }
        store.limits.page_entries = 2;
        store
            .retire(b"a", SessionMeta { start: 0, end: 0 })
            .await
            .unwrap();
        assert_eq!(
            store.next_session(b"a", None).await.unwrap(),
            Some(SessionMeta { start: 30, end: 30 })
        );
        let snapshot = store.snapshot().await.unwrap();
        assert!(
            store
                .next_row(&snapshot, b"a", SessionMeta { start: 30, end: 30 }, None,)
                .await
                .unwrap()
                .is_some()
        );
    }
}
