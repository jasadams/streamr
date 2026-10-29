//! Bounded external sorting for native checkpoint reachability.
//!
//! All candidates are materialized before the caller starts deleting payloads.
//! Scratch ownership is local to one pass; cancelling it only removes scratch.
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub run_bytes: usize,
    pub fan_in: usize,
    pub buffer_bytes: usize,
    pub record_bytes: usize,
    pub scratch_bytes: u64,
    pub input_bytes: u64,
    pub input_records: u64,
    pub disk_reserve_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            run_bytes: 1024 * 1024,
            fan_in: 16,
            buffer_bytes: 64 * 1024,
            record_bytes: 4096,
            scratch_bytes: 4 * 1024 * 1024 * 1024,
            input_bytes: 2 * 1024 * 1024 * 1024,
            input_records: 16_000_000,
            disk_reserve_bytes: 0,
        }
    }
}
#[derive(Debug)]
struct Quota {
    live: u64,
    input: u64,
    records: u64,
    sequence: u64,
    written: u64,
    disk_allowance: u64,
}
#[derive(Clone)]
struct Context {
    directory: Arc<tempfile::TempDir>,
    quota: Arc<Mutex<Quota>>,
    limits: Limits,
}
fn resource(message: &str) -> io::Error {
    io::Error::other(format!(
        "native checkpoint cleanup resource limit: {message}"
    ))
}

#[cfg(unix)]
#[allow(
    clippy::unnecessary_cast,
    reason = "statvfs widths vary across Unix targets"
)]
fn free_bytes(path: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| resource("invalid scratch path"))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: CString is NUL terminated; successful statvfs initializes stat.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}
#[cfg(not(unix))]
fn free_bytes(_path: &Path) -> io::Result<u64> {
    Err(resource("disk accounting requires Unix"))
}
impl Context {
    fn input(&self, value: &str) -> io::Result<()> {
        if value.is_empty()
            || value.len() > self.limits.record_bytes
            || value.contains(['\n', '\r', '\0'])
        {
            return Err(resource("invalid or oversized path record"));
        }
        let mut quota = self
            .quota
            .lock()
            .map_err(|_| resource("poisoned scratch quota"))?;
        let bytes = value.len() as u64 + 1;
        if quota.records >= self.limits.input_records
            || quota.input.saturating_add(bytes) > self.limits.input_bytes
        {
            return Err(resource("input inventory quota exceeded"));
        }
        quota.records += 1;
        quota.input += bytes;
        Ok(())
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        let mut quota = self
            .quota
            .lock()
            .map_err(|_| resource("poisoned scratch quota"))?;
        if quota.live.saturating_add(bytes) > self.limits.scratch_bytes {
            return Err(resource("scratch byte quota exceeded"));
        }
        if quota.disk_allowance < bytes {
            // Recheck before each buffered growth quantum, rather than doing a
            // statvfs syscall for every file descriptor in a large inventory.
            let free = free_bytes(self.directory.path())?;
            let required = bytes
                .saturating_add(self.limits.disk_reserve_bytes)
                .saturating_add(self.limits.buffer_bytes as u64);
            if free < required {
                return Err(resource("scratch disk reserve exhausted"));
            }
            quota.disk_allowance = (free - required)
                .min(self.limits.buffer_bytes as u64)
                .saturating_add(bytes);
        }
        quota.disk_allowance -= bytes;
        quota.live += bytes;
        quota.written = quota.written.saturating_add(bytes);
        Ok(())
    }
    fn create(&self) -> io::Result<Run> {
        let mut quota = self
            .quota
            .lock()
            .map_err(|_| resource("poisoned scratch quota"))?;
        let path = self
            .directory
            .path()
            .join(format!("run-{:016x}", quota.sequence));
        quota.sequence += 1;
        drop(quota);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        Ok(Run {
            path,
            bytes: 0,
            context: self.clone(),
            writer: Some(BufWriter::with_capacity(self.limits.buffer_bytes, file)),
        })
    }
}
struct Run {
    path: PathBuf,
    bytes: u64,
    context: Context,
    writer: Option<BufWriter<File>>,
}
impl Run {
    fn write(&mut self, record: &str) -> io::Result<()> {
        self.context.allocate(record.len() as u64 + 1)?;
        self.bytes += record.len() as u64 + 1;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| resource("writing finished spool"))?;
        writer.write_all(record.as_bytes())?;
        writer.write_all(b"\n")
    }
    fn finish(&mut self) -> io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }
    fn reader(&self) -> io::Result<BufReader<File>> {
        Ok(BufReader::with_capacity(
            self.context.limits.buffer_bytes,
            File::open(&self.path)?,
        ))
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        self.writer.take();
        if std::fs::remove_file(&self.path).is_ok()
            && let Ok(mut quota) = self.context.quota.lock()
        {
            quota.live = quota.live.saturating_sub(self.bytes);
        }
    }
}
struct Sorter {
    context: Context,
    records: Vec<String>,
    payload: usize,
    runs: Vec<(usize, Run)>,
}
impl Sorter {
    fn new(context: Context) -> Self {
        let capacity = (context.limits.run_bytes / 64).clamp(1, 4096);
        Self {
            context,
            records: Vec::with_capacity(capacity),
            payload: 0,
            runs: vec![],
        }
    }
    fn add(&mut self, record: &str) -> io::Result<()> {
        self.context.input(record)?;
        // Fixed record container capacity prevents an unnoticed Vec reallocation.
        let required =
            self.records.capacity() * std::mem::size_of::<String>() + self.payload + record.len();
        if !self.records.is_empty()
            && (required > self.context.limits.run_bytes
                || self.records.len() == self.records.capacity())
        {
            self.flush()?;
        }
        if self.records.capacity() * std::mem::size_of::<String>() + record.len()
            > self.context.limits.run_bytes
        {
            return Err(resource("record cannot fit sort run"));
        }
        let owned = record.to_string();
        self.payload += owned.capacity();
        self.records.push(owned);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        self.records.dedup();
        let mut run = self.context.create()?;
        for record in self.records.drain(..) {
            run.write(&record)?;
        }
        run.finish()?;
        self.payload = 0;
        self.runs.push((0, run));
        // Carry only equal-size levels. Repeatedly merging an old large run
        // with fresh small runs would turn a bounded sorter into quadratic I/O.
        // At most fan_in-1 runs survive at each logarithmic level.
        let fan_in = self.context.limits.fan_in;
        while self.runs.len() >= fan_in {
            let start = self.runs.len() - fan_in;
            let level = self.runs[start].0;
            if !self.runs[start..]
                .iter()
                .all(|(candidate, _)| *candidate == level)
            {
                break;
            }
            let group = self
                .runs
                .split_off(start)
                .into_iter()
                .map(|(_, run)| run)
                .collect();
            self.runs.push((level + 1, merge(&self.context, group)?));
        }
        Ok(())
    }
    fn compact(&mut self) -> io::Result<()> {
        let mut old = std::mem::take(&mut self.runs);
        old.sort_unstable_by_key(|(level, _)| *level);
        let mut input = old.into_iter();
        loop {
            let mut group = input
                .by_ref()
                .take(self.context.limits.fan_in)
                .collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                self.runs.push(group.pop().expect("nonempty merge group"));
                continue;
            }
            let level = group
                .iter()
                .map(|(level, _)| *level)
                .max()
                .expect("nonempty merge group")
                + 1;
            self.runs.push((
                level,
                merge(
                    &self.context,
                    group.into_iter().map(|(_, run)| run).collect(),
                )?,
            ));
        }
        Ok(())
    }
    fn finish(mut self) -> io::Result<Run> {
        self.flush()?;
        while self.runs.len() > 1 {
            self.compact()?;
        }
        match self.runs.pop() {
            Some((_, run)) => Ok(run),
            None => {
                let mut run = self.context.create()?;
                run.finish()?;
                Ok(run)
            }
        }
    }
}
fn read_record(reader: &mut BufReader<File>, limit: usize) -> io::Result<Option<String>> {
    let mut bytes = Vec::with_capacity(limit.min(256));
    let read = reader
        .take((limit + 2) as u64)
        .read_until(b'\n', &mut bytes)?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.last() != Some(&b'\n') || bytes.len() > limit + 1 {
        return Err(resource("corrupt or oversized spool record"));
    }
    bytes.pop();
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| resource("invalid UTF-8 spool record"))
}
fn merge(context: &Context, group: Vec<Run>) -> io::Result<Run> {
    let mut readers = group
        .iter()
        .map(Run::reader)
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader, context.limits.record_bytes)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut output = context.create()?;
    let mut previous = None;
    while let Some(Reverse((record, index))) = heap.pop() {
        if previous.as_ref() != Some(&record) {
            output.write(&record)?;
            previous = Some(record);
        }
        if let Some(next) = read_record(&mut readers[index], context.limits.record_bytes)? {
            heap.push(Reverse((next, index)));
        }
    }
    output.finish()?;
    drop(group);
    Ok(output)
}

pub struct ReachabilitySpool {
    context: Context,
    retained: Sorter,
    candidates: Sorter,
    metadata: Sorter,
}
pub struct DeletionPlan {
    run: Run,
    reader: BufReader<File>,
    metadata: Run,
    metadata_reader: BufReader<File>,
}
impl ReachabilitySpool {
    pub fn configured() -> io::Result<Self> {
        let config = arroyo_rpc::config::config();
        let directory = config
            .worker
            .disk_sql_state
            .as_ref()
            .map(|s| s.directory.as_path());
        let reserve = config
            .worker
            .live_state_resources
            .as_ref()
            .map_or(0, |r| r.disk_reserve_bytes);
        Self::new(
            directory,
            Limits {
                disk_reserve_bytes: reserve,
                ..Limits::default()
            },
        )
    }
    pub fn new(directory: Option<&Path>, limits: Limits) -> io::Result<Self> {
        if limits.fan_in < 2
            || limits.buffer_bytes == 0
            || limits.run_bytes < 4096
            || limits.record_bytes == 0
        {
            return Err(resource("invalid sort limits"));
        }
        let directory = Arc::new(match directory {
            Some(path) => {
                std::fs::create_dir_all(path)?;
                tempfile::Builder::new()
                    .prefix("native-gc-")
                    .tempdir_in(path)?
            }
            None => tempfile::Builder::new().prefix("native-gc-").tempdir()?,
        });
        let context = Context {
            directory,
            quota: Arc::new(Mutex::new(Quota {
                live: 0,
                input: 0,
                records: 0,
                sequence: 0,
                written: 0,
                disk_allowance: 0,
            })),
            limits,
        };
        if free_bytes(context.directory.path())? < limits.disk_reserve_bytes {
            return Err(resource("scratch disk reserve exhausted"));
        }
        Ok(Self {
            retained: Sorter::new(context.clone()),
            candidates: Sorter::new(context.clone()),
            metadata: Sorter::new(context.clone()),
            context,
        })
    }
    pub fn descriptor_bytes(&mut self, bytes: usize) -> io::Result<()> {
        let mut quota = self
            .context
            .quota
            .lock()
            .map_err(|_| resource("poisoned scratch quota"))?;
        if quota.input.saturating_add(bytes as u64) > self.context.limits.input_bytes {
            return Err(resource("input descriptor byte quota exceeded"));
        }
        quota.input += bytes as u64;
        Ok(())
    }
    pub fn retain(&mut self, path: &str) -> io::Result<()> {
        self.retained.add(path)
    }
    pub fn candidate(&mut self, path: &str) -> io::Result<()> {
        self.candidates.add(path)
    }
    pub fn metadata(&mut self, record: &str) -> io::Result<()> {
        self.metadata.add(record)
    }
    pub fn finish(self) -> io::Result<DeletionPlan> {
        let retained = self.retained.finish()?;
        let candidates = self.candidates.finish()?;
        let mut marks = retained.reader()?;
        let mut source = candidates.reader()?;
        let mut mark = read_record(&mut marks, self.context.limits.record_bytes)?;
        let mut selected = self.context.create()?;
        while let Some(candidate) = read_record(&mut source, self.context.limits.record_bytes)? {
            while mark.as_ref().is_some_and(|m| m < &candidate) {
                mark = read_record(&mut marks, self.context.limits.record_bytes)?;
            }
            if mark.as_ref() != Some(&candidate) {
                selected.write(&candidate)?;
            }
        }
        selected.finish()?;
        let reader = selected.reader()?;
        let metadata = self.metadata.finish()?;
        let metadata_reader = metadata.reader()?;
        Ok(DeletionPlan {
            run: selected,
            reader,
            metadata,
            metadata_reader,
        })
    }
}
impl Iterator for DeletionPlan {
    type Item = io::Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        read_record(&mut self.reader, self.run.context.limits.record_bytes).transpose()
    }
}

impl DeletionPlan {
    pub fn next_metadata(&mut self) -> io::Result<Option<String>> {
        read_record(
            &mut self.metadata_reader,
            self.metadata.context.limits.record_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_union_uses_multiple_passes_and_keeps_shared_references() {
        let directory = tempfile::tempdir().unwrap();
        let limits = Limits {
            run_bytes: 4096,
            fan_in: 2,
            buffer_bytes: 64,
            record_bytes: 128,
            ..Limits::default()
        };
        let mut spool = ReachabilitySpool::new(Some(directory.path()), limits).unwrap();
        for key in (0..2000).rev() {
            let path = format!("job/owner/native-attempt/{key:06}.sst");
            spool.candidate(&path).unwrap();
            spool.candidate(&path).unwrap();
            if key % 3 == 0 {
                spool.retain(&path).unwrap();
            }
        }
        spool.metadata("00000000000000000002|manifest2").unwrap();
        spool.metadata("00000000000000000001|manifest1").unwrap();
        let mut plan = spool.finish().unwrap();
        let actual = plan.by_ref().collect::<io::Result<Vec<_>>>().unwrap();
        let expected = (0..2000)
            .filter(|key| key % 3 != 0)
            .map(|key| format!("job/owner/native-attempt/{key:06}.sst"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(
            plan.next_metadata().unwrap().unwrap(),
            "00000000000000000001|manifest1"
        );
        assert_eq!(
            plan.next_metadata().unwrap().unwrap(),
            "00000000000000000002|manifest2"
        );
        assert!(plan.next_metadata().unwrap().is_none());
        drop(plan);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[test]
    fn quota_record_disk_and_cancellation_fail_without_leaving_scratch() {
        let directory = tempfile::tempdir().unwrap();
        let limits = Limits {
            input_records: 1,
            ..Limits::default()
        };
        let mut spool = ReachabilitySpool::new(Some(directory.path()), limits).unwrap();
        spool.candidate("one").unwrap();
        assert!(spool.candidate("two").is_err());
        drop(spool);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let mut spool = ReachabilitySpool::new(
            Some(directory.path()),
            Limits {
                scratch_bytes: 2,
                ..Limits::default()
            },
        )
        .unwrap();
        spool.candidate("long-candidate").unwrap();
        assert!(spool.finish().is_err());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        assert!(
            ReachabilitySpool::new(
                Some(directory.path()),
                Limits {
                    disk_reserve_bytes: u64::MAX,
                    ..Limits::default()
                }
            )
            .is_err()
        );
        let mut spool = ReachabilitySpool::new(Some(directory.path()), Limits::default()).unwrap();
        assert!(spool.retain("bad\nrecord").is_err());
        assert!(spool.retain(&"x".repeat(4097)).is_err());
        drop(spool);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[cfg(test)]
mod merge_work_tests {
    use super::*;
    #[test]
    fn leveled_spooling_bounds_completed_write_work_for_long_streams() {
        let directory = tempfile::tempdir().unwrap();
        let limits = Limits {
            run_bytes: 4096,
            fan_in: 2,
            buffer_bytes: 64,
            record_bytes: 128,
            ..Limits::default()
        };
        let mut spool = ReachabilitySpool::new(Some(directory.path()), limits).unwrap();
        let context = spool.context.clone();
        let mut input_bytes = 0u64;
        for key in (0..20_000).rev() {
            let path = format!("job/owner/native-attempt/{key:06}.sst");
            input_bytes += path.len() as u64 + 1;
            spool.candidate(&path).unwrap();
        }
        let plan = spool.finish().unwrap();
        assert_eq!(
            plan.map(|path| {
                path.unwrap();
                1usize
            })
            .sum::<usize>(),
            20_000
        );
        let written = context.quota.lock().unwrap().written;
        assert!(
            written <= input_bytes * 14,
            "completed spool I/O must grow by logarithmic merge levels: input={input_bytes} written={written}"
        );
        drop(context);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
