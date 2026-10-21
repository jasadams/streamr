//! Opt-in finite scan qualification. No iterator/options/admission changes.
use rocksdb::perf::{PerfContext, PerfMetric, PerfStatsLevel, set_perf_stats};
use serde_json::{Value, json};
use std::sync::OnceLock;

pub(super) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("STREAMR_LIVE_SCAN_DIAGNOSTICS").as_deref() == Ok("1"))
}
static ENABLED: OnceLock<bool> = OnceLock::new();

pub(super) fn emit(value: Value) {
    eprintln!("STREAMR_SCAN_DIAGNOSTIC {value}");
}

pub(super) struct NativeScanProbe {
    perf: PerfContext,
    fields: Value,
    completed: bool,
    next_calls: u64,
    candidates: u64,
}
impl NativeScanProbe {
    /// Must be constructed inside the existing blocking closure immediately
    /// before the native Seek. Its !Send PerfContext never crosses an await.
    pub(super) fn start(fields: Value) -> Self {
        set_perf_stats(PerfStatsLevel::EnableTimeAndCPUTimeExceptForMutex);
        let mut perf = PerfContext::default();
        perf.reset();
        Self {
            perf,
            fields,
            completed: false,
            next_calls: 0,
            candidates: 0,
        }
    }
    pub(super) fn next(&mut self) {
        self.next_calls += 1;
    }
    pub(super) fn candidate(&mut self) {
        self.candidates += 1;
    }
    pub(super) fn complete(&mut self, rows: usize, bytes: usize, continuation: bool) {
        self.fields["returned_rows"] = json!(rows);
        self.fields["returned_encoded_bytes"] = json!(bytes);
        self.fields["continuation"] = json!(continuation);
        self.completed = true;
    }
}
impl Drop for NativeScanProbe {
    fn drop(&mut self) {
        self.fields["event"] = json!("native_scan_page");
        self.fields["success"] = json!(self.completed);
        self.fields["issued_seek_calls"] = json!(1);
        self.fields["issued_next_calls"] = json!(self.next_calls);
        self.fields["valid_candidate_visits"] = json!(self.candidates);
        for (name, metric) in [
            ("SeekChildSeekCount", PerfMetric::SeekChildSeekCount),
            ("SeekOnMemtableCount", PerfMetric::SeekOnMemtableCount),
            (
                "InternalKeySkippedCount",
                PerfMetric::InternalKeySkippedCount,
            ),
            (
                "InternalDeleteSkippedCount",
                PerfMetric::InternalDeleteSkippedCount,
            ),
            (
                "InternalRecentSkippedCount",
                PerfMetric::InternalRecentSkippedCount,
            ),
            ("BlockReadCount", PerfMetric::BlockReadCount),
            ("BlockReadByte", PerfMetric::BlockReadByte),
            ("SeekInternalSeekTime", PerfMetric::SeekInternalSeekTime),
            ("FindNextUserEntryTime", PerfMetric::FindNextUserEntryTime),
        ] {
            self.fields[name] = json!(self.perf.metric(metric));
        }
        // Every ordinary and early-error path restores disabled profiling.
        set_perf_stats(PerfStatsLevel::Disable);
        emit(std::mem::take(&mut self.fields));
    }
}

/// Declared LAST in NativeReadView: the owned snapshot and its permit drop
/// before this marker, including cancelled scans retained by blocking work.
pub(super) struct SnapshotReleased(pub(super) u64);
impl Drop for SnapshotReleased {
    fn drop(&mut self) {
        emit(json!({"event":"native_snapshot_resources_released","snapshot_id":self.0}));
    }
}
