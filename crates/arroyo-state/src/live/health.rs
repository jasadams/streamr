//! Cached, fixed-label observations. Scrapes do no database or filesystem I/O
//! and weak registrations never keep an attempt (or its reservations) alive.
use super::resources::available_disk_bytes;
use prometheus::{IntGaugeVec, Opts};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

#[derive(Clone, Copy, Default)]
pub(crate) struct LogicalSize {
    pub keys: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

pub(crate) struct BackendHealth {
    pub backend: &'static str,
    pub logical: Mutex<Option<LogicalSize>>,
    native: Mutex<NativeSample>,
}

#[derive(Default)]
struct NativeSample {
    at: Option<Instant>,
    sst_bytes: Option<u64>,
    free_bytes: Option<u64>,
    write_stopped: Option<u64>,
    delayed_write_bytes_per_second: Option<u64>,
}

impl BackendHealth {
    pub fn new(backend: &'static str, known: bool) -> Arc<Self> {
        Arc::new(Self {
            backend,
            logical: Mutex::new(known.then(LogicalSize::default)),
            native: Mutex::new(NativeSample::default()),
        })
    }

    /// Called only within existing admitted blocking work. Property failures are
    /// represented as unavailable, never as a zero measurement.
    pub fn sample_native(&self, db: &rocksdb::DB, path: &Path) {
        let property = |name| db.property_int_value(name).ok().flatten();
        let sample = NativeSample {
            at: Some(Instant::now()),
            sst_bytes: property("rocksdb.total-sst-files-size"),
            free_bytes: available_disk_bytes(path).ok(),
            write_stopped: property("rocksdb.is-write-stopped"),
            delayed_write_bytes_per_second: property("rocksdb.actual-delayed-write-rate"),
        };
        *self.native.lock().unwrap_or_else(|e| e.into_inner()) = sample;
    }
}

pub(crate) struct HealthMetrics {
    observed: Mutex<Vec<Weak<BackendHealth>>>,
    keys: IntGaugeVec,
    bytes: IntGaugeVec,
    native: IntGaugeVec,
    availability: IntGaugeVec,
}

impl HealthMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let keys = IntGaugeVec::new(
            Opts::new(
                "arroyo_live_state_logical_keys",
                "Current live records across known attempts; check logical availability",
            ),
            &["backend"],
        )?;
        let bytes = IntGaugeVec::new(
            Opts::new(
                "arroyo_live_state_logical_bytes",
                "Current unencoded logical key/value payload bytes, excluding namespace/routing/encoding/allocator overhead; check logical availability",
            ),
            &["backend", "part"],
        )?;
        let native = IntGaugeVec::new(
            Opts::new(
                "arroyo_live_state_native_health",
                "Cached native observations: total SST file lengths in bytes (excludes WAL/manifests/snapshots); minimum filesystem free bytes; stopped database count; delayed write bytes per second; maximum sample age in milliseconds. Consult availability; absent properties are excluded",
            ),
            &["measurement"],
        )?;
        let availability = IntGaugeVec::new(
            Opts::new(
                "arroyo_live_state_health_observations",
                "Number of live databases with available/unavailable observations; memory databases only participate in logical measurements",
            ),
            &["measurement", "outcome"],
        )?;
        for backend in ["memory", "rocksdb"] {
            keys.with_label_values(&[backend]);
            for part in ["key", "value"] {
                bytes.with_label_values(&[backend, part]);
            }
        }
        for measurement in [
            "logical",
            "sst_bytes",
            "free_bytes",
            "write_stopped",
            "delayed_write_bytes_per_second",
            "sample_age_milliseconds",
        ] {
            for outcome in ["available", "unavailable"] {
                availability.with_label_values(&[measurement, outcome]);
            }
            if measurement != "logical" {
                native.with_label_values(&[measurement]);
            }
        }
        Ok(Self {
            observed: Mutex::new(Vec::new()),
            keys,
            bytes,
            native,
            availability,
        })
    }

    pub fn observe(&self, health: &Arc<BackendHealth>) {
        let mut observed = self.observed.lock().unwrap_or_else(|e| e.into_inner());
        observed.retain(|entry| entry.strong_count() != 0);
        observed.push(Arc::downgrade(health));
    }
}

impl prometheus::core::Collector for HealthMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut result = self.keys.desc();
        result.extend(self.bytes.desc());
        result.extend(self.native.desc());
        result.extend(self.availability.desc());
        result
    }
    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut observed = self.observed.lock().unwrap_or_else(|e| e.into_inner());
        observed.retain(|entry| entry.strong_count() != 0);
        let live: Vec<_> = observed.iter().filter_map(Weak::upgrade).collect();
        let set = |gauge: &IntGaugeVec, labels: &[&str], value: u64| {
            gauge
                .with_label_values(labels)
                .set(value.min(i64::MAX as u64) as i64)
        };
        let mut logical_available = 0;
        for backend in ["memory", "rocksdb"] {
            let mut total = LogicalSize::default();
            for health in live.iter().filter(|health| health.backend == backend) {
                if let Some(size) = *health.logical.lock().unwrap_or_else(|e| e.into_inner()) {
                    logical_available += 1;
                    total.keys = total.keys.saturating_add(size.keys);
                    total.key_bytes = total.key_bytes.saturating_add(size.key_bytes);
                    total.value_bytes = total.value_bytes.saturating_add(size.value_bytes);
                }
            }
            set(&self.keys, &[backend], total.keys);
            set(&self.bytes, &[backend, "key"], total.key_bytes);
            set(&self.bytes, &[backend, "value"], total.value_bytes);
        }
        set(
            &self.availability,
            &["logical", "available"],
            logical_available,
        );
        set(
            &self.availability,
            &["logical", "unavailable"],
            live.len() as u64 - logical_available,
        );
        for measurement in [
            "sst_bytes",
            "free_bytes",
            "write_stopped",
            "delayed_write_bytes_per_second",
            "sample_age_milliseconds",
        ] {
            let mut count = 0;
            let mut total = None::<u64>;
            let mut native_count = 0;
            for health in live.iter().filter(|health| health.backend == "rocksdb") {
                native_count += 1;
                let sample = health.native.lock().unwrap_or_else(|e| e.into_inner());
                let value = match measurement {
                    "sst_bytes" => sample.sst_bytes,
                    "free_bytes" => sample.free_bytes,
                    "write_stopped" => sample.write_stopped,
                    "delayed_write_bytes_per_second" => sample.delayed_write_bytes_per_second,
                    _ => sample
                        .at
                        .map(|at| at.elapsed().as_millis().min(u64::MAX as u128) as u64),
                };
                if let Some(value) = value {
                    count += 1;
                    total = Some(match total {
                        None => value,
                        Some(previous) if measurement == "free_bytes" => previous.min(value),
                        Some(previous) if measurement == "sample_age_milliseconds" => {
                            previous.max(value)
                        }
                        Some(previous) => previous.saturating_add(value),
                    });
                }
            }
            set(&self.native, &[measurement], total.unwrap_or(0));
            set(&self.availability, &[measurement, "available"], count);
            set(
                &self.availability,
                &[measurement, "unavailable"],
                native_count - count,
            );
        }
        let mut result = self.keys.collect();
        result.extend(self.bytes.collect());
        result.extend(self.native.collect());
        result.extend(self.availability.collect());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;

    #[test]
    fn fixed_series_aggregate_known_attempts_without_retaining_them() {
        let metrics = HealthMetrics::new().unwrap();
        let before = metrics.collect();
        let memory = BackendHealth::new("memory", true);
        *memory.logical.lock().unwrap() = Some(LogicalSize {
            keys: 2,
            key_bytes: 5,
            value_bytes: 9,
        });
        metrics.observe(&memory);
        let unknown = BackendHealth::new("rocksdb", false);
        metrics.observe(&unknown);
        let during = metrics.collect();
        let cardinality = |families: &[prometheus::proto::MetricFamily]| {
            families
                .iter()
                .map(|family| family.get_metric().len())
                .sum::<usize>()
        };
        assert_eq!(cardinality(&before), cardinality(&during));
        for family in during {
            for metric in family.get_metric() {
                for label in metric.get_label() {
                    assert!(match label.name() {
                        "backend" => ["memory", "rocksdb"].contains(&label.value()),
                        "part" => ["key", "value"].contains(&label.value()),
                        "outcome" => ["available", "unavailable"].contains(&label.value()),
                        "measurement" => [
                            "logical",
                            "sst_bytes",
                            "free_bytes",
                            "write_stopped",
                            "delayed_write_bytes_per_second",
                            "sample_age_milliseconds"
                        ]
                        .contains(&label.value()),
                        _ => false,
                    });
                }
            }
        }
        let weak = Arc::downgrade(&memory);
        drop(memory);
        drop(unknown);
        assert!(weak.upgrade().is_none());
        metrics.collect();
        assert!(metrics.observed.lock().unwrap().is_empty());
        assert_eq!(metrics.keys.with_label_values(&["memory"]).get(), 0);
        assert_eq!(
            metrics
                .availability
                .with_label_values(&["logical", "unavailable"])
                .get(),
            0
        );
    }
}
