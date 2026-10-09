//! Fixed lifecycle stages measured with a monotonic clock. No job IDs, storage
//! paths, keys, or future/task owners are retained by these collectors.
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};
use std::future::Future;
use std::sync::OnceLock;
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    ProtocolPublication,
    MetadataPublication,
    WorkerInitialization,
    StartupReadiness,
    RestartReadiness,
}
impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::ProtocolPublication => "protocol_publication",
            Self::MetadataPublication => "metadata_publication",
            Self::WorkerInitialization => "worker_initialization",
            Self::StartupReadiness => "startup_readiness",
            Self::RestartReadiness => "restart_readiness",
        }
    }
}

struct Metrics {
    duration: HistogramVec,
    operations: IntCounterVec,
}
impl Metrics {
    fn register(registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let duration = HistogramVec::new(
            HistogramOpts::new("arroyo_worker_lifecycle_duration_seconds", "Elapsed time within a named worker lifecycle stage, not end-to-end restart readiness")
                .buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0, 60.0, 300.0, 1800.0, 3600.0]),
            &["phase", "outcome"],
        )?;
        let operations = IntCounterVec::new(
            Opts::new(
                "arroyo_worker_lifecycle_operations_total",
                "Worker lifecycle stages completed, failed or cancelled",
            ),
            &["phase", "outcome"],
        )?;
        for phase in [
            Phase::ProtocolPublication,
            Phase::MetadataPublication,
            Phase::WorkerInitialization,
            Phase::StartupReadiness,
            Phase::RestartReadiness,
        ] {
            for outcome in ["success", "error", "cancelled"] {
                duration.with_label_values(&[phase.label(), outcome]);
                operations.with_label_values(&[phase.label(), outcome]);
            }
        }
        registry.register(Box::new(duration.clone()))?;
        registry.register(Box::new(operations.clone()))?;
        Ok(Self {
            duration,
            operations,
        })
    }
}
struct Observation<'a> {
    metrics: &'a Metrics,
    phase: Phase,
    started: Instant,
    outcome: &'static str,
}
impl Drop for Observation<'_> {
    fn drop(&mut self) {
        let labels = &[self.phase.label(), self.outcome];
        self.metrics
            .duration
            .with_label_values(labels)
            .observe(self.started.elapsed().as_secs_f64());
        self.metrics.operations.with_label_values(labels).inc();
    }
}
async fn observe_with<T, E>(
    metrics: &Metrics,
    phase: Phase,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut observation = Observation {
        metrics,
        phase,
        started: Instant::now(),
        outcome: "cancelled",
    };
    let result = future.await;
    observation.outcome = if result.is_ok() { "success" } else { "error" };
    result
}

fn metrics() -> Option<&'static Metrics> {
    static METRICS: OnceLock<Result<Metrics, prometheus::Error>> = OnceLock::new();
    match METRICS.get_or_init(|| Metrics::register(prometheus::default_registry())) {
        Ok(metrics) => Some(metrics),
        Err(error) => {
            tracing::error!(%error, "worker lifecycle metrics unavailable");
            None
        }
    }
}

pub(crate) async fn observe<T, E>(
    phase: Phase,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    match metrics() {
        Some(metrics) => observe_with(metrics, phase, future).await,
        None => future.await,
    }
}

/// Lives with EngineState through leader waiting and execution. It owns only
/// local task identities and a timer, never an engine, future or reservation.
#[derive(Clone)]
pub(crate) struct Readiness(std::sync::Arc<std::sync::Mutex<ReadinessState>>);
struct ReadinessState {
    pending: std::collections::HashSet<(u32, u32)>,
    observation: Option<Observation<'static>>,
}
impl Readiness {
    pub fn new(tasks: impl Iterator<Item = (u32, u32)>, restoring: bool) -> Self {
        let pending: std::collections::HashSet<_> = tasks.collect();
        let observation = metrics().map(|metrics| Observation {
            metrics,
            phase: if restoring {
                Phase::RestartReadiness
            } else {
                Phase::StartupReadiness
            },
            started: Instant::now(),
            outcome: "cancelled",
        });
        let result = Self(std::sync::Arc::new(std::sync::Mutex::new(ReadinessState {
            pending,
            observation,
        })));
        if result
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending
            .is_empty()
        {
            result.finish("success");
        }
        result
    }
    pub fn task_started(&self, task: (u32, u32)) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.remove(&task);
        if state.pending.is_empty()
            && let Some(mut observation) = state.observation.take()
        {
            observation.outcome = "success";
        }
    }
    pub fn failed(&self) {
        self.finish("error");
    }
    fn finish(&self, outcome: &'static str) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut observation) = state.observation.take() {
            observation.outcome = outcome;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readiness_waits_for_all_assigned_tasks_and_records_one_outcome() {
        let metrics = metrics().unwrap();
        let counter = |phase: Phase, outcome| {
            metrics
                .operations
                .with_label_values(&[phase.label(), outcome])
                .get()
        };
        let before = counter(Phase::RestartReadiness, "success");
        let readiness = Readiness::new([(1, 0), (2, 0)].into_iter(), true);
        readiness.task_started((99, 0));
        readiness.task_started((1, 0));
        readiness.task_started((1, 0));
        assert_eq!(counter(Phase::RestartReadiness, "success"), before);
        readiness.task_started((2, 0));
        readiness.task_started((2, 0));
        readiness.failed();
        drop(readiness);
        assert_eq!(counter(Phase::RestartReadiness, "success"), before + 1);
        let before = counter(Phase::RestartReadiness, "error");
        let failed = Readiness::new([(1, 0)].into_iter(), true);
        failed.failed();
        failed.task_started((1, 0));
        drop(failed);
        assert_eq!(counter(Phase::RestartReadiness, "error"), before + 1);
        let before = counter(Phase::RestartReadiness, "cancelled");
        let cancelled = Readiness::new([(1, 0)].into_iter(), true);
        let clone = cancelled.clone();
        drop(cancelled);
        assert_eq!(counter(Phase::RestartReadiness, "cancelled"), before);
        drop(clone);
        assert_eq!(counter(Phase::RestartReadiness, "cancelled"), before + 1);
    }

    #[tokio::test]
    async fn monotonic_stage_outcomes_include_dropped_pending_future() {
        let registry = prometheus::Registry::new();
        let metrics = Metrics::register(&registry).unwrap();
        observe_with(&metrics, Phase::ProtocolPublication, async {
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
        assert!(
            observe_with(&metrics, Phase::MetadataPublication, async {
                Err::<(), _>(())
            })
            .await
            .is_err()
        );
        let mut cancelled = Box::pin(observe_with(
            &metrics,
            Phase::WorkerInitialization,
            std::future::pending::<Result<(), ()>>(),
        ));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        let families = registry.gather();
        for family in families {
            assert_eq!(family.get_metric().len(), 15);
            for metric in family.get_metric() {
                assert_eq!(metric.get_label().len(), 2);
                if let Some(histogram) = metric.get_histogram().as_ref() {
                    assert!(histogram.sample_sum() >= 0.0);
                }
            }
            if family.name() == "arroyo_worker_lifecycle_operations_total" {
                assert_eq!(
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_counter().as_ref().unwrap().value())
                        .sum::<f64>(),
                    3.0
                );
            }
        }
    }
}
