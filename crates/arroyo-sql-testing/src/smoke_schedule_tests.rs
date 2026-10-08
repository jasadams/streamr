//! Optional caller-declared observations of the live initial capture.
//! This releases ordinary source controls; it does not change operator clocks.
use super::*;
use serde::Deserialize;
use tokio::time::Instant;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaptureSchedule {
    max_output_bytes: usize,
    steps: Vec<ScheduleStep>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ScheduleStep {
    Advance {
        at_ms: u64,
        source_row_target: usize,
    },
    Observe {
        at_ms: u64,
        label: String,
    },
}

impl ScheduleStep {
    fn at_ms(&self) -> u64 {
        match self {
            Self::Advance { at_ms, .. } | Self::Observe { at_ms, .. } => *at_ms,
        }
    }
}

impl CaptureSchedule {
    pub(super) fn from_env() -> Result<Option<Self>> {
        let Some(path) = env::var_os("STREAMR_CAPTURE_INITIAL_SCHEDULE") else {
            return Ok(None);
        };
        let path = PathBuf::from(path);
        anyhow::ensure!(path.is_absolute(), "initial schedule path must be absolute");
        anyhow::ensure!(
            env::var("STREAMR_TEST_SOURCE_BATCH_ROWS").as_deref() == Ok("1"),
            "initial schedule requires source batch target 1"
        );
        let file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut std::io::Read::take(file, 65537), &mut bytes)?;
        anyhow::ensure!(bytes.len() <= 65536, "initial schedule exceeds 64 KiB");
        let schedule: Self = serde_json::from_slice(&bytes)?;
        schedule.validate(test_runtime_timeout())?;
        Ok(Some(schedule))
    }

    fn validate(&self, timeout: Duration) -> Result<()> {
        anyhow::ensure!(
            (1..=4 * 1024 * 1024).contains(&self.max_output_bytes),
            "scheduled observation byte limit must be 1..=4194304"
        );
        anyhow::ensure!(
            (1..=64).contains(&self.steps.len()),
            "schedule needs 1..=64 steps"
        );
        let mut previous_time = 0;
        let mut rows = 1;
        let mut labels = HashSet::new();
        for step in &self.steps {
            anyhow::ensure!(
                step.at_ms() >= previous_time
                    && step.at_ms() <= 120000
                    && Duration::from_millis(step.at_ms()) < timeout,
                "schedule times must be nondecreasing and fit within the runtime timeout and 120s"
            );
            previous_time = step.at_ms();
            match step {
                ScheduleStep::Advance {
                    source_row_target, ..
                } => {
                    anyhow::ensure!(
                        *source_row_target > rows,
                        "source row targets must increase beyond the automatic first row"
                    );
                    rows = *source_row_target;
                }
                ScheduleStep::Observe { label, .. } => {
                    anyhow::ensure!(
                        !label.is_empty()
                            && label.len() <= 32
                            && label
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                            && labels.insert(label),
                        "observation labels must be unique ASCII alphanumeric/underscore names of 1..=32 bytes"
                    );
                }
            }
        }
        anyhow::ensure!(
            matches!(self.steps.last(), Some(ScheduleStep::Observe { .. })),
            "schedule must end with an observation before EOF is released"
        );
        Ok(())
    }

    pub(super) async fn validate_input(&self, path: &str) -> Result<()> {
        let mut lines = BufReader::new(File::open(path).await?).lines();
        let mut rows = 0usize;
        while let Some(line) = lines.next_line().await? {
            anyhow::ensure!(!line.is_empty(), "scheduled input has a blank source row");
            rows = rows
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source row count overflow"))?;
        }
        anyhow::ensure!(rows > 0, "scheduled source must not be empty");
        for step in &self.steps {
            if let ScheduleStep::Advance {
                source_row_target, ..
            } = step
            {
                anyhow::ensure!(
                    *source_row_target <= rows,
                    "scheduled source target exceeds available rows"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn run(
        &self,
        engine: &RunningEngine,
        control_rx: &mut Receiver<ControlResp>,
        output: &Path,
        started: Instant,
    ) {
        let sources = engine.source_controls();
        assert_eq!(sources.len(), 1, "schedule requires exactly one source");
        let mut released_rows = 1usize;
        let mut previous_snapshot = Vec::new();
        println!(
            "CAPTURE_SCHEDULE phase=initial event=start epoch=before_engine_start automatic_first_row=true"
        );
        for (index, step) in self.steps.iter().enumerate() {
            let deadline = started + Duration::from_millis(step.at_ms());
            loop {
                tokio::select! {
                    response = control_rx.recv() => check_idle_response(response),
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
            match step {
                ScheduleStep::Advance {
                    source_row_target, ..
                } => {
                    for _ in released_rows..*source_row_target {
                        sources[0]
                            .send(ControlMessage::NoOp)
                            .await
                            .expect("scheduled source closed before its row target");
                    }
                    released_rows = *source_row_target;
                    println!(
                        "CAPTURE_SCHEDULE phase=initial event=release step={index} scheduled_ms={} elapsed_ms={} source_row_target={released_rows} noops_total={}",
                        step.at_ms(),
                        started.elapsed().as_millis(),
                        released_rows - 1
                    );
                }
                ScheduleStep::Observe { label, .. } => {
                    let observed_ms = started.elapsed().as_millis();
                    let (rows, bytes) = self.snapshot(output).await;
                    assert!(
                        bytes.starts_with(&previous_snapshot),
                        "scheduled sink output prefix changed"
                    );
                    let path = output.with_extension(format!("schedule-initial-{label}.jsonl"));
                    tokio::fs::write(&path, &bytes)
                        .await
                        .expect("cannot write scheduled observation");
                    println!(
                        "CAPTURE_SCHEDULE phase=initial event=observe step={index} label={label} scheduled_ms={} observed_ms={observed_ms} completed_ms={} source_row_target={released_rows} rows={rows} bytes={} path={}",
                        step.at_ms(),
                        started.elapsed().as_millis(),
                        bytes.len(),
                        path.display()
                    );
                    previous_snapshot = bytes;
                }
            }
        }
        while let Ok(response) = control_rx.try_recv() {
            check_idle_response(Some(response));
        }
        assert!(
            !control_rx.is_closed(),
            "control channel closed during initial schedule"
        );
    }

    async fn snapshot(&self, output: &Path) -> (usize, Vec<u8>) {
        let file = match File::open(output).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return (0, Vec::new()),
            Err(error) => panic!("cannot read scheduled sink: {error}"),
        };
        let mut bytes = Vec::new();
        file.take(self.max_output_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .expect("cannot read scheduled sink");
        assert!(
            bytes.len() <= self.max_output_bytes,
            "scheduled sink exceeds declared byte limit"
        );
        // A concurrently written final record is not a complete observation.
        // Do not wait for output: doing so would hide an early/late emission.
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        bytes.truncate(complete);
        if bytes.is_empty() {
            return (0, bytes);
        }
        let (rows, _) =
            complete_idle_jsonl_rows(&bytes).expect("complete scheduled JSONL must decode");
        (rows, bytes)
    }
}

#[test]
fn initial_schedule_rejects_invalid_times_targets_and_labels() {
    for body in [
        r#"{"max_output_bytes":1,"steps":[{"kind":"advance","at_ms":1,"source_row_target":1},{"kind":"observe","at_ms":2,"label":"a"}]}"#,
        r#"{"max_output_bytes":1,"steps":[{"kind":"observe","at_ms":2,"label":"a"},{"kind":"observe","at_ms":1,"label":"b"}]}"#,
        r#"{"max_output_bytes":1,"steps":[{"kind":"observe","at_ms":1,"label":"../a"}]}"#,
        r#"{"max_output_bytes":1,"steps":[{"kind":"observe","at_ms":1,"label":"a"},{"kind":"observe","at_ms":2,"label":"a"}]}"#,
    ] {
        let schedule: CaptureSchedule = serde_json::from_str(body).unwrap();
        assert!(schedule.validate(Duration::from_secs(10)).is_err());
    }
}

#[test]
fn initial_schedule_accepts_zero_time_observation_and_increasing_releases() {
    let schedule: CaptureSchedule = serde_json::from_str(r#"{"max_output_bytes":1024,"steps":[{"kind":"observe","at_ms":0,"label":"start"},{"kind":"advance","at_ms":1000,"source_row_target":3},{"kind":"observe","at_ms":1500,"label":"end"}]}"#).unwrap();
    schedule.validate(Duration::from_secs(10)).unwrap();
}
