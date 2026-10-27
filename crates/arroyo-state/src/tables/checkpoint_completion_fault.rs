//! Debug-only, explicitly armed withholding of one operator completion.
//! This delays acknowledgement after export; it does not simulate export I/O.
use anyhow::{Context, Result, bail, ensure};
use arroyo_types::TaskInfo;
use std::fmt::Write as _;
use std::io::ErrorKind;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep};

pub(super) async fn pause(task: &TaskInfo, epoch: u32) -> Result<()> {
    let Some(directory) = std::env::var_os("STREAMR_TEST_CHECKPOINT_COMPLETION_FAULT_DIR") else {
        return Ok(());
    };
    let root = Path::new(&directory);
    ensure!(
        tokio::fs::metadata(root).await?.is_dir(),
        "checkpoint completion fault root is not a directory"
    );
    let scope = scope(&task.job_id, &task.operator_id, task.task_index, epoch);
    pause_at(root, &scope, Duration::from_secs(30)).await?;
    Ok(())
}

fn scope(job: &str, operator: &str, subtask: u32, epoch: u32) -> String {
    let hex = |value: &str| {
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.bytes() {
            write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
        }
        encoded
    };
    format!(
        "completion-{}-{}-{subtask}-{epoch}",
        hex(job),
        hex(operator)
    )
}

async fn pause_at(root: &Path, scope: &str, deadline: Duration) -> Result<bool> {
    let request = root.join(format!("{scope}.request"));
    let claimed = root.join(format!("{scope}.claimed"));
    // Creating this link never overwrites an existing claim, including after a
    // recreated request or concurrent call for the same task/epoch.
    match tokio::fs::hard_link(&request, &claimed).await {
        Ok(()) => {}
        Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::AlreadyExists) => {
            return Ok(false);
        }
        Err(error) => return Err(error).context("claim checkpoint completion fault"),
    }
    tokio::fs::remove_file(request)
        .await
        .context("consume checkpoint completion fault request")?;
    let release = root.join(format!("{scope}.release"));
    let started = Instant::now();
    tracing::warn!(
        scope,
        pid = std::process::id(),
        deadline_ms = deadline.as_millis() as u64,
        "Checkpoint completion fault entered"
    );
    loop {
        if started.elapsed() >= deadline {
            bail!("checkpoint completion fault deadline exceeded for {scope}; completion withheld");
        }
        let released = tokio::fs::try_exists(&release).await?;
        if started.elapsed() >= deadline {
            bail!("checkpoint completion fault deadline exceeded for {scope}; completion withheld");
        }
        if released {
            break;
        }
        sleep(Duration::from_millis(10).min(deadline.saturating_sub(started.elapsed()))).await;
    }
    tracing::warn!(
        scope,
        pid = std::process::id(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Checkpoint completion fault released"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_completion_fault_is_exact_and_one_shot() {
        let root = tempfile::tempdir().unwrap();
        let selected = scope("job_a", "state_b", 0, 7);
        let request = root.path().join(format!("{selected}.request"));
        tokio::fs::write(&request, b"").await.unwrap();
        for other in [
            scope("job_c", "state_b", 0, 7),
            scope("job_a", "state_c", 0, 7),
            scope("job_a", "state_b", 1, 7),
            scope("job_a", "state_b", 0, 8),
        ] {
            assert!(!pause_at(root.path(), &other, Duration::ZERO).await.unwrap());
        }
        assert!(request.exists());
        tokio::fs::write(root.path().join(format!("{selected}.release")), b"")
            .await
            .unwrap();
        assert!(
            pause_at(root.path(), &selected, Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert!(!request.exists());
        tokio::fs::write(&request, b"").await.unwrap();
        assert!(
            !pause_at(root.path(), &selected, Duration::ZERO)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn checkpoint_completion_fault_yields_and_releases_once() {
        let root = tempfile::tempdir().unwrap();
        let selected = scope("job_a", "state_b", 0, 7);
        tokio::fs::write(root.path().join(format!("{selected}.request")), b"")
            .await
            .unwrap();
        let directory = root.path().to_path_buf();
        let spawned_scope = selected.clone();
        let task = tokio::spawn(async move {
            pause_at(&directory, &spawned_scope, Duration::from_secs(2)).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !root.path().join(format!("{selected}.claimed")).exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // On this current-thread runtime another task progressed while the
        // acknowledgement remained pending.
        assert!(!task.is_finished());
        tokio::fs::write(root.path().join(format!("{selected}.release")), b"")
            .await
            .unwrap();
        assert!(task.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn checkpoint_completion_fault_deadline_never_acknowledges() {
        let root = tempfile::tempdir().unwrap();
        let selected = scope("job_a", "state_b", 0, 7);
        tokio::fs::write(root.path().join(format!("{selected}.request")), b"")
            .await
            .unwrap();
        let error = pause_at(root.path(), &selected, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("completion withheld"));
        assert!(root.path().join(format!("{selected}.claimed")).exists());
        assert!(!root.path().join(format!("{selected}.release")).exists());
    }

    #[tokio::test]
    async fn checkpoint_completion_fault_missing_request_is_inert() {
        let root = tempfile::tempdir().unwrap();
        assert!(
            !pause_at(root.path(), "absent", Duration::ZERO)
                .await
                .unwrap()
        );
        assert_ne!(scope("a-b", "c", 0, 7), scope("a", "b-c", 0, 7));
    }
}
