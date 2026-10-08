//! Explicit attempt-scoped local directories. Existing state is only opened via
//! `reopen`, never silently reused by a fresh attempt.
use super::{LiveStateError, Result};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RocksStateConfig {
    pub root: PathBuf,
    pub job_id: String,
    pub operator_id: String,
    pub subtask: u32,
    pub generation: u64,
    pub attempt: u32,
}

impl RocksStateConfig {
    pub fn path(&self) -> PathBuf {
        self.root
            .join(component(&self.job_id))
            .join(component(&self.operator_id))
            .join(format!("subtask-{}", self.subtask))
            .join(format!("generation-{}", self.generation))
            .join(format!("attempt-{}", self.attempt))
    }

    fn ownership_marker(&self) -> Vec<u8> {
        format!(
            "arroyo-live-state-v1\n{}\n{}\n{}\n{}\n{}\n",
            component(&self.job_id),
            component(&self.operator_id),
            self.subtask,
            self.generation,
            self.attempt
        )
        .into_bytes()
    }

    pub(crate) fn prepare(&self, reopen: bool) -> Result<PathBuf> {
        if self.job_id.is_empty() || self.operator_id.is_empty() {
            return Err(LiveStateError::Backend("empty state identity".into()));
        }
        let path = self.path();
        if reopen {
            // Validate the complete immutable attempt identity, not just a
            // RocksDB manifest copied from an unrelated task.
            let expected = self.ownership_marker();
            let file = std::fs::File::open(path.join("OWNERSHIP")).map_err(io_error)?;
            let mut marker = Vec::with_capacity(expected.len() + 1);
            file.take((expected.len() + 1) as u64)
                .read_to_end(&mut marker)
                .map_err(io_error)?;
            if marker != expected {
                return Err(LiveStateError::Backend(
                    "live-state ownership marker mismatch".into(),
                ));
            }
            if !path.join("CURRENT").is_file() {
                return Err(LiveStateError::Backend(format!(
                    "no database to reopen at {}",
                    path.display()
                )));
            }
        } else {
            let parent = path.parent().expect("attempt path has a parent");
            std::fs::create_dir_all(parent).map_err(io_error)?;
            std::fs::create_dir(&path).map_err(io_error)?;
            let marker = (|| {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path.join("OWNERSHIP"))
                    .map_err(io_error)?;
                file.write_all(&self.ownership_marker()).map_err(io_error)?;
                file.sync_all().map_err(io_error)
            })();
            if let Err(error) = marker {
                let _ = std::fs::remove_dir_all(&path);
                return Err(error);
            }
        }
        Ok(path)
    }
}

fn component(value: &str) -> String {
    // Exact, collision-free, filesystem-safe identity (including slashes and
    // dot components in externally supplied identifiers).
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn io_error(error: std::io::Error) -> LiveStateError {
    LiveStateError::Backend(error.to_string())
}

pub(crate) fn directory_bytes(path: &Path) -> Result<u64> {
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(path).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let metadata = entry.metadata().map_err(io_error)?;
        let size = if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
        bytes = bytes.saturating_add(size);
    }
    Ok(bytes)
}
