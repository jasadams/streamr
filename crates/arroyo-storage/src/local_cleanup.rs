use crate::{BackendConfig, StorageProvider};
use arroyo_rpc::errors::StorageError;
use object_store::path::Path;
use std::io::ErrorKind;

fn path_error(path: &std::path::Path, error: std::io::Error) -> StorageError {
    StorageError::PathError(format!(
        "empty-directory cleanup at {}: {error}",
        path.display()
    ))
}

impl StorageProvider {
    /// Remove empty descendants of a local prefix, preserving the prefix itself.
    ///
    /// Object stores have no directories. Local traversal never unlinks files or
    /// follows child symlinks. Like the underlying local object store, this
    /// assumes a trusted root, not adversarial concurrent symlink replacement.
    pub async fn prune_empty_directories(&self, prefix: Path) -> Result<(), StorageError> {
        let BackendConfig::Local(config) = &self.config else {
            return Ok(());
        };
        if prefix.as_ref().is_empty() {
            return Err(StorageError::PathError("empty pruning prefix".into()));
        }
        // Match the local object store's canonical root, then qualify once.
        let mut directory = tokio::fs::canonicalize(&config.path)
            .await
            .map_err(|error| path_error(std::path::Path::new(&config.path), error))?;
        for part in self.qualify_path(&prefix).parts() {
            directory.push(part.as_ref());
            match tokio::fs::symlink_metadata(&directory).await {
                Ok(metadata) if metadata.file_type().is_symlink() => return Ok(()),
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(path_error(&directory, error)),
            }
        }
        let root = directory.clone();
        let entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(path_error(&directory, error)),
        };
        // Only active ancestors are retained: O(depth) paths and ReadDir handles.
        // Each await is one filesystem operation, not a detached tree walk.
        let mut stack = vec![(directory, entries)];
        while let Some((directory, entries)) = stack.last_mut() {
            let entry = match entries.next_entry().await {
                Ok(entry) => entry,
                Err(error) if error.kind() == ErrorKind::NotFound => None,
                Err(error) => return Err(path_error(directory, error)),
            };
            if let Some(entry) = entry {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => return Err(path_error(&path, error)),
                };
                if file_type.is_dir() {
                    match tokio::fs::read_dir(&path).await {
                        Ok(entries) => stack.push((path, entries)),
                        Err(error) if error.kind() == ErrorKind::NotFound => {}
                        Err(error) => return Err(path_error(&path, error)),
                    }
                }
            } else {
                let (path, entries) = stack.pop().expect("active directory frame");
                drop(entries);
                if path != root {
                    match tokio::fs::remove_dir(&path).await {
                        Ok(()) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                            ) => {}
                        Err(error) => return Err(path_error(&path, error)),
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
