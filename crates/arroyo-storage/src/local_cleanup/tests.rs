use super::*;
use crate::LocalConfig;
use bytes::Bytes;

struct LocalRoot(std::path::PathBuf);

impl LocalRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "arroyo-empty-cleanup-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    async fn provider(&self) -> StorageProvider {
        StorageProvider::construct_local(LocalConfig {
            path: self.0.to_str().unwrap().to_owned(),
            key: Some("tenant".into()),
        })
        .await
        .unwrap()
    }
}

impl Drop for LocalRoot {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn local_pruning_removes_history_preserving_prefix_key_neighbors_and_bytes() {
    let root = LocalRoot::new();
    let storage = root.provider().await;
    for directory in [
        "tenant/J/checkpoints/old/a/b",
        "tenant/J/checkpoints/old/c",
        "tenant/J/checkpoints/retained/empty",
        "tenant/J/checkpoints-extra/empty",
        "tenant/other/checkpoints/empty",
        "J/checkpoints/unqualified/empty",
    ] {
        std::fs::create_dir_all(root.0.join(directory)).unwrap();
    }
    let key = "J/checkpoints/retained/state";
    storage.put(key, vec![1, 2, 3]).await.unwrap();
    for _ in 0..2 {
        storage
            .prune_empty_directories("J/checkpoints".into())
            .await
            .unwrap();
        assert!(!root.0.join("tenant/J/checkpoints/old").exists());
        assert!(!root.0.join("tenant/J/checkpoints/retained/empty").exists());
        for preserved in [
            "tenant/J/checkpoints",
            "tenant/J/checkpoints-extra/empty",
            "tenant/other/checkpoints/empty",
            "J/checkpoints/unqualified/empty",
        ] {
            assert!(root.0.join(preserved).is_dir(), "{preserved}");
        }
        assert_eq!(storage.get(key).await.unwrap().as_ref(), &[1, 2, 3]);
    }
    storage
        .prune_empty_directories("missing".into())
        .await
        .unwrap();
    assert!(storage.prune_empty_directories("".into()).await.is_err());
    // A real filesystem type error is not swallowed as an empty/missing race.
    let error = storage
        .prune_empty_directories(key.into())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("retained/state"));
}

#[tokio::test]
async fn local_pruning_preserves_real_staged_upload_and_recreates_removed_parents() {
    let root = LocalRoot::new();
    let storage = root.provider().await;
    let key: Path = "J/checkpoints/epoch/operator/table/state".into();
    let mut upload = storage
        .object_store
        .put_multipart(&storage.qualify_path(&key))
        .await
        .unwrap();
    upload
        .put_part(Bytes::from_static(b"checkpoint-state").into())
        .await
        .unwrap();
    let table = root.0.join("tenant/J/checkpoints/epoch/operator/table");
    assert!(
        std::fs::read_dir(&table).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("state#")
        }),
        "real local multipart upload must have a staging directory entry"
    );
    storage
        .prune_empty_directories("J/checkpoints".into())
        .await
        .unwrap();
    assert!(table.is_dir());
    upload.complete().await.unwrap();
    assert_eq!(
        storage.get(key.clone()).await.unwrap().as_ref(),
        b"checkpoint-state"
    );
    storage.delete_if_present(key.clone()).await.unwrap();
    storage
        .prune_empty_directories("J/checkpoints".into())
        .await
        .unwrap();
    assert!(!table.exists());
    storage
        .put(key.clone(), b"late-state".to_vec())
        .await
        .unwrap();
    assert_eq!(storage.get(key).await.unwrap().as_ref(), b"late-state");
}

#[cfg(unix)]
#[tokio::test]
async fn local_pruning_does_not_follow_child_prefix_or_ancestor_symlinks() {
    let root = LocalRoot::new();
    let storage = root.provider().await;
    let outside = root.0.join("outside/empty/descendant");
    std::fs::create_dir_all(&outside).unwrap();
    let prefix = root.0.join("tenant/J/checkpoints");
    std::fs::create_dir_all(&prefix).unwrap();
    std::os::unix::fs::symlink(root.0.join("outside"), prefix.join("linked")).unwrap();
    std::os::unix::fs::symlink(root.0.join("outside"), root.0.join("tenant/alias")).unwrap();
    for key in ["J/checkpoints", "J/checkpoints/linked", "alias/empty"] {
        storage.prune_empty_directories(key.into()).await.unwrap();
        assert!(outside.is_dir());
    }
    assert!(
        prefix
            .join("linked")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );
}
