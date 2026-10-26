use super::*;
#[tokio::test]
async fn cleans_abandoned_pages_only_below_retained_minimum_and_for_owned_operator() {
    let root = tempfile::tempdir().unwrap();
    let storage =
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.path().display()))
            .await
            .unwrap();
    let abandoned = "J/checkpoints/checkpoint-0000001/operator-o/table-m-000/disk-orphan.bin";
    let retained = "J/checkpoints/checkpoint-0000002/operator-o/table-m-000/disk-keep.bin";
    let active = "J/checkpoints/checkpoint-0000003/operator-o/table-m-000/disk-upload.bin";
    let foreign = "J/checkpoints/checkpoint-0000001/operator-other/table-m-000/disk-foreign.bin";
    let legacy = "J/checkpoints/checkpoint-0000001/operator-o/table-legacy-000";
    for path in [abandoned, retained, active, foreign, legacy] {
        storage.put(path, vec![1]).await.unwrap();
    }
    cleanup_abandoned_controller_disk_files(&storage, "J", "o", 2, &HashSet::new())
        .await
        .unwrap();
    assert!(!storage.exists(abandoned).await.unwrap());
    for path in [retained, active, foreign, legacy] {
        assert!(storage.exists(path).await.unwrap());
    }
    cleanup_abandoned_controller_disk_files(&storage, "J", "o", 2, &HashSet::new())
        .await
        .unwrap();
}

use std::collections::BTreeMap;
use std::path::Path;
use std::task::Poll;

type ObjectMap = BTreeMap<String, Vec<u8>>;

fn write_objects(root: &Path, objects: &ObjectMap) {
    for (key, bytes) in objects {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}

fn read_objects(root: &Path) -> ObjectMap {
    fn visit(root: &Path, directory: &Path, objects: &mut ObjectMap) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, objects);
            } else {
                objects.insert(
                    path.strip_prefix(root).unwrap().to_str().unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut objects = BTreeMap::new();
    visit(root, root, &mut objects);
    objects
}

async fn provider(root: &Path) -> arroyo_storage::StorageProvider {
    arroyo_storage::StorageProvider::for_url(&format!("file://{}", root.display()))
        .await
        .unwrap()
}

#[tokio::test]
async fn exact_cleanup_oracle_retention_isolation_and_late_uploads() {
    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let mut expected = ObjectMap::new();
    let mut retained = HashSet::new();
    let mut orphans: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for job in ["J", "J-other"] {
        for operator in ["o", "p", "other"] {
            for epoch in 1..=4 {
                for table in ["a", "b"] {
                    let base = format!(
                        "{job}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/table-{table}"
                    );
                    for name in ["disk-orphan.bin", "disk-reference.bin", "ordinary.bin"] {
                        let key = format!("{base}/{name}");
                        expected.insert(key.clone(), key.as_bytes().to_vec());
                        if job == "J" && epoch < 3 {
                            if name == "disk-reference.bin" {
                                retained.insert(key.clone());
                            } else if name == "disk-orphan.bin" {
                                orphans.entry(operator).or_default().push(key);
                            }
                        }
                    }
                }
            }
        }
    }
    for key in [
        "unrelated/object",
        "J/checkpoints-extra/checkpoint-1/operator-o/table-a/disk-neighbor",
        "J/checkpoints/checkpoint-invalid/operator-o/table-a/disk-invalid",
        "J/checkpoints/checkpoint-4294967296/operator-o/table-a/disk-overflow",
        "J/checkpoints/checkpoint-1/operator-o/table-a/disk-extra/child",
        "J/checkpoints/checkpoint-1/operator-o/table-file",
        "J/checkpoints/checkpoint-1/operator-o/not-table/disk-not-table",
        "J/checkpoints/checkpoint-1/operator-other-prefix/table-a/disk-other",
    ] {
        expected.insert(key.into(), key.as_bytes().to_vec());
    }
    write_objects(root.path(), &expected);
    std::fs::create_dir_all(root.path().join("J/checkpoints/checkpoint-0/empty-table")).unwrap();
    for operator in ["o", "p"] {
        cleanup_abandoned_controller_disk_files(&storage, "J", operator, 3, &retained)
            .await
            .unwrap();
        for orphan in &orphans[operator] {
            expected.remove(orphan).unwrap();
        }
        assert_eq!(read_objects(root.path()), expected);
    }
    cleanup_abandoned_controller_disk_files(&storage, "J", "o", 3, &retained)
        .await
        .unwrap();
    assert_eq!(read_objects(root.path()), expected);

    let old = "J/checkpoints/checkpoint-1/operator-o/table-a/disk-late";
    let active = "J/checkpoints/checkpoint-4/operator-o/table-a/disk-active";
    for key in [old, active] {
        storage.put(key, vec![9]).await.unwrap();
    }
    // A fresh owner must find late uploads without any cached inventory.
    let fresh = provider(root.path()).await;
    cleanup_abandoned_controller_disk_files(&fresh, "J", "o", 3, &retained)
        .await
        .unwrap();
    expected.insert(active.into(), vec![9]);
    assert_eq!(read_objects(root.path()), expected);
    for missing in ["absent", "empty"] {
        if missing == "empty" {
            std::fs::create_dir_all(root.path().join("empty/checkpoints")).unwrap();
        }
        cleanup_abandoned_controller_disk_files(&fresh, missing, "o", 3, &retained)
            .await
            .unwrap();
        assert_eq!(read_objects(root.path()), expected);
    }
}

#[tokio::test]
async fn observed_partial_cleanup_retries_with_fresh_storage_owner() {
    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let orphan_keys: Vec<_> = (0..256)
        .map(|i| format!("J/checkpoints/checkpoint-1/operator-o/table-a/disk-{i:04}"))
        .collect();
    let kept = "J/checkpoints/checkpoint-3/operator-o/table-a/disk-keep";
    let mut objects: ObjectMap = orphan_keys
        .iter()
        .map(|key| (key.clone(), vec![3]))
        .collect();
    objects.insert(kept.into(), vec![7]);
    write_objects(root.path(), &objects);
    let retained = HashSet::new();
    let mut cleanup = Box::pin(cleanup_abandoned_controller_disk_files(
        &storage, "J", "o", 3, &retained,
    ));
    let partial = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let poll = futures::poll!(cleanup.as_mut());
            let deleted = orphan_keys
                .iter()
                .filter(|key| !root.path().join(key).exists())
                .count();
            if deleted > 0 && deleted < orphan_keys.len() {
                break deleted;
            }
            assert!(
                matches!(poll, Poll::Pending),
                "cleanup finished before interruption was observed"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must observe real partial deletion before aborting");
    drop(cleanup);
    // A pending local delete may finish after dropping the future. Retrying
    // remains safe because delete_if_present treats NotFound as success.
    let fresh = provider(root.path()).await;
    cleanup_abandoned_controller_disk_files(&fresh, "J", "o", 3, &HashSet::new())
        .await
        .unwrap();
    assert_eq!(
        read_objects(root.path()),
        ObjectMap::from([(kept.into(), vec![7])])
    );
    eprintln!("STR156_INTERRUPTION observed_deleted={partial} total=256 final_deleted=256");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn prefix_listing_physically_excludes_unrelated_directories() {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    let root = tempfile::tempdir().unwrap();
    let storage = provider(root.path()).await;
    let watched = [
        "J/checkpoints",
        "J/checkpoints/checkpoint-empty",
        "J-other/checkpoints",
        "J-other/checkpoints/checkpoint-empty",
        "J/checkpoints-extra",
    ];
    for path in watched {
        std::fs::create_dir_all(root.path().join(path)).unwrap();
    }
    storage
        .put(
            "J/checkpoints/checkpoint-1/operator-o/table-a/disk-old",
            vec![1],
        )
        .await
        .unwrap();
    storage
        .put(
            "J-other/checkpoints/checkpoint-1/operator-o/table-a/disk-other",
            vec![2],
        )
        .await
        .unwrap();
    // SAFETY: inotify_init1 has no pointer arguments; ownership is transferred
    // exactly once to OwnedFd after validating the descriptor.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    assert!(fd >= 0, "{}", std::io::Error::last_os_error());
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut descriptors = BTreeMap::new();
    for directory in watched {
        let path = CString::new(root.path().join(directory).as_os_str().as_bytes()).unwrap();
        // SAFETY: path is NUL terminated and remains valid through this call.
        let wd = unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), libc::IN_OPEN) };
        assert!(wd >= 0, "{}", std::io::Error::last_os_error());
        descriptors.insert(wd, directory);
    }
    use std::io::Write;
    eprintln!("STR156_CLEANUP_START");
    std::io::stderr().flush().unwrap();
    cleanup_abandoned_controller_disk_files(&storage, "J", "o", 3, &HashSet::new())
        .await
        .unwrap();
    eprintln!("STR156_CLEANUP_END");
    std::io::stderr().flush().unwrap();
    let mut opened = HashSet::new();
    let mut buffer = [0_u8; 16384];
    loop {
        // SAFETY: buffer is writable for exactly its supplied length.
        let count = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count < 0 {
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            break;
        }
        assert!(count > 0);
        let mut offset = 0;
        while offset < count as usize {
            assert!(offset + std::mem::size_of::<libc::inotify_event>() <= count as usize);
            // SAFETY: the bounds above cover the fixed header; read_unaligned
            // avoids imposing alignment on a byte buffer returned by read.
            let event = unsafe {
                std::ptr::read_unaligned(buffer.as_ptr().add(offset).cast::<libc::inotify_event>())
            };
            assert_eq!(
                event.mask & libc::IN_Q_OVERFLOW,
                0,
                "inotify overflow invalidates evidence"
            );
            if event.mask & libc::IN_OPEN != 0 && event.mask & libc::IN_ISDIR != 0 {
                opened.insert(*descriptors.get(&event.wd).unwrap());
            }
            offset += std::mem::size_of::<libc::inotify_event>() + event.len as usize;
            assert!(offset <= count as usize);
        }
    }
    assert!(opened.contains("J/checkpoints"));
    assert!(opened.contains("J/checkpoints/checkpoint-empty"));
    let expect_root_scan = std::env::var_os("STR156_EXPECT_ROOT_SCAN").is_some();
    for unrelated in &watched[2..] {
        assert_eq!(
            opened.contains(unrelated),
            expect_root_scan,
            "directory {unrelated}: {opened:?}"
        );
    }
    // Inotify can coalesce events: this is intentionally zero/nonzero evidence,
    // never a claim about exact directory-open or returned-object counts.
    eprintln!("STR156_PHYSICAL root_scan={expect_root_scan} opened={opened:?}");
}

#[cfg(target_os = "linux")]
fn process_usage() -> libc::rusage {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied writable rusage on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    unsafe { usage.assume_init() }
}

#[cfg(target_os = "linux")]
fn io_counters() -> BTreeMap<String, u64> {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_owned(), value.trim().parse().unwrap())
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn host_counters() -> BTreeMap<String, serde_json::Value> {
    [
        "/proc/stat",
        "/proc/loadavg",
        "/proc/pressure/cpu",
        "/proc/pressure/io",
        "/proc/pressure/memory",
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/memory.current",
    ]
    .into_iter()
    .map(|path| {
        let value = match std::fs::read_to_string(path) {
            Ok(value) => serde_json::json!({"value": value}),
            Err(error) => serde_json::json!({"unavailable": error.to_string()}),
        };
        (path.to_owned(), value)
    })
    .collect()
}

/// One arm only. The external, queue-owning runner supplies ABBA order,
/// watchdogs, immutable binaries, contention samples and separate strace phases.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in finite STR-156 release comparison; requires evidence paths"]
async fn cleanup_prefix_benchmark() {
    use futures::TryStreamExt;
    use futures::stream::FuturesUnordered;
    use std::io::Write;
    use std::time::{Duration, Instant};

    let evidence_root = std::env::var("STR156_BENCH_ROOT").expect("STR156_BENCH_ROOT required");
    let receipt_path = std::env::var("STR156_RECEIPT").expect("STR156_RECEIPT required");
    assert!(Path::new(&evidence_root).is_absolute());
    assert!(Path::new(&receipt_path).is_absolute());
    assert!(
        !Path::new(&receipt_path).exists(),
        "do not overwrite a previous arm receipt"
    );
    let root = tempfile::Builder::new()
        .prefix("str156-arm-")
        .tempdir_in(evidence_root)
        .unwrap();
    let storage = provider(root.path()).await;
    let mut objects = ObjectMap::new();
    let mut expected_deleted = HashSet::new();
    let mut retained = HashSet::new();
    for job_index in 0..32 {
        let job = if job_index == 0 {
            "J".to_owned()
        } else {
            format!("unrelated-{job_index:02}")
        };
        for epoch in 0..16 {
            for operator in 0..4 {
                for table in 0..2 {
                    for name in ["disk-orphan", "disk-reference", "ordinary"] {
                        let key = format!(
                            "{job}/checkpoints/checkpoint-{epoch:07}/operator-{operator}/table-{table}/{name}"
                        );
                        objects.insert(key.clone(), vec![42; 32]);
                        if job_index == 0 && epoch < 8 {
                            if name == "disk-orphan" {
                                expected_deleted.insert(key);
                            } else if name == "disk-reference" {
                                retained.insert(key);
                            }
                        }
                    }
                }
            }
        }
        // These literal names cannot alias the sixteen populated epoch paths.
        for empty in 0..16 {
            std::fs::create_dir_all(
                root.path()
                    .join(format!("{job}/checkpoints/checkpoint-empty-{empty:02}")),
            )
            .unwrap();
        }
    }
    assert_eq!(objects.len(), 12_288);
    assert_eq!(expected_deleted.len(), 64);
    assert_eq!(retained.len(), 64);
    write_objects(root.path(), &objects);
    assert_eq!(read_objects(root.path()), objects);
    let before_files = objects.len();
    let host_before = host_counters();
    let io_before = io_counters();
    let cpu_before = process_usage();
    eprintln!("STR156_CLEANUP_START");
    std::io::stderr().flush().unwrap();
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let cleanups: FuturesUnordered<_> = (0..4)
            .map(|operator| {
                let storage = &storage;
                let retained = &retained;
                async move {
                    let started = Instant::now();
                    cleanup_abandoned_controller_disk_files(
                        storage,
                        "J",
                        &operator.to_string(),
                        8,
                        retained,
                    )
                    .await?;
                    Ok::<_, StateError>(serde_json::json!({
                        "operator": operator,
                        "elapsed_seconds": started.elapsed().as_secs_f64(),
                    }))
                }
            })
            .collect();
        cleanups.try_collect::<Vec<_>>().await
    })
    .await;
    let elapsed_seconds = start.elapsed().as_secs_f64();
    eprintln!("STR156_CLEANUP_END");
    std::io::stderr().flush().unwrap();
    let cpu_after = process_usage();
    let io_after = io_counters();
    let host_after = host_counters();
    let completions = result
        .expect("120-second processing bound exceeded")
        .unwrap();
    assert_eq!(completions.len(), 4);
    for deleted in &expected_deleted {
        objects.remove(deleted).unwrap();
    }
    let actual = read_objects(root.path());
    assert_eq!(actual, objects, "full relative-key and byte oracle");
    let io_delta: BTreeMap<_, _> = io_after
        .into_iter()
        .map(|(key, value)| {
            let previous = io_before[&key];
            (key, value.checked_sub(previous).unwrap())
        })
        .collect();
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    let receipt = serde_json::json!({
        "status": "pass",
        "arm": std::env::var("STR156_ARM").unwrap_or_else(|_| "unspecified".into()),
        "completed_operators": completions.len(),
        "operator_completions": completions,
        "deleted_files": before_files - actual.len(),
        "expected_deletions": expected_deleted.len(),
        "root_files": before_files,
        "before_files": before_files,
        "after_files": actual.len(),
        "oracle_pass": true,
        "elapsed_seconds": elapsed_seconds,
        "user_cpu_seconds": seconds(cpu_after.ru_utime) - seconds(cpu_before.ru_utime),
        "system_cpu_seconds": seconds(cpu_after.ru_stime) - seconds(cpu_before.ru_stime),
        "lifetime_maxrss_kib": cpu_after.ru_maxrss,
        "maxrss_scope": "process lifetime high-water mark, includes fixture setup; not interval peak",
        "io_delta": io_delta,
        "host_before": host_before,
        "host_after": host_after,
    });
    // Fixture cleanup is outside measured processing and must succeed before
    // writing the successful receipt. Raw failures remain in the runner log.
    root.close().unwrap();
    std::fs::write(receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    eprintln!("STR156_RECEIPT {}", receipt);
}
