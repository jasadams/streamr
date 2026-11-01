#!/usr/bin/env python3
"""Apply exactly one ignored measurement test to either frozen product revision."""
from pathlib import Path
import sys

path = Path('crates/arroyo-state/src/live/native_checkpoint.rs')
source = path.read_text()
anchor = '    fn resources() -> WorkerStateResources {'
assert source.count(anchor) == 1
case = r'''
    #[tokio::test]
    #[ignore = "temporary isolated STR-163 restore measurement"]
    async fn local_native_download_measurement() {
        use std::io::Read;
        let fixture = PathBuf::from(std::env::var("STR163_RESTORE_FIXTURE").unwrap());
        let manifest: Vec<(String, u64, Vec<u8>)> = serde_json::from_slice(
            &std::fs::read(fixture.join("manifest.json")).unwrap()
        ).unwrap();
        assert_eq!(manifest.len(), 194);
        assert_eq!(manifest.iter().map(|(_, size, _)| *size).sum::<u64>(), 503_318_140);
        let storage = Arc::new(arroyo_storage::StorageProvider::for_url(
            fixture.join("source").to_str().unwrap()
        ).await.unwrap());
        let metadata = NativeDatabaseCheckpointMetadata {
            format_version: protocol::FORMAT_VERSION,
            compatibility_identity: compatibility_identity(),
            files: manifest.iter().map(|(name, size, checksum)| NativeCheckpointFile {
                local_name: name.clone(), path: name.clone(), size_bytes: *size,
                checksum: checksum.clone(), ..Default::default()
            }).collect(),
            ..Default::default()
        };
        let request = NativeRestoreRequest { metadata, storage };
        let output = tempfile::tempdir().unwrap();
        let resources = resources();
        let started = std::time::Instant::now();
        download(&request, output.path(), &resources).await.unwrap();
        let elapsed = started.elapsed();
        println!("STR163_DOWNLOAD_SECONDS={:.9}", elapsed.as_secs_f64());
        drop(resources.try_decoded_value(8 << 20).unwrap());
        let mut buffer = vec![0u8; CHUNK_BYTES];
        for (name, size, checksum) in manifest {
            let mut input = std::fs::File::open(output.path().join(name)).unwrap();
            let mut digest = Sha256::new();
            let mut count = 0u64;
            loop {
                let read = input.read(&mut buffer).unwrap();
                if read == 0 { break; }
                digest.update(&buffer[..read]);
                count += read as u64;
            }
            assert_eq!(count, size);
            assert_eq!(digest.finalize().as_slice(), checksum);
        }
        println!("STR163_OUTPUT_ORACLE=PASS");
    }

'''
path.write_text(source.replace(anchor, case + anchor, 1))
