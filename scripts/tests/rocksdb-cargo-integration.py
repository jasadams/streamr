#!/usr/bin/env python3
"""Run inside the maintained dev image/queue: real Cargo host/runtime cache checks.

This tiny package deliberately uses the native crate name to exercise only the
launcher and Cargo fingerprint behavior. It does not compile RocksDB C++ and is
not a native correctness test.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

with tempfile.TemporaryDirectory(prefix='streamr-native-cargo-') as folder:
    root = Path(folder)
    native = root / 'native'
    (native / 'src').mkdir(parents=True)
    (root / 'src').mkdir()
    (native / 'Cargo.toml').write_text('''[package]
name="librocksdb-sys"
version="0.17.3+10.4.2"
edition="2021"
[features]
static=[]
lz4=["lz4-sys"]
lz4-sys=[]
bindgen-runtime=[]
''')
    (native / 'src/lib.rs').write_text('pub fn native() {}\n')
    (native / 'build.rs').write_text('''fn main() {
 println!("cargo:rerun-if-env-changed=ROCKSDB_COMPILE");
 println!("cargo:rerun-if-env-changed=STREAMR_ROCKSDB_SOURCE_REASON");
 eprintln!("fixture-original: opt={} debug={} lib={}", std::env::var("OPT_LEVEL").unwrap(), std::env::var("DEBUG").unwrap(), std::env::var("ROCKSDB_LIB_DIR").unwrap_or_default());
}''')
    (root / 'Cargo.toml').write_text('''[package]
name="streamr-native-cargo-fixture"
version="0.0.0"
edition="2021"
[dependencies]
librocksdb-sys={path="native",features=["static","lz4","bindgen-runtime"]}
[build-dependencies]
librocksdb-sys={path="native",features=["static","lz4","bindgen-runtime"]}
[profile.release]
debug=1
''')
    (root / 'build.rs').write_text('fn main() { librocksdb_sys::native(); }\n')
    (root / 'src/lib.rs').write_text('pub fn fixture() { librocksdb_sys::native(); }\n')
    env = dict(os.environ) | {'CARGO_TARGET_DIR':str(root / 'target'), 'CARGO_BUILD_JOBS':'1'}
    # First populate a genuine older unwrapped Cargo cache.
    env['RUSTC_WRAPPER']='/usr/local/bin/streamr-rustc-cache'
    subprocess.run(['cargo','build','--offline'],cwd=root,env=env,check=True)
    env['RUSTC_WRAPPER']='/usr/local/bin/streamr-rocksdb-rustc-wrapper'
    subprocess.run(['/usr/local/bin/streamr-rocksdb-cache-guard'],env=env,check=True)
    subprocess.run(['cargo','build','--offline'],cwd=root,env=env,check=True)
    subprocess.run(['cargo','build','--offline','--release'],cwd=root,env=env,check=True)
    selections=[]
    for stderr in (root / 'target').glob('*/build/librocksdb-sys-*/stderr'):
        for line in stderr.read_text().splitlines():
            if line.startswith('streamr-rocksdb: '):
                selections.append(json.loads(line.split(': ',1)[1]))
    assert {'dev','release','release-host'} <= {item['selection'] for item in selections}, selections
    # A source comparison must remove the inherited prebuilt lib directory;
    # switching back must re-run the same wrapped upstream executable.
    env.update(ROCKSDB_COMPILE='1',STREAMR_ROCKSDB_SOURCE_REASON='Cargo fixture explicit source comparison')
    subprocess.run(['cargo','build','--offline'],cwd=root,env=env,check=True)
    stderr_files=list((root / 'target/debug/build').glob('librocksdb-sys-*/stderr'))
    assert any('"mode": "source"' in file.read_text() and 'lib=\n' in file.read_text() for file in stderr_files)
    env['ROCKSDB_COMPILE']='0'
    subprocess.run(['cargo','build','--offline'],cwd=root,env=env,check=True)
    assert any('"selection": "dev"' in file.read_text() for file in stderr_files)
    subprocess.run(['cargo','build','--offline'],cwd=root,env=env,check=True)
    print('Real Cargo dev/release/host selection, old-cache invalidation, source switch and warm reuse passed')
