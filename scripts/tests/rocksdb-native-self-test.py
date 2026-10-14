#!/usr/bin/env python3
"""Exercise actual host/runtime selection and fail-closed input validation."""
import hashlib
import importlib.machinery
import json
from pathlib import Path
import tempfile
import os
import runpy
import sys
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
selector = importlib.machinery.SourceFileLoader('rocksdb_select', str(ROOT / 'docker/rocksdb-select')).load_module()

class Selection(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.pin = json.loads((ROOT / 'docker/rocksdb-prebuilt.json').read_text())
        (self.root / 'manifest.json').write_text(json.dumps(self.pin))
        for name, settings in self.pin['profiles'].items():
            folder = self.root / name
            (folder / 'lib').mkdir(parents=True)
            (folder / 'lib/librocksdb.a').write_bytes(name.encode())
            receipt = {key: self.pin[key] for key in ('rustc','rustc_commit','cxx','target','features','rustflags','target_features')}
            receipt.update(source_checksum=self.pin['checksum'], lz4_checksum=self.pin['lz4_checksum'], profile=settings, archive_sha256=hashlib.sha256(name.encode()).hexdigest())
            (folder / 'receipt.json').write_text(json.dumps(receipt))
        self.env = dict(CARGO_PKG_VERSION=self.pin['version'], TARGET=self.pin['target'], OPT_LEVEL='0', DEBUG='true', CARGO_ENCODED_RUSTFLAGS='\x1f'.join(self.pin['rustflags']), CARGO_CFG_TARGET_FEATURE=','.join(self.pin['target_features']))
        self.env.update({'CARGO_FEATURE_'+f.upper().replace('-','_'): '1' for f in self.pin['features']})
        self.toolchain = patch.object(selector.subprocess, 'check_output', side_effect=lambda cmd, **kw: f"release: {self.pin['rustc']}\ncommit-hash: {self.pin['rustc_commit']}\nhost: {self.pin['target']}\n" if cmd[0]=='rustc' else self.pin['cxx'])
        self.toolchain.start()
        self.addCleanup(self.toolchain.stop)

    def test_actual_native_settings(self):
        for name, settings in self.pin['profiles'].items():
            with self.subTest(name=name):
                env = self.env | dict(OPT_LEVEL=settings['opt-level'], DEBUG=settings['debug'])
                self.assertEqual(selector.select(self.root, env), (self.root / name / 'lib', name))

    def test_selector_executes_upstream_with_actual_settings(self):
        script = self.root / 'selector'
        script.write_text((ROOT / 'docker/rocksdb-select').read_text().replace('/opt/streamr-rocksdb', str(self.root)))
        upstream = self.root / 'upstream'
        upstream.write_text('#!/bin/sh\nprintf "selected=%s\\n" "$ROCKSDB_LIB_DIR"\n')
        upstream.chmod(0o755)
        tools = self.root / 'bin'
        tools.mkdir()
        for executable, version in [('rustc',f"release: {self.pin['rustc']}\ncommit-hash: {self.pin['rustc_commit']}\nhost: {self.pin['target']}\n"), ('c++',self.pin['cxx'])]:
            tool = tools / executable
            tool.write_text('#!/bin/sh\nprintf "%s\\n" '+__import__('shlex').quote(version)+'\n')
            tool.chmod(0o755)
        for name, settings in self.pin['profiles'].items():
            env = dict(os.environ) | self.env | dict(OPT_LEVEL=settings['opt-level'], DEBUG=settings['debug'], PATH=str(tools)+':'+os.environ['PATH'])
            result = subprocess.run([sys.executable, str(script), str(upstream)], env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('selected='+str(self.root / name / 'lib'), result.stdout)
            self.assertEqual(json.loads(result.stderr.split('streamr-rocksdb: ')[1])['selection'], name)

    def test_compiler_logging_does_not_change_archive_selection(self):
        self.assertEqual(selector.select(self.root, self.env | {'CC_ENABLE_DEBUG_OUTPUT': '1'})[1], 'dev')

    def test_mismatches_fail_closed(self):
        for changed in ({'TARGET':'aarch64-unknown-linux-gnu'}, {'HOST_CXXFLAGS':'-O1'}, {'TARGET_CFLAGS':'-O1'}, {'OPT_LEVEL':'2'}, {'DEBUG':'false','OPT_LEVEL':'3','CXXFLAGS':'-O0'}, {'CARGO_FEATURE_SNAPPY':'1'}, {'CARGO_ENCODED_RUSTFLAGS':'-C\x1ftarget-cpu=native'}, {'CARGO_CFG_TARGET_FEATURE':'avx,sse,sse2'}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                selector.select(self.root, self.env | changed)

    def test_archive_tamper(self):
        (self.root / 'dev/lib/librocksdb.a').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            selector.select(self.root, self.env)

    def test_source_mode_requires_reason(self):
        with self.assertRaises(ValueError):
            selector.select(self.root, self.env | {'ROCKSDB_COMPILE':'1'})
        self.assertEqual(selector.select(self.root, self.env | {'ROCKSDB_COMPILE':'1','STREAMR_ROCKSDB_SOURCE_REASON':'deliberate comparison'}), (None,'deliberate comparison'))

class CacheGuard(unittest.TestCase):
    def test_only_invalidates_original_build_script(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            native = root / 'debug/build/librocksdb-sys-old'
            native.mkdir(parents=True)
            (native / 'build-script-build').write_bytes(b'\x7fELF original')
            (native / 'build_script_build-old').write_bytes(b'original')
            fingerprint = root / 'debug/.fingerprint/librocksdb-sys-old'
            fingerprint.mkdir(parents=True)
            (fingerprint / 'state').write_text('fresh')
            out = root / 'debug/build/librocksdb-sys-output/out'
            out.mkdir(parents=True)
            (out / 'librocksdb.a').write_text('preserved')
            wrapped = root / 'release/build/librocksdb-sys-new'
            wrapped.mkdir(parents=True)
            (wrapped / 'build-script-build').write_text('#!/bin/sh\nexec /usr/local/bin/streamr-rocksdb-select original')
            with patch.dict(os.environ, {'CARGO_TARGET_DIR':folder}):
                runpy.run_path(str(ROOT / 'docker/rocksdb-cache-guard'), run_name='__main__')
            self.assertFalse((native / 'build-script-build').exists())
            self.assertFalse(fingerprint.exists())
            self.assertEqual((out / 'librocksdb.a').read_text(), 'preserved')
            self.assertTrue((wrapped / 'build-script-build').exists())

class Wrapper(unittest.TestCase):
    def test_wraps_only_rocksdb_and_preserves_compiler_artifact(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            binary = root / 'build_script_build-test'
            cached = root / 'cached-artifact'
            cached.write_bytes(b'original executable')
            def compile_command(args):
                os.link(cached, binary)
                return 0
            args = ['wrapper','rustc','--crate-name','build_script_build','--emit=dep-info,link','-C','extra-filename=-test','--out-dir',folder]
            with patch.object(sys, 'argv', args), patch.dict(os.environ, {'CARGO_PKG_NAME':'librocksdb-sys'}), patch('subprocess.call', side_effect=compile_command):
                runpy.run_path(str(ROOT / 'docker/rocksdb-rustc-wrapper'), run_name='__main__')
            original = root / 'build_script_build-test.streamr-upstream'
            self.assertEqual(cached.read_bytes(), b'original executable')
            self.assertEqual(original.read_bytes(), b'original executable')
            self.assertIn(str(original), binary.read_text())
            self.assertNotEqual(cached.stat().st_ino, binary.stat().st_ino)

if __name__ == '__main__':
    unittest.main()
