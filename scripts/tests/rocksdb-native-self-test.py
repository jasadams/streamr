#!/usr/bin/env python3
"""Exercise actual host/runtime selection and fail-closed input validation."""
import hashlib
import importlib.machinery
import json
import tomllib
from pathlib import Path
import tempfile
import os
import runpy
import sys
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'docker'))
import rocksdb_native_lock as native_lock
selector = importlib.machinery.SourceFileLoader('rocksdb_select', str(ROOT / 'docker/rocksdb-select')).load_module()

preflight = importlib.machinery.SourceFileLoader('rocksdb_mode', str(ROOT / 'docker/rocksdb-build-mode.py')).load_module()

class NativeLockGraph(unittest.TestCase):
    def setUp(self):
        self.lock = (ROOT / 'Cargo.lock').read_text()
        self.sha = hashlib.sha256(self.lock.encode()).hexdigest()

    def test_unrelated_lock_change_reuses_archive(self):
        changed = self.lock + '\n[[package]]\nname="unrelated-test"\nversion="9.0.0"\n'
        native_lock.verify_native_lock(changed, self.lock, [self.sha])

    def test_real_image_minimal_graph_accepts_workspace_feature_union(self):
        fixtures = ROOT / 'scripts/tests/fixtures'
        archived = (fixtures / 'rocksdb-native-image.lock').read_text()
        current = (fixtures / 'rocksdb-native-workspace.lock').read_text()
        self.assertGreater(len(native_lock.native_graph(current)), len(native_lock.native_graph(archived)))
        native_lock.verify_native_lock(current, archived, [hashlib.sha256(archived.encode()).hexdigest()])

    def test_feature_union_optional_dependency_names_reuse_archive(self):
        # Identical required source gains an optional dependency from another
        # workspace consumer. The archive-required edge identities remain exact.
        header = 'name = "cc"\nversion = "1.2.26"'
        start = self.lock.index(header)
        end = self.lock.index('\n[[package]]', start)
        original = self.lock[start:end]
        expanded = original.replace('dependencies = [', 'dependencies = [\n "optional-native-test",', 1)
        changed = self.lock[:start]+expanded+self.lock[end:]
        changed += '\n[[package]]\nname="optional-native-test"\nversion="1.0.0"\n'
        native_lock.verify_native_lock(changed, self.lock, [self.sha])

    def test_required_edge_extra_version_is_rejected(self):
        # Extra optional dependency names are permitted; adding another resolved
        # version of a required name still changes the required build contract.
        changed = self.lock.replace(' "cc",', ' "cc 1.2.26",\n "cc 1.2.27",')
        changed += '\n[[package]]\nname="cc"\nversion="1.2.27"\n'
        with self.assertRaisesRegex(ValueError, 'Native build dependencies differ'):
            native_lock.verify_native_lock(changed, self.lock, [self.sha])

    def test_cc_change_rejected(self):
        changed = self.lock.replace('name = "cc"\nversion = "1.2.26"', 'name = "cc"\nversion = "1.2.27"')
        with self.assertRaisesRegex(ValueError, 'Native build dependencies differ'):
            native_lock.verify_native_lock(changed, self.lock, [self.sha])

    def test_duplicate_old_cc_elsewhere_does_not_hide_native_edge_change(self):
        old = self.lock.split('[[package]]\nname = "cc"\n', 1)[1].split('\n[[package]]', 1)[0]
        changed = self.lock.replace('name = "cc"\nversion = "1.2.26"', 'name = "cc"\nversion = "1.2.27"')
        changed = changed.replace(' "cc",', ' "cc 1.2.27",')
        changed += '\n[[package]]\nname = "cc"\n'+old+'\n'
        with self.assertRaisesRegex(ValueError, 'Native build dependencies differ'):
            native_lock.verify_native_lock(changed, self.lock, [self.sha])

    def test_archived_lock_tamper_rejected_before_graph(self):
        with self.assertRaisesRegex(ValueError, 'checksum differs'):
            native_lock.verify_native_lock(self.lock, self.lock+'\n', [self.sha])

    def test_harness_path_root_matches_pinned_registry_root(self):
        packages = tomllib.loads(self.lock)['package']
        rocks = next(package for package in packages if package['name']=='librocksdb-sys')
        header = 'name = "librocksdb-sys"\nversion = "'+rocks['version']+'"\n'
        registry = header+'source = "'+rocks['source']+'"\nchecksum = "'+rocks['checksum']+'"\n'
        archived = self.lock.replace(registry, header)
        native_lock.verify_native_lock(self.lock, archived, [hashlib.sha256(archived.encode()).hexdigest()])

class NativeImageProbe(unittest.TestCase):
    def test_bounded_helper_cleanup_on_success(self):
        metadata = {'build_lock':'test-lock','build_lock_sha256':['digest']}
        responses = [subprocess.CompletedProcess([],0,json.dumps(metadata),''), subprocess.CompletedProcess([],0,'','')]
        with patch.object(native_lock.subprocess, 'run', side_effect=responses) as run:
            self.assertEqual(native_lock.image_lock_metadata('sha256:'+'a'*64), metadata)
        launch, cleanup = run.call_args_list
        self.assertEqual(launch.kwargs['timeout'],30)
        self.assertEqual(cleanup.kwargs['timeout'],15)
        launch_args = launch.args[0]
        name = launch_args[launch_args.index('--name')+1]
        self.assertTrue(name.startswith('streamr-native-lock-check-'))
        self.assertEqual(cleanup.args[0], ['podman','rm','--force','--ignore',name])
        self.assertIn('--network=none', launch_args)

    def test_bounded_helper_cleanup_on_timeout(self):
        responses = [subprocess.TimeoutExpired('podman',30), subprocess.CompletedProcess([],0,'','')]
        with patch.object(native_lock.subprocess, 'run', side_effect=responses) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                native_lock.image_lock_metadata('b'*64)
        self.assertEqual(run.call_args_list[-1].args[0][1:4], ['rm','--force','--ignore'])

    def test_requires_immutable_image(self):
        with patch.object(native_lock.subprocess, 'run') as run:
            with self.assertRaises(ValueError):
                native_lock.image_lock_metadata('mutable-image:latest')
        run.assert_not_called()

class ProfilePreflight(unittest.TestCase):
    def setUp(self):
        self.pin = json.loads((ROOT / 'docker/rocksdb-prebuilt.json').read_text())

    def test_baselines_and_matching_custom_profiles(self):
        workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())
        for profile in ('dev', 'test', 'release', 'bench'):
            self.assertTrue(preflight.profiles_match_archive(workspace, profile, self.pin), profile)
        workspace.setdefault('profile', {})['fast-check'] = {'inherits':'dev','opt-level':3}
        self.assertTrue(preflight.profiles_match_archive(workspace, 'fast-check', self.pin))
        workspace['profile']['lean'] = {'inherits':'release', 'opt-level':0, 'debug':False}
        self.assertTrue(preflight.profiles_match_archive(workspace, 'lean', self.pin))

    def test_unsupported_native_overrides(self):
        overrides = [
            {'opt-level':2}, {'debug':'unknown'},
            {'build-override':{'opt-level':2}},
            {'package':{'*':{'opt-level':1}}},
            {'package':{'librocksdb-sys':{'opt-level':2}}},
            {'package':{'librocksdb-sys:0.17.3+10.4.2':{'opt-level':'s'}}},
            {'package':{'lz4-sys':{'opt-level':2}}},
        ]
        for override in overrides:
            with self.subTest(override=override):
                self.assertFalse(preflight.profiles_match_archive({'profile':{'dev':override}}, 'test', self.pin))
        self.assertFalse(preflight.profiles_match_archive({'profile':{'a':{'inherits':'b'}, 'b':{'inherits':'a'}}}, 'a', self.pin))

    def test_inherited_native_package_fields_are_checked(self):
        workspace = {'profile':{'dev':{'package':{'librocksdb-sys':{'opt-level':2}}},
                    'child':{'inherits':'dev', 'package':{'librocksdb-sys':{'debug':True}}}}}
        self.assertFalse(preflight.profiles_match_archive(workspace, 'child', self.pin))

    def test_non_native_overrides_do_not_force_source(self):
        workspace = {'profile':{'release':{'debug':1,'package':{'unrelated-crate':{'opt-level':2}}}}}
        self.assertTrue(preflight.profiles_match_archive(workspace, 'release', self.pin))

class Selection(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.pin = json.loads((ROOT / 'docker/rocksdb-prebuilt.json').read_text())
        self.archived_lock = (ROOT / 'Cargo.lock').read_text()
        (self.root / 'build-Cargo.lock').write_text(self.archived_lock)
        (self.root / 'Cargo.lock').write_text(self.archived_lock)
        workspace_lock = patch.object(selector, 'WORKSPACE_LOCK', self.root / 'Cargo.lock')
        workspace_lock.start()
        self.addCleanup(workspace_lock.stop)
        (self.root / 'manifest.json').write_text(json.dumps(self.pin))
        for name, settings in self.pin['profiles'].items():
            folder = self.root / name
            (folder / 'lib').mkdir(parents=True)
            (folder / 'lib/librocksdb.a').write_bytes(name.encode())
            receipt = {key: self.pin[key] for key in ('rustc','rustc_commit','cxx','target','features','rustflags','target_features')}
            receipt.update(build_lock_sha256=hashlib.sha256(self.archived_lock.encode()).hexdigest(), source_checksum=self.pin['checksum'], lz4_checksum=self.pin['lz4_checksum'], profile=settings, archive_sha256=hashlib.sha256(name.encode()).hexdigest())
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
        script.write_text((ROOT / 'docker/rocksdb-select').read_text().replace('/opt/streamr-rocksdb', str(self.root)).replace('/app/Cargo.lock', str(self.root / 'Cargo.lock')))
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
            env = dict(os.environ) | self.env | dict(OPT_LEVEL=settings['opt-level'], DEBUG=settings['debug'], PATH=str(tools)+':'+os.environ['PATH'], PYTHONPATH=str(ROOT / 'docker'))
            result = subprocess.run([sys.executable, str(script), str(upstream)], env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('selected='+str(self.root / name / 'lib'), result.stdout)
            self.assertEqual(json.loads(result.stderr.split('streamr-rocksdb: ')[1])['selection'], name)

    def test_upstream_implicit_lz4_dependency_feature_is_redundant(self):
        for name, settings in self.pin['profiles'].items():
            env = self.env | dict(CARGO_FEATURE_LZ4_SYS='1', OPT_LEVEL=settings['opt-level'], DEBUG=settings['debug'])
            self.assertEqual(selector.select(self.root, env), (self.root / name / 'lib', name))
        only_dependency = self.env | {'CARGO_FEATURE_LZ4_SYS':'1'}
        del only_dependency['CARGO_FEATURE_LZ4']
        with self.assertRaisesRegex(ValueError, 'native features'):
            selector.select(self.root, only_dependency)
        with self.assertRaisesRegex(ValueError, 'native features'):
            selector.select(self.root, self.env | {'CARGO_FEATURE_LZ4_SYS':'1','CARGO_FEATURE_ZSTD_SYS':'1'})

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
