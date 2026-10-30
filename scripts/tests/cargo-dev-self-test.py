#!/usr/bin/env python3
"""Test container and queue wrappers with stub commands, without a container."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]
IMAGE_ID = "b" * 64
CONTAINER_SHELL = '/usr/local/bin/streamr-rocksdb-cache-guard && "$@"; result=$?; sccache --stop-server >/dev/null 2>&1; exit "$result"'


class CargoDevTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="cargo dev ")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "repo"
        (self.root / "scripts").mkdir(parents=True)
        (self.root / "Dockerfile.dev").write_bytes((REPO / "Dockerfile.dev").read_bytes())
        (self.root / "docker").mkdir()
        shutil.copy2(REPO / "docker/cargo-dev-config.toml",
                     self.root / "docker/cargo-dev-config.toml")
        shutil.copy2(REPO / "docker/rustc-cache", self.root / "docker/rustc-cache")
        for name in ("build-rocksdb-prebuilt", "rocksdb-prebuilt.json", "rocksdb-build-mode.py", "rocksdb-rustc-wrapper", "rocksdb-select", "rocksdb-cache-guard", "rocksdb_native_lock.py"):
            shutil.copy2(REPO / "docker" / name, self.root / "docker" / name)
        shutil.copy2(REPO / "Cargo.lock", self.root / "Cargo.lock")
        shutil.copy2(REPO / "Cargo.toml", self.root / "Cargo.toml")
        state = self.root / "crates/arroyo-state"
        state.mkdir(parents=True)
        shutil.copy2(REPO / "crates/arroyo-state/Cargo.toml", state / "Cargo.toml")
        for name in ("cargo-dev", "rust-build"):
            shutil.copy2(REPO / "scripts" / name, self.root / "scripts" / name)
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.log = self.base / "log.jsonl"
        self.env = dict(os.environ, HOME=str(self.base / "home"),
                        XDG_CACHE_HOME=str(self.base / "cache"),
                        PATH=f"{self.bin}:{os.environ['PATH']}", WRAPPER_TEST_LOG=str(self.log))
        self.env.pop("RUST_BUILD_QUEUE_HELD", None)
        self.env.pop("STREAMR_DEV_IMAGE", None)
        self.env.pop("STREAMR_BUILD_EVIDENCE_ROOT", None)
        self.env.pop("CARGO_BUILD_JOBS", None)
        self.env.pop("CARGO_INCREMENTAL", None)
        self.env.pop("ROCKSDB_COMPILE", None)
        self.env.pop("CARGO_PROFILE_DEV_INCREMENTAL", None)
        self.env.pop("CARGO_PROFILE_TEST_INCREMENTAL", None)
        self.write_stub("podman", """
record = {'command': 'podman', 'args': sys.argv[1:], 'cwd': os.getcwd(),
          'queue': os.environ.get('RUST_BUILD_QUEUE_HELD')}
record['auxiliary'] = (sys.argv[1:3] == ['image', 'inspect'] and '{{.Id}}' in sys.argv) or any(x.startswith('streamr-native-lock-check-') for x in sys.argv)
if sys.argv[1] == 'build':
    context = pathlib.Path(sys.argv[-1])
    record['context'] = str(context)
    record['files'] = sorted(str(p.relative_to(context)) for p in context.rglob('*'))
    record['cargo_config'] = (context / 'docker/cargo-dev-config.toml').read_text()
    record['dockerfile'] = (context / 'Dockerfile.dev').read_text()
with open(os.environ['WRAPPER_TEST_LOG'], 'a') as log:
    log.write(json.dumps(record) + '\\n')
if sys.argv[1:3] == ['image', 'inspect'] and '{{.Id}}' in sys.argv:
    print(os.environ.get('IMAGE_ID', 'b' * 64))
    sys.exit(0)
if any(x.startswith('streamr-native-lock-check-') for x in sys.argv):
    if sys.argv[1] == 'run':
        lock = pathlib.Path('Cargo.lock').read_text()
        print(json.dumps({'build_lock':lock, 'build_lock_sha256':[hashlib.sha256(lock.encode()).hexdigest()]}))
        sys.exit(int(os.environ.get('NATIVE_HELPER_STATUS', '0')))
    sys.exit(0)
if sys.argv[1:3] == ['image', 'inspect']:
    pin = json.loads(pathlib.Path('docker/rocksdb-prebuilt.json').read_text())
    expected = ':'.join(pin[k] for k in ('version', 'checksum', 'lz4_version', 'lz4_checksum')) + ':' + hashlib.sha256(pathlib.Path('docker/rocksdb-prebuilt.json').read_bytes()).hexdigest()
    print(os.environ.get('PREBUILT_PIN', expected))
    sys.exit(int(os.environ.get('INSPECT_STATUS', '0')))
if sys.argv[1:3] == ['image', 'exists']:
    sys.exit(int(os.environ.get('IMAGE_STATUS', '0')))
if sys.argv[1] == 'run' and os.environ.get('EXECUTE_CONTAINER_SHELL'):
    import subprocess
    shell_index = sys.argv.index('sh')
    sys.argv[shell_index+2] = sys.argv[shell_index+2].replace('/usr/local/bin/streamr-rocksdb-cache-guard && ', '')
    sys.exit(subprocess.call(sys.argv[shell_index:]))
sys.exit(int(os.environ.get('COMMAND_STATUS', '0')))
""")

    def write_stub(self, name, source):
        path = self.bin / name
        path.write_text("#!/usr/bin/env python3\nimport hashlib, json, os, pathlib, sys\n" + source)
        path.chmod(0o755)

    def run_wrapper(self, *args, name="cargo-dev", **env):
        self.log.unlink(missing_ok=True)
        return subprocess.run([str(self.root / "scripts" / name), *args],
                              cwd=self.base, env=dict(self.env, **env),
                              capture_output=True, text=True, check=False, timeout=10)

    def records(self):
        return [row for line in self.log.read_text().splitlines() if not (row := json.loads(line)).get('auxiliary')]

    def test_container_arguments_cwd_and_queue(self):
        result = self.run_wrapper("test", "-p", "crate", "--", "a spaced argument")
        self.assertEqual(result.returncode, 0, result.stderr)
        image, inspect, run = self.records()
        self.assertEqual(image['args'], ['image', 'exists', 'arroyo-dev'])
        self.assertEqual(run['args'], [
            'run', '--rm', '-w', '/app', '-e', 'CARGO_TARGET_DIR=/app/target/milestone2-runtime',
            '-e', 'CARGO_PROFILE_DEV_INCREMENTAL=true',
            '-e', 'CARGO_PROFILE_TEST_INCREMENTAL=true', '-e', 'ROCKSDB_COMPILE=0',
            '-e', 'STREAMR_ROCKSDB_SOURCE_REASON=',
            '-e', 'RUSTC_WRAPPER=/usr/local/bin/streamr-rocksdb-rustc-wrapper',
            '-e', 'CARGO_BUILD_JOBS=4',
            '-v', f'{self.root}:/app:z',
            '-v', f'{self.root}/target:/app/target:z',
            '-v', 'streamr-cargo-registry:/usr/local/cargo/registry',
            '-v', 'streamr-cargo-git:/usr/local/cargo/git',
            '-v', 'streamr-sccache:/var/cache/sccache',
            IMAGE_ID, 'sh', '-c', CONTAINER_SHELL, 'sh', 'cargo', 'test', '-p', 'crate', '--', 'a spaced argument'])
        self.assertEqual(run['cwd'], str(self.root))
        self.assertEqual(run['queue'], str(self.base / 'cache/rust-build/build.lock'))
        self.assertIsNone(image['queue'])

    def test_evidence_root_mount_preserves_absolute_paths_and_spaced_arguments(self):
        evidence = self.base / "evidence with spaces"
        evidence.mkdir()
        result = self.run_wrapper('--exec', 'python3', str(evidence / 'fixture.py'),
                                  STREAMR_BUILD_EVIDENCE_ROOT=str(evidence))
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.records()[-1]['args']
        self.assertIn(f'{evidence}:{evidence}:z', args)
        self.assertEqual(args[-2:], ['python3', str(evidence / 'fixture.py')])
        alias = self.base / 'alias'
        alias.symlink_to(evidence, target_is_directory=True)
        for invalid in ('relative', str(self.base / 'missing'), '/', '/app',
                        str(evidence) + ':suffix', str(alias)):
            result = self.run_wrapper('--exec', 'true', STREAMR_BUILD_EVIDENCE_ROOT=invalid)
            self.assertEqual(result.returncode, 2, (invalid, result.stderr))
            self.assertFalse(self.log.exists())

    def test_build_database_and_migrations_use_owned_network(self):
        db = 'a' * 64
        result = self.run_wrapper('build', STREAMR_BUILD_DATABASE_CONTAINER=db,
                                  DATABASE_URL='postgres://fixture', STREAMR_BUILD_MIGRATE='1')
        self.assertEqual(result.returncode, 0, result.stderr)
        run = self.records()[-1]['args']
        self.assertIn('container:' + db, run)
        self.assertIn('DATABASE_URL=postgres://fixture', run)
        self.assertTrue(any('bash scripts/migrate-build-db.sh' in x for x in run))
        result = self.run_wrapper('build', STREAMR_BUILD_DATABASE_CONTAINER='unowned-name',
                                  DATABASE_URL='postgres://fixture')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.log.exists())
        result = self.run_wrapper('build', STREAMR_BUILD_MIGRATE='1')
        self.assertEqual(result.returncode, 2)

    def test_snapshot_shared_target_override(self):
        result = self.run_wrapper('build', STREAMR_SHARED_TARGET_DIR=str(self.base / 'shared'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(str(self.base / 'shared') + ':/app/target:z', self.records()[-1]['args'])
        result = self.run_wrapper('build', STREAMR_SHARED_TARGET_DIR='relative')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(any(r['args'][0] == 'run' for r in self.records()))

    def test_isolated_cache_namespace_and_owned_container(self):
        result = self.run_wrapper('build', STREAMR_CACHE_NAMESPACE='streamr-test-cold',
                                  STREAMR_BUILD_CONTAINER_NAME='streamr-build-pin-test')
        self.assertEqual(result.returncode, 0, result.stderr)
        run = self.records()[-1]['args']
        self.assertIn('streamr-test-cold-sccache:/var/cache/sccache', run)
        self.assertIn('streamr-build-pin-test', run)
        self.assertNotIn('--rm', run, 'The explicit owner must observe and clean even a short-lived container')
        result = self.run_wrapper('--exec', 'true',
                                  STREAMR_BUILD_CONTAINER_NAME='streamr-build-pin-short-test')
        self.assertEqual(result.returncode, 0, result.stderr)
        run = self.records()[-1]['args']
        self.assertIn('streamr-build-pin-short-test', run)
        self.assertNotIn('--rm', run)
        result = self.run_wrapper('build', STREAMR_CACHE_NAMESPACE='../unsafe')
        self.assertEqual(result.returncode, 2)

    def test_finite_container_command_uses_maintained_environment(self):
        result = self.run_wrapper('--exec', 'python3', '/app/scripts/probe.py', 'spaced argument')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.records()[-1]['args'][-3:],
                         ['python3', '/app/scripts/probe.py', 'spaced argument'])
        self.assertEqual(self.run_wrapper('--exec').returncode, 2)

    def test_native_metadata_probe_is_owned_and_failure_stops_build(self):
        result = self.run_wrapper('build', NATIVE_HELPER_STATUS='42')
        self.assertEqual(result.returncode, 2, result.stderr)
        rows = [json.loads(line) for line in self.log.read_text().splitlines()]
        helpers = [r['args'] for r in rows if any(x.startswith('streamr-native-lock-check-') for x in r['args'])]
        self.assertEqual(len(helpers), 2)
        name = helpers[0][helpers[0].index('--name') + 1]
        self.assertEqual(helpers[1], ['rm', '--force', '--ignore', name])
        self.assertFalse(any(r['args'][0] == 'run' and not r.get('auxiliary') for r in rows))

    def test_stats_uses_same_container_shell_and_cache(self):
        result = self.run_wrapper('--stats')
        self.assertEqual(result.returncode, 0, result.stderr)
        image, inspect, run = self.records()
        self.assertEqual(run['args'][-7:],
                         [IMAGE_ID, 'sh', '-c', CONTAINER_SHELL, 'sh',
                          'sccache', '--show-stats'])
        self.assertIn('streamr-sccache:/var/cache/sccache', run['args'])

    def test_linked_worktree_mounts_original_checkout_target(self):
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        subprocess.run(['git', '-C', str(self.root), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(self.root), '-c', 'user.name=Fixture',
                        '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false',
                        'commit', '-qm', 'fixture'], check=True)
        linked = self.base / 'linked worktree'
        subprocess.run(['git', '-C', str(self.root), 'worktree', 'add', '-q',
                        '--detach', str(linked)], check=True)
        result = subprocess.run([str(linked / 'scripts/cargo-dev'), 'build'],
                                cwd=self.base, env=self.env, capture_output=True,
                                text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        run = self.records()[-1]
        self.assertIn(f'{linked}:/app:z', run['args'])
        self.assertIn(f'{self.root}/target:/app/target:z', run['args'])

    def test_incremental_overrides_are_forwarded_without_changing_release_profile(self):
        result = self.run_wrapper('build', '--release', CARGO_INCREMENTAL='0',
                                  CARGO_PROFILE_DEV_INCREMENTAL='false',
                                  CARGO_PROFILE_TEST_INCREMENTAL='false')
        self.assertEqual(result.returncode, 0, result.stderr)
        _, _, run = self.records()
        self.assertIn('CARGO_INCREMENTAL=0', run['args'])
        self.assertIn('CARGO_PROFILE_DEV_INCREMENTAL=false', run['args'])
        self.assertIn('CARGO_PROFILE_TEST_INCREMENTAL=false', run['args'])
        self.assertFalse(any(arg.startswith('CARGO_PROFILE_RELEASE_') for arg in run['args']))

    def test_rust_only_cache_shim_preserves_arguments_and_failure(self):
        self.write_stub('sccache', """
with open(os.environ['WRAPPER_TEST_LOG'], 'a') as log:
    log.write(json.dumps({'args': sys.argv[1:]}) + '\\n')
sys.exit(23)
""")
        result = subprocess.run(['sh', str(self.root / 'docker/rustc-cache'),
                                 '/path with spaces/rustc', '--crate-name', 'probe'],
                                env=self.env, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 23)
        self.assertEqual(self.records(),
                         [{'args': ['/path with spaces/rustc', '--crate-name', 'probe']}])

    def test_container_shell_preserves_command_status_when_shutdown_fails(self):
        self.write_stub('cargo', """
with open(os.environ['WRAPPER_TEST_LOG'], 'a') as log:
    log.write(json.dumps({'command': 'cargo', 'args': sys.argv[1:]}) + '\\n')
sys.exit(int(os.environ.get('CARGO_STATUS', '0')))
""")
        self.write_stub('sccache', """
with open(os.environ['WRAPPER_TEST_LOG'], 'a') as log:
    log.write(json.dumps({'command': 'sccache', 'args': sys.argv[1:]}) + '\\n')
sys.exit(37)
""")
        for status in ('0', '101'):
            with self.subTest(status=status):
                result = self.run_wrapper('test', '--', 'a spaced argument',
                                          EXECUTE_CONTAINER_SHELL='1', CARGO_STATUS=status)
                self.assertEqual(result.returncode, int(status), result.stderr)
                self.assertEqual(self.records()[-2:], [
                    {'command': 'cargo', 'args': ['test', '--', 'a spaced argument']},
                    {'command': 'sccache', 'args': ['--stop-server']}])

    def test_custom_image_jobs_and_failure_status(self):
        result = self.run_wrapper('check', STREAMR_DEV_IMAGE='custom:image',
                                  CARGO_BUILD_JOBS='2', COMMAND_STATUS='101')
        self.assertEqual(result.returncode, 101)
        image, inspect, run = self.records()
        self.assertEqual(image['args'][-1], 'custom:image')
        self.assertIn(IMAGE_ID, run['args'])
        self.assertIn('CARGO_BUILD_JOBS=2', run['args'])

    def test_native_source_override_requires_reason_and_release_uses_archives(self):
        result = self.run_wrapper('build', ROCKSDB_COMPILE='1',
                                  STREAMR_ROCKSDB_SOURCE_REASON='native comparison')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ROCKSDB_COMPILE=1', self.records()[-1]['args'])
        result = self.run_wrapper('build', ROCKSDB_COMPILE='1')
        self.assertNotEqual(result.returncode, 0)
        for args in [('build', '--release'), ('test', '--profile=release')]:
            result = self.run_wrapper(*args)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('ROCKSDB_COMPILE=0', self.records()[-1]['args'])

    def test_old_or_mismatched_prebuilt_image_fails_before_build(self):
        result = self.run_wrapper('build', PREBUILT_PIN='old-image')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('scripts/cargo-dev --build', result.stderr)
        self.assertEqual([r['args'][0] for r in self.records()], ['image', 'image'])

    def test_test_harness_arguments_do_not_change_native_mode(self):
        result = self.run_wrapper('test', '--', '--release', '--target=custom')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ROCKSDB_COMPILE=0', self.records()[-1]['args'])

    def test_custom_native_profile_uses_source_and_forwards_settings(self):
        result = self.run_wrapper('build', CARGO_PROFILE_DEV_OPT_LEVEL='2', STREAMR_ROCKSDB_SOURCE_REASON='unsupported profile')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ROCKSDB_COMPILE=1', self.records()[-1]['args'])
        self.assertIn('CARGO_PROFILE_DEV_OPT_LEVEL=2', self.records()[-1]['args'])
        manifest = self.root / 'Cargo.toml'
        manifest.write_text(manifest.read_text().replace('[profile.dev]', '[profile.dev]\nopt-level = 2'))
        result = self.run_wrapper('build')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(r['args'][0] == 'run' for r in self.records()))
        result = self.run_wrapper('build', STREAMR_ROCKSDB_SOURCE_REASON='unsupported manifest profile')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ROCKSDB_COMPILE=1', self.records()[-1]['args'])

    def test_mismatched_lock_or_features_fails_before_build(self):
        lock = self.root / 'Cargo.lock'
        lock.write_text(lock.read_text().replace('version = "0.17.3+10.4.2"',
                                                'version = "0.17.3+99.0.0"'))
        result = self.run_wrapper('build')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('differs from the prebuilt pin', result.stderr)
        shutil.copy2(REPO / 'Cargo.lock', lock)
        state = self.root / 'crates/arroyo-state/Cargo.toml'
        state.write_text(state.read_text().replace('"bindgen-runtime", "lz4"',
                                                  '"bindgen-runtime", "zstd"'))
        result = self.run_wrapper('build')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('features differ', result.stderr)

    def test_build_context_is_minimal_and_always_removed(self):
        for status in ('0', '17'):
            with self.subTest(status=status):
                result = self.run_wrapper('--build', COMMAND_STATUS=status)
                self.assertEqual(result.returncode, int(status), result.stderr)
                record, = self.records()
                self.assertEqual(record['args'][:2], ['build', '-f'])
                self.assertEqual(record['args'][3:5], ['-t', 'arroyo-dev'])
                self.assertEqual(record['files'], ['Cargo.lock', 'Dockerfile.dev', 'docker',
                                                   'docker/build-rocksdb-prebuilt',
                                                   'docker/cargo-dev-config.toml',
                                                   'docker/rocksdb-cache-guard',
                                                   'docker/rocksdb-prebuilt.json',
                                                   'docker/rocksdb-rustc-wrapper',
                                                   'docker/rocksdb-select',
                                                   'docker/rocksdb_native_lock.py',
                                                   'docker/rustc-cache'])
                self.assertEqual(record['cargo_config'],
                                 (REPO / 'docker/cargo-dev-config.toml').read_text())
                self.assertEqual(record['dockerfile'], (REPO / 'Dockerfile.dev').read_text())
                self.assertTrue(record['queue'])
                self.assertFalse(Path(record['context']).exists())

    def test_usage_and_image_errors_do_not_start_container(self):
        for args in ((), ('--build', 'extra'), ('--stats', 'extra')):
            self.assertEqual(self.run_wrapper(*args).returncode, 2)
            self.assertFalse(self.log.exists())
        result = self.run_wrapper('check', IMAGE_STATUS='125')
        self.assertEqual(result.returncode, 125)
        self.assertEqual(len(self.records()), 1)
        self.assertIn('--build', result.stderr)

    def test_queue_reentrant_and_host_forwarding_preserve_args_and_status(self):
        # A nested wrapper must avoid flocking the already-held reservation.
        self.write_stub('flock', 'sys.exit(99)\n')
        lock = str(self.base / 'cache/rust-build/build.lock')
        result = self.run_wrapper('podman', 'probe', 'a spaced argument', name='rust-build',
                                  RUST_BUILD_QUEUE_HELD=lock, COMMAND_STATUS='23')
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual(self.records()[0]['args'], ['probe', 'a spaced argument'])
        host = self.base / 'home/.local/bin/rust-build'
        host.parent.mkdir(parents=True)
        host.write_text('#!/usr/bin/env bash\nexec "$@"\n')
        host.chmod(0o755)
        result = self.run_wrapper('podman', 'host', 'spaced argument', name='rust-build',
                                  COMMAND_STATUS='24')
        self.assertEqual(result.returncode, 24, result.stderr)
        self.assertEqual(self.records()[0]['args'], ['host', 'spaced argument'])

    def test_queue_is_released_after_command_failure(self):
        result = self.run_wrapper('podman', 'probe', name='rust-build', COMMAND_STATUS='19')
        self.assertEqual(result.returncode, 19, result.stderr)
        lock = self.base / 'cache/rust-build/build.lock'
        # A fresh process must be able to acquire the same lock immediately.
        result = subprocess.run(['flock', '-n', str(lock), 'true'],
                                capture_output=True, check=False, timeout=5)
        self.assertEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
