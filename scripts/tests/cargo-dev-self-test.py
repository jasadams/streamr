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
CONTAINER_SHELL = '"$@"; result=$?; sccache --stop-server >/dev/null 2>&1; exit "$result"'


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
        self.env.pop("CARGO_BUILD_JOBS", None)
        self.write_stub("podman", """
record = {'command': 'podman', 'args': sys.argv[1:], 'cwd': os.getcwd(),
          'queue': os.environ.get('RUST_BUILD_QUEUE_HELD')}
if sys.argv[1] == 'build':
    context = pathlib.Path(sys.argv[-1])
    record['context'] = str(context)
    record['files'] = sorted(str(p.relative_to(context)) for p in context.rglob('*'))
    record['cargo_config'] = (context / 'docker/cargo-dev-config.toml').read_text()
    record['dockerfile'] = (context / 'Dockerfile.dev').read_text()
with open(os.environ['WRAPPER_TEST_LOG'], 'a') as log:
    log.write(json.dumps(record) + '\\n')
if sys.argv[1:3] == ['image', 'exists']:
    sys.exit(int(os.environ.get('IMAGE_STATUS', '0')))
if sys.argv[1] == 'run' and os.environ.get('EXECUTE_CONTAINER_SHELL'):
    import subprocess
    shell_index = sys.argv.index('sh')
    sys.exit(subprocess.call(sys.argv[shell_index:]))
sys.exit(int(os.environ.get('COMMAND_STATUS', '0')))
""")

    def write_stub(self, name, source):
        path = self.bin / name
        path.write_text("#!/usr/bin/env python3\nimport json, os, pathlib, sys\n" + source)
        path.chmod(0o755)

    def run_wrapper(self, *args, name="cargo-dev", **env):
        self.log.unlink(missing_ok=True)
        return subprocess.run([str(self.root / "scripts" / name), *args],
                              cwd=self.base, env=dict(self.env, **env),
                              capture_output=True, text=True, check=False, timeout=10)

    def records(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_container_arguments_cwd_and_queue(self):
        result = self.run_wrapper("test", "-p", "crate", "--", "a spaced argument")
        self.assertEqual(result.returncode, 0, result.stderr)
        image, run = self.records()
        self.assertEqual(image['args'], ['image', 'exists', 'arroyo-dev'])
        self.assertEqual(run['args'], [
            'run', '--rm', '-w', '/app', '-e', 'CARGO_TARGET_DIR=/app/target/milestone2-runtime',
            '-e', 'CARGO_INCREMENTAL=0', '-e', 'CARGO_BUILD_JOBS=4',
            '-v', f'{self.root}:/app:z',
            '-v', 'streamr-cargo-registry:/usr/local/cargo/registry',
            '-v', 'streamr-cargo-git:/usr/local/cargo/git',
            '-v', 'streamr-sccache:/var/cache/sccache',
            'arroyo-dev', 'sh', '-c', CONTAINER_SHELL, 'sh', 'cargo', 'test', '-p', 'crate', '--', 'a spaced argument'])
        self.assertEqual(run['cwd'], str(self.root))
        self.assertEqual(run['queue'], str(self.base / 'cache/rust-build/build.lock'))
        self.assertIsNone(image['queue'])

    def test_stats_uses_same_container_shell_and_cache(self):
        result = self.run_wrapper('--stats')
        self.assertEqual(result.returncode, 0, result.stderr)
        image, run = self.records()
        self.assertEqual(run['args'][-7:],
                         ['arroyo-dev', 'sh', '-c', CONTAINER_SHELL, 'sh',
                          'sccache', '--show-stats'])
        self.assertIn('streamr-sccache:/var/cache/sccache', run['args'])

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
        image, run = self.records()
        self.assertEqual(image['args'][-1], 'custom:image')
        self.assertIn('custom:image', run['args'])
        self.assertIn('CARGO_BUILD_JOBS=2', run['args'])

    def test_build_context_is_minimal_and_always_removed(self):
        for status in ('0', '17'):
            with self.subTest(status=status):
                result = self.run_wrapper('--build', COMMAND_STATUS=status)
                self.assertEqual(result.returncode, int(status), result.stderr)
                record, = self.records()
                self.assertEqual(record['args'][:2], ['build', '-f'])
                self.assertEqual(record['args'][3:5], ['-t', 'arroyo-dev'])
                self.assertEqual(record['files'], ['Dockerfile.dev', 'docker',
                                                   'docker/cargo-dev-config.toml'])
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
