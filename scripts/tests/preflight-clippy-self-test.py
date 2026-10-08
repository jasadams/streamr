#!/usr/bin/env python3
"""Exercise the gate without compiling Rust or using the real container runner."""

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]


class ClippyGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="clippy gate ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        scripts = self.root / "scripts"
        scripts.mkdir(parents=True)
        self.gate = scripts / "preflight-clippy.sh"
        shutil.copy2(REPO / "scripts/preflight-clippy.sh", self.gate)
        self.runner = scripts / "cargo-dev"
        self.runner.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['CLIPPY_TEST_LOG'], 'w') as log:\n"
            "    json.dump({'cwd': os.getcwd(), 'args': sys.argv[1:]}, log)\n"
            "print('runner output remains visible')\n"
            "print('runner diagnostic remains visible', file=sys.stderr)\n"
            "sys.exit(int(os.environ.get('CLIPPY_TEST_STATUS', '0')))\n"
        )
        self.runner.chmod(0o755)
        self.log = Path(self.temp.name) / "invocation.json"
        self.env = dict(os.environ, CLIPPY_TEST_LOG=str(self.log))

    def run_gate(self, *args, status=0):
        self.log.unlink(missing_ok=True)
        return subprocess.run(
            [str(self.gate), *args],
            cwd=self.temp.name,
            env=dict(self.env, CLIPPY_TEST_STATUS=str(status)),
            text=True,
            capture_output=True,
            check=False,
        )

    def invocation(self):
        return json.loads(self.log.read_text())

    def test_workspace_matches_ci_with_locked(self):
        workflow = (REPO / ".github/workflows/ci.yml").read_text()
        commands = re.findall(r"^\s*run:\s*(cargo clippy .+)$", workflow, re.M)
        self.assertEqual(len(commands), 1, "review parity if CI changes Clippy passes")
        ci = shlex.split(commands[0])[1:]
        expected = [ci[0], "--locked", *ci[1:]]
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.invocation()["args"], expected)
        self.assertEqual(self.invocation()["cwd"], str(self.root))
        self.assertIn("--all-targets", expected)
        self.assertIn("runner output remains visible", result.stdout)
        self.assertIn("runner diagnostic remains visible", result.stderr)

    def test_repeated_packages_only_replace_workspace(self):
        result = self.run_gate("-p", "arroyo-api", "-p", "arroyo-types")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.invocation()["args"],
            ["clippy", "--locked", "--no-deps", "--all-features", "--all-targets",
             "-p", "arroyo-api", "-p", "arroyo-types", "--", "-D", "warnings"],
        )

    def test_runner_failures_fail_closed(self):
        for status in (1, 2, 3, 101, 127):
            with self.subTest(status=status):
                result = self.run_gate(status=status)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(self.log.exists())
                self.assertIn("runner diagnostic remains visible", result.stderr)

    def test_invalid_arguments_do_not_run_cargo(self):
        for args in (("--unknown",), ("-p",), ("-p", ""),
                     ("-p", "--help"), ("crate",), ("--",)):
            with self.subTest(args=args):
                result = self.run_gate(*args)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Usage:", result.stderr)
                self.assertFalse(self.log.exists())

    def test_help_does_not_require_runner(self):
        self.runner.unlink()
        result = self.run_gate("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Usage:", result.stdout)
        self.assertFalse(self.log.exists())

    def test_missing_or_nonexecutable_runner(self):
        self.runner.chmod(0o644)
        self.assertEqual(self.run_gate().returncode, 3)
        self.runner.unlink()
        result = self.run_gate()
        self.assertEqual(result.returncode, 3)
        self.assertIn("missing executable runner", result.stderr)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
