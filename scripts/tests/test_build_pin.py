"""Input freeze and actual Cargo native dependency proof regression checks."""
import importlib.machinery
import importlib.util
import json
import os
import shutil
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


loader = importlib.machinery.SourceFileLoader("build_pin", str(Path(__file__).parents[1] / "build-pin"))
spec = importlib.util.spec_from_loader(loader.name, loader)
pin = importlib.util.module_from_spec(spec)
loader.exec_module(pin)


class FreezeTests(unittest.TestCase):
    def test_freezes_dirty_and_untracked_inputs_and_explicit_ui(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "Cargo.toml").write_text("original")
            subprocess.run(["git", "-C", str(repo), "add", "Cargo.toml"], check=True)
            (repo / "Cargo.toml").write_text("dirty")
            (repo / "new.rs").write_text("untracked")
            assets = root / "assets"
            assets.mkdir()
            (assets / "index.html").write_text("actual built UI")
            source = root / "snapshot"
            files = pin.freeze(repo, source, assets)
            self.assertEqual(files, pin.inventory(source))
            self.assertEqual((source / "Cargo.toml").read_text(), "dirty")
            self.assertEqual((source / "new.rs").read_text(), "untracked")
            self.assertFalse((source / ".git").exists())
            (repo / "Cargo.toml").write_text("later edit")
            self.assertEqual((source / "Cargo.toml").read_text(), "dirty")

    def test_rejects_external_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (root / "external").write_text("outside input")
            (repo / "linked").symlink_to(root / "external")
            with self.assertRaisesRegex(ValueError, "External symlink"):
                pin.freeze(repo, root / "snapshot", None)

    def test_ignored_cargo_overrides_are_frozen(self):
        for filename in ("config", "config.toml"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                repo = root / "repo"
                (repo / ".cargo").mkdir(parents=True)
                (repo / ".gitignore").write_text(".cargo/\n")
                override = repo / ".cargo" / filename
                override.write_text('[build]\nrustflags = ["-C", "target-cpu=native"]\n')
                subprocess.run(["git", "init", "-q", str(repo)], check=True)
                # Verify this is the ignored-input case, not the ordinary
                # nonignored untracked input path exercised above.
                subprocess.run(["git", "-C", str(repo), "check-ignore", "-q", str(override)], check=True)
                assets = root / "assets"
                assets.mkdir()
                (assets / "index.html").write_text("actual built UI")
                source = root / "snapshot"
                files = pin.freeze(repo, source, assets)
                name = ".cargo/" + filename
                self.assertEqual(files[name], pin.sha(override))
                self.assertEqual((source / name).read_text(), override.read_text())
                self.assertTrue(pin.compiled(name))

    def test_vendored_native_edit_changes_compiled_inventory_and_diff(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            native = repo / "vendor/rdkafka-sys/librdkafka/src/rdkafka_txnmgr.c"
            native.parent.mkdir(parents=True)
            native.write_text("before native transaction API\n")
            docs = repo / "docs/status.md"
            docs.parent.mkdir()
            docs.write_text("receipt before\n")
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "-c", "user.name=Pin test", "-c",
                            "user.email=pin-test@example.invalid", "commit", "-qm", "baseline"], check=True)
            assets = root / "assets"
            assets.mkdir()
            (assets / "index.html").write_text("unit fixture built UI")
            before = pin.freeze(repo, root / "before", assets)
            native.write_text("after native transaction API\n")
            docs.write_text("receipt after\n")
            after = pin.freeze(repo, root / "after", assets)
            name = str(native.relative_to(repo))
            compiled_after = {n: h for n, h in after.items() if pin.compiled(n)}
            self.assertIn(name, compiled_after)
            self.assertNotEqual(before[name], compiled_after[name])
            difference = pin.compiled_diff(repo).decode()
            self.assertIn(name, difference)
            self.assertIn("+after native transaction API", difference)
            self.assertNotIn("docs/status.md", difference)

    def test_compiled_inventory_excludes_receipt_only_script_edits(self):
        self.assertTrue(pin.compiled("crates/arroyo-state/src/lib.rs"))
        self.assertTrue(pin.compiled("Cargo.lock"))
        self.assertFalse(pin.compiled("scripts/build-pin"))
        self.assertFalse(pin.compiled("docs/milestone-3-status.md"))


class EnvironmentReceiptTests(unittest.TestCase):
    def test_cache_namespace_matches_shell_unset_empty_and_custom_defaults(self):
        self.assertEqual(pin.cache_namespace({}), "streamr")
        self.assertEqual(pin.cache_namespace({"STREAMR_CACHE_NAMESPACE": ""}), "streamr")
        self.assertEqual(pin.cache_namespace({"STREAMR_CACHE_NAMESPACE": "custom-cache"}), "custom-cache")

    def test_exact_native_forwarding_filter_preserves_diagnostics(self):
        forwarded = {name: "value-" + name for name in (
            "CC_ENABLE_DEBUG_OUTPUT", "CC_x86_64_unknown_linux_gnu", "CXX_x86_64_unknown_linux_gnu",
            "CFLAGS_x86_64_unknown_linux_gnu", "CXXFLAGS_x86_64_unknown_linux_gnu", "CARGO_PROFILE_RELEASE_DEBUG",
            "HOST_CC", "HOST_CXX", "HOST_CFLAGS", "HOST_CXXFLAGS", "TARGET_CC", "TARGET_CXX", "TARGET_CFLAGS",
            "TARGET_CXXFLAGS", "CRATE_CC_NO_DEFAULTS", "CARGO_INCREMENTAL", "CARGO_ENCODED_RUSTFLAGS",
            "RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_BUILD_JOBS", "ROCKSDB_CXX_STD", "CC", "CXX", "CFLAGS", "CXXFLAGS")}
        env = {**forwarded, "HOME": "/host/home", "HOSTNAME": "unrelated-host", "TARGET_UNRELATED": "noise",
               "DATABASE_URL": "private-database", "UNRELATED_SECRET": "never-record", "STREAMR_CACHE_NAMESPACE": "receipt-separate-field"}
        self.assertEqual(pin.environment_overrides(env), forwarded)


class NativeProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.target = self.root / "target"
        self.profile = self.target / "milestone2-runtime/release"
        self.fps = self.profile / ".fingerprint"
        (self.profile / "deps").mkdir(parents=True)
        self.binary = self.profile / "arroyo"
        self.binary.write_bytes(b"ELF fixture")
        os.link(self.binary, self.profile / "deps/arroyo-cli")
        self.artifact = {"executable": "/app/target/milestone2-runtime/release/arroyo", "profile": {"opt_level": "3"}}
        self.events = [{"reason": "compiler-artifact", "package_id": "arroyo-openapi", "target": {"kind": ["custom-build"]},
            "filenames": ["/app/target/milestone2-runtime/release/build/arroyo-openapi-api/build-script-build"]}]
        self.image_native = {"profiles": {}}
        self.fp("arroyo-cli/bin-arroyo", 1, [[0, "arroyo_state", False, 2], [0, "build_script_build", False, 6]])
        self.fp("arroyo-state-state/lib-arroyo_state", 2, [[0, "rocksdb", False, 3]])
        self.fp("rocksdb-rock/lib-rocksdb", 3, [[0, "librocksdb_sys", False, 4]])
        self.fp("arroyo-openapi-api/build-script-build-script-build", 6, [[0, "librocksdb_sys", False, 5]])
        self.native("runtime", "release", "3", 4)
        self.native("host", "release-host", "0", 5)

    def fp(self, name, value, deps):
        path = self.fps / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value.to_bytes(8, "little").hex())
        path.with_suffix(".json").write_text(json.dumps({"deps": deps}))

    def native(self, suffix, selection, opt, value):
        self.fp(f"librocksdb-sys-{suffix}/lib-librocksdb_sys", value, [])
        out = self.profile / f"build/librocksdb-sys-{suffix}/out"
        out.mkdir(parents=True)
        container_out = "/app/target/" + str(out.relative_to(self.target))
        (out.parent / "output").write_text("cargo:rustc-link-search=native=/opt/streamr-rocksdb/" + selection + "/lib")
        (out.parent / "stderr").write_text("streamr-rocksdb: " + json.dumps({"mode": "prebuilt", "selection": selection,
            "opt_level": opt, "debug": "false"}) + "\n")
        rlib = self.profile / f"deps/liblibrocksdb_sys-{suffix}.rlib"
        rlib.write_bytes(b"rlib fixture")
        (self.profile / f"deps/librocksdb_sys-{suffix}.d").write_text("# env-dep:OUT_DIR=" + container_out + "\n")
        self.events.extend([
            {"reason": "compiler-artifact", "target": {"name": "librocksdb_sys", "kind": ["lib"]},
             "filenames": ["/app/target/" + str(rlib.relative_to(self.target))], "profile": {"opt_level": opt}},
            {"reason": "build-script-executed", "package_id": "librocksdb-sys", "out_dir": container_out,
             "linked_paths": ["native=/opt/streamr-rocksdb/" + selection + "/lib"]},
        ])
        self.image_native["profiles"][selection] = {"archive_sha256": suffix, "receipt": {"archive_sha256": suffix,
            "profile": {"opt-level": opt, "debug": "false"}}}

    def proof(self):
        return pin.native_proof(self.events, self.target, self.artifact, self.root, self.image_native)

    def test_runtime_o3_and_host_o0_are_separately_proven_and_preserved(self):
        proof = self.proof()
        runtime = next(m for m in proof["mappings"] if m["opt_level"] == "3")
        host = next(m for m in proof["mappings"] if m["opt_level"] == "0")
        self.assertIn(runtime["fingerprint"], proof["runtime_closure"])
        self.assertNotIn(host["fingerprint"], proof["runtime_closure"])
        self.assertIn(host["fingerprint"], proof["openapi_host_closures"][0])
        for name, digest in proof["preserved_files"].items():
            self.assertEqual(pin.sha(self.root / "native-proof" / name), digest)

    def test_wrong_archive_optimization_is_rejected(self):
        self.image_native["profiles"]["release"]["receipt"]["profile"]["opt-level"] = "0"
        with self.assertRaisesRegex(ValueError, "archive differs"):
            self.proof()

    def test_wrong_runtime_dependency_is_rejected(self):
        self.fp("rocksdb-rock/lib-rocksdb", 3, [[0, "librocksdb_sys", False, 5]])
        with self.assertRaisesRegex(ValueError, "runtime optimization differs"):
            self.proof()

    def test_native_archive_hash_mismatch_is_rejected(self):
        self.image_native["profiles"]["release"]["archive_sha256"] = "modified"
        with self.assertRaisesRegex(ValueError, "archive hash differs"):
            self.proof()


class CleanupTests(unittest.TestCase):
    def test_helper_removed_after_interruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ok = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            with patch.object(pin.subprocess, "run", side_effect=[KeyboardInterrupt(), ok]) as mocked:
                with self.assertRaises(KeyboardInterrupt):
                    pin.owned_image_run(["podman", "run", "--rm", "image", "true"], root, "helper")
            receipt = json.loads((root / "helper-container.json").read_text())
            self.assertEqual(receipt["cleanup"]["exit_code"], 0)
            self.assertIn("--name", mocked.call_args_list[0].args[0])
            self.assertEqual(mocked.call_args_list[1].args[0][-1], receipt["container"])

    def test_database_removed_when_cargo_removal_times_out(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ok = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            with patch.object(pin.subprocess, "run", side_effect=[subprocess.TimeoutExpired("podman", 30), ok]) as mocked:
                with self.assertRaisesRegex(ValueError, "cleanup failed"):
                    pin.cleanup_owned(root, "cargo-owned", "database-owned")
            self.assertEqual(mocked.call_count, 2)
            self.assertIsNone(json.loads((root / "build-container-cleanup.json").read_text())["exit_code"])
            self.assertEqual(json.loads((root / "build-db-cleanup.json").read_text())["exit_code"], 0)

    def test_only_owned_database_cleanup_removes_anonymous_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            ok = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            with patch.object(pin.subprocess, "run", return_value=ok) as mocked:
                pin.cleanup_owned(Path(directory), "owned-cargo", "owned-database")
            cargo_command, database_command = [call.args[0] for call in mocked.call_args_list]
            self.assertEqual(cargo_command[-1], "owned-cargo")
            self.assertNotIn("--volumes", cargo_command)
            self.assertEqual(database_command[-1], "owned-database")
            self.assertIn("--volumes", database_command)


class NativeLockReceiptTests(unittest.TestCase):
    def test_preserves_raw_lock_and_checks_all_profiles(self):
        import hashlib
        lock = '# Exact native build graph\nversion = 4\n'
        digest = hashlib.sha256(lock.encode()).hexdigest()
        receipt = {"build_lock": lock, "build_lock_sha256": digest, "profiles": {
            name: {"receipt": {"build_lock_sha256": digest}} for name in ("dev", "release", "release-host")}}
        result = subprocess.CompletedProcess([], 0, stdout=json.dumps(receipt), stderr="")
        with patch.object(pin, "owned_image_run", return_value=result):
            preserved = pin.native_image_receipt("immutable-image-id", Path("unused"))
        self.assertEqual(preserved["build_lock"], lock)
        self.assertEqual(preserved["build_lock_sha256"], digest)
        receipt["profiles"]["release-host"]["receipt"]["build_lock_sha256"] = "different"
        result.stdout = json.dumps(receipt)
        with patch.object(pin, "owned_image_run", return_value=result):
            with self.assertRaisesRegex(ValueError, "build lock differs: release-host"):
                pin.native_image_receipt("immutable-image-id", Path("unused"))


class CargoTimingTests(unittest.TestCase):
    def test_preserves_exact_logged_report_through_cache_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            reports = target / "milestone2-runtime/cargo-timings"
            reports.mkdir(parents=True)
            current = reports / "cargo-timing-exact-current.html"
            current.write_text("current build Rust/codegen/link timings")
            stale = reports / "cargo-timing-newer-stale.html"
            stale.write_text("unrelated newer report")
            log = root / "build.jsonl"
            log.write_text("      Timing report saved to /app/target/milestone2-runtime/cargo-timings/" + current.name + "\n")
            evidence = root / "evidence"
            evidence.mkdir()
            digest = pin.sha(current)
            receipt = pin.preserve_cargo_timing(log, target, evidence)
            shutil.rmtree(target)
            preserved = evidence / receipt["preserved_report"]
            self.assertEqual(preserved.read_text(), "current build Rust/codegen/link timings")
            self.assertEqual(pin.sha(preserved), digest)
            self.assertEqual(receipt["report_sha256"], digest)
            self.assertEqual(receipt["build_log_sha256"], pin.sha(log))
            self.assertEqual(json.loads((evidence / "cargo-timing-receipt.json").read_text()), receipt)

    def test_refuses_missing_or_ambiguous_current_report(self):
        for lines in ("no report", "Timing report saved to /app/target/one.html\nTiming report saved to /app/target/two.html\n"):
            with self.subTest(lines=lines), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                log = root / "build.jsonl"
                log.write_text(lines)
                with self.assertRaisesRegex(ValueError, "Unique current"):
                    pin.preserve_cargo_timing(log, root / "target", root)


class LauncherTests(unittest.TestCase):
    def test_launcher_preserves_current_executable_identity(self):
        # readlink observes its own /proc/self/exe, matching Rust current_exe().
        # Invoking it through ld-linux directly fails this check.
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / "bin").mkdir()
            (package / "lib").mkdir()
            executable = package / "bin/arroyo"
            shutil.copy2(shutil.which("readlink"), executable)
            pin.write_launcher(package)
            result = subprocess.check_output([str(package / "bin/run-arroyo"), "/proc/self/exe"], text=True).strip()
            self.assertEqual(result, str(executable))


if __name__ == "__main__":
    unittest.main()
