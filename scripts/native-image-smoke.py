#!/usr/bin/env python3
"""Compile and save a generic Rust UDF in an owned native engine container."""
import argparse
import json
import pathlib
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--evidence", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    receipt = {"image": args.image, "source_revision": args.expected_revision, "passed": False}
    container = None

    def run(*command):
        return subprocess.check_output(command, text=True).strip()

    try:
        inspected = json.loads(run("docker", "image", "inspect", args.image))[0]
        receipt["image_id"] = inspected["Id"]
        assert inspected["Config"]["Labels"]["org.opencontainers.image.revision"] == args.expected_revision
        name = "streamr-image-smoke-" + uuid.uuid4().hex
        container = run("docker", "run", "--detach", "--name", name,
                        "--publish", "127.0.0.1::5115", "--env", "CARGO_BUILD_JOBS=2",
                        inspected["Id"])
        receipt["rustc"] = run("docker", "exec", container, "rustc", "--version")
        assert receipt["rustc"].startswith("rustc 1.96.0 "), receipt["rustc"]
        port = run("docker", "port", container, "5115/tcp").rsplit(":", 1)[1]
        base = f"http://127.0.0.1:{port}/api/v1/"
        deadline = time.monotonic() + 180
        while True:
            try:
                with urllib.request.urlopen(base + "pipelines", timeout=5) as response:
                    json.load(response)
                break
            except (urllib.error.URLError, TimeoutError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)

        def post(path, payload):
            request = urllib.request.Request(base + path, json.dumps(payload).encode(),
                                             {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=1800) as response:
                return json.load(response)

        definition = "use arroyo_udf_plugin::udf;\n#[udf]\npub fn image_smoke_increment(value: i64) -> i64 { value + 1 }\n"
        udf = {"definition": definition, "language": "rust"}
        # A compiler-startup 503 is safe to retry on this validation-only route.
        while True:
            try:
                validated = post("udfs/validate", udf)
                break
            except urllib.error.HTTPError as error:
                detail = error.read().decode()
                if error.code != 503 or "compiler-service not available" not in detail or time.monotonic() >= deadline:
                    raise RuntimeError(detail) from error
                time.sleep(2)
        receipt["validation"] = validated
        assert validated["errors"] == [], validated
        saved = post("udfs", {**udf, "prefix": "", "description": "Native image packaging smoke"})
        receipt["saved_udf"] = saved
        assert saved.get("id") and saved.get("dylib_url"), saved
        # Saving exercises cargo build/cdylib linking, beyond cargo check.
        receipt["passed"] = True
    finally:
        if container:
            logs = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
            (args.evidence / "engine.log").write_text(logs.stdout + logs.stderr)
            cleanup = subprocess.run(["docker", "rm", "--force", container], text=True, capture_output=True)
            receipt["cleanup_exit"] = cleanup.returncode
            if cleanup.returncode:
                receipt["passed"] = False
        (args.evidence / "result.json").write_text(json.dumps(receipt, indent=2) + "\n")
    assert receipt["passed"], receipt


if __name__ == "__main__":
    main()
