#!/usr/bin/env python3
"""Validate the image's native pin and choose prebuilt versus source compilation."""
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
import tomllib

# Preflight also runs inside frozen build-pin source snapshots. Importing its
# shared verifier must not add files to those compiled inputs.
sys.dont_write_bytecode = True
from rocksdb_native_lock import image_lock_metadata, verify_native_lock


def profiles_match_archive(workspace, selected, pin):
    """Conservatively catch unsupported native settings before launching Cargo.

    Cargo's execution-time selector remains authoritative: this check models
    only profile inheritance and native package/build overrides, never guesses
    whether a particular dependency will be used on the host or target.
    """
    profiles = workspace.get("profile", {})
    active = set()

    def resolve(name):
        if name in active:
            return None
        active.add(name)
        table = profiles.get(name, {})
        parent = table.get("inherits", {"test": "dev", "bench": "release"}.get(name))
        if parent:
            inherited = resolve(parent)
            if inherited is None:
                return None
            result = dict(inherited)
            # Nested package/build overrides inherit independently.
            for nested in ("package", "build-override"):
                result[nested] = dict(inherited.get(nested, {})) | table.get(nested, {})
                if nested == "package":
                    for package, override in table.get(nested, {}).items():
                        result[nested][package] = inherited.get(nested, {}).get(package, {}) | override
        elif name in ("dev", "release"):
            result = {"opt-level": 0 if name == "dev" else 3,
                      "debug": "line-tables-only" if name == "dev" else False}
        else:
            return None
        result.update({key: value for key, value in table.items() if key not in ("package", "build-override")})
        if not parent:
            result.update({key: table[key] for key in ("package", "build-override") if key in table})
        active.remove(name)
        return result

    settings = resolve(selected)
    if settings is None:
        return False
    supported = {(profile["opt-level"], profile["debug"]) for profile in pin["profiles"].values()}

    def matches(table):
        debug = table["debug"]
        if debug in (False, 0, "none", "0", "false"):
            native_debug = "false"
        elif debug in (True, 1, 2, "1", "2", "true", "limited", "full", "line-tables-only", "line-directives-only"):
            native_debug = "true"
        else:
            return False
        return (str(table["opt-level"]), native_debug) in supported

    runtime = {key: settings[key] for key in ("opt-level", "debug")}
    host = {"opt-level": 0, "debug": False} | settings.get("build-override", {})
    if not matches(runtime) or not matches(host):
        return False
    package_overrides = settings.get("package", {})
    wildcard = package_overrides.get("*", {})
    for name in ("librocksdb-sys", "lz4-sys"):
        relevant = [value for key, value in package_overrides.items()
                    if key == name or key.startswith((name + ":", name + "@"))]
        for override in relevant or [{}]:
            if not matches(runtime | wildcard | override) or not matches(host | wildcard | override):
                return False
    return True


def build_mode(root, image_pin, args, env, image_metadata=None):
    pin = json.loads((root / "docker/rocksdb-prebuilt.json").read_text())
    packages = tomllib.loads((root / "Cargo.lock").read_text())["package"]
    for name, version, checksum in [
        (pin["crate"], pin["version"], pin["checksum"]),
        ("lz4-sys", pin["lz4_version"], pin["lz4_checksum"]),
    ]:
        matches = [p for p in packages if p["name"] == name]
        if len(matches) != 1 or (matches[0]["version"], matches[0].get("checksum")) != (version, checksum):
            raise ValueError(f"{name} differs from the prebuilt pin; update the pin and rebuild the dev image")
    expected = ":".join(pin[k] for k in ("version", "checksum", "lz4_version", "lz4_checksum")) + ":" + hashlib.sha256((root / "docker/rocksdb-prebuilt.json").read_bytes()).hexdigest()
    if image_pin != expected:
        raise ValueError("dev image lacks the matching prebuilt RocksDB; run scripts/cargo-dev --build")
    state = tomllib.loads((root / "crates/arroyo-state/Cargo.toml").read_text())
    rocks = state["dependencies"]["rocksdb"]
    if rocks.get("default-features", True) or set(rocks.get("features", [])) != {"bindgen-runtime", "lz4"}:
        raise ValueError("RocksDB features differ from the prebuilt image; update its native build contract")
    if env.get("ROCKSDB_COMPILE", "").lower() in {"1", "true"}:
        return "1"
    # Parse the requested profile for conservative preflight; the executable
    # selector later validates the actual host/runtime native settings.
    profile = {"test": "test", "bench": "bench"}.get(args[0] if args else "", "dev")
    target = pin["target"]
    for i, arg in enumerate(args):
        if arg == "--":
            break
        if arg == "--release":
            profile = "release"
        elif arg == "--profile" and i + 1 < len(args):
            profile = args[i + 1]
        elif arg.startswith("--profile="):
            profile = arg.split("=", 1)[1]
        elif arg == "--target" and i + 1 < len(args):
            target = args[i + 1]
        elif arg.startswith("--target="):
            target = arg.split("=", 1)[1]
        elif arg == "--config" or arg.startswith("--config="):
            return "1"
    if target != pin["target"]:
        return "1"
    if any((root / ".cargo" / name).exists() for name in ("config", "config.toml")):
        return "1"
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    if not profiles_match_archive(workspace, profile, pin):
        return "1"
    replacements = list(workspace.get("replace", {}))
    for registry in workspace.get("patch", {}).values():
        for name, replacement in registry.items():
            if name in {"librocksdb-sys", "lz4-sys"} or replacement.get("package") in {"librocksdb-sys", "lz4-sys"}:
                return "1"
    if any(key.split(":", 1)[0] in {"librocksdb-sys", "lz4-sys"} for key in replacements):
        return "1"
    for name in env:
        if name.startswith("CARGO_PROFILE_") and not name.endswith("_INCREMENTAL"):
            return "1"
        if (name != "CC_ENABLE_DEBUG_OUTPUT" and name.startswith(("CC_", "CXX_", "CFLAGS_", "CXXFLAGS_"))) or name in {"HOST_CC", "HOST_CXX", "TARGET_CC", "TARGET_CXX", "HOST_CFLAGS", "HOST_CXXFLAGS", "TARGET_CFLAGS", "TARGET_CXXFLAGS", "CRATE_CC_NO_DEFAULTS"}:
            return "1"
        if name in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CC", "CXX", "CFLAGS", "CXXFLAGS", "ROCKSDB_CXX_STD"}:
            return "1"
    if image_metadata is not None:
        verify_native_lock((root / "Cargo.lock").read_text(), image_metadata["build_lock"], image_metadata["build_lock_sha256"])
    return "0"


if __name__ == "__main__":
    try:
        root = Path(sys.argv[1])
        mode = build_mode(root, sys.argv[2], sys.argv[4:], os.environ)
        if mode == "0":
            metadata = image_lock_metadata(sys.argv[3])
            mode = build_mode(root, sys.argv[2], sys.argv[4:], os.environ, metadata)
        print(mode)
    except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as error:
        print(f"Build configuration error: {error}", file=sys.stderr)
        sys.exit(2)
