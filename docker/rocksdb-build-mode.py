#!/usr/bin/env python3
"""Validate the image's native pin and choose prebuilt versus source compilation."""
import json
import os
from pathlib import Path
import sys
import tomllib


def build_mode(root, image_pin, args, env):
    pin = json.loads((root / "docker/rocksdb-prebuilt.json").read_text())
    packages = tomllib.loads((root / "Cargo.lock").read_text())["package"]
    for name, version, checksum in [
        (pin["crate"], pin["version"], pin["checksum"]),
        ("lz4-sys", pin["lz4_version"], pin["lz4_checksum"]),
    ]:
        matches = [p for p in packages if p["name"] == name]
        if len(matches) != 1 or (matches[0]["version"], matches[0].get("checksum")) != (version, checksum):
            raise ValueError(f"{name} differs from the prebuilt pin; update the pin and rebuild the dev image")
    expected = ":".join(pin[k] for k in ("version", "checksum", "lz4_version", "lz4_checksum"))
    if image_pin != expected:
        raise ValueError("dev image lacks the matching prebuilt RocksDB; run scripts/cargo-dev --build")
    state = tomllib.loads((root / "crates/arroyo-state/Cargo.toml").read_text())
    rocks = state["dependencies"]["rocksdb"]
    if rocks.get("default-features", True) or set(rocks.get("features", [])) != {"bindgen-runtime", "lz4"}:
        raise ValueError("RocksDB features differ from the prebuilt image; update its native build contract")
    if env.get("ROCKSDB_COMPILE", "").lower() in {"1", "true"}:
        return "1"
    # Preserve release/custom-profile native optimization by using the original
    # source build. The prebuilt archive has the development O0 configuration.
    profile = "dev"
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
    if args and args[0] == "bench":
        profile = "bench"
    if profile not in {"dev", "test"} or target != pin["target"]:
        return "1"
    if any((root / ".cargo" / name).exists() for name in ("config", "config.toml")):
        return "1"
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    profiles = workspace.get("profile", {})
    for settings in (profiles.get("dev", {}), profiles.get(profile, {})):
        native_settings = [settings, settings.get("build-override", {})]
        native_settings.extend(settings.get("package", {}).get(key, {}) for key in ("*", "librocksdb-sys"))
        for native in native_settings:
            if native.get("opt-level", 0) != 0 or native.get("debug", "line-tables-only") != "line-tables-only":
                return "1"
    for name in env:
        if name.startswith("CARGO_PROFILE_") and not name.endswith("_INCREMENTAL"):
            return "1"
        if name in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CC", "CXX", "CFLAGS", "CXXFLAGS", "ROCKSDB_CXX_STD"}:
            return "1"
    return "0"


if __name__ == "__main__":
    try:
        print(build_mode(Path(sys.argv[1]), sys.argv[2], sys.argv[3:], os.environ))
    except (ValueError, KeyError, OSError) as error:
        print(f"Build configuration error: {error}", file=sys.stderr)
        sys.exit(2)
