#!/usr/bin/env bash
# Package an already-built binary locally. This script never compiles or pushes.
set -euo pipefail

task_repo_root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$task_repo_root"

source_digest() {
    python3 - <<'PY'
import hashlib
import pathlib
import subprocess

paths = ['Cargo.toml', 'Cargo.lock', '.cargo', 'crates', 'webui',
         'docker/Dockerfile.streamr-candidate', 'docker/build-streamr-candidate.sh']
listed = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others',
                                  '--exclude-standard', '--', *paths])
digest = hashlib.sha256()
for name in sorted(set(listed.split(b'\0')) - {b''}):
    path = pathlib.Path(name.decode())
    if not path.is_file():
        continue
    digest.update(name + b'\0')
    digest.update(hashlib.sha256(path.read_bytes()).digest())
# The API embeds the ignored UI build output, so include it explicitly.
for path in sorted(pathlib.Path('webui/dist').rglob('*')):
    if path.is_file():
        digest.update(str(path).encode() + b'\0')
        digest.update(hashlib.sha256(path.read_bytes()).digest())
print(digest.hexdigest())
PY
}

if [[ ${1:-} == --source-digest ]]; then
    source_digest
    exit 0
fi

task_binary=${1:-target/debug/arroyo}
task_tag=${2:-localhost/streamr:str-1-candidate}
task_engine=${STREAMR_CONTAINER_ENGINE:-podman}
task_base=${STREAMR_BASE_IMAGE:-localhost/streamr-dev:str-1}
task_expected_source=${STREAMR_SOURCE_SHA256:?Capture --source-digest before building and supply STREAMR_SOURCE_SHA256}
task_source=$(source_digest)
if [[ "$task_source" != "$task_expected_source" ]]; then
    echo 'Source changed since the build digest was captured; rebuild before packaging.' >&2
    exit 1
fi
if [[ ! -f "$task_binary" || ! -x "$task_binary" ]]; then
    echo "Prebuilt executable missing: $task_binary" >&2
    exit 1
fi
if [[ "$task_engine" != podman && "$task_engine" != docker ]]; then
    echo 'STREAMR_CONTAINER_ENGINE must be podman or docker.' >&2
    exit 1
fi

task_base_id=$("$task_engine" image inspect --format '{{.Id}}' "$task_base")
task_revision=$(git rev-parse HEAD)
task_context=$(mktemp -d -t streamr-candidate.XXXXXXXX)
trap 'rm -rf "$task_context"' EXIT
cp "$task_binary" "$task_context/arroyo"
task_input_sha=$(sha256sum "$task_context/arroyo" | cut -d ' ' -f 1)
strip --strip-debug "$task_context/arroyo"
task_binary_sha=$(sha256sum "$task_context/arroyo" | cut -d ' ' -f 1)
if [[ ! -f webui/dist/index.html ]]; then
    echo 'Built console missing: webui/dist/index.html' >&2
    exit 1
fi
cp -a webui/dist "$task_context/webui-dist"
cp docker/Dockerfile.streamr-candidate "$task_context/Dockerfile"

python3 - "$task_context/provenance.json" "$task_revision" "$task_source" \
    "$task_input_sha" "$task_binary_sha" "$task_base_id" <<'PY'
import json
import hashlib
import pathlib
import re
import shutil
import sys

destination, revision, source, original, packaged, base = sys.argv[1:]
context = pathlib.Path(destination).parent
swagger_stage = context / 'swagger-ui'
swagger_stage.mkdir()
swagger_directories = []
for embed in sorted(pathlib.Path('target/debug/build').glob('utoipa-swagger-ui-*/out/embed.rs')):
    match = re.search(r'#\[folder\s*=\s*r"([^"]+)"\s*\]', embed.read_text())
    if match is None:
        raise SystemExit(f'Cannot read Swagger asset folder from {embed}')
    folder = match.group(1)
    if (not re.fullmatch(r'/app/target/debug/build/utoipa-swagger-ui-[0-9a-f]+/out/[^/]+/dist/?', folder)
            or any(part in ('.', '..') for part in folder.split('/'))):
        raise SystemExit(f'Unexpected Swagger asset folder in {embed}: {folder}')
    relative = pathlib.PurePosixPath(folder).relative_to('/app')
    source_dir = pathlib.Path(relative)
    if (not source_dir.is_dir()
            or any(parent.is_symlink() for parent in (source_dir, *source_dir.parents))):
        raise SystemExit(f'Generated Swagger asset directory missing or symlinked: {source_dir}')
    # Copy only public asset files, never embed.rs or the rest of target/.
    destination_dir = swagger_stage / relative
    destination_dir.mkdir(parents=True, exist_ok=True)
    for asset in sorted(source_dir.rglob('*')):
        if asset.is_symlink():
            raise SystemExit(f'Symlink not allowed in Swagger public assets: {asset}')
        if asset.is_file():
            asset_relative = asset.relative_to(source_dir)
            staged_asset = destination_dir / asset_relative
            staged_asset.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(asset, staged_asset)
    swagger_directories.append('/app/' + relative.as_posix())
if not swagger_directories:
    raise SystemExit('No generated Swagger UI public assets found under target/debug/build')

swagger_files = []
swagger_digest = hashlib.sha256()
for asset in sorted(swagger_stage.rglob('*')):
    if asset.is_file():
        runtime_path = '/app/' + asset.relative_to(swagger_stage).as_posix()
        file_hash = hashlib.sha256(asset.read_bytes()).hexdigest()
        swagger_files.append({'path': runtime_path, 'sha256': file_hash})
        swagger_digest.update(runtime_path.encode() + b'\0')
        swagger_digest.update(bytes.fromhex(file_hash))
pathlib.Path(destination).write_text(json.dumps({
    'git_revision': revision,
    'source_sha256': source,
    'original_binary_sha256': original,
    'packaged_binary_sha256': packaged,
    'base_image_id': base,
    'swagger_public_assets': {
        'directories': sorted(set(swagger_directories)),
        'sha256': swagger_digest.hexdigest(),
        'files': swagger_files,
    },
    'scope': 'local development candidate; source digest captured before build',
}, indent=2) + '\n')
PY

task_pull_flag=--pull=never
if [[ "$task_engine" == docker ]]; then
    task_pull_flag=--pull=false
fi
"$task_engine" build "$task_pull_flag" \
    --build-arg "BASE_IMAGE=$task_base_id" \
    --build-arg "BASE_IMAGE_ID=$task_base_id" \
    --build-arg "GIT_REVISION=$task_revision" \
    --build-arg "SOURCE_SHA256=$task_source" \
    --build-arg "BINARY_SHA256=$task_binary_sha" \
    --tag "$task_tag" "$task_context"

"$task_engine" image inspect --format '{{.Id}}' "$task_tag"
echo "Source SHA256: $task_source"
echo "Packaged binary SHA256: $task_binary_sha"
