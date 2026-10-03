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
cp docker/Dockerfile.streamr-candidate "$task_context/Dockerfile"

python3 - "$task_context/provenance.json" "$task_revision" "$task_source" \
    "$task_input_sha" "$task_binary_sha" "$task_base_id" <<'PY'
import json
import pathlib
import sys

destination, revision, source, original, packaged, base = sys.argv[1:]
pathlib.Path(destination).write_text(json.dumps({
    'git_revision': revision,
    'source_sha256': source,
    'original_binary_sha256': original,
    'packaged_binary_sha256': packaged,
    'base_image_id': base,
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
