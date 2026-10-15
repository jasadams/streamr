"""Compare the resolved native build graph without invalidating unrelated Rust crates."""
import hashlib
import json
import re
import os
import signal
import uuid
import subprocess
import tomllib

ROOTS = ('librocksdb-sys', 'lz4-sys')


def native_graph(lock_text):
    packages = tomllib.loads(lock_text)['package']

    def identity(package):
        # Image harness uses the exact checksum-verified librocksdb-sys source
        # as a path crate; workspace lock uses its registry package identity.
        source = '' if package['name'] == 'librocksdb-sys' else package.get('source', '')
        return package['name'], package['version'], source

    def resolve(dependency):
        match = re.fullmatch(r'(\S+)(?: ([^\s(]+))?(?: \((.+)\))?', dependency)
        if not match:
            raise ValueError(f'Invalid Cargo.lock dependency: {dependency}')
        name, version, source = match.groups()
        matches = [package for package in packages if package['name'] == name
                   and (version is None or package['version'] == version)
                   and (source is None or package.get('source') == source)]
        if len(matches) != 1:
            raise ValueError(f'Cannot resolve native lock dependency uniquely: {dependency}')
        return matches[0]

    pending = []
    for name in ROOTS:
        matches = [package for package in packages if package['name'] == name]
        if len(matches) != 1:
            raise ValueError(f'Cannot resolve native lock root uniquely: {name}')
        pending.extend(matches)
    result = {}
    while pending:
        package = pending.pop()
        key = identity(package)
        if key in result:
            continue
        dependencies = [resolve(dependency) for dependency in package.get('dependencies', [])]
        result[key] = (
            '' if package['name'] == 'librocksdb-sys' else package.get('checksum', ''),
            tuple(sorted(identity(dependency) for dependency in dependencies)),
        )
        pending.extend(dependencies)
    return result


def verify_native_lock(current_lock, archived_lock, expected_hashes):
    if not expected_hashes:
        raise ValueError('Image lacks native build-lock receipt hashes; rebuild dev image')
    actual = hashlib.sha256(archived_lock.encode()).hexdigest()
    if any(expected != actual for expected in expected_hashes):
        raise ValueError('Archived native build lock checksum differs from archive receipt; rebuild dev image')
    archived = native_graph(archived_lock)
    current = native_graph(current_lock)
    changed = set()
    for key, (checksum, required_edges) in archived.items():
        current_node = current.get(key)
        if current_node is None:
            changed.add(key[0])
            continue
        current_checksum, current_edges = current_node
        if current_checksum != checksum:
            changed.add(key[0])
        # Cargo.lock records the feature union of the whole workspace. A
        # workspace can add optional dependency names to an otherwise identical
        # package without changing any source used to build this archive. Only
        # archive-required names must retain their exact resolved identities.
        required_names = {edge[0] for edge in required_edges}
        for name in required_names:
            required = {edge for edge in required_edges if edge[0] == name}
            actual = {edge for edge in current_edges if edge[0] == name}
            if actual != required:
                changed.add(key[0])
                changed.add(name)
    if changed:
        raise ValueError('Native build dependencies differ from image (' + ', '.join(sorted(changed)) +
                         '); rebuild dev image or explicitly select source mode with a reason')



def image_lock_metadata(image_id):
    """Read only immutable image metadata; no mounts, network or compilation."""
    if not re.fullmatch(r'(sha256:)?[0-9a-f]{64}', image_id):
        raise ValueError('Native metadata requires an immutable image ID')
    reader = '''import json,pathlib
root=pathlib.Path('/opt/streamr-rocksdb')
pin=json.loads((root/'manifest.json').read_text())
print(json.dumps({'build_lock':(root/'build-Cargo.lock').read_bytes().decode(),
 'build_lock_sha256':[json.loads((root/name/'receipt.json').read_text())['build_lock_sha256'] for name in pin['profiles']]}))
'''
    name = f'streamr-native-lock-check-{os.getpid()}-{uuid.uuid4().hex[:12]}'
    previous = {}

    def interrupted(signum, frame):
        raise InterruptedError(f'Native metadata inspection interrupted by signal {signum}')

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, interrupted)
        result = subprocess.run(['podman', 'run', '--rm', '--network=none', '--name', name,
                                 '--entrypoint', 'python3', image_id, '-c', reader], capture_output=True,
                                text=True, timeout=30, check=False)
    finally:
        # The run client can time out or receive a signal after creating the
        # container. Always remove this uniquely owned helper by its exact name.
        for signum in previous:
            signal.signal(signum, signal.SIG_IGN)
        try:
            cleanup = subprocess.run(['podman', 'rm', '--force', '--ignore', name],
                                     capture_output=True, text=True, timeout=15, check=False)
            if cleanup.returncode:
                raise ValueError(f'Native metadata helper cleanup failed for {name}: ' + cleanup.stderr.strip())
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    if result.returncode:
        raise ValueError('Cannot read image native build metadata; rebuild dev image: ' + result.stderr.strip())
    try:
        metadata = json.loads(result.stdout)
        if len(result.stdout) > 2 * 1024 * 1024:
            raise ValueError('Native image metadata exceeds supported size')
        return metadata
    except (json.JSONDecodeError, KeyError) as error:
        raise ValueError('Invalid native image build metadata; rebuild dev image') from error
