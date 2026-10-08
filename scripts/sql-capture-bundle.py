#!/usr/bin/env python3
"""Package and run a caller-defined SQL checkpoint fixture without a repo mount.

The artifact is a SQL-testing ELF, its resolved shared libraries, this runner and
caller fixtures. It is not the production Arroyo service or process-loss recovery.
"""
import argparse
from bounded_row_oracle import exact_rows, expected_rows
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time


def sha256(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            result.update(block)
    return result.hexdigest()


def new_directory(path):
    if path.exists() and any(path.iterdir()):
        raise ValueError(f'preserve existing artifacts: {path} must be new or empty')
    path.mkdir(parents=True, exist_ok=True)


def exact(path, expected, ordered, prefix=None):
    exact_rows(path, expected_rows(expected), prefix_rows=prefix, ordered=ordered)


def package(args):
    binary = args.binary.resolve(strict=True)
    fixture_path = args.fixture.resolve(strict=True)
    fixture = json.loads(fixture_path.read_text())
    base = fixture_path.parent
    if any(name in fixture for name in ('idle', 'schedule')):
        raise ValueError('this bundle runner qualifies prefix/EOF capture only; idle/schedule probes require their dedicated runner')
    with binary.open('rb') as stream:
        if stream.read(4) != b'\x7fELF':
            raise ValueError('selected executable must be a trusted Linux ELF')
    query = (base/fixture['query']).resolve(strict=True).read_text()
    if '{{INPUT}}' not in query or '{{OUTPUT}}' not in query:
        raise ValueError('SQL must use quoted {{INPUT}} and {{OUTPUT}} connector path placeholders')
    # ldd is for the explicitly selected trusted, rebuilt executable only.
    dependencies = subprocess.run(['ldd', str(binary)], capture_output=True,
                                  text=True, check=True).stdout
    if 'not found' in dependencies:
        raise ValueError('unresolved shared library in selected executable')
    libraries = []
    loader = None
    for line in dependencies.splitlines():
        match = re.search(r'(?:=>\s+)?(/[^\s]+)\s+\(', line)
        if not match:
            continue  # virtual linux-vdso has no file to package
        path = Path(match[1]).resolve(strict=True)
        libraries.append(path)
        if 'ld-linux' in Path(match[1]).name or Path(match[1]).name.startswith('ld-'):
            loader = Path(match[1]).name
    if not loader:
        raise ValueError('ELF loader not identified; static/other-platform executables are unsupported')
    root = args.directory.resolve()
    new_directory(root)
    (root/'bin').mkdir()
    (root/'lib').mkdir()
    (root/'fixture').mkdir()
    # Copy bytes and mode, not source-mount extended attributes (such as SELinux
    # labels), which are neither portable nor writable by a rootless builder.
    shutil.copy(binary, root/'bin/sql-testing')
    # Preserve names reported by ldd (including SONAME aliases).
    for line in dependencies.splitlines():
        match = re.search(r'(?:=>\s+)?(/[^\s]+)\s+\(', line)
        if not match:
            continue
        path = Path(match[1])
        destination = root/'lib'/path.name
        if destination.exists() and sha256(destination) != sha256(path):
            raise ValueError(f'shared-library basename collision: {path.name}')
        if not destination.exists():
            shutil.copy(path, destination, follow_symlinks=True)
    copied = dict(fixture)
    for field in ('query', 'input', 'expected', 'expected_checkpoint'):
        source = (base/fixture[field]).resolve(strict=True)
        target = f'{field}{source.suffix}'
        shutil.copy(source, root/'fixture'/target)
        copied[field] = target
    (root/'fixture/manifest.json').write_text(json.dumps(copied, indent=2)+'\n')
    shutil.copy(Path(__file__).resolve(), root/'run.py')
    shutil.copy(Path(__file__).with_name('bounded_row_oracle.py'), root/'bounded_row_oracle.py')
    # Candidate source/build provenance is caller supplied and independently
    # retained. It does not substitute for verifying how this ELF was built.
    provenance = json.loads(args.provenance.resolve(strict=True).read_text())
    files = {str(path.relative_to(root)): sha256(path)
             for path in sorted(root.rglob('*')) if path.is_file()}
    manifest = dict(format_version=1, route=args.route, loader=loader,
                    provenance=provenance, original_fixture_sha256=sha256(fixture_path),
                    selected_binary_sha256=sha256(binary), files=files,
                    limits_scope='SQL-test process; no production service/process-loss qualification')
    (root/'bundle.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(f'PACKAGED {root}: ELF, {len(libraries)} libraries, caller fixture and hashes')


def child(command, env, log_path, timeout):
    with log_path.open('w') as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic()+timeout
        while True:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(status)
                return process.returncode, usage.ru_maxrss*1024
            if time.monotonic() >= deadline:
                process.kill()
                os.wait4(process.pid, 0)
                raise TimeoutError(f'capture exceeded {timeout}s: {log_path}')
            time.sleep(.2)


def run(args):
    root = Path(__file__).resolve().parent
    manifest = json.loads((root/'bundle.json').read_text())
    if manifest['format_version'] != 1:
        raise ValueError('unsupported bundle format')
    for name, wanted in manifest['files'].items():
        if sha256(root/name) != wanted:
            raise ValueError(f'bundle hash mismatch: {name}')
    fixture = json.loads((root/'fixture/manifest.json').read_text())
    base = root/'fixture'
    wanted = base/fixture['expected']
    checkpoint_wanted = base/fixture['expected_checkpoint']
    total = sum(1 for _ in expected_rows(wanted))
    committed = sum(1 for _ in expected_rows(checkpoint_wanted))
    prefix = fixture['checkpoint_input_rows']
    with (base/fixture['input']).open() as stream:
        input_count = sum(1 for _ in stream)
    if type(prefix) is not int or not 0 < prefix < input_count:
        raise ValueError('checkpoint_input_rows must be a positive proper source prefix')
    if fixture.get('checkpoint_output_rows', committed) != committed:
        raise ValueError('checkpoint output declaration disagrees with exact oracle')
    results = args.results.resolve()
    new_directory(results)
    timeout = fixture.get('timeout_seconds', 900)
    rss_limit = fixture.get('rss_limit_mib', 512)*1024*1024
    measurements = []
    for backend in dict.fromkeys(args.backend or ('rocksdb',)):
        for protocol in dict.fromkeys(args.protocol or ('controller', 'leader')):
            directory = results/f'{backend}-{protocol}'
            directory.mkdir()
            output = directory/'output.jsonl'
            sql = (base/fixture['query']).read_text()
            sql = sql.replace('{{INPUT}}', str(base/fixture['input']).replace("'", "''"))
            sql = sql.replace('{{OUTPUT}}', str(output).replace("'", "''"))
            query = directory/'query.sql'
            query.write_text(sql)
            env = dict(os.environ)
            for name in list(env):
                if name.startswith(('STREAMR_TEST_', 'STREAMR_CAPTURE_')):
                    env.pop(name)
            flag = {'native-windows':'STREAMR_TEST_NATIVE_WINDOWS',
                    'native-aggregates':'STREAMR_TEST_NATIVE_AGGREGATES',
                    'typed-sql':'STREAMR_TEST_TYPED_SQL'}[manifest['route']]
            # Keep checkpoint evidence in the results mount, rather than the
            # disposable runtime container's default /tmp storage.
            env.update({'ARROYO__CHECKPOINT_URL':str(directory/'checkpoints'),
                flag:'1', 'STREAMR_TEST_BACKEND':backend,
                'STREAMR_TEST_CHECKPOINT_MODE':protocol,
                'STREAMR_TEST_SOURCE_BATCH_ROWS':str(fixture.get('batch_rows', 1)),
                'STREAMR_TEST_EXECUTION_BYTES':str(16*1024*1024),
                'STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS':str(timeout),
                'STREAMR_CAPTURE_QUERY':str(query), 'STREAMR_CAPTURE_OUTPUT':str(output),
                'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT':str(prefix),
                'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS':str(total),
                'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS':str(committed),
                'STREAMR_CAPTURE_EXPECTED_ROWS':str(total), 'STREAMR_CAPTURE_CHECKPOINT_EPOCH':'1'})
            command = [str(root/'lib'/manifest['loader']), '--library-path', str(root/'lib'),
                       str(root/'bin/sql-testing'), 'external_sql_checkpoint_capture',
                       '--ignored', '--test-threads=1', '--nocapture']
            log = directory/'capture.log'
            status, rss = child(command, env, log, timeout)
            text = log.read_text()
            if status or '1 passed' not in text:
                raise RuntimeError(f'capture failed ({status}): {log}')
            ordered = fixture.get('ordered', False)
            exact(output.with_suffix('.initial.jsonl'), wanted, ordered)
            exact(output, wanted, ordered)
            observed = re.findall(r'^CAPTURE_RESULT phase=recovered checkpoint=\d+ '
                r'input_rows_before_checkpoint=\d+ committed_rows=(\d+) ', text, re.MULTILINE)
            if observed != [str(committed)]:
                raise AssertionError('missing/wrong committed checkpoint output count')
            exact(output, checkpoint_wanted, ordered, prefix=committed)
            if rss > rss_limit:
                raise RuntimeError(f'capture RSS {rss} exceeds declared {rss_limit}')
            measurements.append(dict(backend=backend, protocol=protocol,
                checkpoint_storage_root=str(directory/'checkpoints'),
                bundle_sha256=sha256(root/'bundle.json'), peak_rss_bytes=rss,
                declared_rss_limit_bytes=rss_limit, checkpoint_input_rows=prefix,
                initial_rows=total, recovered_rows=total, committed_rows=committed,
                query_sha256=sha256(query), capture_log_sha256=sha256(log),
                initial_output_sha256=sha256(output.with_suffix('.initial.jsonl')),
                recovered_output_sha256=sha256(output)))
            (results/'measurements.json').write_text(json.dumps(measurements, indent=2)+'\n')
            print(f'PASS {backend}/{protocol}: exact initial/recovered/prefix rows; RSS={rss}',flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command',required=True)
    pack = commands.add_parser('package')
    pack.add_argument('--binary',type=Path,required=True)
    pack.add_argument('--fixture',type=Path,required=True)
    pack.add_argument('--provenance',type=Path,required=True,help='Caller-retained source/build evidence JSON')
    pack.add_argument('--directory',type=Path,required=True)
    pack.add_argument('--route',choices=('native-windows','native-aggregates','typed-sql'),required=True)
    execute = commands.add_parser('run')
    execute.add_argument('--results',type=Path,required=True)
    execute.add_argument('--backend',choices=('memory','rocksdb'),action='append')
    execute.add_argument('--protocol',choices=('controller','leader'),action='append')
    args = parser.parse_args()
    if args.command == 'package':
        package(args)
    else:
        run(args)


if __name__ == '__main__':
    main()
