#!/usr/bin/env python3
"""Finite real-SQL multi-epoch/fresh-process checkpoint qualification.

Preparation is not qualification. Requires the reviewed SQL-testing executable;
every restore starts another OS process and compares independently prepared rows.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import resource


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def payload(key, version, size):
    return hashlib.shake_256(f'{key}/{version}'.encode()).hexdigest((size + 1) // 2)[:size]


def query(input_path, output_path):
    def quoted(path):
        return str(path).replace("'", "''")
    return f"""CREATE TABLE mutations (seq BIGINT NOT NULL, k BIGINT NOT NULL, mode TEXT NOT NULL,
 quantity BIGINT, payload TEXT, expected_old TEXT)
WITH (connector='single_file', path='{quoted(input_path)}', format='json', type='source', wait_for_control='true');
CREATE TABLE observations (seq BIGINT, k BIGINT, action TEXT, old_quantity BIGINT,
 new_quantity BIGINT, old_matches BOOLEAN, new_matches BOOLEAN,
 lookup_matches BOOLEAN, lookup_absent BOOLEAN)
WITH (connector='single_file', path='{quoted(output_path)}', format='json', type='sink');
CREATE STATE TABLE inventory (k BIGINT PRIMARY KEY, quantity BIGINT, payload TEXT) PARTITION BY k;
CREATE VIEW applied AS MERGE INTO inventory AS target USING mutations AS source ON target.k=source.k
WHEN MATCHED AND source.mode='delete' THEN DELETE
WHEN MATCHED AND source.mode='put' THEN UPDATE SET quantity=source.quantity, payload=source.payload
WHEN NOT MATCHED AND source.mode='put' THEN INSERT (k,quantity,payload) VALUES (source.k,source.quantity,source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO observations SELECT r.source.seq AS seq, r.source.k AS k, r.action AS action,
 r.old.quantity AS old_quantity, r.new.quantity AS new_quantity,
 r.old.payload=r.source.expected_old AS old_matches,
 r.new.payload=r.source.payload AS new_matches,
 i.payload=r.source.payload AS lookup_matches, i.k IS NULL AS lookup_absent
FROM applied r LEFT JOIN inventory i ON i.k=r.source.k;
"""


def emit_event(stream, oracle, state, seq, key, mode, version=0, size=128):
    old_version = state.get(key)
    old = (old_version, payload(key, old_version, size)) if old_version is not None else None
    value = (version, payload(key, version, size)) if mode == 'put' else None
    action = 'none'
    if mode == 'put':
        action = 'insert' if old is None else 'update'
        state[key] = version
    elif mode == 'delete' and old is not None:
        action = 'delete'
        del state[key]
    new_version = state.get(key)
    new = (new_version, payload(key, new_version, size)) if new_version is not None else None
    event = dict(seq=seq, k=key, mode=mode, quantity=version,
                 payload=new[1] if new else None,
                 expected_old=old[1] if old else None)
    stream.write(json.dumps(event, separators=(',', ':')) + '\n')
    expected = dict(seq=seq, k=key, action=action,
                    old_quantity=old[0] if old else None, new_quantity=new[0] if new else None,
                    old_matches=True if old else None, new_matches=True if new else None,
                    lookup_matches=True if new else None, lookup_absent=new is None)
    oracle.write(json.dumps(expected, separators=(',', ':')) + '\n')


def clean_environment(case, backend, protocol, timeout, max_databases=1):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('STREAMR_TEST_', 'STREAMR_CAPTURE_', 'STREAMR_NATIVE_', 'ARROYO__'))
           and key != 'CHECKPOINT_URL'}
    env.update(STREAMR_TEST_TYPED_SQL='1', STREAMR_TEST_SOURCE_BATCH_ROWS='1',
               STREAMR_TEST_BACKEND=backend, STREAMR_TEST_CHECKPOINT_MODE=protocol,
               STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(timeout),
               STREAMR_TEST_MAX_OPEN_DATABASES=str(max_databases),
               CHECKPOINT_URL=(case / 'checkpoints').as_uri(),
               STREAMR_CAPTURE_QUERY=str(case / 'query.sql'),
               STREAMR_CAPTURE_OUTPUT=str(case / 'output.jsonl'),
               STREAMR_NATIVE_INPUT_PATH=str(case / 'input.jsonl'),
               STREAMR_NATIVE_JOB='native-cp-' + case.name)
    return env


def process_sample(pid):
    try:
        root = Path('/proc') / str(pid)
        stat = (root / 'stat').read_text().rsplit(')', 1)[1].split()
        io = dict(line.split(':', 1) for line in (root / 'io').read_text().splitlines())
        return dict(monotonic=time.monotonic(), unix=time.time(), pid=pid, start=stat[19],
                    cpu_seconds=(int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK'),
                    rss_bytes=int(stat[21]) * os.sysconf('SC_PAGE_SIZE'),
                    read_bytes=int(io['read_bytes']), write_bytes=int(io['write_bytes']))
    except (FileNotFoundError, ProcessLookupError):
        return None


def run_child(binary, test, case, env, timeout, rss_limit, label):
    """Bounded process session with raw CPU/RSS/I/O samples and owned cleanup."""
    command = [str(binary), test, '--ignored', '--test-threads=1', '--nocapture']
    started = time.monotonic()
    sampler_start = resource.getrusage(resource.RUSAGE_SELF)
    samples, failure, interrupted = [], None, None
    sampling_exit_races = 0
    output_offset, output_rows, output_partial = 0, 0, b''
    with (case / f'{label}.log').open('xb') as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                 cwd=case, start_new_session=True)
        try:
            while child.poll() is None:
                try:
                    sample = process_sample(child.pid)
                except PermissionError as permission_error:
                    # Linux may revoke /proc/PID/io access during process exit
                    # after the loop's poll. A live child's sampling failure
                    # remains fatal; allow at most one sampling interval for
                    # Linux's exit teardown to become waitable, then require
                    # confirmed termination rather than hiding live failures.
                    try:
                        child.wait(timeout=0.1)
                    except subprocess.TimeoutExpired:
                        raise permission_error
                    sampling_exit_races += 1
                    sample = None
                if sample:
                    if label == 'performance':
                        try:
                            with (case / 'output.jsonl').open('rb') as output:
                                output.seek(output_offset)
                                added = output.read(1048576)
                            output_offset += len(added)
                            complete = output_partial + added
                            boundary = complete.rfind(b'\n') + 1
                            output_rows += complete[:boundary].count(b'\n')
                            output_partial = complete[boundary:]
                            require(len(output_partial) <= 65536, 'performance output row exceeds declared bound')
                        except FileNotFoundError:
                            pass
                        sample.update(observed_completed_rows=output_rows,
                                      observed_complete_output_bytes=output_offset - len(output_partial))
                        if len(samples) % 10 == 0:
                            with Path('/proc/stat').open() as host:
                                sample['host_cpu_ticks'] = host.readline().strip()
                            sample['host_loadavg'] = list(os.getloadavg())
                    samples.append(sample)
                    if sample['rss_bytes'] > rss_limit:
                        failure = 'declared child RSS envelope exceeded'
                        break
                if time.monotonic() - started >= timeout:
                    failure = 'finite child deadline exceeded'
                    break
                time.sleep(0.1)
        except BaseException as error:
            failure = f'{type(error).__name__}: {error}'
            interrupted = error
        finally:
            if child.poll() is None:
                # New session is owned exclusively by this Popen. Kill and join it.
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait(timeout=3)
    (case / f'{label}.samples.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in samples))
    sampler_end = resource.getrusage(resource.RUSAGE_SELF)
    result = dict(command=command, returncode=child.returncode, failure=failure,
                  elapsed_seconds=time.monotonic() - started,
                  supervisor_cpu_seconds=(sampler_end.ru_utime + sampler_end.ru_stime
                                          - sampler_start.ru_utime - sampler_start.ru_stime),
                  peak_sampled_rss_bytes=max((row['rss_bytes'] for row in samples), default=0),
                  samples=len(samples), sampling_exit_races=sampling_exit_races,
                  sample_scope='owned test process including its engine threads; 100ms sampled RSS')
    write(case / f'{label}.process.json', result)
    if interrupted is not None:
        raise interrupted
    require(not failure and child.returncode == 0, f'{label}: {failure or child.returncode}; inspect saved log')
    require('1 passed' in (case / f'{label}.log').read_text(), f'{label}: expected exactly one passed native test')
    return result


def compare_jsonl(actual, expected, prefix=None):
    with Path(actual).open() as left, Path(expected).open() as right:
        rows = 0
        while prefix is None or rows < prefix:
            a, b = left.readline(), right.readline()
            if not a or not b:
                require(not a and not b, f'{actual}: missing/extra output at row {rows + 1}')
                break
            require(json.loads(a) == json.loads(b), f'{actual}: exact output differs at row {rows + 1}')
            rows += 1
        require(not left.readline(), f'{actual}: extra output after declared prefix')
        if prefix is not None:
            require(rows == prefix, f'{actual}: incomplete declared prefix')
    return rows


def frozen_files(paths):
    return {str(Path(path).resolve()): digest(path) for path in paths}


def verify_frozen(files):
    for name, expected in files.items():
        require(digest(name) == expected, 'frozen artifact changed: ' + name)


def wire(data):
    """Read bounded metadata protobuf, without depending on product-generated code."""
    require(len(data) <= 3 * 1048576, 'checkpoint metadata exceeds 3MiB')
    position, fields = 0, {}
    def number():
        nonlocal position
        value = 0
        for shift in range(0, 70, 7):
            require(position < len(data), 'truncated checkpoint protobuf')
            byte = data[position]
            position += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError('oversized protobuf integer')
    while position < len(data):
        tag = number()
        field, kind = tag >> 3, tag & 7
        require(field > 0, 'invalid protobuf field')
        if kind == 0:
            value = number()
        elif kind == 2:
            length = number()
            require(length <= len(data) - position, 'truncated protobuf field')
            value = data[position:position + length]
            position += length
        elif kind in (1, 5):
            length = 8 if kind == 1 else 4
            require(position + length <= len(data), 'truncated fixed protobuf field')
            value = data[position:position + length]
            position += length
        else:
            raise ValueError('unsupported protobuf wire type')
        fields.setdefault(field, []).append(value)
    return fields


def one(fields, field, default=None):
    values = fields.get(field, [])
    require(len(values) <= 1, 'duplicate protobuf scalar')
    return values[0] if values else default


def native_inventory(checkpoint_root, job, protocol, epoch, verify_objects=True):
    """Native descriptors from an exact known epoch; never list all object history."""
    if protocol == 'leader':
        path = checkpoint_root / f'pipe-test/{job}/generations/0/checkpoints/checkpoint-{epoch:07}/checkpoint-manifest.pb'
        manifest = wire(path.read_bytes())
        require(one(manifest, 2) == job.encode() and one(manifest, 4, 0) == epoch, 'selected leader checkpoint differs')
        operators = [wire(value) for value in manifest.get(10, [])]
    else:
        directory = checkpoint_root / f'{job}/checkpoints/checkpoint-{epoch:07}'
        manifest = wire((directory / 'metadata').read_bytes())
        require(one(manifest, 1) == job.encode() and one(manifest, 2, 0) == epoch, 'selected controller checkpoint differs')
        operators = [wire((directory / f'operator-{name.decode()}/metadata').read_bytes())
                     for name in manifest.get(6, [])]
    descriptors = []
    for operator in operators:
        native = one(operator, 15)
        if native is None:
            continue
        metadata = wire(native)
        require(one(metadata, 1, 0) == 1 and one(metadata, 3, 0) == epoch,
                'native format/epoch differs from selected checkpoint')
        files = []
        for encoded in metadata.get(8, []):
            record = wire(encoded)
            relative = one(record, 2, b'').decode()
            require(relative and not relative.startswith('/') and all(part not in ('', '.', '..') for part in relative.split('/')),
                    'unsafe native file path')
            checksum = one(record, 4, b'')
            require(len(checksum) == 32, 'native checksum length differs')
            full = (checkpoint_root / relative).resolve()
            require(full.is_relative_to(checkpoint_root.resolve()), 'native file escapes checkpoint root')
            size = one(record, 3, 0)
            if verify_objects:
                require(full.is_file() and full.stat().st_size == size and digest(full) == checksum.hex(),
                        'native checkpoint file size/checksum differs')
            files.append(dict(name=one(record, 1, b'').decode(), path=relative, size_bytes=size,
                              sha256=checksum.hex(), shared=bool(one(record, 5, 0)),
                              birth_epoch=one(record, 7, 0)))
        require(files and len({item['name'] for item in files}) == len(files), 'missing/duplicate native file inventory')
        descriptors.append(dict(database_id=one(metadata, 5, b'').hex(), epoch=epoch,
                                completed_base_epoch=one(metadata, 9), files=files))
    return descriptors


def lifecycle_case(root, binary, protocol, backend, timeout, rss_limit):
    case = root / f'{protocol}-{backend}'
    case.mkdir()
    (case / 'checkpoints').mkdir()
    events = [(1, 'put', 1), (2, 'put', 1), (1, 'put', 2),
              (2, 'delete', 0), (1, 'delete', 0), (3, 'put', 3),
              (1, 'probe', 0), (2, 'probe', 0), (3, 'probe', 0)]
    state = {}
    with (case / 'input.jsonl').open('w') as stream, (case / 'expected.jsonl').open('w') as oracle:
        for seq, (key, mode, version) in enumerate(events, 1):
            emit_event(stream, oracle, state, seq, key, mode, version)
    (case / 'query.sql').write_text(query(case / 'input.jsonl', case / 'output.jsonl'))
    files = frozen_files([binary, Path(__file__), case / 'input.jsonl', case / 'expected.jsonl', case / 'query.sql'])
    env = clean_environment(case, backend, protocol, timeout)
    env['STREAMR_NATIVE_EPOCH_PREFIXES'] = '2,2,4,5,5,6'
    write(case / 'controls.json', dict(files=files, env={key: value for key, value in env.items()
          if key.startswith(('STREAMR_', 'CHECKPOINT_URL'))}, prefixes=[2, 2, 4, 5, 5, 6]))
    run_child(binary, 'native_incremental_checkpoint_capture', case, env, timeout, rss_limit, 'capture')
    inventories = []
    for epoch, prefix in enumerate([2, 2, 4, 5, 5, 6], 1):
        compare_jsonl(case / f'output.checkpoint-{epoch}.jsonl', case / 'expected.jsonl', prefix)
        inventory = native_inventory(case / 'checkpoints', env['STREAMR_NATIVE_JOB'], protocol, epoch)
        require(bool(inventory) == (backend == 'rocksdb'), 'selected backend/native representation differs')
        inventories.append(inventory)
    if backend == 'rocksdb':
        # Background compaction may legitimately replace files even without SQL
        # input. Every still-identical SST must retain its verified object path.
        def shared(values):
            return {(desc['database_id'], item['name'], item['size_bytes'], item['sha256']): item['path']
                    for desc in values for item in desc['files'] if item['shared']}
        previous, current = shared(inventories[0]), shared(inventories[1])
        eligible = previous.keys() & current.keys()
        require(eligible, 'idle reuse coverage missing: no unchanged native SST identity survived capture')
        require(all(previous[key] == current[key] for key in eligible), 'unchanged native SST was uploaded to a new object')
    # A fresh process for every selected epoch and each destination backend.
    for target in ('rocksdb', 'memory'):
        for epoch in range(1, 7):
            output = case / 'output.jsonl'
            output.write_bytes((case / f'output.checkpoint-{epoch}.jsonl').read_bytes())
            restored = dict(env, STREAMR_TEST_BACKEND=target, STREAMR_NATIVE_RESTORE_EPOCH=str(epoch))
            label = f'restore-{target}-{epoch}'
            run_child(binary, 'native_incremental_checkpoint_capture', case, restored, timeout, rss_limit, label)
            compare_jsonl(output, case / 'expected.jsonl')
            output.rename(case / f'{label}.jsonl')
            verify_frozen(files)
    write(case / 'result.json', dict(status='PASS', source_backend=backend, protocol=protocol,
          exact_epochs=6, fresh_process_restores=12, max_open_databases=1,
          native_inventories=inventories, files=files))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--protocol', choices=('controller', 'leader'), action='append')
    parser.add_argument('--backend', choices=('memory', 'rocksdb'), action='append')
    parser.add_argument('--timeout-seconds', type=int, default=120)
    parser.add_argument('--rss-limit-mib', type=int, default=512)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.directory.resolve()
    require(not root.exists(), 'evidence directory must be new')
    require(args.timeout_seconds > 0 and args.rss_limit_mib > 0, 'positive limits required')
    root.mkdir(parents=True)
    for protocol in dict.fromkeys(args.protocol or ('controller', 'leader')):
        for backend in dict.fromkeys(args.backend or ('rocksdb', 'memory')):
            lifecycle_case(root, binary, protocol, backend, args.timeout_seconds, args.rss_limit_mib * 1048576)
    write(root / 'result.json', dict(status='PASS', scope='native SQL six epochs and fresh-process cross-backend continuation; not process-loss or beyond-RAM qualification'))


if __name__ == '__main__':
    main()
