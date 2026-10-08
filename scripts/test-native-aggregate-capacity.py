#!/usr/bin/env python3
"""Exact native updating-aggregate capacity fixtures; no captured-value oracles.

Preparation alone is not qualification. Run one child/protocol at a time in the
required container. No resource overrides: current native aggregate pool sum78MiB.
"""
import argparse
from datetime import datetime, timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('capacity', Path(__file__).with_name('test-native-window-capacity.py'))
capacity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capacity)
POOL_MIB = 78
BASE = datetime(2023, 10, 9, 17, 13, 20)


def token(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def distinct_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def source_inventory(repo, evidence):
    paths = ['crates', '.cargo', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'rust-toolchain', 'build.rs']
    build = json.loads(evidence.read_text(), object_pairs_hook=distinct_object)
    declared = dict(build['compiled_sources'])
    declared.update(build.get('workspace_build_files', {}))
    declared.update(build.get('reviewed_source', {}).get('rust_sources', {}))
    files = {}
    for name, expected in declared.items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or not (name.startswith(('crates/', '.cargo/')) or name in paths):
            raise ValueError('invalid host compiled-source path')
        actual = digest(repo / path)
        if actual != expected:
            raise ValueError('mounted compiled source differs from host evidence: ' + name)
        files[name] = actual
    for name in ('Cargo.toml', 'Cargo.lock', 'crates/arroyo-sql-testing/src/smoke_tests.rs',
                 'crates/arroyo-sql-testing/src/smoke_schedule_tests.rs'):
        if name not in files:
            raise ValueError('host evidence missing required workspace/capture source: ' + name)
    for name in paths[4:]:
        if (repo / name).is_file():
            files[name] = digest(repo / name)
    if (repo / '.cargo').is_dir():
        for path in (repo / '.cargo').rglob('*'):
            if path.is_file():
                files[str(path.relative_to(repo))] = digest(path)
    for path in (Path(__file__).resolve(), Path(capacity.__file__).resolve()):
        files[str(path.relative_to(repo))] = digest(path)
    return dict(compiled_diff_sha256=build['compiled_diff_sha256'], files=files,
                supplied_repository_head=build['repository_head'])


def payload(item, size):
    return capacity.payload_for(item, size)


def many_row(key, count, size):
    a, b = payload(2 * key, size), payload(2 * key + 1, size)
    return dict(k=str(key), n=count, max_payload=a if count == 1 else max(a, b))


def hot_row(stage, members, size):
    kind, n = stage
    if kind == 'append':
        return dict(k='hot', n=n, lo=1, hi=n, last_payload=payload(n - 1, size))
    if kind == 'delete-min':
        return dict(k='hot', n=members - 1, lo=2, hi=members,
                    last_payload=payload(members - 1, size))
    if kind == 'delete-max':
        return dict(k='hot', n=members - 2, lo=2, hi=members - 1,
                    last_payload=payload(members - 2, size))
    if kind == 'replace':
        return dict(k='hot', n=members - 2, lo=2, hi=members + 100,
                    last_payload=payload(members + 1, size))
    raise ValueError('unknown oracle stage')


def hot_stage(row, members, size):
    n = row.get('n')
    if type(n) is not int or not 1 <= n <= members:
        raise ValueError('invalid hot aggregate count/type')
    candidates = [('append', n), ('delete-min', 0), ('delete-max', 0), ('replace', 0)]
    for stage in candidates:
        if token(row) == token(hot_row(stage, members, size)):
            return stage
    raise ValueError('hot row differs from every exact declared input-prefix aggregate')


def query(scenario, input_path, output_path):
    def q(path):
        return str(path).replace("'", "''")
    source = ("event_ts TIMESTAMP NOT NULL, k TEXT NOT NULL, payload TEXT NOT NULL, "
              "WATERMARK FOR event_ts AS event_ts")
    if scenario == 'hot':
        source = 'row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, ordinal BIGINT NOT NULL, metric BIGINT NOT NULL, payload TEXT NOT NULL'
    fields = 'k TEXT, n BIGINT, max_payload TEXT' if scenario == 'many' else 'k TEXT, n BIGINT, lo BIGINT, hi BIGINT, last_payload TEXT'
    aggregates = 'COUNT(*) AS n, MAX(payload) AS max_payload' if scenario == 'many' else 'COUNT(*) AS n, MIN(metric) AS lo, MAX(metric) AS hi, LAST_VALUE(payload ORDER BY ordinal) AS last_payload'
    return f"""SET updating_ttl = NULL;
CREATE TABLE aggregate_input ({source}) WITH (connector='single_file',
 path='{q(input_path)}', format='{'json' if scenario == 'many' else 'debezium_json'}', type='source', wait_for_control='true');
CREATE TABLE aggregate_output ({fields}) WITH (connector='single_file',
 path='{q(output_path)}', format='debezium_json', type='sink');
INSERT INTO aggregate_output SELECT k, {aggregates} FROM aggregate_input GROUP BY k;
"""


def prepare(root, scenario, keys, members, size, require_10x, timeout, rss_limit):
    checkpoint = 2 * keys - 1 if scenario == 'many' else members - 1
    source_rows = 3 * keys if scenario == 'many' else members + 3
    cp_floor = (keys if scenario == 'many' else checkpoint) * size
    full_floor = (keys if scenario == 'many' else members) * size
    threshold = 10 * POOL_MIB * 1024 * 1024
    if require_10x and min(cp_floor, full_floor) < threshold:
        raise ValueError('checkpoint and live payload floors must each reach10x actual78MiB pool sum (before fixture writes)')
    root.mkdir(parents=True, exist_ok=False)
    settings = dict(scenario=scenario, keys=keys, members=members, payload_bytes=size,
        input_rows=source_rows, checkpoint_input_rows=checkpoint,
        checkpoint_retained_payload_floor_bytes=cp_floor, live_retained_payload_floor_bytes=full_floor,
        declared_pool_budget_mib=POOL_MIB,
        pool_budget_mib=dict(execution=16, block_cache=8, memtable=4, queued_write=32, decoded=16, scan=2),
        checkpoint_exceeds_10x_pool=cp_floor >= threshold, live_exceeds_10x_pool=full_floor >= threshold,
        require_10x=require_10x, timeout_seconds=timeout, child_rss_limit_mib=rss_limit,
        retention_premise=('Append-only MAX stores at least one full independent payload per key; count metadata not counted.' if scenario == 'many' else
            'Retracting LAST_VALUE stores each accepted full payload in M/R occurrence indexes; assumes actual admission. Large8192-byte args exceed unchanged512-byte R key limit: valid configured-limit refusal, NOT capacity qualification or justification for a new architecture.'))
    input_path = root / 'input.jsonl'
    with input_path.open('w') as out:
        def emit(value):
            out.write(token(value) + '\n')
        if scenario == 'many':
            for occurrence in (0, 1):
                for key in range(keys):
                    item = occurrence * keys + key
                    emit(dict(event_ts=(BASE + timedelta(milliseconds=item)).isoformat(),
                              k=str(key), payload=payload(2 * key + occurrence, size)))
            # Touch every restored group: saved sink-prefix rows alone cannot
            # prove aggregate state restoration. Empty is below nonempty hex.
            for key in range(keys):
                emit(dict(event_ts=(BASE + timedelta(milliseconds=2 * keys + key)).isoformat(),
                          k=str(key), payload=''))
        else:
            def member(item, replacement=False):
                return dict(row_id=item, k='hot', ordinal=members + 100 if replacement else item,
                            metric=members + 100 if replacement else item + 1,
                            payload=payload(members + 1 if replacement else item, size))
            def change(before, after, op, sequence):
                emit(dict(before=before, after=after, op=op,
                          ts_ms=1696871600000 + sequence))
            for item in range(members):
                change(None, member(item), 'c', item)
            change(member(0), None, 'd', members)
            change(member(members - 1), None, 'd', members + 1)
            change(member(members - 2), member(members - 2, True), 'u', members + 2)
    for phase in ('checkpoint', 'final'):
        with (root / ('expected.' + phase + '.jsonl')).open('w') as out:
            if scenario == 'many':
                for key in range(keys):
                    count = (1 if key == keys - 1 else 2) if phase == 'checkpoint' else 3
                    out.write(token(many_row(key, count, size)) + '\n')
            else:
                stage = ('append', checkpoint) if phase == 'checkpoint' else ('replace', 0)
                out.write(token(hot_row(stage, members, size)) + '\n')
    write(root / 'fixture.json', settings)
    return settings


def compare_capture(path, settings, checkpoint_rows, expected_checkpoint, expected_final):
    """Streaming exact CDC; keep only per-key stage/count, never payload history."""
    current = {}
    size, keys, members = (settings[n] for n in ('payload_bytes', 'keys', 'members'))
    scenario = settings['scenario']

    def decode(row):
        if type(row) is not dict:
            raise ValueError('aggregate row must be an object')
        if scenario == 'many':
            if set(row) != {'k', 'n', 'max_payload'} or type(row['k']) is not str or type(row['n']) is not int:
                raise ValueError('many row fields/types differ')
            key = int(row['k'])
            if str(key) != row['k'] or not 0 <= key < keys or row['n'] not in (1, 2, 3):
                raise ValueError('unknown many key/count')
            if token(row) != token(many_row(key, row['n'], size)):
                raise ValueError('many full payload/whole prefix row differs')
            return row['k'], row['n']
        if set(row) != {'k', 'n', 'lo', 'hi', 'last_payload'} or row['k'] != 'hot' or any(type(row[n]) is not int for n in ('n', 'lo', 'hi')):
            raise ValueError('hot row fields/types differ')
        return 'hot', hot_stage(row, members, size)

    def endpoint(expected_path):
        seen = set()
        with expected_path.open() as expected:
            for line in expected:
                row = json.loads(line, object_pairs_hook=distinct_object)
                key, stage = decode(row)
                if key in seen or current.get(key) != stage:
                    raise ValueError('materialized all-key checkpoint/final endpoint differs')
                seen.add(key)
        if seen != current.keys():
            raise ValueError('materialized endpoint missing/extra key')

    count = 0
    with path.open() as rows:
        for line in rows:
            if not line.endswith('\n') or not line.strip():
                raise ValueError('incomplete/blank output JSONL')
            envelope = json.loads(line, object_pairs_hook=distinct_object)
            if type(envelope) is dict and set(envelope) == {'payload'}:
                envelope = envelope['payload']
            if type(envelope) is not dict or set(envelope) != {'before', 'after', 'op'}:
                raise ValueError('CDC envelope differs')
            before, after, op = (envelope[n] for n in ('before', 'after', 'op'))
            if op not in ('c', 'u') or (before is None) != (op == 'c') or after is None:
                raise ValueError('unexpected CDC operation/images')
            key, stage = decode(after)
            if before is None:
                if key in current:
                    raise ValueError('duplicate CDC create')
            else:
                old_key, old = decode(before)
                if old_key != key or current.get(key) != old:
                    raise ValueError('CDC before-image continuity differs')
                def order(value):
                    if scenario == 'many':
                        return value
                    return value[1] if value[0] == 'append' else {
                        'delete-min': members + 1, 'delete-max': members + 2,
                        'replace': members + 3}[value[0]]
                if order(stage) < order(old):
                    raise ValueError('aggregate output regressed to an earlier input prefix')
                if scenario == 'many' and stage == old:
                    raise ValueError('duplicate unchanged COUNT/MAX CDC update')
            current[key] = stage
            count += 1
            if checkpoint_rows is not None and count == checkpoint_rows:
                endpoint(expected_checkpoint)
    if checkpoint_rows is not None and not 1 <= checkpoint_rows <= count:
        raise ValueError('committed output prefix missing')
    endpoint(expected_final)
    return dict(rows=count, complete_keys=len(current), strict_before_images=True,
                exact_full_payloads=True, committed_prefix_verified=checkpoint_rows is not None)


def run(repo, binary, evidence, root, settings, backend, protocol):
    d = root / f'{backend}-{protocol}'
    d.mkdir(exist_ok=False)
    output = d / 'output.jsonl'
    (d / 'query.sql').write_text(query(settings['scenario'], root / 'input.jsonl', output))
    pinned = {str(path): digest(path) for path in (root / 'input.jsonl', root / 'fixture.json',
        root / 'expected.checkpoint.jsonl', root / 'expected.final.jsonl', d / 'query.sql',
        binary, Path(__file__).resolve(), Path(capacity.__file__).resolve())}
    if evidence:
        pinned[str(evidence)] = digest(evidence)
    compiled = source_inventory(repo, evidence)
    if json.loads(evidence.read_text()).get('sql_test_sha256') != digest(binary):
        raise ValueError('binary differs from caller build evidence')
    write(d / 'inventory.json', dict(files=pinned, compiled_source=compiled,
        source_revision=compiled['supplied_repository_head'],
        source_binary_relationship='Host build snapshot validated against mounted sources and supplied SQL binary hash; coordinator owns gate/source evidence.'))
    env = {k: v for k, v in os.environ.items() if not k.startswith(('STREAMR_TEST_', 'STREAMR_CAPTURE_'))}
    env.update(STREAMR_TEST_NATIVE_AGGREGATES='1', STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol, STREAMR_TEST_SOURCE_BATCH_ROWS='32',
        STREAMR_TEST_EXECUTION_BYTES=str(16 * 1024 * 1024), STREAMR_TEST_AGGREGATE_FLUSH_SECONDS='3600',
        STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(settings['timeout_seconds']),
        STREAMR_CAPTURE_QUERY=str(d / 'query.sql'), STREAMR_CAPTURE_OUTPUT=str(output),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(settings['checkpoint_input_rows']),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH='1')
    maximum = 4 * settings['input_rows'] + 16
    for phase in ('INITIAL_', 'CHECKPOINT_', ''):
        env['STREAMR_CAPTURE_EXPECTED_' + phase + 'ROWS'] = '1'
        env['STREAMR_CAPTURE_MAX_' + phase + 'ROWS'] = str(maximum)
    write(d / 'capture-env.json', {k: v for k, v in env.items() if k.startswith(('STREAMR_TEST_', 'STREAMR_CAPTURE_'))})
    print(f'RUN {backend}/{protocol} scenario={settings["scenario"]} checkpoint_floor={settings["checkpoint_retained_payload_floor_bytes"]} pools={POOL_MIB}MiB', flush=True)
    status, rss = capacity.child_with_usage([str(binary), 'external_sql_checkpoint_capture',
        '--ignored', '--test-threads=1', '--nocapture'], env, d / 'capture.log', settings['timeout_seconds'])
    write(d / 'child-result.json', dict(exit_code=status, peak_rss_bytes=rss,
         child_rss_limit_mib=settings['child_rss_limit_mib']))
    if any(digest(Path(path)) != value for path, value in pinned.items()) or source_inventory(repo, evidence) != compiled:
        raise ValueError('compiled source/harness/binary/query/input/oracle changed during capture')
    if status:
        raise RuntimeError(f'capture failed ({status}): {d / "capture.log"}')
    if rss > settings['child_rss_limit_mib'] * 1024 * 1024:
        raise ValueError('whole SQL child exceeded declared RSS ceiling')
    log = (d / 'capture.log').read_text()
    records = re.findall(r'^CAPTURE_RESULT phase=recovered checkpoint=(\d+) input_rows_before_checkpoint=(\d+) committed_rows=(\d+) rows=(\d+) bytes=(\d+) path=(.+) job=(\S+)$', log, re.M)
    initial = re.findall(r'^CAPTURE_RESULT phase=initial rows=(\d+) path=(.+)$', log, re.M)
    if len(records) != 1 or len(initial) != 1 or len(re.findall(r'^test result: ok\. 1 passed; 0 failed;', log, re.M)) != 1:
        raise ValueError('capture phase records differ')
    epoch, prefix, committed, count, _, path, _ = records[0]
    if (epoch, int(prefix), path, initial[0][1]) != ('1', settings['checkpoint_input_rows'], str(output), str(output.with_suffix('.initial.jsonl'))):
        raise ValueError('checkpoint epoch/input prefix or output paths differ')
    first = compare_capture(output.with_suffix('.initial.jsonl'), settings, None,
         root / 'expected.checkpoint.jsonl', root / 'expected.final.jsonl')
    recovered = compare_capture(output, settings, int(committed),
         root / 'expected.checkpoint.jsonl', root / 'expected.final.jsonl')
    if first['rows'] != int(initial[0][0]) or recovered['rows'] != int(count):
        raise ValueError('capture row cardinalities differ')
    if any(digest(Path(path)) != value for path, value in pinned.items()) or source_inventory(repo, evidence) != compiled:
        raise ValueError('source/oracle provenance changed during verification')
    result = dict(status='pass', backend=backend, protocol=protocol,
        initial=first, recovered=recovered, peak_child_rss_bytes=rss,
        checkpoint_floor_bytes=settings['checkpoint_retained_payload_floor_bytes'],
        live_floor_bytes=settings['live_retained_payload_floor_bytes'],
        checkpoint_exceeds_10x_pool=settings['checkpoint_exceeds_10x_pool'],
        live_exceeds_10x_pool=settings['live_exceeds_10x_pool'],
        artifacts={p.name: digest(p) for p in (d / 'capture.log', output, output.with_suffix('.initial.jsonl'))})
    write(d / 'comparison.json', result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path)
    p.add_argument('--scenario', choices=('many', 'hot'), required=True)
    p.add_argument('--keys', type=int, default=100000)
    p.add_argument('--rows', '--members', dest='members', type=int, default=8,
                   help='Hot indexed member count; checkpoint retains rows minus one')
    p.add_argument('--payload-bytes', type=int, help='Defaults many8192/hot256; no engine limit changes')
    p.add_argument('--binary', type=Path)
    p.add_argument('--source-evidence', type=Path)
    p.add_argument('--backend', choices=('memory', 'rocksdb'), action='append')
    p.add_argument('--protocol', choices=('controller', 'leader'), action='append')
    p.add_argument('--require-10x', action='store_true')
    p.add_argument('--timeout-seconds', type=int, default=1800)
    p.add_argument('--rss-limit-mib', type=int, default=512)
    a = p.parse_args()
    if a.payload_bytes is None:
        a.payload_bytes = 8192 if a.scenario == 'many' else 256
    if a.keys < 1 or a.members < 4 or a.payload_bytes < 1 or a.timeout_seconds < 1 or a.rss_limit_mib < 1:
        p.error('keys/payload/timeout/RSS positive; hot members at least4')
    if a.binary and not a.source_evidence:
        p.error('runtime requires --source-evidence host build snapshot; container git metadata is not used')
    root = a.directory.resolve()
    settings = prepare(root, a.scenario, a.keys, a.members, a.payload_bytes,
                       a.require_10x, a.timeout_seconds, a.rss_limit_mib)
    if not a.binary:
        print(json.dumps(dict(status='prepared; runtime unverified', **settings)))
        return
    repo = Path(__file__).resolve().parent.parent
    binary = a.binary.resolve(strict=True)
    evidence = a.source_evidence.resolve(strict=True) if a.source_evidence else None
    results = []
    for backend in dict.fromkeys(a.backend or ('rocksdb',)):
        for protocol in dict.fromkeys(a.protocol or ('controller', 'leader')):
            results.append(run(repo, binary, evidence, root, settings, backend, protocol))
            write(root / 'comparisons.json', results)


if __name__ == '__main__':
    main()
