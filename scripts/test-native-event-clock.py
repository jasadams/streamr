#!/usr/bin/env python3
"""Execute STR-61 SQL through the worker checkpoint harness; never builds Rust.

Use --prepare-only to inspect SQL and independent expected images. Otherwise
pass the freshly built arroyo-sql-testing executable as the positional argument.
Every case executes initial and fresh-worker restored runs on both backends.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent / 'fixtures' / 'native-event-clock'


def read_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def normalize(value):
    # Compare the full nanosecond value, accepting only equivalent ISO formatting.
    if not isinstance(value, str):
        raise ValueError('clock output must be a string')
    match = re.fullmatch(r'(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)?', value)
    if not match:
        raise ValueError(f'invalid UTC clock output: {value!r}')
    return match[1] + 'T' + match[2] + '.' + (match[3] or '').ljust(9, '0')


def image(row):
    if set(row) != {'session_id', 'duration', 'event_reference', 'date_reference', 'progress_expression'}:
        raise ValueError(f'output fields differ: {row}')
    return dict(row, event_reference=normalize(row['event_reference']),
                progress_expression=normalize(row['progress_expression']))


def changes(rows, updating):
    if not updating:
        return [('+', image(row)) for row in rows]
    result = []
    for row in rows:
        if set(row) != {'before', 'after', 'op'}:
            raise ValueError('CDC envelope fields differ')
        before, after, op = row['before'], row['after'], row['op']
        if op not in ('c', 'u', 'd') or (before is None) != (op == 'c') or (after is None) != (op == 'd'):
            raise ValueError('CDC operation/images differ')
        if before is not None:
            result.append(('-', image(before)))
        if after is not None:
            result.append(('+', image(after)))
    return result


def query(directory, case):
    updating = case.startswith('cdc')
    field = 'start_time' if case == 'cdc-start' else 'end_time'
    fmt = 'debezium_json' if updating else 'json'
    primary = ' PRIMARY KEY' if updating else ''
    source = f"""CREATE TABLE clock_input (
 session_id BIGINT{primary}, duration BIGINT NOT NULL,
 start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NOT NULL,
 completeness_time TIMESTAMP NOT NULL,
 WATERMARK FOR {field} AS completeness_time - INTERVAL '5' SECOND
) WITH (connector='single_file', path='{directory}/input.jsonl',
 format='{fmt}', type='source', wait_for_control='true');
CREATE TABLE clock_output (session_id BIGINT, duration BIGINT, event_reference TEXT,
 date_reference TEXT, progress_expression TEXT)
WITH (connector='single_file', path='{directory}/output.jsonl', format='{fmt}', type='sink');
"""
    projection = "session_id, duration, CAST(WATERMARK_TIMESTAMP() AS TEXT) AS event_reference, CAST(WATERMARK_DATE() AS TEXT) AS date_reference, CAST(completeness_time - INTERVAL '5' SECOND AS TEXT) AS progress_expression"
    if case == 'append-lookup':
        source += '''CREATE STATE TABLE retained (session_id BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY session_id;
CREATE VIEW applied AS MERGE INTO retained AS target USING clock_input AS source
ON target.session_id=source.session_id
WHEN MATCHED THEN UPDATE SET _timestamp=source.completeness_time - INTERVAL '1' DAY
WHEN NOT MATCHED THEN INSERT (session_id, _timestamp) VALUES (source.session_id, source.completeness_time - INTERVAL '1' DAY)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW triggered AS SELECT source.session_id AS session_id, source.duration AS duration, source.completeness_time AS completeness_time FROM applied;
'''
        relation = "triggered LEFT JOIN retained AS target ON triggered.session_id=target.session_id"
        projection = projection.replace('session_id, duration,', 'triggered.session_id AS session_id, duration,')
        return source + f"INSERT INTO clock_output SELECT {projection} FROM {relation} WHERE target._timestamp=completeness_time - INTERVAL '1' DAY;\n"
    if case == 'append-view':
        source += 'CREATE VIEW renamed AS SELECT session_id, duration, end_time AS renamed_time, completeness_time FROM clock_input;\n'
        source += 'CREATE VIEW hidden AS SELECT session_id, duration, completeness_time FROM renamed;\n'
        relation = 'hidden'
    else:
        relation = 'clock_input'
    return source + f'INSERT INTO clock_output SELECT {projection} FROM {relation};\n'


def expected(case):
    return [(sign, image(row)) for sign, row in json.loads((ROOT / f'{case}.expected.json').read_text())]


def prepare(directory):
    cases = {}
    for backend in ('memory', 'rocksdb'):
        for mode in ('controller', 'leader'):
            for case in ('append', 'append-negative-progress', 'append-view', 'append-lookup', 'cdc-end', 'cdc-start'):
                for batch in (1, 8):
                    path = directory / f'{backend}-{mode}-batch{batch}-{case}'
                    path.mkdir(parents=True, exist_ok=True)
                    input_case = 'append' if case in ('append-view', 'append-lookup') else case
                    (path / 'input.jsonl').write_text((ROOT / f'{input_case}.input.jsonl').read_text())
                    (path / 'query.sql').write_text(query(path, case))
                    oracle = expected(input_case)
                    (path / 'expected.images.json').write_text(json.dumps(oracle, indent=2) + '\n')
                    # Updating sinks may pair replacement images or emit delete/create.
                    # We compare EVERY ordered image; this bound does not drop emissions.
                    count = len(oracle)
                    checkpoint_images = 3 if case.startswith('cdc') else 2
                    coalesced = case.startswith('cdc') and batch == 8
                    initial_rows, recovered_rows, checkpoint_rows = (0, 2, 1) if coalesced else (1, 1, 1)
                    env = dict(RUST_LOG='arroyo_worker::arrow::watermark_generator=debug', STREAMR_TEST_TYPED_SQL='1', STREAMR_TEST_BACKEND=backend,
                        STREAMR_TEST_CHECKPOINT_MODE=mode, STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
                        STREAMR_CAPTURE_QUERY=str(path / 'query.sql'), STREAMR_CAPTURE_OUTPUT=str(path / 'output.jsonl'),
                        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='2', STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
                        STREAMR_CAPTURE_EVENT_CLOCK_PROBE='1', STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(initial_rows), STREAMR_CAPTURE_MAX_INITIAL_ROWS=str(0 if coalesced else count),
                        STREAMR_CAPTURE_EXPECTED_ROWS=str(recovered_rows), STREAMR_CAPTURE_MAX_ROWS=str(2 if coalesced else count),
                        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(checkpoint_rows), STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS=str(1 if coalesced else checkpoint_images))
                    cases[path.name] = (path, input_case, env, oracle, checkpoint_images)
    return cases


def timestamp_nanos(value):
    value = normalize(value)
    seconds = int((dt.datetime.fromisoformat(value[:19]) - dt.datetime(1970, 1, 1)).total_seconds())
    return seconds * 1_000_000_000 + int(value[20:])


def compare_probe(path, case, env, oracle, prefix):
    traces = {phase: read_rows(path / f'output.{phase}.probe.jsonl')
              for phase in ('initial', 'checkpoint', 'recovered')}
    wanted = [{'session_id': row['session_id'], 'raw_nanos': timestamp_nanos(row['event_reference']),
               'retract': sign == '-'} for sign, row in oracle]
    seen_ids = set()
    for phase, expected_rows in [('initial', wanted), ('checkpoint', wanted[:prefix]), ('recovered', wanted[prefix:])]:
        rows = [r for r in traces[phase] if r['kind'] == 'row']
        actual = [{k: r[k] for k in ('session_id', 'raw_nanos', 'retract')} for r in rows]
        if actual != expected_rows:
            raise ValueError(f'{path.name}/{phase}: raw metadata differs: {actual} != {expected_rows}')
        for row in rows:
            if case.startswith('cdc'):
                if not isinstance(row['id'], str) or not re.fullmatch('[0-9a-f]{32}', row['id']):
                    raise ValueError('internal CDC id is missing or malformed')
                seen_ids.add(row['id'])
            elif row['id'] is not None:
                raise ValueError('append fixture unexpectedly has CDC identity')
    if case.startswith('cdc') and len(seen_ids) != 1:
        raise ValueError('internal primary-key id changed across image replacement or fresh restore')
    # Independently specified observed control signals, not the projected AS SQL
    # expression. Default period is one second; checkpoint flushes partial batches.
    batch = int(env['STREAMR_TEST_SOURCE_BATCH_ROWS'])
    if case == 'append-negative-progress':
        first, second, third, fourth = '1969-12-31T23:59:58', '1970-01-01T00:00:05', '1970-01-01T00:00:15', '1970-01-01T00:00:25'
        controls = {'initial': [first, second, third, fourth] if batch == 1 else [first],
                    'checkpoint': [first, second] if batch == 1 else [first],
                    'recovered': [third, fourth] if batch == 1 else [third]}
    elif case.startswith('cdc'):
        # One source envelope unrolls into both images even at source batch 1.
        # A day move emits the minimum old/new AS while advancing the raw FOR maximum.
        first = '2026-10-10T00:00:05'
        controls = {'initial': [first, first] if batch == 1 else [first],
                    'checkpoint': [first], 'recovered': [first]}
    else:
        first = '2026-10-09T23:59:58'
        controls = {'initial': [first, '2026-10-10T00:00:25', '2026-10-10T00:00:27'] if batch == 1 else ['1970-01-01T00:00:00'],
                    'checkpoint': [first] if batch == 1 else ['2026-10-09T23:59:45'],
                    'recovered': ['2026-10-10T00:00:25', '2026-10-10T00:00:27'] if batch == 1 else ['1970-01-01T00:00:00']}
    for phase, values in controls.items():
        starting = [r['nanos'] for r in traces[phase] if r['kind'] == 'start_watermark']
        restored = str(timestamp_nanos(controls['checkpoint'][-1])) if phase == 'recovered' else None
        if starting != [restored]:
            raise ValueError(f'{path.name}/{phase}: restored context watermark {starting} != {[restored]}')
        terminal = sum(r['kind'] == 'watermark' and int(r['nanos']) == 2**64 - 1 for r in traces[phase])
        if terminal != (0 if phase == 'checkpoint' else 1):
            raise ValueError(f'{path.name}/{phase}: EOF watermark count differs')
        actual = [int(r['nanos']) for r in traces[phase] if r['kind'] == 'watermark' and int(r['nanos']) != 2**64 - 1]
        wanted_controls = [timestamp_nanos(v) for v in values]
        if actual != wanted_controls:
            raise ValueError(f'{path.name}/{phase}: actual watermark controls {actual} != {wanted_controls}')


def execute(binary, cases):
    for name, (path, case, env, oracle, prefix) in cases.items():
        with (path / 'capture.log').open('w') as output:
            result = subprocess.run([str(binary), 'external_sql_checkpoint_capture', '--ignored',
                '--test-threads=1', '--nocapture'], env=dict(os.environ, **env), stdout=output, stderr=subprocess.STDOUT)
        log = (path / 'capture.log').read_text()
        if result.returncode or '1 passed' not in log:
            raise RuntimeError(f'{name}: worker capture failed ({result.returncode}); {path}/capture.log')
        marker = re.search(r'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=2 committed_rows=(\d+) rows=(\d+)', log)
        if marker is None:
            raise ValueError(f'{name}: missing fresh-worker checkpoint marker')
        coalesced = case.startswith('cdc') and env['STREAMR_TEST_SOURCE_BATCH_ROWS'] == '8'
        # Exact sink contract: one same-ID batch collapses create-to-delete to
        # no output. Checkpoint creates corrected b; restored suffix deletes b.
        sink_oracle = [oracle[2], oracle[3]] if coalesced else oracle
        for filename in ('output.initial.jsonl', 'output.jsonl'):
            rows = read_rows(path / filename)
            actual = changes(rows, case.startswith('cdc'))
            expected_sink = [] if coalesced and filename == 'output.initial.jsonl' else sink_oracle
            if actual != expected_sink:
                raise ValueError(f'{name}/{filename}: ordered sink images differ\nactual={actual}\nexpected={expected_sink}')
        restored = read_rows(path / 'output.jsonl')
        committed = int(marker[1])
        if changes(restored[:committed], case.startswith('cdc')) != sink_oracle[:1 if coalesced else prefix]:
            raise ValueError(f'{name}: checkpoint committed sink image prefix differs')
        compare_probe(path, case, env, oracle, prefix)
        print(f'PASS {name}: exact sink images, raw metadata/IDs/progress and checkpoint prefix; fresh worker restore', flush=True)


def self_test():
    assert normalize('1969-12-31 23:59:59.999999999') == '1969-12-31T23:59:59.999999999'
    assert normalize('2026-10-10T00:00:03') == '2026-10-10T00:00:03.000000000'
    for case in ('append', 'append-negative-progress', 'cdc-end', 'cdc-start'):
        oracle = expected(case)
        assert oracle
        for _, row in oracle:
            assert dt.date.fromisoformat(row['date_reference']).isoformat() == row['event_reference'][:10]
    assert [row['event_reference'][11:19] for _, row in expected('append')][-3:] == ['00:00:20', '00:00:10', '00:00:30']
    with tempfile.TemporaryDirectory(prefix='event-clock-comparator-') as directory:
        path = Path(directory)
        oracle = expected('cdc-end')
        for phase, images, controls in [('initial', oracle, ['2026-10-10T00:00:05', '2026-10-10T00:00:05']),
                                        ('checkpoint', oracle[:3], ['2026-10-10T00:00:05']),
                                        ('recovered', oracle[3:], ['2026-10-10T00:00:05'])]:
            rows = [dict(kind='row', session_id=row['session_id'], raw_nanos=timestamp_nanos(row['event_reference']),
                         retract=sign == '-', id='ab' * 16) for sign, row in images]
            rows += [dict(kind='watermark', nanos=str(timestamp_nanos(value))) for value in controls]
            rows.append(dict(kind='start_watermark', nanos=str(timestamp_nanos('2026-10-10T00:00:05')) if phase == 'recovered' else None))
            if phase != 'checkpoint':
                rows.append(dict(kind='watermark', nanos=str(2**64 - 1)))
            (path / f'output.{phase}.probe.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
        env = {'STREAMR_TEST_SOURCE_BATCH_ROWS': '1'}
        compare_probe(path, 'cdc-end', env, oracle, 3)
        probe = path / 'output.recovered.probe.jsonl'
        original = probe.read_text()
        for wrong in (original.replace('abababab', 'cdcdcdcd'), original.replace(str(timestamp_nanos('2026-10-10T00:00:05')), '123'), original.replace('"kind": "start_watermark", "nanos": "'+str(timestamp_nanos('2026-10-10T00:00:05'))+'"', '"kind": "start_watermark", "nanos": "0"')):
            probe.write_text(wrong)
            try:
                compare_probe(path, 'cdc-end', env, oracle, 3)
            except ValueError:
                pass
            else:
                raise AssertionError('changed internal id or actual progress signal accepted')
            probe.write_text(original)
    print('PASS fixture comparator self-test; runtime not executed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path, nargs='?')
    parser.add_argument('--directory', type=Path, default=Path('/app/target/native-event-clock'))
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        cases = prepare(args.directory.resolve())
        print(f'Prepared {len(cases)} SQL cases; runtime not executed', flush=True)
        if not args.prepare_only:
            if args.binary is None:
                parser.error('binary is required unless --prepare-only')
            execute(args.binary.resolve(strict=True), cases)
