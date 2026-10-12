#!/usr/bin/env python3
"""Finite real Impulse execution and saved-source-state checkpoint recovery.

The existing Impulse event_rate controls batching: rates 10/80 produce source
batches 1/8. event_time_interval=1000 controls event time independently of pacing.
No source fields, clocks, state formats or source execution are replaced.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

COUNT = 24
INTERVAL = 1000


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def parse_nanos(value):
    match = re.fullmatch(r'(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)?', value)
    if match is None:
        raise ValueError(f'invalid timestamp: {value!r}')
    instant = dt.datetime.fromisoformat(match[1] + 'T' + match[2])
    delta = instant - dt.datetime(1970, 1, 1)
    return (delta.days * 86400 + delta.seconds) * 10**9 + int((match[3] or '').ljust(9, '0'))


def validate_rows(actual, start, counters):
    if len(actual) != len(counters):
        raise ValueError('finite Impulse output cardinality differs')
    for row, counter in zip(actual, counters):
        if set(row) != {'session_id', 'event_reference', 'date_reference'} or row['session_id'] != counter:
            raise ValueError(f'counter/fields differ: {row}')
        nanos = start + counter * INTERVAL
        date = (dt.datetime(1970, 1, 1) + dt.timedelta(seconds=nanos // 10**9)).date().isoformat()
        if parse_nanos(row['event_reference']) != nanos or row['date_reference'] != date:
            raise ValueError(f'Impulse timestamp/UTC date differs for counter {counter}: {row}')


def prepare(root):
    cases = []
    for backend in ('memory', 'rocksdb'):
        for batch in (1, 8):
            directory = root / f'{backend}-batch{batch}'
            directory.mkdir(parents=True, exist_ok=True)
            query = f"""CREATE TABLE pulses WITH (
 connector='impulse', event_rate='{batch * 10}', event_time_interval='1000', message_count='{COUNT}');
CREATE TABLE output (session_id BIGINT, event_reference TEXT, date_reference TEXT)
WITH (connector='single_file', path='{directory}/output.jsonl', format='json', type='sink');
INSERT INTO output SELECT counter AS session_id,
 CAST(EVENT_TIMESTAMP() AS TEXT) AS event_reference,
 CAST(EVENT_DATE() AS TEXT) AS date_reference FROM pulses;
"""
            (directory / 'query.sql').write_text(query)
            env = dict(STREAMR_TEST_TYPED_SQL='1', STREAMR_TEST_BACKEND=backend,
                       STREAMR_TEST_CHECKPOINT_MODE='controller', STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
                       STREAMR_CAPTURE_IMPULSE='1', STREAMR_CAPTURE_EVENT_CLOCK_PROBE='1',
                       STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'), STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
                       STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(batch), STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
                       STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(COUNT), STREAMR_CAPTURE_EXPECTED_ROWS=str(COUNT),
                       STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(batch), STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS=str(COUNT - 1))
            (directory / 'env.json').write_text(json.dumps(env, indent=2) + '\n')
            cases.append((directory, batch, env))
    return cases


def check(directory, batch):
    try:
        check_artifacts(directory, batch)
    except (KeyError, TypeError, IndexError, AttributeError) as error:
        raise ValueError(f'malformed Impulse validation artifact: {error}') from error


def check_artifacts(directory, batch):
    state = json.loads((directory / 'output.impulse-state.json').read_text())
    counter, start = state['counter'], int(state['start_nanos'])
    if state['epoch'] != 1 or not batch <= counter < COUNT:
        raise ValueError('checkpoint must save a nonempty prefix and finite continuation')
    prefix = rows(directory / 'output.checkpoint-1.jsonl')
    validate_rows(prefix, start, range(counter))
    validate_rows(rows(directory / 'output.jsonl'), start, range(COUNT))
    initial = rows(directory / 'output.initial.jsonl')
    initial_start = parse_nanos(initial[0]['event_reference'])
    validate_rows(initial, initial_start, range(COUNT))
    for phase, origin, counters in [('initial', initial_start, range(COUNT)),
                                    ('checkpoint', start, range(counter)),
                                    ('recovered', start, range(counter, COUNT))]:
        trace = rows(directory / f'output.{phase}.probe.jsonl')
        actual = [row for row in trace if row['kind'] == 'row']
        wanted = [(c, origin + c * INTERVAL) for c in counters]
        if [(row['session_id'], row['raw_nanos']) for row in actual] != wanted:
            raise ValueError(f'{phase}: raw source timestamp/counter differs')
        if any(row['id'] is not None or row['retract'] for row in actual):
            raise ValueError('Impulse must remain append-only')
        sizes = [row['rows'] for row in trace if row['kind'] == 'batch']
        if sum(sizes) != len(wanted) or any(size > batch for size in sizes):
            raise ValueError(f'{phase}: actual source batching differs: {sizes}')
        if phase == 'initial' and sizes != [batch] * (COUNT // batch):
            raise ValueError(f'initial actual source batch sizes differ: {sizes}')
    # Independent oracle is based on decoded saved source state, never projected
    # EVENT_TIMESTAMP output or wall-clock pacing.
    (directory / 'oracle.json').write_text(json.dumps({
        'saved_source_state': state,
        'expected_timestamp_nanos': [str(start + c * INTERVAL) for c in range(COUNT)],
        'checkpoint_rows': counter, 'continuation_rows': COUNT - counter,
    }, indent=2) + '\n')


def self_test():
    # A deterministic oracle test crosses UTC midnight and retains all 9 digits.
    start = parse_nanos('2026-10-09T23:59:59.999999000Z')
    counter = 9
    with tempfile.TemporaryDirectory(prefix='streamr-impulse-oracle-') as temporary:
        directory = Path(temporary)
        def output(counters):
            result = []
            for c in counters:
                nanos = start + c * INTERVAL
                instant = dt.datetime(1970, 1, 1) + dt.timedelta(seconds=nanos // 10**9)
                result.append(dict(session_id=c,
                    event_reference=instant.isoformat() + f'.{nanos % 10**9:09d}',
                    date_reference=instant.date().isoformat()))
            return result
        def write_lines(name, values):
            (directory / name).write_text(''.join(json.dumps(v) + '\n' for v in values))
        (directory / 'output.impulse-state.json').write_text(json.dumps(dict(epoch=1, counter=counter, start_nanos=str(start))))
        complete = output(range(COUNT))
        prefix = output(range(counter))
        write_lines('output.jsonl', complete)
        write_lines('output.initial.jsonl', complete)
        write_lines('output.checkpoint-1.jsonl', prefix)
        for phase, counters, sizes in [('initial', range(COUNT), [8, 8, 8]),
                                        ('checkpoint', range(counter), [8, 1]),
                                        ('recovered', range(counter, COUNT), [8, 7])]:
            trace = [dict(kind='row', session_id=c, raw_nanos=start+c*INTERVAL,
                          id=None, retract=False) for c in counters]
            trace += [dict(kind='batch', rows=size) for size in sizes]
            write_lines(f'output.{phase}.probe.jsonl', trace)
        check(directory, 8)
        for corrupt in (complete[:-1], [dict(complete[0], event_reference='2026-10-09T23:59:59.999999001Z')] + complete[1:]):
            write_lines('output.jsonl', corrupt)
            try:
                check(directory, 8)
            except ValueError:
                pass
            else:
                raise AssertionError('oracle accepted missing/corrupt restored output')
        write_lines('output.jsonl', complete)
        write_lines('output.checkpoint-1.jsonl', prefix[:-1])
        try:
            check(directory, 8)
        except ValueError:
            pass
        else:
            raise AssertionError('oracle accepted missing committed prefix')
    print('PASS oracle self-test: nanosecond corruption, missing restored row and missing prefix rejected')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', nargs='?', type=Path)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.output_dir is None:
        parser.error('--output-dir is required unless --self-test is set')
    cases = prepare(args.output_dir.resolve())
    if args.prepare_only:
        print(f'Prepared {len(cases)} finite Impulse cases in {args.output_dir}')
        return
    if args.binary is None:
        parser.error('binary is required unless --prepare-only is set')
    failures = []
    for directory, batch, env in cases:
        try:
            with (directory / 'capture.log').open('w') as log:
                result = subprocess.run([str(args.binary.resolve()), 'external_sql_checkpoint_capture', '--ignored',
                                         '--test-threads=1', '--nocapture'], env=dict(os.environ, **env),
                                        stdout=log, stderr=subprocess.STDOUT, timeout=180)
            if result.returncode or '1 passed' not in (directory / 'capture.log').read_text():
                raise RuntimeError(f'{directory.name}: worker capture failed; see {directory}/capture.log')
            check(directory, batch)
            print(f'PASS {directory.name}: exact finite timestamps/UTC dates, saved prefix, fresh-worker continuation')
        except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as error:
            failures.append(str(error))
            print(f"FAIL {directory.name}: {error}", flush=True)
    if failures:
        raise RuntimeError(f"{len(failures)} Impulse scenario(s) failed: " + "; ".join(failures))


if __name__ == '__main__':
    main()
