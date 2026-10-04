#!/usr/bin/env python3
"""Generic finite-source TUMBLE/HOP checkpoint, EOF, and value fixture.

Prepare fixtures without --binary. With --binary, run the existing external SQL
checkpoint-capture harness against each configured backend/protocol/batch size.
The sink is append-only JSON: each window row is one record, not a Debezium
before/after pair. SQL results are compared as unordered sets of full rows.
"""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

BASE = datetime.fromisoformat('2023-10-09T17:13:20')
EVENTS = [
    ('a', 0, 1), ('a', 0, 3), ('b', 0, 5), ('b', 0, 2),
    ('a', 1, 4), ('b', 2, 7), ('a', 5, 6),
]
FIELDS = {'segment', 'start', 'end', 'count', 'total', 'lo', 'hi'}


def expected(kind):
    groups = {}
    width = 2 if kind == 'tumble' else 4
    slide = 2
    for segment, second, metric in EVENTS:
        pane = (second // slide) * slide
        starts = [pane] if kind == 'tumble' else [pane - slide, pane]
        for start in starts:
            # For HOP, a timestamp in the pane belongs to exactly two width-4,
            # slide-2 windows. The event at offset 2 seconds is a true boundary.
            assert start <= second < start + width
            groups.setdefault((segment, start), []).append(metric)
    rows = []
    for (segment, start), values in groups.items():
        rows.append(dict(
            segment=segment,
            start=(BASE + timedelta(seconds=start)).isoformat(),
            end=(BASE + timedelta(seconds=start + width)).isoformat(),
            count=len(values), total=sum(values), lo=min(values), hi=max(values),
        ))
    return sorted(rows, key=lambda row: (row['start'], row['segment']))


def write_fixture(directory, kind):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'input.jsonl').open('w') as output:
        for segment, second, metric in EVENTS:
            output.write(json.dumps(dict(
                timestamp=(BASE + timedelta(seconds=second)).isoformat(),
                segment=segment, metric=metric,
            )) + '\n')
    window = "TUMBLE(INTERVAL '2 second')" if kind == 'tumble' else "HOP(INTERVAL '2 second', INTERVAL '4 second')"
    query = f"""
CREATE TABLE window_input (timestamp TIMESTAMP, segment TEXT, metric BIGINT,
  WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE window_output (segment TEXT, start TIMESTAMP, end TIMESTAMP,
  count BIGINT, total BIGINT, lo BIGINT, hi BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'json', type = 'sink');
INSERT INTO window_output
SELECT segment, window.start, window.end, count, total, lo, hi FROM (
  SELECT segment, {window} AS window, COUNT(*) AS count,
         SUM(metric) AS total, MIN(metric) AS lo, MAX(metric) AS hi
  FROM window_input GROUP BY 1, 2
);
"""
    (directory / 'query.sql').write_text(query)
    (directory / 'expected.final.json').write_text(json.dumps(expected(kind), indent=2) + '\n')
    (directory / 'expected.checkpoint.json').write_text('[]\n')


def assert_rows(path, wanted):
    actual = [json.loads(line) for line in path.read_text().splitlines()]
    for row in actual:
        if set(row) != FIELDS:
            raise AssertionError((path, 'field set', row))
    keys = [(row['segment'], row['start'], row['end']) for row in actual]
    if len(keys) != len(set(keys)):
        raise AssertionError((path, 'duplicate window key', keys))
    canonical = lambda rows: sorted(rows, key=lambda row: (row['start'], row['segment']))
    if canonical(actual) != canonical(wanted):
        raise AssertionError((path, actual, wanted))


def run_case(binary, directory, kind, backend, batch, mode, native):
    write_fixture(directory, kind)
    wanted = expected(kind)
    env = dict(os.environ)
    env.pop('STREAMR_TEST_TYPED_SQL', None)
    env.pop('STREAMR_TEST_NATIVE_AGGREGATES', None)
    env.pop('STREAMR_TEST_NATIVE_WINDOWS', None)
    env.update(
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=mode,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_EXECUTION_BYTES='16777216',
        STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'),
        STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='4',
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(len(wanted)),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='0',
        STREAMR_CAPTURE_EXPECTED_ROWS=str(len(wanted)),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
    )
    if native:
        env['STREAMR_TEST_NATIVE_WINDOWS'] = '1'
    log = directory / 'capture.log'
    with log.open('w') as output:
        result = subprocess.run(
            [str(binary), 'external_sql_checkpoint_capture', '--ignored',
             '--test-threads=1', '--nocapture'],
            env=env, stdout=output, stderr=subprocess.STDOUT,
        )
    if result.returncode or '1 passed' not in log.read_text():
        raise RuntimeError(f'capture failed: {log}')
    assert_rows(directory / 'output.initial.jsonl', wanted)
    assert_rows(directory / 'output.jsonl', wanted)
    # The capture harness itself asserts zero rows at the stopping checkpoint.
    # The sink opens an empty file before the stopping checkpoint. A nonempty
    # prefix would mean these first four same-time events closed a window.
    if (directory / 'output.jsonl').read_text() == '':
        raise AssertionError((directory, 'recovered output is empty'))
    print(f'PASS {directory.name}: {len(wanted)} exact windows, checkpoint prefix 0', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path, help='fresh Bookworm arroyo-sql-testing binary')
    parser.add_argument('--baseline', action='store_true', help='also run existing memory windows')
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not args.binary:
        for kind in ('tumble', 'hop'):
            write_fixture(directory / kind, kind)
            print(kind, len(expected(kind)), expected(kind))
        return
    binary = args.binary.resolve(strict=True)
    for kind in ('tumble', 'hop'):
        for backend in ('memory', 'rocksdb'):
            for batch in (1, 8):
                for mode in ('controller', 'leader'):
                    name = f'native-{kind}-{backend}-{batch}-{mode}'
                    run_case(binary, directory / name, kind, backend, batch, mode, True)
        if args.baseline:
            for batch in (1, 8):
                name = f'baseline-{kind}-memory-{batch}-controller'
                run_case(binary, directory / name, kind, 'memory', batch, 'controller', False)


if __name__ == '__main__':
    main()
