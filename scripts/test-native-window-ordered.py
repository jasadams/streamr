#!/usr/bin/env python3
"""Native fixed-window FILTER/FIRST/LAST qualification over open checkpoints.

The source has 78 rows, 70 before the stopping checkpoint. With a 128-row
source batch, the checkpoint flushes 70 source rows, but this driver does not
independently assert the window operator's batch length or exact 64+6 split.
Ordering keys are unique and permuted across the prefix and restored suffix.
All prefix rows share a timestamp, so no window has closed at the checkpoint.
"""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

BASE = datetime.fromisoformat('2023-10-09T17:13:20')
FIELDS = {'segment', 'start', 'end', 'events', 'selected_count', 'selected_total',
          'earliest', 'latest', 'latest_selected'}


def events():
    rows = []
    for rank in range(78):
        second = 0 if rank < 70 else (1 if rank < 73 else 2 if rank < 76 else 5)
        rows.append(dict(
            timestamp=(BASE + timedelta(seconds=second)).isoformat(),
            segment='a' if rank % 2 == 0 else 'b',
            metric=None if rank % 11 == 0 else rank - 20,
            ordinal=(rank * 20) % 79,
            selected=rank % 3 != 0,
        ))
    assert len({row['ordinal'] for row in rows}) == len(rows)
    assert any(rows[i]['ordinal'] > rows[i + 1]['ordinal'] for i in range(len(rows) - 1))
    for segment in ('a', 'b'):
        members = [(index, row['ordinal']) for index, row in enumerate(rows) if row['segment'] == segment]
        assert max((item for item in members if item[0] < 70), key=lambda item: item[1])[0] >= 64
        assert max(members, key=lambda item: item[1])[0] >= 70
    return rows


def expected(kind):
    groups = {}
    width = 2 if kind == 'tumble' else 4
    for row in events():
        second = int((datetime.fromisoformat(row['timestamp']) - BASE).total_seconds())
        pane = (second // 2) * 2
        starts = [pane] if kind == 'tumble' else [pane - 2, pane]
        for start in starts:
            assert start <= second < start + width
            groups.setdefault((row['segment'], start), []).append(row)
    result = []
    for (segment, start), rows in groups.items():
        ranked = sorted(rows, key=lambda row: row['ordinal'])
        filtered = [row['metric'] for row in ranked
                    if row['selected'] and row['metric'] is not None]
        selected_rows = [row for row in ranked if row['selected']]
        result.append(dict(
            segment=segment,
            start=(BASE + timedelta(seconds=start)).isoformat(),
            end=(BASE + timedelta(seconds=start + width)).isoformat(),
            events=len(ranked),
            selected_count=len(filtered),
            selected_total=sum(filtered) if filtered else None,
            earliest=next((row['metric'] for row in ranked if row['metric'] is not None), None),
            latest=ranked[-1]['metric'],
            latest_selected=selected_rows[-1]['metric'] if selected_rows else None,
        ))
    return sorted(result, key=lambda row: (row['start'], row['segment']))


def write_fixture(directory, kind):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in events()))
    window = "TUMBLE(INTERVAL '2 second')" if kind == 'tumble' else "HOP(INTERVAL '2 second', INTERVAL '4 second')"
    (directory / 'query.sql').write_text(f"""
CREATE TABLE ordered_input (timestamp TIMESTAMP, segment TEXT, metric BIGINT,
  ordinal BIGINT, selected BOOLEAN, WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE ordered_output (segment TEXT, start TIMESTAMP, end TIMESTAMP,
  events BIGINT, selected_count BIGINT, selected_total BIGINT,
  earliest BIGINT, latest BIGINT, latest_selected BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'json', type = 'sink');
INSERT INTO ordered_output
SELECT segment, window.start, window.end, events, selected_count,
       selected_total, earliest, latest, latest_selected FROM (
  SELECT segment, {window} AS window,
         COUNT(*) AS events,
         COUNT(metric) FILTER (WHERE selected) AS selected_count,
         SUM(metric) FILTER (WHERE selected) AS selected_total,
         FIRST_VALUE(metric ORDER BY ordinal) IGNORE NULLS AS earliest,
         LAST_VALUE(metric ORDER BY ordinal) AS latest,
         LAST_VALUE(metric ORDER BY ordinal) FILTER (WHERE selected) AS latest_selected
  FROM ordered_input GROUP BY 1, 2
);
""")
    (directory / 'expected.final.json').write_text(json.dumps(expected(kind), indent=2) + '\n')


def assert_rows(path, wanted):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        if set(row) != FIELDS:
            raise AssertionError((path, 'field set', row))
    keys = [(row['segment'], row['start'], row['end']) for row in rows]
    if len(keys) != len(set(keys)):
        raise AssertionError((path, 'duplicate window', keys))
    canonical = lambda values: sorted(values, key=lambda row: (row['start'], row['segment']))
    if canonical(rows) != canonical(wanted):
        raise AssertionError((path, rows, wanted))


def run_case(binary, directory, kind, backend, batch, protocol, native):
    write_fixture(directory, kind)
    wanted = expected(kind)
    env = dict(os.environ)
    for flag in ('STREAMR_TEST_TYPED_SQL', 'STREAMR_TEST_NATIVE_AGGREGATES',
                 'STREAMR_TEST_NATIVE_WINDOWS'):
        env.pop(flag, None)
    env.update(
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_EXECUTION_BYTES='16777216',
        STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'),
        STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='70',
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
    print(f'PASS {directory.name}: {len(wanted)} exact windows, 70-source-row open checkpoint; operator batch length unobserved', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--baseline', action='store_true')
    parser.add_argument('--backend', choices=('memory', 'rocksdb'), action='append')
    parser.add_argument('--batch', choices=(1, 128), type=int, action='append')
    parser.add_argument('--protocol', choices=('controller', 'leader'), action='append')
    args = parser.parse_args()
    root = args.directory.resolve()
    if not args.binary:
        for kind in ('tumble', 'hop'):
            write_fixture(root / kind, kind)
            print(kind, len(expected(kind)), expected(kind))
        return
    binary = args.binary.resolve(strict=True)
    for kind in ('tumble', 'hop'):
        for backend in args.backend or ('memory', 'rocksdb'):
            for batch in args.batch or (1, 128):
                for protocol in args.protocol or ('controller', 'leader'):
                    name = f'native-{kind}-{backend}-{batch}-{protocol}'
                    run_case(binary, root / name, kind, backend, batch, protocol, True)
        if args.baseline:
            for batch in args.batch or (1, 128):
                name = f'baseline-{kind}-memory-{batch}-controller'
                run_case(binary, root / name, kind, 'memory', batch, 'controller', False)


if __name__ == '__main__':
    main()
