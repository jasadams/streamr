#!/usr/bin/env python3
"""Exact native TUMBLE collection, DISTINCT and UNNEST checkpoint fixture.

ARRAY_AGG without ORDER BY has unspecified element order, so the oracle checks
the complete multiset. Duplicates cross the 64-row partial-chunk boundary.
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

BASE = datetime.fromisoformat('2023-10-09T17:13:20')


def rows(count=78):
    return [dict(timestamp=BASE.isoformat(), metric=None if rank % 11 == 0 else rank % 13)
            for rank in range(count)]


def write_case(directory, mode, count=78):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows(count)))
    source = f"""
CREATE TABLE collection_input (timestamp TIMESTAMP, metric BIGINT, WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
"""
    if mode == 'arrays':
        output = f"""
CREATE TABLE collection_output (start TIMESTAMP, end TIMESTAMP, distinct_count BIGINT,
  items BIGINT[], unique_items BIGINT[])
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'json', type = 'sink');
INSERT INTO collection_output
SELECT window.start, window.end, distinct_count, items, unique_items FROM (
  SELECT TUMBLE(INTERVAL '2 second') AS window,
         COUNT(DISTINCT metric) AS distinct_count,
         ARRAY_AGG(metric) AS items,
         ARRAY_AGG(DISTINCT metric) AS unique_items
  FROM collection_input GROUP BY 1
);
"""
    elif mode == 'unnest':
        output = f"""
CREATE TABLE collection_output (metric BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'json', type = 'sink');
CREATE VIEW collection_view AS
SELECT ARRAY_AGG(metric) AS items, TUMBLE(INTERVAL '2 second') AS window
FROM collection_input GROUP BY TUMBLE(INTERVAL '2 second');
INSERT INTO collection_output SELECT UNNEST(items) AS metric FROM collection_view;
"""
    elif mode == 'oversize':
        output = f"""
CREATE TABLE collection_output (items BIGINT[])
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'json', type = 'sink');
INSERT INTO collection_output
SELECT ARRAY_AGG(metric) FROM collection_input
GROUP BY TUMBLE(INTERVAL '2 second');
"""
    else:
        raise ValueError(mode)
    (directory / 'query.sql').write_text(source + output)


def check_output(directory, mode):
    expected = rows()
    values = Counter(row['metric'] for row in expected)
    distinct = Counter({value: 1 for value in values})
    for path in (directory / 'output.initial.jsonl', directory / 'output.jsonl'):
        records = [json.loads(line) for line in path.read_text().splitlines()]
        if mode == 'arrays':
            assert len(records) == 1, (path, records)
            record = records[0]
            assert set(record) == {'start', 'end', 'distinct_count', 'items', 'unique_items'}
            assert record['start'] == BASE.isoformat(), record
            assert record['end'] == (BASE + timedelta(seconds=2)).isoformat(), record
            assert record['distinct_count'] == len([value for value in values if value is not None]), record
            assert Counter(record['items']) == values, record
            assert Counter(record['unique_items']) == distinct, record
        else:
            assert len(records) == len(expected), (path, len(records))
            assert all(set(record) == {'metric'} for record in records), records
            assert Counter(record['metric'] for record in records) == values, records


def run_case(binary, directory, mode, backend, protocol, batch):
    write_case(directory, mode, 4096 if mode == 'oversize' else 78)
    env = dict(os.environ)
    for flag in ('STREAMR_TEST_TYPED_SQL', 'STREAMR_TEST_NATIVE_AGGREGATES'):
        env.pop(flag, None)
    env.update(
        STREAMR_TEST_NATIVE_WINDOWS='1',
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_EXECUTION_BYTES='16777216',
        STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'),
        STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='70',
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(1 if mode == 'arrays' else 78),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='0',
        STREAMR_CAPTURE_EXPECTED_ROWS=str(1 if mode == 'arrays' else 78),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
    )
    with (directory / 'capture.log').open('w') as log:
        result = subprocess.run(
            [str(binary), 'external_sql_checkpoint_capture', '--ignored',
             '--test-threads=1', '--nocapture'], env=env, stdout=log,
            stderr=subprocess.STDOUT, timeout=120)
    output = (directory / 'capture.log').read_text()
    if mode == 'oversize':
        assert result.returncode != 0, 'oversized collection unexpectedly succeeded'
        assert 'native window collection output exceeds configured partial limit' in output, output[-2000:]
        print(f'PASS {directory.name}: oversized result rejected before final execution', flush=True)
        return
    assert result.returncode == 0 and '1 passed' in output, output[-2000:]
    check_output(directory, mode)
    print(f'PASS {directory.name}: exact initial and recovered collection', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--backend', choices=('memory', 'rocksdb'), action='append')
    parser.add_argument('--protocol', choices=('controller', 'leader'), action='append')
    parser.add_argument('--batch', choices=(1, 128), type=int, action='append')
    parser.add_argument('--oversize', action='store_true')
    args = parser.parse_args()
    root = args.directory.resolve()
    if not args.binary:
        for mode in ('arrays', 'unnest'):
            write_case(root / mode, mode)
        if args.oversize:
            write_case(root / 'oversize', 'oversize', 4096)
        return
    for mode in ('arrays', 'unnest'):
        for backend in args.backend or ('memory', 'rocksdb'):
            for protocol in args.protocol or ('controller', 'leader'):
                for batch in args.batch or (1, 128):
                    run_case(args.binary.resolve(strict=True),
                             root / f'{mode}-{backend}-{protocol}-{batch}',
                             mode, backend, protocol, batch)
    if args.oversize:
        run_case(args.binary.resolve(strict=True), root / 'oversize-memory-controller-128',
                 'oversize', 'memory', 'controller', 128)


if __name__ == '__main__':
    main()
