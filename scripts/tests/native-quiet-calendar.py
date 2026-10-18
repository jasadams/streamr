#!/usr/bin/env python3
"""Real native SQL quiet-calendar regression; no builds or callback simulation.

Prepare without --binary. Run a preserved arroyo-sql-testing ELF with --binary,
optionally selecting --backend/--mode. Source batch 1 is intentional: source
controls hold event time while snapshots prove live output before EOF. This is
local SQL-worker checkpoint recovery, not Kafka or production process loss.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess

EVENTS = (
    dict(k='keyA', timestamp='2026-10-16T12:00:00Z'),
    dict(k='keyB', timestamp='2026-10-16T12:00:01Z'),
    dict(k='keyB', timestamp='2026-10-17T12:00:00Z'),
    dict(k='keyB', timestamp='2026-10-17T12:00:01Z'),
)
FIELDS = {'k', 'lifetime_count', 'today_count'}


def snapshot(prefix):
    """Recompute from raw UTC dates, independently of the engine's bucket index."""
    events = EVENTS[:prefix]
    reference = max(datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00'))
                    for e in events).astimezone(timezone.utc).date()
    result = {}
    for event in events:
        key = event['k']
        result.setdefault(key, dict(k=key, lifetime_count=0, today_count=0))
        result[key]['lifetime_count'] += 1
        result[key]['today_count'] += int(datetime.fromisoformat(
            event['timestamp'].replace('Z', '+00:00')).astimezone(timezone.utc).date() == reference)
    return result


def prepare(path):
    path.mkdir(parents=True, exist_ok=True)
    (path / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in EVENTS))
    (path / 'query.sql').write_text(f"""SET updating_ttl = NULL;
CREATE TABLE events (k TEXT NOT NULL, timestamp TIMESTAMP NOT NULL,
 WATERMARK FOR timestamp AS timestamp)
WITH (connector='single_file', path='{path}/input.jsonl', format='json',
 type='source', wait_for_control='true');
CREATE TABLE result (k TEXT, lifetime_count BIGINT, today_count BIGINT)
WITH (connector='single_file', path='{path}/output.jsonl',
 format='debezium_json', type='sink');
INSERT INTO result SELECT k, COUNT(*) AS lifetime_count,
 COUNT(*) FILTER (WHERE CAST(timestamp AS DATE) = EVENT_DATE()) AS today_count
 FROM events GROUP BY k;
""")
    for prefix in (1, 2, 3, 4):
        (path / f'expected.prefix-{prefix}.json').write_text(
            json.dumps(snapshot(prefix), indent=2, sort_keys=True) + '\n')


def read(path):
    raw = path.read_bytes()
    assert raw and raw.endswith(b'\n'), ('missing/incomplete capture', path)
    return [json.loads(line) for line in raw.splitlines()]


def reduce(records, expected, require_expiry=False):
    current, chains, positions = {}, {}, {}
    prefixes = [snapshot(prefix) for prefix in (1, 2, 3, 4)]
    for record in records:
        envelope = record.get('payload', record)
        assert {'before', 'after', 'op'} <= envelope.keys(), envelope
        before, after, op = (envelope[key] for key in ('before', 'after', 'op'))
        assert op in ('c', 'u') and isinstance(after, dict) and set(after) == FIELDS, envelope
        key = after['k']
        assert key in ('keyA', 'keyB'), envelope
        assert all(type(after[field]) is int for field in ('lifetime_count', 'today_count')), envelope
        assert (before is None) == (op == 'c') and before == current.get(key), envelope
        assert before != after, ('duplicate logical emission', envelope)
        candidates = [index for index, value in enumerate(prefixes)
                      if index >= positions.get(key, -1) and value.get(key) == after]
        assert candidates, ('not a forward independent source-prefix result', envelope)
        positions[key] = candidates[0]
        current[key] = after
        chains.setdefault(key, []).append(envelope)
    assert current == expected, ('materialization differs', current, expected)
    if require_expiry:
        transitions = [row for row in chains.get('keyA', []) if row['before'] == snapshot(1)['keyA']
                       and row['after'] == snapshot(3)['keyA'] and row['op'] == 'u']
        assert len(transitions) == 1, ('missing/duplicate retained-key expiry', chains)
    return current


def validate(path):
    initial, recovered = read(path / 'output.initial.jsonl'), read(path / 'output.jsonl')
    reduce(initial, snapshot(4), require_expiry=True)
    reduce(recovered, snapshot(4), require_expiry=True)
    log = (path / 'capture.log').read_text()
    receipts = re.findall(r'^CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=1 '
                          r'committed_rows=(\d+) rows=(\d+) bytes=\d+ path=(\S+)', log, re.M)
    assert len(receipts) == 1, ('missing/ambiguous checkpoint receipt', receipts)
    committed, total, output = receipts[0]
    assert output == str(path / 'output.jsonl') and int(total) == len(recovered), receipts
    assert 0 < int(committed) < len(recovered), receipts
    reduce(recovered[:int(committed)], snapshot(1))
    for phase, hold in (('initial', 2), ('recovered', 2),
                        ('initial-resumed', 3), ('recovered-resumed', 3)):
        before, after = [path / f'output.idle-{phase}-{boundary}.jsonl'
                         for boundary in ('before', 'after')]
        records = read(before)
        read(after)
        assert before.read_bytes() == after.read_bytes(), ('silence changed CDC', phase, hold)
        reduce(records, snapshot(hold), require_expiry=hold == 3)
        complete = initial if phase.startswith('initial') else recovered
        assert complete[:len(records)] == records, ('idle snapshot not output prefix', phase)
    assert '1 passed' in log, ('capture test did not pass', path)


def environment(path, backend, mode):
    # Do not inherit another capture's clocks, controls or SQL fixture switches.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('STREAMR_CAPTURE_', 'STREAMR_TEST_'))}
    env.update(STREAMR_TEST_NATIVE_AGGREGATES='1', STREAMR_TEST_BACKEND=backend,
               STREAMR_TEST_CHECKPOINT_MODE=mode, STREAMR_TEST_SOURCE_BATCH_ROWS='1',
               STREAMR_TEST_AGGREGATE_FLUSH_SECONDS='1', STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS='120',
               STREAMR_CAPTURE_QUERY=str(path / 'query.sql'),
               STREAMR_CAPTURE_OUTPUT=str(path / 'output.jsonl'),
               STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='1', STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
               STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS='5', STREAMR_CAPTURE_MAX_INITIAL_ROWS='5',
               STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='1', STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS='1',
               STREAMR_CAPTURE_EXPECTED_ROWS='5', STREAMR_CAPTURE_MAX_ROWS='5',
               STREAMR_CAPTURE_IDLE_SOURCE_ROW_TARGET='2', STREAMR_CAPTURE_IDLE_SECONDS='3',
               STREAMR_CAPTURE_IDLE_MIN_PRE_ROWS='2',
               STREAMR_CAPTURE_IDLE_RESUME_SOURCE_ROW_TARGET='3',
               STREAMR_CAPTURE_IDLE_RESUME_MIN_PRE_ROWS='4',
               STREAMR_CAPTURE_IDLE_MAX_BYTES='262144')
    return env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--backend', choices=('memory', 'rocksdb'))
    parser.add_argument('--mode', choices=('controller', 'leader'))
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not (root / 'results.json').exists(), ('preserve existing results', root)
    outcomes = []
    for backend in ([args.backend] if args.backend else ('memory', 'rocksdb')):
        for mode in ([args.mode] if args.mode else ('controller', 'leader')):
            path = root / f'{backend}-{mode}'
            outcome = dict(backend=backend, checkpoint_mode=mode, source_batch_rows=1,
                           directory=str(path), status='prepared', engine_exit_code=None)
            try:
                assert not path.exists() or not any(path.iterdir()), ('preserve existing evidence', path)
                prepare(path)
                (path / 'capture.environment.json').write_text(json.dumps(
                    {k: v for k, v in environment(path, backend, mode).items()
                     if k.startswith(('STREAMR_CAPTURE_', 'STREAMR_TEST_'))}, indent=2) + '\n')
                if args.binary:
                    with (path / 'capture.log').open('w') as log:
                        result = subprocess.run([str(args.binary.resolve(strict=True)),
                                                 'external_sql_checkpoint_capture', '--ignored',
                                                 '--test-threads=1', '--nocapture'],
                                                env=environment(path, backend, mode),
                                                stdout=log, stderr=subprocess.STDOUT, timeout=500)
                    outcome['engine_exit_code'] = result.returncode
                    assert result.returncode == 0, ('engine failed', path / 'capture.log')
                    validate(path)
                    outcome['status'] = 'passed'
                    print(f'PASS {path}: live quiet expiry, silence, checkpoint and fresh-worker CDC', flush=True)
                else:
                    print(f'PREPARED {path}; no engine execution', flush=True)
            except Exception as error:
                outcome.update(status='failed', error=f'{type(error).__name__}: {error}')
                print(f'FAIL {path}: {outcome["error"]}', flush=True)
            outcomes.append(outcome)
            (root / 'results.json').write_text(json.dumps(
                dict(scope='native SQL local worker; no Kafka/process-loss/capacity qualification',
                     executed=bool(args.binary), cases=outcomes), indent=2) + '\n')
    if any(outcome['status'] == 'failed' for outcome in outcomes):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
