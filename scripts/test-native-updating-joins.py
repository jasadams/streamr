#!/usr/bin/env python3
"""Prepare/run generic native updating-join SQL checkpoint fixtures."""
import argparse
from collections import Counter
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import subprocess

FIXTURES = Path(__file__).resolve().parent / 'fixtures/native-updating-joins'
QUERY_TEMPLATES = {'direct': 'direct.sql', 'chained-inner': 'chained-inner.sql', 'downstream-inner': 'downstream-inner.sql'}
FIELDS = {'left_id', 'right_id', 'k1', 'k2', 'left_total', 'right_total', 'left_count', 'right_count'}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_fixture(fixture):
    template = fixture.get('query_template', 'direct')
    if template not in QUERY_TEMPLATES:
        raise ValueError(f'unsupported query_template: {template!r}')
    if template != 'direct' and fixture['join'] != 'INNER':
        raise ValueError('composed query templates require INNER join')
    if fixture['join'] not in {'INNER', 'LEFT'}:
        raise ValueError('fixture join must be INNER or LEFT')
    seen = set()
    for index, event in enumerate(fixture['events']):
        if event['side'] not in {'l', 'r'} or type(event['seq']) is not int:
            raise ValueError(f'event {index}: side must be l/r and seq must be an integer')
        ordering = (event['side'], event['item_id'], event['seq'])
        if ordering in seen:
            raise ValueError(f'event {index}: ambiguous LAST_VALUE ORDER BY seq tie for side={event["side"]!r}, item_id={event["item_id"]!r}, seq={event["seq"]}')
        seen.add(ordering)


def aggregate(events, side, prefix):
    groups, latest = {}, {}
    for event in [e for e in events if e['side'] == side][:prefix]:
        row = groups.setdefault(event['item_id'], dict(item_id=event['item_id'], total=0, count=0))
        if event['item_id'] not in latest or event['seq'] > latest[event['item_id']]:
            row.update(k1=event['k1'], k2=event['k2'])
            latest[event['item_id']] = event['seq']
        row['total'] += event['amount']
        row['count'] += 1
    return groups


def snapshot(fixture, left_prefix, right_prefix, events=None):
    events = fixture['events'] if events is None else events
    left = aggregate(events, 'l', left_prefix)
    right = aggregate(events, 'r', right_prefix)
    result = {}
    for l in left.values():
        matches = [r for r in right.values() if l['k1'] is not None and l['k2'] is not None
                   and (l['k1'], l['k2']) == (r['k1'], r['k2'])]
        if not matches and fixture['join'] == 'LEFT':
            matches = [None]
        for r in matches:
            result[(l['item_id'], r['item_id'] if r else None)] = dict(
                left_id=l['item_id'], right_id=r['item_id'] if r else None,
                k1=l['k1'], k2=l['k2'], left_total=l['total'], right_total=r['total'] if r else None,
                left_count=l['count'], right_count=r['count'] if r else None)
    return result


def validate(records, fixture, prefix=None):
    """Every pair follows a forward independent-branch prefix; final bag is exact.

    Fanout changes may span several CDC records, so intermediate bags need not
    be atomic snapshots. Each complete row must come from real branch prefixes.
    """
    validate_fixture(fixture)
    events = fixture['events'][:prefix] if prefix is not None else fixture['events']
    nl, nr = (sum(e['side'] == side for e in events) for side in ('l', 'r'))
    snapshots = {(l, r): snapshot(fixture, l, r, events)
                 for l, r in itertools.product(range(nl + 1), range(nr + 1))}
    current, positions = {}, {}
    for index, raw in enumerate(records):
        row = raw.get('payload', raw)
        assert set(row) >= {'before', 'after', 'op'}, (index, row)
        before, after, op = row['before'], row['after'], row['op']
        assert op in {'c', 'u', 'd'}, (index, row)
        assert (before is None) == (op == 'c'), (index, row)
        assert (after is None) == (op == 'd'), (index, row)
        value = after if after is not None else before
        assert set(value) == FIELDS, (index, value)
        for field in ('left_total', 'right_total', 'left_count', 'right_count', 'k2'):
            assert value[field] is None or type(value[field]) is int, (index, field, value)
        assert isinstance(value['left_id'], str), (index, value)
        assert value['right_id'] is None or isinstance(value['right_id'], str), (index, value)
        identity = (value['left_id'], value['right_id'])
        if before is not None:
            assert (before['left_id'], before['right_id']) == identity, (index, row)
        assert current.get(identity) == before, ('CDC continuity', index, identity, current.get(identity), before)
        previous = positions.get(identity, {(0, 0)})
        if after is None:
            # Join retract/add rows are separate batches; Debezium can publish
            # a temporary vacancy even for an unchanged pair. Preserve
            # the value frontier: vacancy cannot authorize a backwards value.
            possible = previous
        else:
            possible = {p for p, state in snapshots.items() if state.get(identity) == after
                        and any(p[0] >= old[0] and p[1] >= old[1] for old in previous)}
        assert possible, ('invalid/backward complete branch-prefix value', index, row)
        positions[identity] = possible
        if after is None:
            del current[identity]
        else:
            current[identity] = after
    expected = snapshot(fixture, nl, nr, events)
    bag = lambda rows: Counter(json.dumps(row, sort_keys=True) for row in rows.values())
    assert bag(current) == bag(expected), ('final bag', current, expected)
    assert current == expected, ('pair identity', current, expected)
    return current


def read_records(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def prepare(fixture, directory):
    validate_fixture(fixture)
    directory.mkdir(parents=True, exist_ok=True)
    assert not any(directory.iterdir()), ('evidence directory must be empty', directory)
    (directory / 'input.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in fixture['events']))
    template = (FIXTURES / 'query.sql').read_text()
    result_sql = (FIXTURES / QUERY_TEMPLATES[fixture.get('query_template', 'direct')]).read_text()
    template = template.replace('@RESULT_SQL@', result_sql.rstrip())
    sql = template.replace('@DIRECTORY@', str(directory).replace("'", "''")).replace('@JOIN@', fixture['join'])
    (directory / 'query.sql').write_text(sql)
    (directory / 'fixture.json').write_text(json.dumps(fixture, indent=2) + '\n')


def run_case(binary, fixture, directory, backend, mode, batch, source_receipt):
    prepare(fixture, directory)
    env = dict(os.environ, STREAMR_TEST_BACKEND=backend, STREAMR_TEST_CHECKPOINT_MODE=mode,
               STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch), STREAMR_TEST_NATIVE_AGGREGATES='1',
               STREAMR_TEST_NATIVE_UPDATING_JOINS='1', STREAMR_TEST_SCAN_PAGE_BYTES='16777216',
               STREAMR_TEST_QUEUED_WRITE_BYTES='67108864',
               STREAMR_TEST_AGGREGATE_FLUSH_SECONDS='3600', STREAMR_TEST_EXECUTION_BYTES='16777216',
               STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'), STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
               STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(fixture['checkpoint']),
               STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS='0', STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='0',
               STREAMR_CAPTURE_EXPECTED_ROWS='0', STREAMR_CAPTURE_CHECKPOINT_EPOCH='1')
    # Finite conservative event/fanout bound, independent of flush cadence.
    maximum = str(4 * (len(fixture['events']) + 1) ** 3)
    for name in ('INITIAL_ROWS', 'CHECKPOINT_ROWS', 'ROWS'):
        env['STREAMR_CAPTURE_MAX_' + name] = maximum
    receipt = dict(binary=str(binary), binary_sha256=digest(binary), source_receipt=source_receipt,
                   driver_sha256=digest(Path(__file__).resolve()),
                   fixture_sha256=digest(directory / 'fixture.json'), sql_sha256=digest(directory / 'query.sql'),
                   input_sha256=digest(directory / 'input.jsonl'), configuration={k: v for k, v in env.items() if k.startswith('STREAMR_')},
                   status='not-run')
    receipt['configuration_sha256'] = hashlib.sha256(json.dumps(receipt['configuration'], sort_keys=True).encode()).hexdigest()
    receipt_path = directory / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    with (directory / 'capture.log').open('w') as log:
        result = subprocess.run([str(binary), 'external_sql_checkpoint_capture', '--ignored', '--test-threads=1', '--nocapture'],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
    receipt.update(exit_status=result.returncode, status='capture-failed')
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    assert result.returncode == 0, directory
    log = (directory / 'capture.log').read_text()
    assert re.search(r'\b1 passed;', log), log[-2000:]
    receipt.update(status='oracle-running')
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    try:
        records = read_records(directory / 'output.jsonl')
        markers = re.findall(r'^CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=\d+ committed_rows=(\d+) rows=(\d+) bytes=\d+ path=(.+) job=\S+$', log, re.MULTILINE)
        assert len(markers) == 1, markers
        committed, count, path = markers[0]
        assert path == str(directory / 'output.jsonl') and int(count) == len(records), markers
        assert 0 <= int(committed) <= len(records), markers
        validate(read_records(directory / 'output.initial.jsonl'), fixture)
        validate(records[:int(committed)], fixture, fixture['checkpoint'])
        validate(records, fixture)
    except Exception as error:
        receipt.update(status='oracle-failed', oracle_error=str(error))
        receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
        raise
    receipt.update(status='passed', committed_rows=int(committed), recovered_rows=len(records))
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    print('PASS', directory.name, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--source-receipt', type=Path, help='Required pinned executable source/build receipt')
    parser.add_argument('--fixture', action='append', type=Path, help='Caller-supplied fixture JSON; defaults to repository fixtures')
    args = parser.parse_args()
    fixtures = [json.loads(p.read_text()) for p in (args.fixture or sorted(FIXTURES.glob('*.json')))]
    for fixture in fixtures:
        validate_fixture(fixture)
    root = args.directory.resolve()
    assert not root.exists() or (root.is_dir() and not any(root.iterdir())), ('evidence root must be absent or empty', root)
    if not args.binary:
        for fixture in fixtures:
            prepare(fixture, root / fixture['name'])
        print(f'Prepared {len(fixtures)} fixtures in {root}; runtime matrix not run')
        return
    if not args.source_receipt:
        parser.error('--binary requires --source-receipt')
    source_receipt = dict(path=str(args.source_receipt.resolve(strict=True)), sha256=digest(args.source_receipt),
                          content=args.source_receipt.read_text())
    binary = args.binary.resolve(strict=True)
    for fixture, backend, mode, batch in itertools.product(fixtures, ('memory', 'rocksdb'), ('controller', 'leader'), (1, 8)):
        run_case(binary, fixture, root / f"{fixture['name']}-{backend}-{mode}-{batch}", backend, mode, batch, source_receipt)


if __name__ == '__main__':
    main()
