#!/usr/bin/env python3
"""Run generic native MERGE/lookup value and checkpoint recovery cases.

Run inside the Bookworm container after building arroyo-sql-testing. This
creates disposable fixtures under --directory and compares complete JSON rows
on memory/RocksDB with controller/leader checkpoint publication.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("binary", type=Path, help="Fresh arroyo-sql-testing test executable")
parser.add_argument("--directory", type=Path, default=Path("/app/target/native-state-table-fixtures"))
parser.add_argument("--target-timestamp", action="store_true", help="Also exercise a retained value named _timestamp")
args = parser.parse_args()
binary = str(args.binary.resolve(strict=True))
root = args.directory.resolve()

events = [
    {'seq': 1, 'scope_key': 'a', 'item_id': 1, 'delta': 10, 'mode': 'write'},
    {'seq': 2, 'scope_key': 'a', 'item_id': 1, 'delta': 3, 'mode': 'write'},
    {'seq': 3, 'scope_key': 'b', 'item_id': 1, 'delta': 7, 'mode': 'write'},
    {'seq': 4, 'scope_key': 'a', 'item_id': 1, 'delta': 99, 'mode': 'noop'},
    {'seq': 5, 'scope_key': 'a', 'item_id': 1, 'delta': 0, 'mode': 'delete'},
    {'seq': 6, 'scope_key': 'a', 'item_id': 1, 'delta': 4, 'mode': 'write'},
    {'seq': 7, 'scope_key': 'a', 'item_id': 2, 'delta': 5, 'mode': 'noop'},
    {'seq': 8, 'scope_key': 'a', 'item_id': None, 'delta': 5, 'mode': 'write'},
    {'seq': 9, 'scope_key': '', 'item_id': 1, 'delta': 2, 'mode': 'write'},
    {'seq': 10, 'scope_key': 'λ', 'item_id': 1, 'delta': 8, 'mode': 'write'},
    {'seq': 11, 'scope_key': 'b', 'item_id': 1, 'delta': 0, 'mode': 'noop'},
    {'seq': 12, 'scope_key': 'a', 'item_id': 1, 'delta': 0, 'mode': 'noop'},
    {'seq': 13, 'scope_key': '', 'item_id': 1, 'delta': 0, 'mode': 'noop'},
    {'seq': 14, 'scope_key': 'λ', 'item_id': 1, 'delta': 0, 'mode': 'noop'},
    {'seq': 15, 'scope_key': 'a', 'item_id': 2, 'delta': 12, 'mode': 'write'},
]
first, second, expected, markers = {}, {}, [], {}
for event in events:
    key = (event['scope_key'], event['item_id'])
    old = first.get(key)
    action = 'none'
    if key[1] is not None:
        if old is not None and event['mode'] == 'delete':
            del first[key]
            action = 'delete'
        elif event['mode'] == 'write':
            first[key] = (old or 0) + event['delta']
            action = 'insert' if old is None else 'update'
    if action == 'delete':
        markers.pop(key, None)
    elif action in ('insert', 'update'):
        markers[key] = event['seq']
    new = first.get(key)
    dep_old, dep_action = second.get(key), 'none'
    if action == 'delete' and key in second:
        del second[key]
        dep_action = 'delete'
    elif action in ('insert', 'update'):
        second[key] = new
        dep_action = 'insert' if dep_old is None else 'update'
    expected.append(dict(seq=event['seq'], scope_key=key[0], item_id=key[1],
                         first_old=old, first_new=new, first_action=action,
                         second_old=dep_old, second_new=second.get(key),
                         second_action=dep_action, lookup_quantity=new))
    if args.target_timestamp:
        expected[-1]['stored_marker'] = markers.get(key)
for backend in ['memory', 'rocksdb']:
    for batch in [1, 8]:
        for mode in ['controller', 'leader']:
            directory = root / f'{backend}-{batch}-{mode}'
            directory.mkdir(parents=True, exist_ok=True)
            (directory / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in events))
            (directory / 'expected.initial.json').write_text(json.dumps(expected, indent=2))
            (directory / 'expected.recovered.json').write_text(json.dumps(expected, indent=2))
            (directory / 'expected.mid.json').write_text(json.dumps([{key: row[key] for key in ['seq', 'scope_key', 'item_id', 'first_old', 'first_new', 'first_action']} for row in expected], indent=2))
            schema = 'seq BIGINT, scope_key TEXT, item_id BIGINT, first_old BIGINT, first_new BIGINT, first_action TEXT, second_old BIGINT, second_new BIGINT, second_action TEXT, lookup_quantity BIGINT'
            if args.target_timestamp:
                schema += ', stored_marker BIGINT'
            marker_field = ', _timestamp BIGINT' if args.target_timestamp else ''
            marker_column = ', _timestamp' if args.target_timestamp else ''
            marker_value = ', source.seq' if args.target_timestamp else ''
            marker_update = ', _timestamp = source.seq' if args.target_timestamp else ''
            marker_lookup = ', i._timestamp AS stored_marker' if args.target_timestamp else ''
            marker_output = ', stored_marker' if args.target_timestamp else ''
            query = f"""
CREATE TABLE mutation_input (seq BIGINT NOT NULL, scope_key TEXT NOT NULL, item_id BIGINT, delta BIGINT NOT NULL, mode TEXT NOT NULL)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE mutation_output ({schema}) WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'json', type = 'sink');
CREATE TABLE mutation_mirror ({schema}) WITH (connector = 'single_file', path = '{directory}/mirror.jsonl', format = 'json', type = 'sink');
CREATE TABLE mutation_mid (seq BIGINT, scope_key TEXT, item_id BIGINT, first_old BIGINT, first_new BIGINT, first_action TEXT) WITH (connector = 'single_file', path = '{directory}/mid.jsonl', format = 'json', type = 'sink');
CREATE STATE TABLE inventory (scope_key TEXT, item_id BIGINT, quantity BIGINT{marker_field}, PRIMARY KEY (scope_key, item_id)) PARTITION BY scope_key;
CREATE STATE TABLE observed (scope_key TEXT, item_id BIGINT, seen_value BIGINT, PRIMARY KEY (scope_key, item_id)) PARTITION BY scope_key;
CREATE VIEW applied AS MERGE INTO inventory AS target USING mutation_input AS source
ON target.scope_key = source.scope_key AND target.item_id = source.item_id
WHEN MATCHED AND source.mode = 'never' THEN UPDATE SET quantity = 1 / (source.delta - source.delta)
WHEN MATCHED AND source.mode = 'delete' THEN DELETE
WHEN MATCHED AND source.mode = 'write' THEN UPDATE SET quantity = target.quantity + source.delta{marker_update}
WHEN NOT MATCHED AND source.mode = 'write' THEN INSERT (scope_key, item_id, quantity{marker_column}) VALUES (source.scope_key, source.item_id, source.delta{marker_value})
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW dependent_events AS SELECT source.seq AS seq, source.scope_key AS scope_key, source.item_id AS item_id,
old.quantity AS first_old, new.quantity AS first_new, action AS first_action FROM applied;
CREATE VIEW dependent_applied AS MERGE INTO observed AS target USING dependent_events AS source
ON target.scope_key = source.scope_key AND target.item_id = source.item_id
WHEN MATCHED AND source.first_action = 'delete' THEN DELETE
WHEN MATCHED AND source.first_action IN ('insert', 'update') THEN UPDATE SET seen_value = source.first_new
WHEN NOT MATCHED AND source.first_action IN ('insert', 'update') THEN INSERT (scope_key, item_id, seen_value) VALUES (source.scope_key, source.item_id, source.first_new)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW checked AS SELECT r.source.seq AS seq, r.source.scope_key AS scope_key, r.source.item_id AS item_id,
r.source.first_old AS first_old, r.source.first_new AS first_new, r.source.first_action AS first_action,
r.old.seen_value AS second_old, r.new.seen_value AS second_new, r.action AS second_action,
i.quantity AS lookup_quantity{marker_lookup} FROM dependent_applied r LEFT JOIN inventory i
ON i.scope_key = r.source.scope_key AND i.item_id = r.source.item_id;
INSERT INTO mutation_mid SELECT seq, scope_key, item_id, first_old, first_new, first_action FROM dependent_events;
INSERT INTO mutation_output SELECT seq, scope_key, item_id, first_old, first_new, first_action, second_old, second_new, second_action, lookup_quantity{marker_output} FROM checked;
INSERT INTO mutation_mirror SELECT seq, scope_key, item_id, first_old, first_new, first_action, second_old, second_new, second_action, lookup_quantity{marker_output} FROM checked;
"""
            (directory / 'query.sql').write_text(query)
print('Prepared 8 generic SQL configurations; not executed or qualified.')

for backend in ['memory', 'rocksdb']:
    for batch in [1, 8]:
        for mode in ['controller', 'leader']:
            case = f'{backend}-{batch}-{mode}'
            directory = root / case
            environment = dict(os.environ,
                STREAMR_TEST_EXECUTION_BYTES='16777216',
                STREAMR_TEST_TYPED_SQL='1',
                STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
                STREAMR_TEST_BACKEND=backend,
                STREAMR_TEST_CHECKPOINT_MODE=mode,
                STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'),
                STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
                STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='4',
                STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS='15',
                STREAMR_CAPTURE_EXPECTED_ROWS='15',
                STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='4',
                STREAMR_CAPTURE_CHECKPOINT_EPOCH='1',
            )
            logfile = Path(f'/tmp/streamr-{root.name}-{case}.log')
            with logfile.open('w') as output:
                result = subprocess.run([binary, 'external_sql_checkpoint_capture',
                    '--ignored', '--test-threads=1', '--nocapture'],
                    env=environment, stdout=output, stderr=subprocess.STDOUT)
            if result.returncode or '1 passed' not in logfile.read_text():
                raise RuntimeError(f'{case} failed: {result.returncode}; inspect {logfile}')
            comparisons = [
                ('output.initial.jsonl', 'expected.initial.json'),
                ('output.jsonl', 'expected.recovered.json'),
                ('mirror.jsonl', 'expected.recovered.json'),
                ('mid.jsonl', 'expected.mid.json'),
            ]
            for filename, oracle in comparisons:
                rows = [json.loads(line) for line in (directory / filename).read_text().splitlines()]
                expected = json.loads((directory / oracle).read_text())
                if rows != expected:
                    raise RuntimeError(f'{case}/{filename}: {rows} != {expected}')
            print(f'PASS {root.name}/{case}: four exact output comparisons; source batch target {batch}', flush=True)
