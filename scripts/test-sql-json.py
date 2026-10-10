#!/usr/bin/env python3
"""STR-56 actual SQL worker MERGE, checkpoint and fresh-worker recovery matrix."""
import argparse
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('binary', type=Path)
parser.add_argument('--directory', type=Path, required=True)
parser.add_argument('--prepare-only', action='store_true')
args = parser.parse_args()
root = args.directory.resolve()
# Expected values are declared independently, without invoking a JSON path oracle.
cases = [
    ('{"traits":{"name":"initial"}}', True, 'initial', '"initial"', False, None, None, None),
    ('{}', False, 'initial', None, False, None, None, None),
    ('{"traits":{"name":null}}', True, None, 'null', True, None, None, None),
    ('{"traits":{"name":""}}', True, '', '""', False, None, None, None),
    ('{"traits":{"name":"Jason","quality_score":0,"steam_wishlisted":false}}', True, 'Jason', '"Jason"', False, 0.0, False, None),
    ('{"traits":{"name":"a\\\"b\\nλ","quality_score":"2.5","steam_wishlisted":"true"}}', True, 'a"b\nλ', '"a\\\"b\\nλ"', False, 2.5, True, None),
    ('{"traits":{"name":false}}', True, 'false', 'false', False, None, None, None),
    ('{"traits":{"name":0}}', True, '0', '0', False, None, None, None),
    ('{"traits":{"name":{"nested":1}}}', True, None, '{"nested":1}', False, None, None, None),
    ('{"traits":{"name":[1,2]}}', True, None, '[1,2]', False, None, None, None),
    ('not json', False, None, None, False, None, None, None),
    (None, None, None, None, None, None, None, None),
    ('{"identifiers":[{"identity_type":"email","value":"one"}]}', False, None, None, False, None, None, 'one'),
    ('{"identifiers":[{"identity_type":"email","value":"one"},{"identity_type":"email","value":"two"}]}', False, None, None, False, None, None, None),
    ('{"traits":{"name":"final"},"identifiers":[{"identity_type":"discord","value":"d1"}]}', True, 'final', '"final"', False, None, None, None),
]
expected = []
old = None
for seq, (payload, present, name, query, is_null, score, wishlisted, email) in enumerate(cases, 1):
    expected.append(dict(seq=seq, old_name=old, new_name=name,
                         old_payload=cases[seq-2][0] if seq > 1 else None,
                         name_present=present, name_json=query, name_is_null=is_null,
                         score=score, wishlisted=wishlisted, email=email,
                         stored_payload=payload, empty_object='{}', null_object='{"x":null}',
                         nested_object='{"outer":{"x":null}}',
                         string_object='{"x":"{}"}', json_object='{"x":{}}'))
    old = name
for backend in ['memory', 'rocksdb']:
    for batch in [1, 8]:
        for mode in ['controller', 'leader']:
            case = f'{backend}-{batch}-{mode}'
            directory = root / case
            directory.mkdir(parents=True, exist_ok=True)
            inputs = [dict(seq=i, k='k', payload=data[0]) for i, data in enumerate(cases, 1)]
            (directory / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in inputs))
            (directory / 'expected.json').write_text(json.dumps(expected, indent=2))
            query = f"""
CREATE TABLE events (seq BIGINT, k TEXT, payload TEXT) WITH (connector='single_file', path='{directory}/input.jsonl', format='json', type='source', wait_for_control='true');
CREATE TABLE result WITH (connector='single_file', path='{directory}/output.jsonl', format='json', type='sink');
CREATE STATE TABLE retained (k TEXT PRIMARY KEY, name TEXT, payload TEXT) PARTITION BY k;
CREATE VIEW applied AS MERGE INTO retained AS target USING events AS source ON target.k=source.k
WHEN MATCHED THEN UPDATE SET name=CASE WHEN JSON_EXISTS(source.payload, '$.traits.name') THEN JSON_VALUE(source.payload, '$.traits.name') ELSE target.name END, payload=source.payload
WHEN NOT MATCHED THEN INSERT (k,name,payload) VALUES(source.k, JSON_VALUE(source.payload, '$.traits.name'), source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO result SELECT source.seq AS seq, old.name AS old_name, new.name AS new_name, old.payload AS old_payload,
JSON_EXISTS(new.payload, '$.traits.name') AS name_present,
JSON_QUERY(new.payload, '$.traits.name') AS name_json,
JSON_EXISTS(new.payload, '$.traits.name ? (@ == null)') AS name_is_null,
JSON_VALUE(new.payload, '$.traits.quality_score' RETURNING DOUBLE PRECISION) AS score,
JSON_VALUE(new.payload, '$.traits.steam_wishlisted' RETURNING BOOLEAN) AS wishlisted,
JSON_VALUE(new.payload, '$.identifiers[*] ? (@.identity_type == "email").value') AS email,
new.payload AS stored_payload,
JSON_OBJECT() AS empty_object, JSON_OBJECT('x' VALUE CAST(NULL AS VARCHAR)) AS null_object,
JSON_OBJECT('outer' VALUE JSON_OBJECT('x' VALUE CAST(NULL AS VARCHAR)) FORMAT JSON) AS nested_object,
JSON_OBJECT('x' VALUE '{{}}') AS string_object, JSON_OBJECT('x' VALUE '{{}}' FORMAT JSON) AS json_object FROM applied;
"""
            (directory / 'query.sql').write_text(query)
            if args.prepare_only:
                continue
            environment = dict(os.environ, STREAMR_TEST_TYPED_SQL='1',
                STREAMR_TEST_TYPED_WORKING_EVENT_BYTES='1048576',
                STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch), STREAMR_TEST_BACKEND=backend,
                STREAMR_TEST_CHECKPOINT_MODE=mode,
                STREAMR_CAPTURE_QUERY=str(directory / 'query.sql'),
                STREAMR_CAPTURE_OUTPUT=str(directory / 'output.jsonl'),
                STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='4',
                STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(len(cases)),
                STREAMR_CAPTURE_EXPECTED_ROWS=str(len(cases)),
                STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='4', STREAMR_CAPTURE_CHECKPOINT_EPOCH='1')
            log = directory / 'run.log'
            with log.open('w') as output:
                result = subprocess.run([str(args.binary.resolve()), 'external_sql_checkpoint_capture',
                    '--ignored', '--test-threads=1', '--nocapture'], env=environment,
                    stdout=output, stderr=subprocess.STDOUT)
            if result.returncode or '1 passed' not in log.read_text():
                raise RuntimeError(f'{case}: exit {result.returncode}; see {log}')
            for filename, wanted in [('output.initial.jsonl', expected), ('output.jsonl', expected),
                                      ('output.checkpoint-1.jsonl', expected[:4])]:
                actual = [json.loads(line) for line in (directory / filename).read_text().splitlines()]
                for row in actual:
                    for field in ['name_present', 'name_is_null', 'wishlisted']:
                        if row[field] is not None and type(row[field]) is not bool:
                            raise RuntimeError(f'{case}: {field} is not a serialized Boolean')
                    if row['score'] is not None and type(row['score']) not in (int, float):
                        raise RuntimeError(f'{case}: score is not a serialized number')
                # Compare serialized JSON documents structurally; preserve SQL NULL
                # versus the VARCHAR JSON text "null" and ordinary VARCHAR values.
                wanted = json.loads(json.dumps(wanted))
                for rows in [actual, wanted]:
                    for row in rows:
                        for field in ['name_json', 'empty_object', 'null_object', 'nested_object', 'string_object', 'json_object']:
                            if row[field] is not None:
                                row[field] = ['json', json.loads(row[field])]
                if actual != wanted:
                    raise RuntimeError(f'{case}/{filename}: {actual!r} != {wanted!r}')
            print(f'PASS {case}: full 15-event initial/recovered oracle and committed prefix', flush=True)
print('Prepared fixtures; not executed.' if args.prepare_only else 'PASS 8 SQL/JSON worker recovery configurations.')
