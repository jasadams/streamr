#!/usr/bin/env python3
"""Prepare and compare generic CDC aggregate captures; never builds or runs Streamr."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path
import re

import native_updating_array_cdc as arrays

VARIANTS = {'direct': 1, 'union': 2, 'shared_cte': 4}
MATRIX = tuple(itertools.product(('memory', 'rocksdb'), ('controller', 'leader'), (1, 8)))
FIELDS = {'k', 'active', 'max_position'}


def events():
    result = arrays.input_events()
    result.extend([
        {'before': arrays.as_row((3, 'b', 'y', 2)),
         'after': arrays.as_row((3, 'c', 'y', 5)), 'op': 'u', 'ts_ms': 13000},
        {'before': arrays.as_row((3, 'c', 'y', 5)),
         'after': None, 'op': 'd', 'ts_ms': 14000},
    ])
    return result


def source_states():
    rows, states = {}, [{}]
    for event in events():
        before, after = event['before'], event['after']
        if before is not None:
            if rows.pop(before['row_id']) != before:
                raise ValueError('fixture before image does not match its retained row')
        if after is not None:
            if after['row_id'] in rows:
                raise ValueError('fixture inserts an existing primary key')
            rows[after['row_id']] = after
        groups = {}
        for row in rows.values():
            groups.setdefault(row['k'], []).append(row['position'])
        states.append({key: {'k': key, 'active': len(values), 'max_position': max(values)}
                       for key, values in groups.items()})
    return states


def expected_state(state, copies):
    return {key: dict(row, active=row['active'] * copies) for key, row in state.items()}


def render_query(runtime, variant):
    source = str(runtime / 'input.jsonl').replace("'", "''")
    sink = str(runtime / 'output.jsonl').replace("'", "''")
    branch = 'SELECT k, position FROM timestamp_input'
    if variant == 'direct':
        prefix, relation = '', 'timestamp_input'
    elif variant == 'union':
        prefix, relation = '', f'({branch} UNION ALL {branch}) AS combined'
    elif variant == 'shared_cte':
        prefix = f'WITH pair AS ({branch} UNION ALL {branch})\n'
        relation = '(SELECT * FROM pair UNION ALL SELECT * FROM pair) AS combined'
    else:
        raise ValueError('unknown query variant')
    return f"""SET updating_ttl = NULL;
CREATE TABLE timestamp_input (
  row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, v TEXT, position BIGINT NOT NULL
) WITH (connector='single_file', path='{source}', format='debezium_json',
        type='source', wait_for_control='true');
CREATE TABLE timestamp_output (k TEXT, active BIGINT, max_position BIGINT)
WITH (connector='single_file', path='{sink}', format='debezium_json', type='sink');
INSERT INTO timestamp_output
{prefix}SELECT k, COUNT(*) AS active, MAX(position) AS max_position
FROM {relation} GROUP BY k;
"""


def environment(runtime, backend, mode, batch):
    return {
        'STREAMR_TEST_BACKEND': backend, 'STREAMR_TEST_CHECKPOINT_MODE': mode,
        'STREAMR_TEST_SOURCE_BATCH_ROWS': str(batch), 'STREAMR_TEST_NATIVE_AGGREGATES': '1',
        'STREAMR_TEST_AGGREGATE_FLUSH_SECONDS': '3600',
        'STREAMR_CAPTURE_QUERY': str(runtime / 'query.sql'),
        'STREAMR_CAPTURE_OUTPUT': str(runtime / 'output.jsonl'),
        'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': '8',
        'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1',
        'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '1', 'STREAMR_CAPTURE_MAX_INITIAL_ROWS': '128',
        'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '1', 'STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS': '128',
        'STREAMR_CAPTURE_EXPECTED_ROWS': '1', 'STREAMR_CAPTURE_MAX_ROWS': '128',
    }


def prepare(host, runtime):
    if not runtime.is_absolute():
        raise ValueError('runtime directory must be absolute')
    host.mkdir(parents=True, exist_ok=False)
    manifest = {'scope': 'generic updating timestamp identities and UNION ALL recovery', 'cases': {}}
    payload = ''.join(json.dumps(event) + '\n' for event in events())
    for backend, mode, batch in MATRIX:
        for variant, copies in VARIANTS.items():
            name = f'{backend}-{mode}-batch{batch}-{variant}'
            directory, runtime_case = host / name, runtime / name
            directory.mkdir()
            (directory / 'input.jsonl').write_text(payload)
            (directory / 'query.sql').write_text(render_query(runtime_case, variant))
            manifest['cases'][name] = {
                'variant': variant, 'copies': copies,
                'input_sha256': arrays.digest(directory / 'input.jsonl'),
                'query_sha256': arrays.digest(directory / 'query.sql'),
                'env': environment(runtime_case, backend, mode, batch),
            }
    (host / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return {'prepared_cases': len(manifest['cases']), 'source_events': 14,
            'checkpoint_after_events': 8, 'runtime_executed': False}


def possible_rows(copies):
    # Fanout branches can progress independently. Enumerate possible aggregate
    # values without requiring unrelated branches to flush in a particular order.
    states = source_states()
    allowed = {}
    for key in ('a', 'b', 'c'):
        members = {(state[key]['active'], state[key]['max_position'])
                   for state in states if key in state} | {(0, None)}
        totals = {(0, None)}
        for _ in range(copies):
            totals = {(n + m, max((x for x in (v, w) if x is not None), default=None))
                      for n, v in totals for m, w in members}
        allowed[key] = {item for item in totals if item[0] > 0}
    return allowed


def replay(entries, allowed):
    current, snapshots = {}, []
    for entry in entries:
        if type(entry) is not dict or set(entry) != {'before', 'after', 'op'}:
            raise ValueError('CDC envelope fields differ')
        before, after, op = entry['before'], entry['after'], entry['op']
        if op not in ('c', 'u', 'd') or (before is None) != (op == 'c') or (
            (after is None) != (op == 'd')
        ):
            raise ValueError('CDC operation/images differ')
        keys = []
        for row in (before, after):
            if row is None:
                continue
            if type(row) is not dict or set(row) != FIELDS or type(row['k']) is not str or (
                type(row['active']) is not int or type(row['max_position']) is not int
            ) or (row['active'], row['max_position']) not in allowed.get(row['k'], set()):
                raise ValueError('typed aggregate row is not reachable from the source fixture')
            keys.append(row['k'])
        key = keys[0]
        if any(value != key for value in keys) or before != current.get(key):
            raise ValueError('CDC before image does not match emitted state')
        if after is None:
            del current[key]
        else:
            current[key] = after
        snapshots.append(current.copy())
    return snapshots


def compare(host):
    manifest = arrays.read_json(host / 'manifest.json')
    names = {f'{backend}-{mode}-batch{batch}-{variant}'
             for backend, mode, batch in MATRIX for variant in VARIANTS}
    if set(manifest['cases']) != names:
        raise ValueError('capture matrix differs')
    states, result = source_states(), {}
    canonical_input = json.dumps(events(), sort_keys=True)
    for backend, mode, batch in MATRIX:
        for variant, copies in VARIANTS.items():
            name = f'{backend}-{mode}-batch{batch}-{variant}'
            directory, case = host / name, manifest['cases'][name]
            runtime = Path(case['env']['STREAMR_CAPTURE_QUERY']).parent
            if not runtime.is_absolute() or case['env'] != environment(runtime, backend, mode, batch):
                raise ValueError(f'{name}: runtime environment differs')
            if case['variant'] != variant or type(case['copies']) is not int or case['copies'] != copies:
                raise ValueError(f'{name}: query multiplicity differs')
            if arrays.digest(directory / 'input.jsonl') != case['input_sha256'] or (
                json.dumps(arrays.read_jsonl(directory / 'input.jsonl'), sort_keys=True) != canonical_input
                or arrays.digest(directory / 'query.sql') != case['query_sha256']
                or (directory / 'query.sql').read_text() != render_query(runtime, variant)
            ):
                raise ValueError(f'{name}: prepared query/input differs')
            log = (directory / 'capture.log').read_text()
            initial_marker = re.findall(r'CAPTURE_RESULT phase=initial rows=(\d+)\b', log)
            restored_marker = re.findall(
                r'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=8 '
                r'committed_rows=(\d+) rows=(\d+)\b', log)
            if len(initial_marker) != 1 or len(restored_marker) != 1 or '1 passed' not in log:
                raise ValueError(f'{name}: checkpoint/recovery capture did not complete')
            committed, count = map(int, restored_marker[0])
            allowed = possible_rows(copies)
            initial = replay(arrays.read_jsonl(directory / 'output.initial.jsonl'), allowed)
            recovered = replay(arrays.read_jsonl(directory / 'output.jsonl'), allowed)
            if len(initial) != int(initial_marker[0]) or len(recovered) != count or (
                not initial or not 1 <= committed <= count
                or initial[-1] != expected_state(states[-1], copies)
                or recovered[committed - 1] != expected_state(states[8], copies)
                or recovered[-1] != expected_state(states[-1], copies)
            ):
                raise ValueError(f'{name}: initial/checkpoint/recovered values differ')
            result[name] = {'initial_rows': len(initial), 'committed_rows': committed,
                            'recovered_rows': count}
    return {'status': 'pass', 'scope': manifest['scope'], 'cases': result}


def self_test():
    allowed = possible_rows(2)
    good = {'k': 'b', 'active': 2, 'max_position': 3}
    if replay([{'before': None, 'after': good, 'op': 'c'}], allowed)[-1] != {'b': good}:
        raise AssertionError('valid fixture row was rejected')
    for wrong in [dict(good, active=True), dict(good, active=0),
                  dict(good, max_position=True), dict(good, max_position=100),
                  dict(good, unexpected=1)]:
        try:
            replay([{'before': None, 'after': wrong, 'op': 'c'}], allowed)
        except ValueError:
            continue
        raise AssertionError('invalid fixture row was accepted')
    try:
        replay([{'before': good, 'after': None, 'op': 'd'}], allowed)
    except ValueError:
        pass
    else:
        raise AssertionError('missing before-image continuity was accepted')
    return {'comparator_self_test': 'pass', 'runtime_executed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, nargs='?')
    parser.add_argument('--runtime-directory', type=Path)
    parser.add_argument('--compare', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_test(), indent=2))
    elif args.directory is None:
        parser.error('directory is required')
    elif args.compare:
        print(json.dumps(compare(args.directory.resolve()), indent=2))
    elif args.runtime_directory is None:
        parser.error('--runtime-directory is required for preparation')
    else:
        print(json.dumps(prepare(args.directory.resolve(), args.runtime_directory), indent=2))
