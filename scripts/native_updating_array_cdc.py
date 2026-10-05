#!/usr/bin/env python3
"""Prepare/compare generic updating ARRAY_AGG CDC captures; never launches Streamr."""
import argparse
import hashlib
import json
from pathlib import Path
import re

HERE = Path(__file__).parent
QUERY = HERE / 'native-updating-array-cdc.sql'
FIELDS = {'k', 'active', 'all_values', 'nonnull_values', 'records'}
EVENTS = [
    ('c', None, (1, 'a', 'x', 2)),
    ('c', None, (2, 'a', 'x', 1)),
    ('c', None, (3, 'b', 'y', 2)),
    ('c', None, (4, 'b', None, 3)),
    ('u', (1, 'a', 'x', 2), (1, 'a', 'z', 4)),
    ('d', (2, 'a', 'x', 1), None),
    ('c', None, (5, 'a', None, 0)),
    ('c', None, (6, 'a', 'q', 3)),
    ('u', (6, 'a', 'q', 3), (6, 'a', 'x', 3)),
    ('d', (1, 'a', 'z', 4), None),
    ('d', (5, 'a', None, 0), None),
    ('d', (6, 'a', 'x', 3), None),
]
MATRIX = [(backend, mode, batch) for backend in ('memory', 'rocksdb')
          for mode in ('controller', 'leader') for batch in (1, 8)]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON field {key!r}')
        result[key] = value
    return result

def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)

def read_jsonl(path):
    raw = path.read_bytes()
    if not raw or not raw.endswith(b'\n'):
        raise ValueError(f'{path}: incomplete or empty JSONL')
    lines = raw[:-1].split(b'\n')
    if any(not line for line in lines):
        raise ValueError(f'{path}: blank JSONL record')
    result = [json.loads(line, object_pairs_hook=unique_object) for line in lines]
    if any(type(row) is not dict for row in result):
        raise ValueError(f'{path}: JSONL rows must be objects')
    return result

def as_row(record):
    if record is None:
        return None
    row_id, key, value, position = record
    return {'row_id': row_id, 'k': key, 'v': value, 'position': position}

def input_events():
    return [{'before': as_row(before), 'after': as_row(after), 'op': op,
             'ts_ms': 1_000 * index}
            for index, (op, before, after) in enumerate(EVENTS, 1)]

def output_state(rows):
    groups = {}
    for row in rows.values():
        groups.setdefault(row['k'], []).append(row)
    result = {}
    for key, members in groups.items():
        members.sort(key=lambda row: (row['position'], row['row_id']))
        result[key] = {
            'k': key, 'active': len(members),
            'all_values': [row['v'] for row in members],
            'nonnull_values': [row['v'] for row in members if row['v'] is not None],
            'records': [{'row_id': row['row_id'], 'sort_pos': row['position'],
                         'label': row['v']} for row in members],
        }
    return result

def prefix_states(events):
    if events != input_events():
        raise ValueError('source CDC differs from pinned generic fixture')
    current, states = {}, []
    for event in events:
        before, after, op = event['before'], event['after'], event['op']
        row = after if after is not None else before
        key = row['row_id']
        if before != current.get(key) or (before is None) != (op == 'c') or (
            (after is None) != (op == 'd')
        ):
            raise ValueError('source CDC before/after chain differs')
        if after is None:
            del current[key]
        else:
            current[key] = after
        states.append(output_state(current))
    return states

def render_query(runtime_case):
    query = QUERY.read_text().replace(
        '$array_input', str(runtime_case / 'input.jsonl').replace("'", "''"))
    query = query.replace(
        '$array_output', str(runtime_case / 'output.jsonl').replace("'", "''"))
    if '$array_' in query:
        raise ValueError('unresolved array path token')
    return query


def oversize_env(runtime_oversized):
    return {'STREAMR_TEST_BACKEND': 'memory',
            'STREAMR_TEST_CHECKPOINT_MODE': 'controller',
            'STREAMR_TEST_NATIVE_AGGREGATES': '1',
            'STREAMR_TEST_SOURCE_BATCH_ROWS': '1',
            'STREAMR_CAPTURE_QUERY': str(runtime_oversized / 'query.sql'),
            'STREAMR_CAPTURE_OUTPUT': str(runtime_oversized / 'output.jsonl'),
            'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': '1',
            'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '1',
            'STREAMR_CAPTURE_MAX_INITIAL_ROWS': '16',
            'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '1',
            'STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS': '16',
            'STREAMR_CAPTURE_EXPECTED_ROWS': '1',
            'STREAMR_CAPTURE_MAX_ROWS': '16',
            'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1'}

def oversize_event():
    return {'before': None, 'after': as_row((99, 'large', 'X' * 65536, 1)),
            'op': 'c', 'ts_ms': 1000}

def prepare(host, runtime):
    host = host.resolve(); runtime = Path(runtime)
    if not runtime.is_absolute():
        raise ValueError('container runtime directory must be absolute')
    events = input_events()
    states = prefix_states(events)
    assert len(states) == 12 and states[7]['a']['all_values'] == [None, 'q', 'z']
    host.mkdir(parents=True, exist_ok=True)
    expected = {'source_events': 12, 'checkpoint_after_events': 8,
                'prefix_states': states, 'checkpoint': states[7], 'final': states[-1]}
    (host / 'expected.json').write_text(json.dumps(expected, indent=2) + '\n')
    manifest = {'scope': 'generic updating ARRAY_AGG CDC and checkpoint recovery',
                'query_template_sha256': digest(QUERY),
                'expected_sha256': digest(host / 'expected.json'),
                'cases': {}}
    for backend, mode, batch in MATRIX:
        name = f'{backend}-{mode}-batch{batch}'
        directory, runtime_case = host / name, runtime / name
        directory.mkdir(exist_ok=True)
        (directory / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in events))
        (directory / 'query.sql').write_text(render_query(runtime_case))
        env = {'STREAMR_TEST_BACKEND': backend,
               'STREAMR_TEST_CHECKPOINT_MODE': mode,
               'STREAMR_TEST_SOURCE_BATCH_ROWS': str(batch),
               'STREAMR_TEST_NATIVE_AGGREGATES': '1',
               'STREAMR_TEST_AGGREGATE_FLUSH_SECONDS': '3600',
               'STREAMR_CAPTURE_QUERY': str(runtime_case / 'query.sql'),
               'STREAMR_CAPTURE_OUTPUT': str(runtime_case / 'output.jsonl'),
               'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': '8',
               'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '1',
               'STREAMR_CAPTURE_MAX_INITIAL_ROWS': '64',
               'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '1',
               'STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS': '64',
               'STREAMR_CAPTURE_EXPECTED_ROWS': '1',
               'STREAMR_CAPTURE_MAX_ROWS': '64',
               'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1'}
        manifest['cases'][name] = {'backend': backend, 'checkpoint_mode': mode,
                                   'source_batch_rows': batch,
                                   'input_sha256': digest(directory / 'input.jsonl'),
                                   'query_sha256': digest(directory / 'query.sql'), 'env': env}
    (host / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    # Separate resource-failure probe. It must fail explicitly; it is not a
    # successful capture case and no output-value oracle is attached.
    oversized = host / 'oversize'; oversized.mkdir(exist_ok=True)
    huge = oversize_event()
    (oversized / 'input.jsonl').write_text(json.dumps(huge) + '\n')
    runtime_oversized = runtime / 'oversize'
    (oversized / 'query.sql').write_text(render_query(runtime_oversized))
    (oversized / 'expected-error.json').write_text(json.dumps({
        'scope': 'explicit resource-failure only',
        'source_events': 1, 'value_bytes': 65536,
        'expected_error_substring': 'native collection exceeds configured encoded-state or pending-output budget',
        'input_sha256': digest(oversized / 'input.jsonl'),
        'query_sha256': digest(oversized / 'query.sql'),
        'query_template_sha256': digest(QUERY),
        'env': oversize_env(runtime_oversized)
    }, indent=2) + '\n')
    return {'normal_cases': 8, 'source_events': 12,
            'checkpoint': expected['checkpoint'], 'final': expected['final'],
            'oversize': 'declared expected failure, never a normal capture pass'}

def strict_row(row, allowed):
    if type(row) is not dict or set(row) != FIELDS or type(row['k']) is not str or (
        row['k'] not in allowed or type(row['active']) is not int
        or not 1 <= row['active'] <= 6 or type(row['all_values']) is not list
        or type(row['nonnull_values']) is not list or type(row['records']) is not list
        or not row['records'] or len(row['all_values']) != row['active']
        or len(row['records']) != row['active']
        or any(value is not None and type(value) is not str for value in row['all_values'])
        or any(type(value) is not str for value in row['nonnull_values'])
    ):
        raise ValueError(f'wrong typed array row: {row!r}')
    for record in row['records']:
        if type(record) is not dict or set(record) != {'row_id', 'sort_pos', 'label'} or (
            type(record['row_id']) is not int or type(record['sort_pos']) is not int
            or record['label'] is not None and type(record['label']) is not str
        ):
            raise ValueError(f'wrong typed struct member: {record!r}')
    if row not in allowed[row['k']]:
        raise ValueError(f'array row differs from all source-prefix states: {row!r}')
    return row['k']

def reduce_cdc(path, allowed, prefix_states):
    current, positions, snapshots = {}, {}, []
    for entry in read_jsonl(path):
        payload = entry['payload'] if set(entry) == {'payload'} else entry
        if type(payload) is not dict or set(payload) != {'before', 'after', 'op'}:
            raise ValueError(f'{path}: CDC envelope differs')
        before, after, op = payload['before'], payload['after'], payload['op']
        if op not in ('c', 'u', 'd') or (before is None) != (op == 'c') or (
            (after is None) != (op == 'd')
        ):
            raise ValueError(f'{path}: CDC operation/images differ')
        key = strict_row(after if after is not None else before, allowed)
        if before is not None and strict_row(before, allowed) != key:
            raise ValueError(f'{path}: CDC before key differs')
        if before != current.get(key) or (op == 'u' and before == after):
            raise ValueError(f'{path}: CDC before continuity differs')
        candidate = [index for index, state in enumerate(prefix_states)
                     if index > positions.get(key, -1) and state.get(key) == after]
        if not candidate:
            raise ValueError(f'{path}: CDC state is not a forward source-prefix value')
        positions[key] = candidate[0]
        if after is None:
            del current[key]
        else:
            current[key] = after
        snapshots.append(current.copy())
    return snapshots

def compare(host):
    host = host.resolve()
    manifest, expected = read_json(host / 'manifest.json'), read_json(host / 'expected.json')
    if digest(QUERY) != manifest['query_template_sha256'] or (
        digest(host / 'expected.json') != manifest['expected_sha256']
    ):
        raise ValueError('query/oracle inventory hash changed')
    states = prefix_states(read_jsonl(host / next(iter(manifest['cases'])) / 'input.jsonl'))
    if expected != {'source_events': 12, 'checkpoint_after_events': 8,
                    'prefix_states': states, 'checkpoint': states[7], 'final': states[-1]}:
        raise ValueError('expected prefix oracle differs from source CDC')
    allowed = {key: [] for state in states for key in state}
    for state in states:
        for key, row in state.items():
            if row not in allowed[key]:
                allowed[key].append(row)
    names = {f'{backend}-{mode}-batch{batch}' for backend, mode, batch in MATRIX}
    if set(manifest['cases']) != names:
        raise ValueError('case matrix differs')
    result = {}
    for backend, mode, batch in MATRIX:
        name = f'{backend}-{mode}-batch{batch}'
        directory, case = host / name, manifest['cases'][name]
        env = case['env']; runtime = Path(env['STREAMR_CAPTURE_QUERY']).parent
        if (case['backend'], case['checkpoint_mode'], case['source_batch_rows']) != (
            backend, mode, batch
        ) or not runtime.is_absolute() or (
            env['STREAMR_TEST_BACKEND'], env['STREAMR_TEST_CHECKPOINT_MODE'],
            env['STREAMR_TEST_SOURCE_BATCH_ROWS'], env['STREAMR_TEST_NATIVE_AGGREGATES'],
            env['STREAMR_TEST_AGGREGATE_FLUSH_SECONDS'],
            env['STREAMR_CAPTURE_QUERY'], env['STREAMR_CAPTURE_OUTPUT'],
            env['STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT'],
            env['STREAMR_CAPTURE_CHECKPOINT_EPOCH']
        ) != (backend, mode, str(batch), '1', '3600', str(runtime / 'query.sql'),
              str(runtime / 'output.jsonl'), '8', '1') or (
            digest(directory / 'input.jsonl') != case['input_sha256']
            or digest(directory / 'query.sql') != case['query_sha256']
            or (directory / 'query.sql').read_text() != render_query(runtime)
            or read_jsonl(directory / 'input.jsonl') != input_events()
        ):
            raise ValueError(f'{name}: prepared case differs')
        log = (directory / 'capture.log').read_text()
        initial_marker = re.search(r'CAPTURE_RESULT phase=initial rows=(\d+)\b', log)
        recovered_marker = re.search(
            r'CAPTURE_RESULT phase=recovered .* committed_rows=(\d+) rows=(\d+)\b', log)
        if '1 passed' not in log or initial_marker is None or recovered_marker is None:
            raise ValueError(f'{name}: generic capture did not pass')
        committed, recovered_count = map(int, recovered_marker.groups())
        initial = reduce_cdc(directory / 'output.initial.jsonl', allowed, states)
        recovered = reduce_cdc(directory / 'output.jsonl', allowed, states)
        if not initial or len(initial) != int(initial_marker.group(1)) or (
            committed < 1 or committed > len(recovered) or len(recovered) != recovered_count
            or initial[-1] != states[-1] or recovered[committed - 1] != states[7]
            or recovered[-1] != states[-1]
        ):
            raise ValueError(f'{name}: initial/checkpoint/recovered typed arrays differ')
        result[name] = {'initial_rows': len(initial), 'committed_rows': committed,
                        'recovered_rows': len(recovered)}
    return {'status': 'pass', 'scope': 'generic updating ARRAY_AGG CDC', 'cases': result}

def compare_oversize(host):
    directory = host.resolve() / 'oversize'
    expected = read_json(directory / 'expected-error.json')
    runtime = Path(expected['env']['STREAMR_CAPTURE_QUERY']).parent
    diagnostic = 'native collection exceeds configured encoded-state or pending-output budget'
    fixed_expected = {
        'scope': 'explicit resource-failure only', 'source_events': 1,
        'value_bytes': 65536, 'expected_error_substring': diagnostic,
        'input_sha256': digest(directory / 'input.jsonl'),
        'query_sha256': digest(directory / 'query.sql'),
        'query_template_sha256': digest(QUERY), 'env': oversize_env(runtime),
    }
    if (not runtime.is_absolute() or runtime.name != 'oversize'
            or expected != fixed_expected
            or read_jsonl(directory / 'input.jsonl') != [oversize_event()]
            or (directory / 'query.sql').read_text() != render_query(runtime)):
        raise ValueError('oversize fixture, env or error oracle changed')
    log = (directory / 'capture.log').read_text()
    exit_code = int((directory / 'exit-code.txt').read_text().strip())
    if exit_code == 0 or '1 passed' in log or (
        diagnostic not in log
    ):
        raise ValueError('oversize case did not explicitly fail at collection budget')
    return {'status': 'expected_failure', 'scope': expected['scope'],
            'process_exit_code': exit_code}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host_directory', type=Path)
    parser.add_argument('--runtime-directory', type=Path)
    parser.add_argument('--compare', action='store_true')
    parser.add_argument('--compare-oversize', action='store_true')
    args = parser.parse_args()
    if args.compare and args.compare_oversize:
        parser.error('choose one comparison')
    if args.compare:
        result = compare(args.host_directory)
    elif args.compare_oversize:
        result = compare_oversize(args.host_directory)
    else:
        if args.runtime_directory is None:
            parser.error('--runtime-directory is required to prepare')
        result = prepare(args.host_directory, args.runtime_directory)
    print(json.dumps(result, sort_keys=True))

if __name__ == '__main__':
    main()
