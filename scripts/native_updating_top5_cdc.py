#!/usr/bin/env python3
"""Prepare and compare a generic two-stage updating top-five ARRAY_AGG probe.

This script never starts Streamr. `array_slice` limits the output to five items;
the native ARRAY_AGG still retains/materializes all distinct items per key.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

import native_updating_array_cdc as arrays


HERE = Path(__file__).parent
MATRIX = arrays.MATRIX
CHECKPOINT = 21
FIELDS = {'k', 'top_items'}
QUERY = """-- Generic existing-SQL top-five array probe; no bounded top-K state claim.
SET updating_ttl = NULL;
CREATE TABLE array_input (
  row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, v TEXT, position BIGINT NOT NULL
) WITH (connector = 'single_file', path = '$array_input',
        format = 'debezium_json', type = 'source', wait_for_control = 'true');
CREATE TABLE ranked_output (
  k TEXT, top_items STRUCT<item_key TEXT, n BIGINT>[]
) WITH (connector = 'single_file', path = '$ranked_output',
        format = 'debezium_json', type = 'sink');
CREATE VIEW item_counts AS
SELECT k, v AS item_key, COUNT(*) AS n
FROM array_input WHERE v IS NOT NULL GROUP BY k, v;
INSERT INTO ranked_output
SELECT k,
       array_slice(ARRAY_AGG(named_struct('item_key', item_key, 'n', n)
                             ORDER BY n DESC, item_key ASC), 1, 5) AS top_items
FROM item_counts GROUP BY k;
"""


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def events():
    result = arrays.input_events()
    for row_id, value in zip(range(100, 107), 'abcdefg'):
        result.append({'before': None,
                       'after': arrays.as_row((row_id, 'c', value, row_id - 100)),
                       'op': 'c', 'ts_ms': 1000 * (len(result) + 1)})
    for row_id, value, position in ((107, 'g', 7), (108, 'f', 8)):
        result.append({'before': None,
                       'after': arrays.as_row((row_id, 'c', value, position)),
                       'op': 'c', 'ts_ms': 1000 * (len(result) + 1)})
    for row_id, value, position in ((106, 'g', 6), (105, 'f', 5)):
        result.append({'before': arrays.as_row((row_id, 'c', value, position)),
                       'after': None, 'op': 'd',
                       'ts_ms': 1000 * (len(result) + 1)})
    assert len(result) == 23
    return result


def state(rows):
    counts = {}
    for row in rows.values():
        if row['v'] is not None:
            key = row['k'], row['v']
            counts[key] = counts.get(key, 0) + 1
    grouped = {}
    for (key, item), count in counts.items():
        grouped.setdefault(key, []).append((item, count))
    return {key: {'k': key, 'top_items': [
                {'item_key': item, 'n': count}
                for item, count in sorted(items, key=lambda value: (-value[1], value[0]))[:5]]}
            for key, items in grouped.items()}


def prefix_states(source):
    if canonical(source) != canonical(events()):
        raise ValueError('source differs from the declared generic CDC events')
    live, states = {}, []
    for event in source:
        before, after, op = event['before'], event['after'], event['op']
        row = after if after is not None else before
        row_id = row['row_id']
        if (canonical(before) != canonical(live.get(row_id))
                or (before is None) != (op == 'c')
                or (after is None) != (op == 'd')):
            raise ValueError('source CDC before/after chain differs')
        if after is None:
            del live[row_id]
        else:
            live[row_id] = after
        states.append(state(live))
    return states


def render_query(runtime_case):
    query = QUERY.replace('$array_input',
                          str(runtime_case / 'input.jsonl').replace("'", "''"))
    query = query.replace('$ranked_output',
                          str(runtime_case / 'output.jsonl').replace("'", "''"))
    if '$array_' in query or '$ranked_' in query:
        raise ValueError('unresolved SQL path token')
    return query


def case_env(runtime_case, backend, mode, batch):
    return {
        'STREAMR_TEST_BACKEND': backend,
        'STREAMR_TEST_CHECKPOINT_MODE': mode,
        'STREAMR_TEST_SOURCE_BATCH_ROWS': str(batch),
        'STREAMR_TEST_NATIVE_AGGREGATES': '1',
        'STREAMR_TEST_AGGREGATE_FLUSH_SECONDS': '3600',
        'STREAMR_CAPTURE_QUERY': str(runtime_case / 'query.sql'),
        'STREAMR_CAPTURE_OUTPUT': str(runtime_case / 'output.jsonl'),
        'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': str(CHECKPOINT),
        'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '1',
        'STREAMR_CAPTURE_MAX_INITIAL_ROWS': '128',
        'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '1',
        'STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS': '128',
        'STREAMR_CAPTURE_EXPECTED_ROWS': '1',
        'STREAMR_CAPTURE_MAX_ROWS': '128',
        'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1',
    }


def prepare(host, runtime):
    host = host.resolve()
    runtime = Path(runtime)
    if not runtime.is_absolute():
        raise ValueError('container runtime directory must be absolute')
    if host.exists():
        raise ValueError('refusing to reuse an existing proof directory')
    source = events()
    states = prefix_states(source)
    assert states[CHECKPOINT - 1]['c']['top_items'] == [
        {'item_key': item, 'n': count}
        for item, count in (('f', 2), ('g', 2), ('a', 1), ('b', 1), ('c', 1))]
    assert states[-1]['c']['top_items'] == [
        {'item_key': item, 'n': 1} for item in 'abcde']
    host.mkdir(parents=True, exist_ok=True)
    expected = {'source_events': len(source), 'checkpoint_after_events': CHECKPOINT,
                'prefix_states': states, 'checkpoint': states[CHECKPOINT - 1],
                'final': states[-1]}
    (host / 'expected.json').write_text(json.dumps(expected, indent=2) + '\n')
    manifest = {'scope': 'existing-SQL updating COUNT to ordered ARRAY_AGG then array_slice',
                'query_template_sha256': hashlib.sha256(QUERY.encode()).hexdigest(),
                'expected_sha256': digest(host / 'expected.json'), 'cases': {}}
    for backend, mode, batch in MATRIX:
        name = f'{backend}-{mode}-batch{batch}'
        directory, runtime_case = host / name, runtime / name
        directory.mkdir(exist_ok=True)
        (directory / 'input.jsonl').write_text(''.join(
            json.dumps(row, ensure_ascii=False) + '\n' for row in source))
        (directory / 'query.sql').write_text(render_query(runtime_case))
        manifest['cases'][name] = {
            'backend': backend, 'checkpoint_mode': mode, 'source_batch_rows': batch,
            'input_sha256': digest(directory / 'input.jsonl'),
            'query_sha256': digest(directory / 'query.sql'),
            'env': case_env(runtime_case, backend, mode, batch),
        }
    (host / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return {'cases': len(manifest['cases']), 'source_events': len(source),
            'checkpoint': expected['checkpoint'], 'final': expected['final'],
            'status': 'prepared; runtime unverified'}


def strict_row(row, allowed):
    if (type(row) is not dict or set(row) != FIELDS or type(row['k']) is not str
            or row['k'] not in allowed or type(row['top_items']) is not list
            or not 1 <= len(row['top_items']) <= 5):
        raise ValueError(f'wrong typed top-five row: {row!r}')
    for member in row['top_items']:
        if (type(member) is not dict or set(member) != {'item_key', 'n'}
                or type(member['item_key']) is not str or type(member['n']) is not int
                or member['n'] < 1):
            raise ValueError(f'wrong typed top-five member: {member!r}')
    if canonical(row) not in allowed[row['k']]:
        raise ValueError(f'top-five row is not a source-prefix value: {row!r}')
    return row['k']


def reduce_cdc(path, allowed, states):
    current, positions, snapshots = {}, {}, []
    for entry in arrays.read_jsonl(path):
        payload = entry['payload'] if set(entry) == {'payload'} else entry
        if type(payload) is not dict or set(payload) != {'before', 'after', 'op'}:
            raise ValueError(f'{path}: CDC envelope differs')
        before, after, op = payload['before'], payload['after'], payload['op']
        if (op not in ('c', 'u', 'd') or (before is None) != (op == 'c')
                or (after is None) != (op == 'd')):
            raise ValueError(f'{path}: CDC operation/images differ')
        key = strict_row(after if after is not None else before, allowed)
        if before is not None and strict_row(before, allowed) != key:
            raise ValueError(f'{path}: CDC before key differs')
        # An outer ARRAY_AGG may emit an update whose projected top five is
        # unchanged when a count outside the slice changes.
        if canonical(before) != canonical(current.get(key)):
            raise ValueError(f'{path}: CDC before continuity differs')
        if op != 'u' or canonical(before) != canonical(after):
            candidates = [index for index, state in enumerate(states)
                          if index > positions.get(key, -1)
                          and canonical(state.get(key)) == canonical(after)]
            if not candidates:
                raise ValueError(f'{path}: CDC state is not a forward source-prefix value')
            positions[key] = candidates[0]
        if after is None:
            del current[key]
        else:
            current[key] = after
        snapshots.append(dict(current))
    return snapshots


def compare(host):
    host = host.resolve()
    manifest, expected = arrays.read_json(host / 'manifest.json'), arrays.read_json(host / 'expected.json')
    if (manifest['query_template_sha256'] != hashlib.sha256(QUERY.encode()).hexdigest()
            or manifest['expected_sha256'] != digest(host / 'expected.json')):
        raise ValueError('query/oracle inventory hash changed')
    names = {f'{backend}-{mode}-batch{batch}' for backend, mode, batch in MATRIX}
    if set(manifest['cases']) != names:
        raise ValueError('case matrix differs')
    states = prefix_states(arrays.read_jsonl(host / next(iter(names)) / 'input.jsonl'))
    actual_expected = {'source_events': 23, 'checkpoint_after_events': CHECKPOINT,
                       'prefix_states': states, 'checkpoint': states[CHECKPOINT - 1],
                       'final': states[-1]}
    if canonical(expected) != canonical(actual_expected):
        raise ValueError('expected prefix oracle differs from source CDC')
    allowed = {key: set() for snapshot in states for key in snapshot}
    for snapshot in states:
        for key, row in snapshot.items():
            allowed[key].add(canonical(row))
    result = {}
    for backend, mode, batch in MATRIX:
        name = f'{backend}-{mode}-batch{batch}'
        directory, case = host / name, manifest['cases'][name]
        env = case['env']
        runtime_case = Path(env['STREAMR_CAPTURE_QUERY']).parent
        if (set(case) != {'backend', 'checkpoint_mode', 'source_batch_rows',
                          'input_sha256', 'query_sha256', 'env'}
                or (case['backend'], case['checkpoint_mode'], case['source_batch_rows'])
                   != (backend, mode, batch)
                or not runtime_case.is_absolute()
                or env != case_env(runtime_case, backend, mode, batch)
                or digest(directory / 'input.jsonl') != case['input_sha256']
                or digest(directory / 'query.sql') != case['query_sha256']
                or canonical(arrays.read_jsonl(directory / 'input.jsonl')) != canonical(events())
                or (directory / 'query.sql').read_text() != render_query(runtime_case)):
            raise ValueError(f'{name}: prepared case differs')
        log = (directory / 'capture.log').read_text()
        initial_markers = re.findall(
            r'^CAPTURE_RESULT phase=initial rows=(\d+) path=(.+)$', log, re.MULTILINE)
        recovered_markers = re.findall(
            r'^CAPTURE_RESULT phase=recovered checkpoint=(\d+) '
            r'input_rows_before_checkpoint=(\d+) committed_rows=(\d+) '
            r'rows=(\d+) bytes=(\d+) path=(.+) job=(\S+)$', log, re.MULTILINE)
        passes = re.findall(r'^test result: ok\. 1 passed; 0 failed;',
                            log, re.MULTILINE)
        if len(initial_markers) != 1 or len(recovered_markers) != 1 or len(passes) != 1:
            raise ValueError(f'{name}: generic capture did not pass')
        initial_count, initial_path = initial_markers[0]
        checkpoint, prefix, committed, recovered_count, _, output_path, _ = recovered_markers[0]
        committed, recovered_count = int(committed), int(recovered_count)
        if (checkpoint, prefix, initial_path, output_path) != (
                '1', str(CHECKPOINT), str(runtime_case / 'output.initial.jsonl'),
                str(runtime_case / 'output.jsonl')):
            raise ValueError(f'{name}: capture phase, checkpoint or output path differs')
        initial = reduce_cdc(directory / 'output.initial.jsonl', allowed, states)
        recovered = reduce_cdc(directory / 'output.jsonl', allowed, states)
        if (not initial or len(initial) != int(initial_count)
                or committed < 1 or committed > len(recovered)
                or len(recovered) != recovered_count
                or canonical(initial[-1]) != canonical(states[-1])
                or canonical(recovered[committed - 1]) != canonical(states[CHECKPOINT - 1])
                or canonical(recovered[-1]) != canonical(states[-1])):
            raise ValueError(f'{name}: initial/checkpoint/recovered top-five values differ')
        result[name] = {'initial_rows': len(initial), 'committed_rows': committed,
                        'recovered_rows': len(recovered)}
    return {'status': 'pass', 'scope': manifest['scope'], 'cases': result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host_directory', type=Path)
    parser.add_argument('--runtime-directory', type=Path)
    parser.add_argument('--compare', action='store_true')
    args = parser.parse_args()
    if args.compare:
        result = compare(args.host_directory)
    else:
        if args.runtime_directory is None:
            parser.error('--runtime-directory is required to prepare')
        result = prepare(args.host_directory, args.runtime_directory)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
