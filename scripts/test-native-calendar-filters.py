#!/usr/bin/env python3
"""Execute generic maintained calendar SQL against a caller-supplied test binary.

No Rust builds are performed. --prepare-only writes input, SQL and independent
oracles; --self-test checks the comparator without claiming engine execution.
The runtime matrix covers memory/RocksDB, source batches 1/8 and both checkpoint
coordinators. Every run compares uninterrupted and fresh-worker-restored output.
Quiet-key scheduling, pruning/capacity failures and replay injection need their
own harness; this runner does not claim those acceptance checks.

Default mode has 25 calendar plus 5 lifetime outputs and includes 14-day coverage.
--catalog-shapes instead has 25 TOTAL outputs: all-row counts 1/7/30/90/lifetime
(5), three enum-gated count families 7/30/lifetime (9), one OR/static-gated
family 7/30/lifetime (3), independently selected-date counts 1/7/30/lifetime
(4), and independently selected-date COALESCE SUM 1/7/30/lifetime (4).
The latter reproduces generic AST shapes, not external catalog SQL or parity;
source-bound external fixtures remain required for exact catalog integration.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

HORIZONS = (1, 7, 14, 30, 90)
METRICS = ('rows', 'nonnull', 'total', 'eligible_rows', 'eligible_total')
DAY = dt.date(2026, 10, 10)
FIELDS = ('tenant', 'group_id') + tuple(f'{metric}_{days}d' for metric in METRICS for days in HORIZONS) + tuple(f'{metric}_lifetime' for metric in METRICS)

# Generic counterparts of catalog AST shapes, deliberately without application
# field names or classifications. The 25 outputs INCLUDE lifetime outputs:
# all rows 5; three enum equality families 9; OR gate family 3;
# independent-date counts 4; COALESCE SUM independent-date family 4.
# These cover expression shapes only; exact external catalog SQL/parity remains
# acceptance work requiring caller-provided source-bound application fixtures.
CATALOG_SHAPES = False
SHAPE_OUTPUTS = []
for family, horizons, gate, date_field, total in [
    ('rows', (1, 7, 30, 90, None), None, 'contribution_time', False),
    ('enum_a', (7, 30, None), "category = 'a'", 'contribution_time', False),
    ('enum_b', (7, 30, None), "category = 'b'", 'contribution_time', False),
    ('enum_c', (7, 30, None), "category = 'c'", 'contribution_time', False),
    # The pinned SQL parser parses IS DISTINCT FROM's RHS as a full expression.
    # Group each Boolean conjunct so a following AND cannot enter its RHS.
    ('or_gate', (7, 30, None), "(category = 'a' OR category = 'b') AND (subtype IS DISTINCT FROM 'blocked') AND (gate_tag NOT IN ('excluded', 'ignored'))", 'contribution_time', False),
    ('alternate_rows', (1, 7, 30, None), None, 'alternate_time', False),
    ('alternate_total', (1, 7, 30, None), None, 'alternate_time', True),
]:
    for horizon in horizons:
        name = f'{family}_{horizon}d' if horizon is not None else f'{family}_lifetime'
        SHAPE_OUTPUTS.append((name, horizon, gate, date_field, total))


def configure_shapes(enabled):
    global CATALOG_SHAPES, FIELDS
    CATALOG_SHAPES = enabled
    FIELDS = ('tenant', 'group_id') + (tuple(value[0] for value in SHAPE_OUTPUTS) if enabled else
        tuple(f'{metric}_{days}d' for metric in METRICS for days in HORIZONS) + tuple(f'{metric}_lifetime' for metric in METRICS))


def shape_eligible(row, name):
    if name.startswith('enum_'):
        return row['category'] == name[5]
    if name.startswith('or_gate'):
        # SQL IS DISTINCT FROM accepts NULL; NOT IN is safe because gate_tag
        # is declared nonnull in this generic fixture.
        return row['category'] in ('a', 'b') and row['subtype'] != 'blocked' and row['gate_tag'] not in ('excluded', 'ignored')
    return True


def shape_values(rows, reference):
    result = {}
    for name, horizon, _, date_field, total in SHAPE_OUTPUTS:
        qualifying = [row for row in rows if shape_eligible(row, name) and
                      (horizon is None or reference - dt.timedelta(days=horizon - 1) <= utc_date(row[date_field]) <= reference)]
        amounts = [row['amount'] for row in qualifying if row['amount'] is not None]
        result[name] = sum(amounts) if total else len(qualifying)
    return result


def utc_date(value):
    stamp = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    return stamp.astimezone(dt.timezone.utc).date()


def event(row_id, tenant, offset, amount, eligible, reference='2026-10-09T23:59:59Z'):
    return dict(row_id=row_id, tenant=tenant, group_id=7,
                contribution_time=f'{DAY - dt.timedelta(days=offset)}T12:00:00Z',
                reference_time=reference, completeness_time=reference,
                amount=amount, eligible=eligible,
                category=('a', 'b', 'c')[row_id % 3],
                subtype=None if row_id % 4 == 0 else 'blocked' if row_id % 4 == 1 else 'open',
                gate_tag='excluded' if row_id % 5 == 3 else 'ignored' if row_id % 5 == 4 else 'include',
                alternate_time=f'{DAY - dt.timedelta(days=offset + 1)}T23:59:58Z')


def fixtures():
    # Inclusive/exclusive edges of every supported horizon, a future date,
    # duplicate contribution days, null values and disqualified source rows.
    offsets = (0, 6, 7, 13, 14, 29, 30, 89, 90, -1, 0, 6, 29, 89, 1, 2)
    append = []
    for tenant_index, tenant in enumerate(('a', 'b')):
        for index, offset in enumerate(offsets):
            reference = '2026-10-10T00:00:03Z' if index == 15 else '2026-10-09T23:59:59Z'
            append.append(event(tenant_index * 100 + index, tenant, offset,
                                None if index % 4 == 0 else (index + 1) * (tenant_index + 1),
                                index % 3 != 0, reference))
    # Both tenants end the checkpoint at the new day, despite AS progress still
    # being on the previous date. The final triggers move back to that older day.
    append += [event(300 + index, tenant, 0, None, False)
               for index, tenant in enumerate(('a', 'b'))]

    first = event(1, 'a', 0, 5, True, '2026-10-10T00:00:03Z')
    anchor = event(2, 'a', 6, None, False, '2026-10-10T00:00:03Z')
    other = event(3, 'b', -1, 7, True, '2026-10-10T00:00:03Z')
    corrected = dict(first, amount=11)
    moved = dict(corrected, contribution_time='2026-10-03T12:00:00Z')
    moved_alternate = dict(moved, alternate_time='2026-10-03T00:00:01Z')
    nullable = dict(other, amount=None)
    nonnull_again = dict(nullable, amount=13)
    gated = dict(anchor, eligible=True, amount=3, category='a', subtype=None,
                 reference_time='2026-10-09T23:59:59Z', completeness_time='2026-10-09T23:59:59Z')
    cdc = [(None, first), (None, anchor), (None, other), (first, corrected),
           (corrected, moved), (moved, moved_alternate), (other, nullable),
           (nullable, nonnull_again), (moved_alternate, None), (anchor, gated)]
    envelopes = [dict(before=before, after=after, op='c' if before is None else 'd' if after is None else 'u', ts_ms=(index + 1) * 1000)
                 for index, (before, after) in enumerate(cdc)]
    return {'append': (append, 32, False), 'cdc': (envelopes, 4, True)}


def metric_values(rows):
    amounts = [row['amount'] for row in rows if row['amount'] is not None]
    eligible = [row for row in rows if row['eligible']]
    eligible_amounts = [row['amount'] for row in eligible if row['amount'] is not None]
    return dict(rows=len(rows), nonnull=len(amounts), total=sum(amounts) if amounts else None,
                eligible_rows=len(eligible), eligible_total=sum(eligible_amounts) if eligible_amounts else None)


def snapshot(current, references):
    result = {}
    for key, reference in references.items():
        rows = [row for row in current.values() if (row['tenant'], row['group_id']) == key]
        output = dict(tenant=key[0], group_id=key[1])
        if CATALOG_SHAPES:
            output.update(shape_values(rows, reference))
        else:
            for days in HORIZONS:
                first_day = reference - dt.timedelta(days=days - 1)
                recent = [row for row in rows if first_day <= utc_date(row['contribution_time']) <= reference]
                for metric, value in metric_values(recent).items():
                    output[f'{metric}_{days}d'] = value
            for metric, value in metric_values(rows).items():
                output[f'{metric}_lifetime'] = value
        result[key] = output
    return result


def oracle(inputs, updating):
    """Recompute from current source rows after each signed contribution image."""
    current, references, snapshots, envelope_ends = {}, {}, [], []
    for record in inputs:
        images = [(-1, record['before']), (1, record['after'])] if updating else [(1, record)]
        trigger = (record['after'] or record['before']) if updating else record
        for sign, row in images:
            if row is None:
                continue
            identity = row['row_id']
            if sign < 0:
                if current.get(identity) != row:
                    raise ValueError(f'fixture CDC before image differs for {identity}')
                del current[identity]
            else:
                if identity in current:
                    raise ValueError(f'fixture duplicate identity {identity}')
                current[identity] = dict(row)
            # Both images belong to the same current source change; the
            # original payload stays intact while its trigger uses the final
            # image's declared clock.
            references[(row['tenant'], row['group_id'])] = utc_date(trigger['reference_time'])
            snapshots.append(snapshot(current, references))
        envelope_ends.append(len(snapshots))
    return snapshots, envelope_ends


def query(directory, updating):
    fmt = 'debezium_json' if updating else 'json'
    primary = ' PRIMARY KEY' if updating else ''
    expressions = []
    arguments = {'rows': 'COUNT(*)', 'nonnull': 'COUNT(amount)', 'total': 'SUM(amount)',
                 'eligible_rows': 'COUNT(*)', 'eligible_total': 'SUM(amount)'}
    if CATALOG_SHAPES:
        for name, horizon, gate, date_field, total in SHAPE_OUTPUTS:
            argument = 'SUM(amount)' if total else 'COUNT(*)'
            clauses = [gate] if gate else []
            if horizon is not None:
                temporal = f'CAST({date_field} AS DATE) = WATERMARK_DATE()' if horizon == 1 else f"CAST({date_field} AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '{horizon - 1}' DAY AND WATERMARK_DATE()"
                clauses.append(temporal)
            if clauses:
                argument += ' FILTER (WHERE ' + ' AND '.join(f'({clause})' for clause in clauses) + ')'
            if total:
                argument = f'COALESCE({argument}, 0)'
            expressions.append(f'{argument} AS {name}')
    else:
        for metric in METRICS:
            for days in HORIZONS:
                temporal = 'CAST(contribution_time AS DATE) = WATERMARK_DATE()' if days == 1 else f"CAST(contribution_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '{days - 1}' DAY AND WATERMARK_DATE()"
                gate = 'eligible AND ' if metric.startswith('eligible_') else ''
                expressions.append(f'{arguments[metric]} FILTER (WHERE {gate}({temporal})) AS {metric}_{days}d')
        for metric in METRICS:
            gate = ' FILTER (WHERE eligible)' if metric.startswith('eligible_') else ''
            expressions.append(f'{arguments[metric]}{gate} AS {metric}_lifetime')
    output_fields = ',\n '.join(f'{field} BIGINT' for field in FIELDS if field not in ('tenant', 'group_id'))
    return f"""SET updating_ttl = NULL;
CREATE TABLE calendar_input (row_id BIGINT{primary}, tenant TEXT NOT NULL, group_id BIGINT NOT NULL,
 contribution_time TIMESTAMP NOT NULL, reference_time TIMESTAMP NOT NULL,
 completeness_time TIMESTAMP NOT NULL, amount BIGINT, eligible BOOLEAN NOT NULL,
 category TEXT NOT NULL, subtype TEXT, gate_tag TEXT NOT NULL, alternate_time TIMESTAMP NOT NULL,
 WATERMARK FOR reference_time AS completeness_time - INTERVAL '5' SECOND)
WITH (connector='single_file', path='{directory}/input.jsonl', format='{fmt}', type='source', wait_for_control='true');
CREATE TABLE calendar_output (tenant TEXT, group_id BIGINT,
 {output_fields})
WITH (connector='single_file', path='{directory}/output.jsonl', format='debezium_json', type='sink');
INSERT INTO calendar_output SELECT tenant, group_id,
 {', '.join(expressions)} FROM calendar_input GROUP BY tenant, group_id;
"""


def json_snapshot(value):
    return [value[key] for key in sorted(value)]


def prepare(directory):
    cases = []
    for name, (inputs, checkpoint, updating) in fixtures().items():
        snapshots, ends = oracle(inputs, updating)
        for backend in ('memory', 'rocksdb'):
            for batch in (1, 8):
                for mode in ('controller', 'leader'):
                    path = directory / f'{"catalog-shapes-" if CATALOG_SHAPES else ""}{name}-{backend}-batch{batch}-{mode}'
                    path.mkdir(parents=True, exist_ok=True)
                    (path / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in inputs))
                    (path / 'query.sql').write_text(query(path, updating))
                    (path / 'expected.prefixes.json').write_text(json.dumps([json_snapshot(value) for value in snapshots], indent=2) + '\n')
                    checkpoint_steps = ends[checkpoint - 1]
                    (path / 'expected.checkpoint.json').write_text(json.dumps(json_snapshot(snapshots[checkpoint_steps - 1]), indent=2) + '\n')
                    (path / 'expected.final.json').write_text(json.dumps(json_snapshot(snapshots[-1]), indent=2) + '\n')
                    if batch == 1:
                        if name == 'append':
                            # NoOp releases authorize source reads; they do not
                            # acknowledge completion through RocksDB and sink
                            # flushing. The first bulk release therefore gets a
                            # conservative 20-second settle before exact values
                            # are observed. No engine throughput SLA is implied.
                            observations = [(30000, 32), (35000, 33), (40000, 34)]
                            advances = [(10000, 32), (31000, 33), (36000, 34)]
                        else:
                            # Each CDC envelope gets five seconds for both
                            # images, state writes and the aggregate flush.
                            # All ten exact live prefix checks finish at 64s,
                            # within the existing 120s harness schedule limit.
                            observations = [(10000, 1)] + [(16000 + (index - 2) * 6000, index) for index in range(2, len(inputs) + 1)]
                            advances = [(11000 + (index - 2) * 6000, index) for index in range(2, len(inputs) + 1)]
                        steps = [dict(kind='advance', at_ms=at_ms, source_row_target=count) for at_ms, count in advances]
                        steps += [dict(kind='observe', at_ms=at_ms, label=f'prefix{count}') for at_ms, count in observations]
                        steps.sort(key=lambda step: step['at_ms'])
                        (path / 'schedule.json').write_text(json.dumps(dict(max_output_bytes=1048576, steps=steps), indent=2) + '\n')
                        (path / 'expected.observations.json').write_text(json.dumps({f'prefix{count}': ends[count - 1] for _, count in observations}, indent=2) + '\n')
                    cases.append((path, backend, batch, mode, checkpoint, snapshots, checkpoint_steps))
    return cases


def reduce_records(records, snapshots, target):
    """Check every CDC before image and every after value; permit batch coalescing."""
    current, positions = {}, {}
    for record in records:
        row = record.get('payload', record)
        if not {'before', 'after', 'op'} <= set(row):
            raise ValueError(f'missing CDC envelope: {row}')
        before, after, op = row['before'], row['after'], row['op']
        if op not in ('c', 'u', 'd') or (before is None) != (op == 'c') or (after is None) != (op == 'd'):
            raise ValueError(f'invalid CDC operation/images: {row}')
        for value in (before, after):
            if value is not None:
                if set(value) != set(FIELDS):
                    raise ValueError(f'output schema differs: {value}')
                if not isinstance(value['tenant'], str) or type(value['group_id']) is not int:
                    raise ValueError(f'group key type differs: {value}')
                for field in FIELDS[2:]:
                    if value[field] is not None and type(value[field]) is not int:
                        raise ValueError(f'aggregate type differs for {field}: {value[field]!r}')
                    if 'total' not in field and value[field] is None:
                        raise ValueError(f'COUNT unexpectedly null for {field}')
        image = after if after is not None else before
        key = image['tenant'], image['group_id']
        if current.get(key) != before:
            raise ValueError(f'CDC continuity differs for {key}: {before} != {current.get(key)}')
        if after is None:
            del current[key]
        else:
            candidates = [index for index, value in enumerate(snapshots)
                          if index >= positions.get(key, -1) and value.get(key) == after]
            if not candidates:
                raise ValueError(f'output is not an independent forward input-prefix result: {after}')
            positions[key] = candidates[0]
            current[key] = after
    if current != target:
        raise ValueError(f'final aggregate differs: actual={current}, expected={target}')
    return current


def read_records(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def execute(binary, cases):
    for path, backend, batch, mode, checkpoint, snapshots, checkpoint_steps in cases:
        environment = dict(os.environ, STREAMR_TEST_NATIVE_AGGREGATES='1',
                           STREAMR_TEST_BACKEND=backend, STREAMR_TEST_CHECKPOINT_MODE=mode,
                           STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch), STREAMR_TEST_AGGREGATE_FLUSH_SECONDS='3600',
                           STREAMR_TEST_EXECUTION_BYTES='16777216', STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS='120', STREAMR_CAPTURE_QUERY=str(path / 'query.sql'),
                           STREAMR_CAPTURE_OUTPUT=str(path / 'output.jsonl'),
                           STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(checkpoint),
                           STREAMR_CAPTURE_CHECKPOINT_EPOCH='1', STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS='2',
                           STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='2', STREAMR_CAPTURE_EXPECTED_ROWS='2',
                           STREAMR_CAPTURE_MAX_INITIAL_ROWS='192', STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS='192',
                           STREAMR_CAPTURE_MAX_ROWS='192')
        # Batch-1 initial execution deliberately pauses source controls across
        # aggregate flushes. Validate every CDC correction and both raw midnight
        # references while the worker is live, before EOF or checkpoint flushing.
        if batch == 1:
            environment['STREAMR_CAPTURE_INITIAL_SCHEDULE'] = str(path / 'schedule.json')
            environment['STREAMR_TEST_AGGREGATE_FLUSH_SECONDS'] = '1'
        else:
            environment.pop('STREAMR_CAPTURE_INITIAL_SCHEDULE', None)
        log_path = path / 'capture.log'
        with log_path.open('w') as log:
            result = subprocess.run([str(binary), 'external_sql_checkpoint_capture', '--ignored', '--test-threads=1', '--nocapture'],
                                    env=environment, stdout=log, stderr=subprocess.STDOUT)
        log = log_path.read_text()
        if result.returncode or '1 passed' not in log:
            raise RuntimeError(f'{path.name}: engine capture failed ({result.returncode}); inspect {log_path}')
        markers = re.findall(r'^CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=(\d+) committed_rows=(\d+) rows=(\d+) bytes=\d+ path=(.+) job=\S+$', log, re.MULTILINE)
        if len(markers) != 1:
            raise ValueError(f'{path.name}: missing unique fresh-worker recovery receipt')
        captured_checkpoint, committed, captured, output_path = markers[0]
        if int(captured_checkpoint) != checkpoint or output_path != str(path / 'output.jsonl'):
            raise ValueError(f'{path.name}: recovery receipt differs')
        initial = read_records(path / 'output.initial.jsonl')
        restored = read_records(path / 'output.jsonl')
        committed, captured = int(committed), int(captured)
        if not 2 <= committed <= len(restored) == captured <= 192:
            raise ValueError(f'{path.name}: committed/captured row bounds differ')
        if batch == 1:
            observations = json.loads((path / 'expected.observations.json').read_text())
            for label, end in observations.items():
                records = read_records(path / f'output.schedule-initial-{label}.jsonl')
                reduce_records(records, snapshots[:end], snapshots[end - 1])
        reduce_records(initial, snapshots, snapshots[-1])
        reduce_records(restored[:committed], snapshots[:checkpoint_steps], snapshots[checkpoint_steps - 1])
        reduce_records(restored, snapshots, snapshots[-1])
        coverage_label = '25 total catalog AST-shape' if CATALOG_SHAPES else '25 calendar plus 5 lifetime'
        print(f'PASS {path.name}: generic {coverage_label} outputs, every CDC image, live batch-1 prefixes, checkpoint prefix and fresh-worker restore', flush=True)


def synthetic_records(snapshots):
    current, records = {}, []
    for snapshot_value in snapshots:
        for key, after in snapshot_value.items():
            before = current.get(key)
            if before != after:
                records.append(dict(before=before, after=after, op='c' if before is None else 'u'))
                current[key] = after
    return records


def self_test():
    if CATALOG_SHAPES:
        qualifying = dict(category='a', subtype=None, gate_tag='include')
        assert shape_eligible(qualifying, 'or_gate_7d')
        assert shape_eligible(dict(qualifying, category='b'), 'or_gate_7d')
        assert not shape_eligible(dict(qualifying, subtype='blocked'), 'or_gate_7d')
        assert not shape_eligible(dict(qualifying, gate_tag='excluded'), 'or_gate_7d')
        assert not shape_eligible(dict(qualifying, gate_tag='ignored'), 'or_gate_7d')
    for name, (inputs, checkpoint, updating) in fixtures().items():
        snapshots, ends = oracle(inputs, updating)
        if CATALOG_SHAPES and name == 'cdc':
            assert snapshots[0][('a', 7)]['rows_1d'] == 1
            assert snapshots[0][('a', 7)]['alternate_rows_1d'] == 0
            assert snapshots[0][('a', 7)]['alternate_total_1d'] == 0
            assert snapshots[0][('a', 7)]['or_gate_7d'] == 0
            assert snapshots[0][('a', 7)]['enum_b_7d'] == 1
        if name == 'cdc':
            # COUNT(value) and SUM must react in both null directions without
            # treating the same stable row identity as an extra source row.
            null_image = snapshots[ends[6] - 1][('b', 7)]
            restored_value = snapshots[ends[7] - 1][('b', 7)]
            if CATALOG_SHAPES:
                assert null_image['alternate_total_1d'] == 0
                assert restored_value['alternate_total_1d'] == 13
                contribution_move = snapshots[ends[4] - 1][('a', 7)]
                alternate_move = snapshots[ends[5] - 1][('a', 7)]
                assert contribution_move['alternate_rows_7d'] == alternate_move['alternate_rows_7d'] + 1
                assert contribution_move['rows_7d'] == alternate_move['rows_7d']
                assert snapshots[-1][('a', 7)]['or_gate_lifetime'] == 1
            else:
                assert null_image['nonnull_lifetime'] == 0
                assert null_image['total_lifetime'] is None
                assert restored_value['nonnull_lifetime'] == 1
                assert restored_value['total_lifetime'] == 13
                assert null_image['rows_lifetime'] == restored_value['rows_lifetime'] == 1
        records = synthetic_records(snapshots)
        reduce_records(records, snapshots, snapshots[-1])
        prefix = snapshots[:ends[checkpoint - 1]]
        reduce_records(synthetic_records(prefix), prefix, prefix[-1])
        altered = json.loads(json.dumps(records))
        altered[-1]['after']['rows_7d'] += 1
        try:
            reduce_records(altered, snapshots, snapshots[-1])
        except ValueError:
            pass
        else:
            raise AssertionError('corrupted calendar count accepted')
        altered = json.loads(json.dumps(records))
        replacement = next(row for row in altered if row['before'] is not None)
        replacement['before']['rows_lifetime'] += 1
        try:
            reduce_records(altered, snapshots, snapshots[-1])
        except ValueError:
            pass
        else:
            raise AssertionError('corrupted CDC before image accepted')
        if name == 'append':
            # Prove the independently calculated fixtures discriminate raw
            # previous-day references from a monotonic maximum reference.
            assert prefix[-1][('a', 7)]['rows_1d'] != snapshots[-1][('a', 7)]['rows_1d']
            assert prefix[-1][('a', 7)]['rows_lifetime'] + 1 == snapshots[-1][('a', 7)]['rows_lifetime']
    with tempfile.TemporaryDirectory(prefix='calendar-oracle-self-test-') as directory:
        cases = prepare(Path(directory))
        assert len(cases) == 16
        assert all((case[0] / 'query.sql').read_text().count('WATERMARK_DATE()') == (33 if CATALOG_SHAPES else 45) for case in cases)
        if CATALOG_SHAPES:
            assert all("AND (subtype IS DISTINCT FROM 'blocked') AND (gate_tag NOT IN ('excluded', 'ignored'))" in (case[0] / 'query.sql').read_text() for case in cases)
        for path, _, batch, _, _, _, _ in cases:
            if batch == 1:
                schedule = json.loads((path / 'schedule.json').read_text())
                assert len(schedule['steps']) <= 64
                assert schedule['steps'][-1]['kind'] == 'observe'
                assert schedule['steps'][-1]['at_ms'] < 120000
                if path.name.startswith(('append-', 'catalog-shapes-append-')):
                    first_release, first_observe = schedule['steps'][:2]
                    assert first_release['source_row_target'] == 32
                    assert first_observe['at_ms'] - first_release['at_ms'] >= 20000
                else:
                    assert schedule['steps'][-1]['at_ms'] == 64000
    print('PASS independent fixture/comparator self-test; engine execution not run', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path, nargs='?', help='current arroyo-sql-testing test executable')
    parser.add_argument('--directory', type=Path, default=Path('/app/target/native-calendar-filters'))
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--catalog-shapes', action='store_true', help='25 total generic catalog AST shapes including lifetime; does not establish external catalog parity')
    args = parser.parse_args()
    configure_shapes(args.catalog_shapes)
    if args.self_test:
        self_test()
        return
    if not args.prepare_only and args.binary is None:
        parser.error('binary is required unless --prepare-only or --self-test')
    cases = prepare(args.directory.resolve())
    print(f'Prepared {len(cases)} actual SQL fixtures; engine execution not run', flush=True)
    if not args.prepare_only:
        execute(args.binary.resolve(strict=True), cases)


if __name__ == '__main__':
    main()
