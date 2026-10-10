#!/usr/bin/env python3
"""Actual SQL admission/pruning corrections, with an independent current-row oracle.

Uses the calendar capture/query/comparator infrastructure, never engine pruning
helpers. Eight-envelope phases ensure batches 1 and 8 see the same real frontier.
Native persisted bucket deletion/cursor inspection is covered by worker tests.
"""
import argparse
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('calendar_oracle', Path(__file__).with_name('test-native-calendar-filters.py'))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


def stamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def fixtures():
    old_clock = '2026-07-10T12:00:10Z'
    new_clock = '2026-10-10T12:00:10Z'
    old = [base.event(i, 'a' if i <= 4 else 'b', 92, 2, True, old_clock) for i in range(1, 9)]
    old[0].update(contribution_time='2026-07-10T12:00:00Z', amount=3)
    old[1].update(contribution_time='2026-07-13T12:00:00Z', amount=5)
    old[2].update(contribution_time='2026-10-11T12:00:00Z', amount=7)
    old[3].update(contribution_time='2026-07-12T12:00:00Z', amount=None)
    progress = [base.event(100 + i, 'b', 0, 1, True, new_clock) for i in range(8)]
    corrected = dict(old[0], contribution_time='2026-10-10T12:00:00Z', amount=11,
                     reference_time='2026-10-10T12:00:11Z', completeness_time='2026-10-10T12:00:11Z')
    moved = dict(old[1], tenant='b', reference_time='2026-10-10T12:00:11Z', completeness_time='2026-10-10T12:00:11Z')
    late_update = dict(corrected, amount=99, reference_time='2026-10-10T12:00:04Z', completeness_time='2026-10-10T12:00:04Z')
    suffix = [(old[0], corrected), (old[1], moved), (old[2], None), (corrected, late_update),
              (corrected, None), (None, base.event(900, 'a', 0, 99, True, '2026-10-10T12:00:04Z')),
              (None, base.event(901, 'a', 0, 13, True, '2026-10-10T12:00:05Z')),
              (None, base.event(902, 'a', -1, 17, True, '2026-10-10T12:00:11Z'))]
    changes = [(None, row) for row in old + progress] + suffix
    return [dict(before=before, after=after, op='c' if before is None else 'd' if after is None else 'u', ts_ms=(i + 1) * 1000)
            for i, (before, after) in enumerate(changes)]


def oracle(inputs, batch):
    current, references, snapshots, ends = {}, {}, [], []
    emitted, candidate, cadence = None, None, None
    dropped = []
    for offset in range(0, len(inputs), batch):
        accepted_clocks, accepted_progress = [], []
        for index, change in enumerate(inputs[offset:offset + batch], offset + 1):
            trigger = change['after'] or change['before']
            clock = stamp(trigger['reference_time'])
            if emitted is not None and clock < emitted:
                dropped.append(index)
                snapshots.append(base.snapshot(current, references))
                ends.append(len(snapshots))
                continue
            accepted_clocks.append(clock)
            accepted_progress.append(stamp(trigger['completeness_time']) - dt.timedelta(seconds=5))
            for sign, row in ((-1, change['before']), (1, change['after'])):
                if row is None:
                    continue
                identity = row['row_id']
                if sign < 0:
                    if current.get(identity) != row:
                        raise ValueError(f'original CDC image mismatch: {identity}')
                    del current[identity]
                else:
                    if identity in current:
                        raise ValueError(f'duplicate current row: {identity}')
                    current[identity] = dict(row)
                references[(row['tenant'], row['group_id'])] = clock.date()
                snapshots.append(base.snapshot(current, references))
            ends.append(len(snapshots))
        if accepted_clocks:
            progress = min(accepted_progress)
            candidate = progress if candidate is None else max(candidate, progress)
            latest = max(accepted_clocks)
            if cadence is None or latest - cadence > dt.timedelta(seconds=1):
                emitted, cadence = candidate, latest
    if dropped != [19, 20, 22]:
        raise ValueError(f'fixture failed to establish common late boundary: {dropped}')
    return snapshots, ends


def prepare(directory):
    cases = []
    inputs = fixtures()
    for backend in ('memory', 'rocksdb'):
        for batch in (1, 8):
            snapshots, ends = oracle(inputs, batch)
            for mode in ('controller', 'leader'):
                path = directory / f'pruning-cdc-{backend}-batch{batch}-{mode}'
                path.mkdir(parents=True, exist_ok=True)
                (path / 'input.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in inputs))
                (path / 'query.sql').write_text(base.query(path, True))
                for name, value in [('prefixes', [base.json_snapshot(s) for s in snapshots]),
                                    ('checkpoint', base.json_snapshot(snapshots[ends[15] - 1])),
                                    ('final', base.json_snapshot(snapshots[-1]))]:
                    (path / f'expected.{name}.json').write_text(json.dumps(value, indent=2) + '\n')
                if batch == 1:
                    # Exact observations after old state, real progress, old-row
                    # replacement, cross-group move, late no-ops, deletion/equality.
                    counts = (8, 16, 17, 18, 20, 21, 23, 24)
                    steps, observations = [], {}
                    for position, count in enumerate(counts):
                        release = 10000 + position * 10000
                        steps.append(dict(kind='advance', at_ms=release, source_row_target=count))
                        steps.append(dict(kind='observe', at_ms=release + 8000, label=f'prefix{count}'))
                        observations[f'prefix{count}'] = ends[count - 1]
                    (path / 'schedule.json').write_text(json.dumps(dict(max_output_bytes=1048576, steps=steps), indent=2) + '\n')
                    (path / 'expected.observations.json').write_text(json.dumps(observations, indent=2) + '\n')
                cases.append((path, backend, batch, mode, 16, snapshots, ends[15]))
    return cases


def self_test():
    for batch in (1, 8):
        snapshots, ends = oracle(fixtures(), batch)
        final = snapshots[-1]
        assert final[('a', 7)]['rows_lifetime'] == 4
        assert final[('a', 7)]['total_lifetime'] == 37
        assert final[('a', 7)]['rows_90d'] == 1
        assert final[('a', 7)]['total_90d'] == 13
        assert final[('b', 7)]['rows_lifetime'] == 13
        assert final[('b', 7)]['total_lifetime'] == 21
        assert final[('b', 7)]['rows_90d'] == 9
        assert final[('b', 7)]['total_90d'] == 13
        assert snapshots[ends[19] - 1] == snapshots[ends[17] - 1]
        base.reduce_records(base.synthetic_records(snapshots), snapshots, final)
    print('PASS independent admission/pruning fixture oracle; engine not run')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path, nargs='?')
    parser.add_argument('--directory', type=Path, default=Path('/app/target/native-calendar-pruning'))
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.prepare_only and args.binary is None:
        parser.error('binary required unless --self-test or --prepare-only')
    cases = prepare(args.directory.resolve())
    print('Prepared eight common admission/pruning SQL fixtures', flush=True)
    if not args.prepare_only:
        base.execute(args.binary.resolve(strict=True), cases)


if __name__ == '__main__':
    main()
