#!/usr/bin/env python3
"""Check logical CDC oracles independently of runtime flush scheduling."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))


def load(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), SCRIPTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CaptureOracleTests(unittest.TestCase):
    def check_schedules(self, snapshots, reduce, corrupt):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'output.jsonl'

            def records_for(states):
                records, current = [], {}
                for state in states:
                    for key in sorted(current.keys() | state.keys()):
                        after = state.get(key)
                        before = current.get(key)
                        if before != after:
                            records.append(dict(before=before, after=after,
                                                op='c' if before is None else 'd' if after is None else 'u'))
                    current = state
                return records

            def write(records):
                path.write_text(''.join(json.dumps(row) + '\n' for row in records))

            for states in (snapshots, [snapshots[-1]]):
                records = records_for(states)
                write(records)
                reduce(path)
                broken = json.loads(json.dumps(records))
                broken[0]['before'] = broken[0]['after']
                broken[0]['op'] = 'u'
                write(broken)
                with self.assertRaises((AssertionError, ValueError)):
                    reduce(path)
                # Preserve the before chain while replacing an observable value.
                arbitrary = json.loads(json.dumps(records))
                corrupt(arbitrary[0]['after'])
                if len(arbitrary) > 1:
                    for row in arbitrary[1:]:
                        if row['before'] == records[0]['after']:
                            row['before'] = arbitrary[0]['after']
                write(arbitrary)
                with self.assertRaises((AssertionError, ValueError)):
                    reduce(path)
            write(records_for(snapshots[:-1]))
            with self.assertRaises((AssertionError, ValueError)):
                reduce(path)

    def test_two_stage(self):
        module = load('test-native-aggregates')
        states = [module.expected(index) for index in range(1, len(module.EVENTS) + 1)]
        self.check_schedules(states, lambda path: module.strict_reduce(path, states[-1]),
                             lambda row: row.update(total=999999))

    def test_two_stage_equal_order_latest(self):
        module = load('test-native-aggregates')
        states = [module.expected(index, False) for index in range(1, len(module.EVENTS) + 1)]
        self.check_schedules(states, lambda path: module.strict_reduce(path, states[-1], False),
                             lambda row: row.update(latest=999999))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'final.jsonl'
            for legal in module.tied_latest_values(len(module.EVENTS))['b']:
                final = json.loads(json.dumps(states[-1]))
                final['b']['latest'] = legal
                path.write_text(''.join(json.dumps(dict(before=None, after=row, op='c')) + '\n'
                                        for row in final.values()))
                module.strict_reduce(path, states[-1], False)

    def test_ordered(self):
        module = load('test-native-aggregate-ordering')
        for total_order in (False, True):
            states = [{key: values[0] for key, values in module.prefix_values(index, total_order).items()}
                      for index in range(1, len(module.EVENTS) + 1)]
            self.check_schedules(states, lambda path: module.reduce(path, states[-1], total_order),
                                 lambda row: row.update(latest=999999))

    def test_top_five(self):
        module = load('native_updating_top5_cdc')
        states = module.prefix_states(module.events())
        allowed = {}
        for snapshot in states:
            for key, row in snapshot.items():
                allowed.setdefault(key, set()).add(module.canonical(row))
        # The last source event can leave the projected slice unchanged.
        observable = []
        for snapshot in states:
            if not observable or snapshot != observable[-1]:
                observable.append(snapshot)

        def reduce(path):
            actual = module.reduce_cdc(path, allowed, states)
            self.assertEqual(actual[-1], states[-1])

        self.check_schedules(observable, reduce,
                             lambda row: row['top_items'][0].update(n=999999))

    def test_append_only_groups_cannot_be_deleted(self):
        ordered = load('test-native-aggregate-ordering')
        aggregates = load('test-native-aggregates')
        unordered = load('test-native-unordered-first-last')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'output.jsonl'
            for first, final, reduce in (
                ({key: values[0] for key, values in ordered.prefix_values(1, True).items()},
                 ordered.FINAL_TOTAL, lambda: ordered.reduce(path, ordered.FINAL_TOTAL, True)),
                (aggregates.expected(1), aggregates.expected(len(aggregates.EVENTS)),
                 lambda: aggregates.strict_reduce(path, aggregates.expected(len(aggregates.EVENTS)))),
                ({'scope': unordered.prefix_value(1)}, {'scope': unordered.FINAL},
                 lambda: unordered.reduce_cdc(path)),
            ):
                row = next(iter(first.values()))
                records = [dict(before=None, after=row, op='c'),
                           dict(before=row, after=None, op='d')]
                records.extend(dict(before=None, after=value, op='c') for value in final.values())
                path.write_text(''.join(json.dumps(record) + '\n' for record in records))
                with self.assertRaises((AssertionError, ValueError)):
                    reduce()

    def test_checkpoint_boundaries(self):
        ordered = load('test-native-aggregate-ordering')
        unordered = load('test-native-unordered-first-last')
        aggregates = load('test-native-aggregates')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'checkpoint.jsonl'
            for checkpoint, future, reduce in (
                (ordered.CHECKPOINT, ordered.FINAL_TOTAL,
                 lambda: ordered.reduce(path, ordered.CHECKPOINT, True, prefix_count=4)),
                ({'scope': unordered.CHECKPOINT}, {'scope': unordered.FINAL},
                 lambda: unordered.reduce_cdc(path, prefix_count=2)),
                (aggregates.expected(4), aggregates.expected(len(aggregates.EVENTS)),
                 lambda: aggregates.strict_reduce(path, aggregates.expected(4), prefix_count=4)),
            ):
                def write(state):
                    path.write_text(''.join(json.dumps(dict(before=None, after=row, op='c')) + '\n'
                                            for row in state.values()))
                write(checkpoint)
                reduce()
                write(future)
                with self.assertRaises((AssertionError, ValueError)):
                    reduce()
                write({})
                with self.assertRaises((AssertionError, ValueError)):
                    reduce()

    def test_unordered(self):
        module = load('test-native-unordered-first-last')
        states = [{'scope': module.prefix_value(index)} for index in range(1, len(module.EVENTS) + 1)]

        def reduce(path):
            _, actual = module.reduce_cdc(path)
            self.assertEqual(actual, module.FINAL)

        self.check_schedules(states, reduce, lambda row: row.update(last_event_ms=999999))


if __name__ == '__main__':
    unittest.main()
