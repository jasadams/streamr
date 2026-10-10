#!/usr/bin/env python3
"""Exercise capture assertions with synthetic CDC, never claim engine execution."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('composition', Path(__file__).parents[1] / 'test-native-result-composition.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def row(key, lifetime, recent):
    return dict(k=key, lifetime_count=lifetime, recent_count=recent)


def records():
    a2, a31, a30 = row('keyA', 2, 0), row('keyA', 3, 1), row('keyA', 3, 0)
    b10, b20 = row('keyB', 1, 0), row('keyB', 2, 0)
    return [dict(before=None, after=a2, op='c'), dict(before=a2, after=a31, op='u'),
            dict(before=None, after=b10, op='c'), dict(before=a31, after=a30, op='u'),
            dict(before=b10, after=b20, op='u')]


class ComparatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        driver.write_fixture(self.path, 'quiet-key-silence')
        self.rows = records()
        self.write('output.jsonl', self.rows)
        self.write('output.initial.jsonl', self.rows)
        for phase in ('initial', 'recovered'):
            for boundary in ('before', 'after'):
                self.write(f'output.idle-{phase}-{boundary}.jsonl', self.rows[:3])
        (self.path / 'capture.log').write_text('CAPTURE_SOURCE_PREFIX sources=1 rows_per_source=2\nCAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=2 committed_rows=1 rows=5\n')

    def write(self, name, rows):
        (self.path / name).write_text(''.join(json.dumps(x) + '\n' for x in rows))

    def validate(self):
        driver.validate_captures(self.path, 'quiet-key-silence')

    def test_correct_full_capture(self):
        self.validate()

    def test_calendar_correct_capture_and_utc_oracle(self):
        driver.independent_oracle('calendar-quiet-key')
        driver.write_fixture(self.path, 'calendar-quiet-key')
        a2, a31, a30 = row('keyA', 2, 1), row('keyA', 3, 1), row('keyA', 3, 0)
        b11, b21 = row('keyB', 1, 1), row('keyB', 2, 1)
        rows = [dict(before=None, after=a2, op='c'), dict(before=a2, after=a31, op='u'),
                dict(before=None, after=b11, op='c'), dict(before=a31, after=a30, op='u'),
                dict(before=b11, after=b21, op='u')]
        for name in ('output.jsonl', 'output.initial.jsonl'):
            self.write(name, rows)
        for phase in ('initial', 'recovered'):
            for boundary in ('before', 'after'):
                self.write(f'output.idle-{phase}-{boundary}.jsonl', rows[:2])
        driver.validate_captures(self.path, 'calendar-quiet-key')

    def test_wrong_keys_counts_and_before_images(self):
        for change in ('key', 'lifetime', 'recent', 'before'):
            with self.subTest(change=change):
                rows = records()
                if change == 'key':
                    rows[-1]['after']['k'] = 'unexpected'
                elif change == 'lifetime':
                    rows[-1]['after']['lifetime_count'] = 3
                elif change == 'recent':
                    rows[-1]['after']['recent_count'] = 1
                else:
                    rows[-1]['before'] = row('keyB', 1, 1)
                self.write('output.jsonl', rows)
                with self.assertRaises(AssertionError):
                    self.validate()

    def test_every_idle_artifact_is_required(self):
        for phase in ('initial', 'recovered'):
            for boundary in ('before', 'after'):
                name = f'output.idle-{phase}-{boundary}.jsonl'
                (self.path / name).unlink()
                with self.assertRaises(AssertionError):
                    self.validate()
                self.write(name, self.rows[:3])

    def test_idle_requires_complete_pending_state(self):
        for phase in ('initial', 'recovered'):
            for boundary in ('before', 'after'):
                self.write(f'output.idle-{phase}-{boundary}.jsonl', self.rows[:1])
        with self.assertRaises(AssertionError):
            self.validate()

    def test_idle_emission_is_rejected(self):
        self.write('output.idle-initial-after.jsonl', self.rows[:4])
        with self.assertRaises(AssertionError):
            self.validate()

    def test_wrong_checkpoint_prefix(self):
        (self.path / 'capture.log').write_text('CAPTURE_SOURCE_PREFIX sources=1 rows_per_source=2\nCAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=2 committed_rows=2 rows=5\n')
        with self.assertRaises(AssertionError):
            self.validate()

    def test_missing_checkpoint_receipt(self):
        (self.path / 'capture.log').write_text('')
        with self.assertRaises(AssertionError):
            self.validate()

    def test_each_phase_requires_quiet_transition(self):
        for name in ('output.initial.jsonl', 'output.jsonl'):
            rows = records()
            # A chain reaching the same final materialization without 3/1.
            rows[1]['after']['recent_count'] = 0
            rows.pop(3)
            self.write(name, rows)
            with self.assertRaises(AssertionError):
                self.validate()
            self.write(name, self.rows)


if __name__ == '__main__':
    unittest.main()
