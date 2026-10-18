#!/usr/bin/env python3
"""Synthetic checks of the real SQL driver's oracle; no runtime claims."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest

spec = importlib.util.spec_from_file_location('quiet', Path(__file__).with_name('native-quiet-calendar.py'))
quiet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quiet)


def stream():
    a1, a0 = quiet.snapshot(1)['keyA'], quiet.snapshot(3)['keyA']
    b1, b2, b3 = [quiet.snapshot(n)['keyB'] for n in (2, 3, 4)]
    return [dict(before=None, after=a1, op='c'), dict(before=None, after=b1, op='c'),
            dict(before=a1, after=a0, op='u'), dict(before=b1, after=b2, op='u'),
            dict(before=b2, after=b3, op='u')]


class QuietCalendarTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        quiet.prepare(self.path)
        self.rows = stream()
        for name in ('output.initial.jsonl', 'output.jsonl'):
            self.write(name, self.rows)
        for phase, count in (('initial', 2), ('recovered', 2),
                             ('initial-resumed', 4), ('recovered-resumed', 4)):
            for boundary in ('before', 'after'):
                self.write(f'output.idle-{phase}-{boundary}.jsonl', self.rows[:count])
        (self.path / 'capture.log').write_text(
            'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=1 '
            f'committed_rows=1 rows=5 bytes=10 path={self.path}/output.jsonl\n1 passed\n')

    def write(self, name, rows):
        (self.path / name).write_text(''.join(json.dumps(row) + '\n' for row in rows))

    def test_query_uses_supported_native_trigger_clock(self):
        sql = (self.path / 'query.sql').read_text()
        self.assertIn('COUNT(*) FILTER (WHERE CAST(timestamp AS DATE) = EVENT_DATE())', sql)
        self.assertIn('WATERMARK FOR timestamp AS timestamp', sql)
        self.assertNotIn('WATERMARK_DATE', sql)
        self.assertNotIn('WATERMARK_TIMESTAMP', sql)

    def test_exact_utc_prefixes(self):
        self.assertEqual(quiet.snapshot(1), {'keyA': dict(k='keyA', lifetime_count=1, today_count=1)})
        self.assertEqual(quiet.snapshot(3)['keyA'], dict(k='keyA', lifetime_count=1, today_count=0))
        self.assertEqual(quiet.snapshot(4)['keyB'], dict(k='keyB', lifetime_count=3, today_count=2))

    def test_correct_initial_restore_and_two_live_holds(self):
        quiet.validate(self.path)

    def test_unrelated_key_order_is_not_fixed(self):
        rows = stream()
        rows[2], rows[3] = rows[3], rows[2]
        quiet.reduce(rows, quiet.snapshot(4), require_expiry=True)

    def test_reject_wrong_counts_keys_before_and_duplicate(self):
        for field in ('count', 'key', 'before', 'duplicate'):
            rows = copy.deepcopy(stream())
            if field == 'count':
                rows[-1]['after']['today_count'] = 100
            elif field == 'key':
                rows[-1]['after']['k'] = 'unexpected'
            elif field == 'before':
                rows[-1]['before'] = dict(k='keyB', lifetime_count=99, today_count=0)
            else:
                rows.append(copy.deepcopy(rows[-1]))
            with self.subTest(field=field), self.assertRaises(AssertionError):
                quiet.reduce(rows, quiet.snapshot(4), require_expiry=True)

    def test_missing_initial_or_restored_expiry_fails(self):
        for name in ('output.initial.jsonl', 'output.jsonl'):
            self.write(name, [row for row in stream() if row['op'] != 'u' or row['after']['k'] != 'keyA'])
            with self.assertRaises(AssertionError):
                quiet.validate(self.path)
            self.write(name, self.rows)

    def test_every_live_artifact_required(self):
        for path in self.path.glob('output.idle-*.jsonl'):
            raw = path.read_bytes()
            path.unlink()
            with self.assertRaises((AssertionError, FileNotFoundError)):
                quiet.validate(self.path)
            path.write_bytes(raw)

    def test_wall_clock_expiry_without_progress_fails(self):
        self.write('output.idle-initial-after.jsonl', self.rows[:3])
        with self.assertRaises(AssertionError):
            quiet.validate(self.path)

    def test_stale_pre_eof_expiry_fails(self):
        for boundary in ('before', 'after'):
            self.write(f'output.idle-initial-resumed-{boundary}.jsonl', self.rows[:2])
        with self.assertRaises(AssertionError):
            quiet.validate(self.path)

    def test_wrong_or_missing_committed_prefix_fails(self):
        log = self.path / 'capture.log'
        raw = log.read_text()
        for text in ('', raw.replace('committed_rows=1', 'committed_rows=2'),
                     raw.replace('input_rows_before_checkpoint=1', 'input_rows_before_checkpoint=2')):
            log.write_text(text)
            with self.assertRaises(AssertionError):
                quiet.validate(self.path)

    def test_driver_records_all_four_independent_failures(self):
        destination = self.path / 'failed-matrix'
        result = subprocess.run([sys.executable, str(Path(quiet.__file__)), str(destination),
                                 '--binary', '/bin/false'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        results = json.loads((destination / 'results.json').read_text())
        self.assertTrue(results['executed'])
        self.assertEqual(len(results['cases']), 4)
        self.assertTrue(all(case['status'] == 'failed' and case['engine_exit_code'] == 1
                            for case in results['cases']))

    def test_environment_has_no_removed_execution_pool(self):
        env = quiet.environment(self.path, 'memory', 'controller')
        self.assertEqual(env['STREAMR_CAPTURE_IDLE_RESUME_SOURCE_ROW_TARGET'], '3')
        self.assertFalse(any('EXECUTION_BYTES' in key or 'QUEUED_WRITE' in key for key in env))


if __name__ == '__main__':
    unittest.main()
