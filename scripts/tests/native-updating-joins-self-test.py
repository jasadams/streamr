#!/usr/bin/env python3
"""Adversarial checks for the updating join CDC oracle; no Rust execution."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

MODULE = Path(__file__).resolve().parents[1] / 'test-native-updating-joins.py'
spec = importlib.util.spec_from_file_location('joins', MODULE)
joins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(joins)


def canonical_capture(fixture, limit=None):
    current, records = {}, []
    left = right = 0
    events = fixture['events'] if limit is None else fixture['events'][:limit]
    for event in events:
        left += event['side'] == 'l'
        right += event['side'] == 'r'
        wanted = joins.snapshot(fixture, left, right)
        for identity in sorted(set(current) | set(wanted), key=str):
            before, after = current.get(identity), wanted.get(identity)
            if before == after:
                continue
            records.append(dict(before=before, after=after, op='c' if before is None else 'd' if after is None else 'u'))
        current = wanted
    return records


class OracleTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = [json.loads(p.read_text()) for p in sorted(joins.FIXTURES.glob('*.json'))]

    def test_all_fixture_final_and_checkpoint_bags(self):
        for fixture in self.fixtures:
            joins.validate(canonical_capture(fixture), fixture)
            joins.validate(canonical_capture(fixture, fixture['checkpoint']), fixture, fixture['checkpoint'])

    def test_coalesced_flush_is_valid(self):
        for fixture in self.fixtures:
            n = len(fixture['events'])
            state = joins.snapshot(fixture, n, n)
            joins.validate([dict(before=None, after=row, op='c') for row in state.values()], fixture)

    def test_independent_branch_prefixes(self):
        fixture = self.fixtures[0]
        first = joins.snapshot(fixture, 1, 2)
        final = joins.snapshot(fixture, 99, 99)
        records = [dict(before=None, after=row, op='c') for row in first.values()]
        for identity in set(first) | set(final):
            before, after = first.get(identity), final.get(identity)
            if before != after:
                records.append(dict(before=before, after=after, op='c' if before is None else 'd' if after is None else 'u'))
        joins.validate(records, fixture)

    def test_reject_wrong_complete_value(self):
        fixture = self.fixtures[0]
        records = canonical_capture(fixture)
        records[0]['after'] = dict(records[0]['after'], right_total=999)
        with self.assertRaises(AssertionError):
            joins.validate(records, fixture)

    def test_reject_lost_before_and_duplicate_create(self):
        fixture = self.fixtures[0]
        records = canonical_capture(fixture)
        update = next(i for i, row in enumerate(records) if row['op'] == 'u')
        bad = copy.deepcopy(records)
        bad[update]['before']['left_total'] = -1
        with self.assertRaises(AssertionError):
            joins.validate(bad, fixture)
        with self.assertRaises(AssertionError):
            joins.validate([records[0], records[0]] + records[1:], fixture)

    def test_reject_missing_final_pair(self):
        fixture = self.fixtures[0]
        state = joins.snapshot(fixture, 99, 99)
        records = [dict(before=None, after=row, op='c') for row in list(state.values())[1:]]
        with self.assertRaises(AssertionError):
            joins.validate(records, fixture)

    def test_reject_backwards_value(self):
        fixture = self.fixtures[0]
        records = canonical_capture(fixture)
        updates = [row for row in records if row['op'] == 'u']
        row = updates[0]
        records.insert(records.index(row) + 1, dict(before=row['after'], after=row['before'], op='u'))
        with self.assertRaises(AssertionError):
            joins.validate(records, fixture)

    def test_nulls_never_match_and_tenant_keys_remain_distinct(self):
        for fixture in self.fixtures:
            state = joins.snapshot(fixture, 99, 99)
            self.assertNotIn(('ln', 'rn'), state)
            self.assertNotIn(('lt', 'rt'), state)
            if fixture['join'] == 'LEFT':
                self.assertIn(('ln', None), state)
                self.assertIn(('lt', None), state)

    def test_left_first_and_last_match_transitions(self):
        fixture = next(f for f in self.fixtures if f['join'] == 'LEFT')
        records = canonical_capture(fixture)
        unmatched = lambda row: (row or {}).get('left_id') == 'l1' and (row or {}).get('right_id') is None
        self.assertTrue(any(r['op'] == 'c' and unmatched(r['after']) for r in records))
        self.assertTrue(any(r['op'] == 'd' and unmatched(r['before']) for r in records))
        self.assertGreater(sum(r['op'] == 'c' and unmatched(r['after']) for r in records), 1)

    def test_out_of_order_seq_uses_ordered_last_value(self):
        fixture = dict(name='out-of-order', join='INNER', checkpoint=2, events=[
            dict(side='l', item_id='left', k1='match', k2=1, amount=2, seq=10),
            dict(side='r', item_id='right', k1='match', k2=1, amount=3, seq=20),
            dict(side='l', item_id='left', k1='wrong', k2=9, amount=5, seq=1),
            dict(side='r', item_id='right', k1='wrong', k2=9, amount=7, seq=2),
        ])
        expected = dict(left_id='left', right_id='right', k1='match', k2=1,
                        left_total=7, right_total=10, left_count=2, right_count=2)
        self.assertEqual(joins.snapshot(fixture, 2, 2), {('left', 'right'): expected})
        joins.validate(canonical_capture(fixture), fixture)
        joins.validate(canonical_capture(fixture, 2), fixture, 2)

    def test_ambiguous_per_group_seq_tie_rejected_before_prepare(self):
        fixture = copy.deepcopy(self.fixtures[0])
        fixture['events'][2]['seq'] = fixture['events'][0]['seq']
        with self.assertRaisesRegex(ValueError, 'ambiguous LAST_VALUE ORDER BY seq tie'):
            joins.validate([], fixture)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'not-created'
            with self.assertRaisesRegex(ValueError, 'ambiguous LAST_VALUE ORDER BY seq tie'):
                joins.prepare(fixture, root)
            self.assertFalse(root.exists())
        # Equal seq across distinct sides/groups is deterministic per group.
        fixture = copy.deepcopy(self.fixtures[0])
        fixture['events'][1]['seq'] = fixture['events'][0]['seq']
        joins.validate_fixture(fixture)

    def test_validated_composed_template_selection(self):
        for fixture in self.fixtures:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                joins.prepare(fixture, root)
                sql = (root / 'query.sql').read_text()
                self.assertNotIn('@', sql)
                template = fixture.get('query_template', 'direct')
                if template == 'chained-inner':
                    self.assertIn('INNER JOIN right_values rr ON j.right_id = rr.item_id', sql)
                elif template == 'downstream-inner':
                    self.assertIn('SUM(left_total) AS left_total', sql)
                    self.assertIn('GROUP BY left_id, right_id, k1, k2', sql)
                else:
                    self.assertNotIn('CREATE VIEW joined_values', sql)
        fixture = copy.deepcopy(self.fixtures[0])
        fixture['query_template'] = '../other.sql'
        with self.assertRaisesRegex(ValueError, 'unsupported query_template'):
            joins.validate_fixture(fixture)
        fixture.update(query_template='downstream-inner', join='LEFT')
        with self.assertRaisesRegex(ValueError, 'require INNER'):
            joins.validate_fixture(fixture)

    def test_split_retraction_vacancy_preserves_value_frontier(self):
        for template in ('direct', 'chained-inner', 'downstream-inner'):
            fixture = dict(name='vacancy', join='INNER', query_template=template, checkpoint=2, events=[
                dict(side='l', item_id='l', k1='a', k2=1, amount=2, seq=1),
                dict(side='r', item_id='r', k1='a', k2=1, amount=3, seq=1),
                dict(side='l', item_id='l', k1='a', k2=1, amount=5, seq=2),
            ])
            records = canonical_capture(fixture)
            update = records[-1]
            split = records[:-1] + [dict(before=update['before'], after=None, op='d'),
                                    dict(before=None, after=update['after'], op='c')]
            joins.validate(split, fixture)
            # After a newer complete value, a temporary absence cannot reset
            # ordering and authorize recreation of an older complete value.
            wrong = records + [dict(before=update['after'], after=None, op='d'),
                               dict(before=None, after=update['before'], op='c')]
            with self.assertRaises(AssertionError):
                joins.validate(wrong, fixture)
            # Missing recreation still fails the exact final bag.
            with self.assertRaises(AssertionError):
                joins.validate(split[:-1], fixture)
            unchanged = records + [dict(before=update['after'], after=None, op='d'),
                                   dict(before=None, after=update['after'], op='c')]
            joins.validate(unchanged, fixture)

    def test_prepare_hashable_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            joins.prepare(self.fixtures[0], root)
            self.assertNotIn('@', (root / 'query.sql').read_text())
            self.assertEqual(len(joins.digest(root / 'fixture.json')), 64)
            self.assertEqual(len(joins.read_records(root / 'input.jsonl')), len(self.fixtures[0]['events']))

    def test_reject_existing_evidence_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / 'output.jsonl'
            evidence.write_text('preserve me')
            with self.assertRaises(AssertionError):
                joins.prepare(self.fixtures[0], root)
            self.assertEqual(evidence.read_text(), 'preserve me')

    def test_oracle_failure_receipt_and_native_hook(self):
        fixture = next(f for f in self.fixtures if f['join'] == 'LEFT')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'case'

            def captured(command, env, stdout, stderr):
                self.assertEqual(env['STREAMR_TEST_NATIVE_UPDATING_JOINS'], '1')
                self.assertEqual(env['STREAMR_TEST_NATIVE_AGGREGATES'], '1')
                self.assertEqual(env['STREAMR_TEST_SCAN_PAGE_BYTES'], '16777216')
                self.assertEqual(env['STREAMR_TEST_QUEUED_WRITE_BYTES'], '67108864')
                stdout.write('1 passed; 0 failed\n')
                stdout.write(f"CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint={fixture['checkpoint']} committed_rows=0 rows=0 bytes=1 path={root}/output.jsonl job=test\n")
                (root / 'output.jsonl').write_text('')
                (root / 'output.initial.jsonl').write_text('')
                return SimpleNamespace(returncode=0)

            with patch.object(joins.subprocess, 'run', side_effect=captured):
                with self.assertRaises(AssertionError):
                    joins.run_case(MODULE, fixture, root, 'memory', 'controller', 1, {})
            receipt = json.loads((root / 'receipt.json').read_text())
            self.assertEqual(receipt['status'], 'oracle-failed')
            self.assertEqual(receipt['exit_status'], 0)
            self.assertEqual(receipt['driver_sha256'], joins.digest(MODULE))
            self.assertEqual(receipt['configuration']['STREAMR_TEST_SCAN_PAGE_BYTES'], '16777216')
            self.assertEqual(receipt['configuration']['STREAMR_TEST_QUEUED_WRITE_BYTES'], '67108864')
            self.assertIn('final bag', receipt['oracle_error'])
            self.assertTrue((root / 'capture.log').exists())


if __name__ == '__main__':
    unittest.main()
