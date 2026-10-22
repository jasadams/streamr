#!/usr/bin/env python3
"""Oracle checks only; these tests do not establish native SQL execution."""
import importlib.util
import json
import contextlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import native_grouped_selectors as fixture

spec = importlib.util.spec_from_file_location('result_manifest', SCRIPTS/'test-native-result-manifest.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class Selectors(unittest.TestCase):
    def final(self):
        return {row['canonical_id']:row for row in fixture.prefix_states(fixture.events())[-1]}

    def test_mandatory_retained_event_time_and_ties(self):
        rows=self.final()
        self.assertEqual((rows['raw']['earliest_value'],rows['raw']['latest_value']),('B','A'))
        self.assertEqual(rows['raw']['fixed_first'],[999,1000])
        self.assertEqual(rows['raw']['fixed_last'],[0,1])
        self.assertEqual(rows['null-order']['fixed_last'],[None,None])
        self.assertEqual(rows['null-order']['fixed_nonnull_last'],[None,None])
        self.assertEqual((rows['ties']['latest_value'],rows['ties']['descending_value']),('C','A'))
        self.assertEqual(rows['raw']['midpoint'],499.5)
        self.assertEqual(rows['ties']['midpoint'],1.0)
        self.assertEqual(rows['ties']['spread'],0.0)
        self.assertIsNone(rows['replacement']['spread'])
        self.assertEqual((rows['ordinal']['ordinal_first'],rows['ordinal']['ordinal_last']),('A','C'))
        self.assertEqual(rows['null-order']['nulls_first'],'A')
        self.assertEqual(rows['null-order']['nulls_last'],'N')

    def test_mandatory_retained_null_blank_and_coherence(self):
        rows=self.final()
        row=rows['nullable']
        self.assertIsNone(row['ordinal_last'])
        self.assertEqual(row['fixed_last'],[0,1])
        self.assertIsNone(row['fixed_nullable_last'])
        self.assertEqual(row['fixed_nonnull_last'],[0,1])
        self.assertEqual(row['spread'],0.0)
        self.assertIsNone(row['filtered_spread'])
        self.assertEqual(row['latest_nonblank'],'A')
        self.assertIsNone(row['assignment'])
        self.assertEqual(row['device_locale'],'fr')
        self.assertEqual(row['email'],'fallback@example.test')
        self.assertIsNone(row['catalog_email'])
        self.assertIs(row['flag'],False)
        self.assertEqual(row['number'],0)
        self.assertEqual((row['touch_source'],row['touch_medium']),('two',''))
        self.assertEqual(rows['blank']['ordinal_last'],'')
        self.assertEqual(rows['blank']['latest_nonblank'],'A')
        self.assertEqual(rows['blank']['assignment'],'')
        self.assertEqual(rows['blank']['catalog_email'],'blank-fallback@example.test')

    def test_mandatory_retained_catalog_priority(self):
        row=self.final()['email-priority']
        self.assertIsNone(row['catalog_email'])
        self.assertEqual(row['email'],'fallback@example.test')

    def test_mandatory_retained_original_identity_and_replacement(self):
        rows=self.final()
        self.assertEqual(rows['identity']['ordinal_last'],'Y')
        self.assertEqual(rows['identity']['interleaved_last'],'Y')
        self.assertEqual(rows['replacement']['ordinal_last'],'Y')
        states=fixture.prefix_states(fixture.events())
        checkpoint={row['canonical_id']:row for row in states[fixture.CHECKPOINT-1]}
        self.assertEqual(checkpoint['identity']['ordinal_last'],'X')
        self.assertEqual(checkpoint['replacement']['ordinal_last'],'Y')
        self.assertEqual(checkpoint['ordinal']['ordinal_last'],'B')

    def test_analytic_order_and_explicit_frames(self):
        first=fixture.analytic_fixture(False)
        last=fixture.analytic_fixture(True)
        self.assertEqual(first[4],[dict(row_id=i,selected=3) for i in (1,2,3)])
        self.assertEqual(last[4],[dict(row_id=1,selected=1),dict(row_id=2,selected=2),
                                  dict(row_id=3,selected=1)])
        self.assertEqual(len(first[0]),18)
        self.assertEqual(first[3],16)
        self.assertEqual(min(int(row['event_time'][17:19]) for row in first[0][8:16])-5,16)
        self.assertEqual(sum(row['row_id']<0 for row in first[0]),12)
        self.assertIn('WHERE row_id>0',first[2])
        self.assertEqual([row['row_id'] for row in first[4]],[1,2,3])
        self.assertIn('UNBOUNDED FOLLOWING',first[2])
        self.assertIn('CURRENT ROW AND 1 FOLLOWING',last[2])
        self.assertEqual([row['selected'] for row in first[1][-1]],[3,3,3,7,7,0])
        self.assertEqual([row['selected'] for row in last[1][-1]],[1,2,1,4,4,0])

    def test_all_manifests_prepare_and_validate(self):
        with tempfile.TemporaryDirectory() as directory:
            for mode in ('cdc','append','analytic-first','analytic-last'):
                path=Path(directory)/mode
                fixture.prepare(path,mode)
                expected=json.loads((path/'expected.json').read_text())
                runner.Oracle(expected)
                query=(path/'query.sql').read_text()
                self.assertIn('{{INPUT}}',query)
                self.assertIn('{{OUTPUT}}',query)
                self.assertNotIn('{{ANALYTIC_EXPRESSION}}',query)
                if mode in ('cdc','append'):
                    source=[json.loads(line) for line in (path/'input.jsonl').read_text().splitlines()]
                    accepted=[event['after'] for event in source if event.get('after') is not None] if mode=='cdc' else source
                    progress=[row['progress_time'] for row in accepted]
                    self.assertEqual(progress,sorted(progress))
                    self.assertEqual(len(progress),len(set(progress)))
                    self.assertTrue(all(row['progress_time']<'2000' and row['event_time']>'2020' for row in accepted))
                    self.assertIn("WATERMARK FOR event_time AS progress_time - INTERVAL '5' SECOND",query)
                    self.assertIn('STDDEV(CAST(1 AS DOUBLE))',query)
                    self.assertIn('MEDIAN(CAST(e.priority AS DOUBLE))',query)
                    self.assertIn("LAST_VALUE(CAST(JSON_VALUE(e.payload,'$.number' RETURNING DOUBLE PRECISION) AS BIGINT))",query)
                    self.assertNotIn('RETURNING BIGINT',query)

                    self.assertIn("FIRST_VALUE(arrow_cast(make_array(e.priority, e.priority + 1), 'FixedSizeList(2, Int64)'))",query)
                    self.assertIn('CAST(fixed_first AS BIGINT[]) AS fixed_first',query)
                    self.assertIn('CAST(fixed_nonnull_last AS BIGINT[]) AS fixed_nonnull_last',query)
                if mode=='append':
                    self.assertIn("format='json',type='source'",query)
                    self.assertNotIn('PRIMARY KEY',query)
                if mode=='cdc':
                    self.assertEqual(len((path/'input.jsonl').read_text().splitlines()),25)
                    self.assertTrue(json.loads((path/'manifest.json').read_text())['require_matching_cdc'])

    def test_exact_cdc_rejects_extra_transition_despite_equal_endpoints(self):
        expected=self.expected()
        rows=[state[0] for state in expected['prefix_states']]
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            output=root/'output.jsonl'
            initial=output.with_suffix('.initial.jsonl')
            events=[dict(before=None,after=rows[0],op='c'),
                    dict(before=rows[0],after=rows[2],op='u')]
            initial.write_text(''.join(json.dumps(e)+'\n' for e in events))
            output.write_text(initial.read_text())
            def log(count):
                (root/'capture.log').write_text(
                    f'CAPTURE_MATCH_INITIAL_CHECKPOINT input_rows=1 output_rows=1\n'
                    f'CAPTURE_RESULT phase=initial rows=2 path={initial}\n'
                    f'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=1 '
                    f'committed_rows=1 rows={count} bytes=1 path={output} job=test\n'
                    'test result: ok. 1 passed; 0 failed;\n')
            log(2)
            runner.compare(root,runner.Oracle(expected),1,output,True)
            events.insert(1,dict(before=rows[0],after=rows[0],op='u'))
            output.write_text(''.join(json.dumps(e)+'\n' for e in events))
            log(3)
            # Legacy endpoint-only checks allow this; exact CDC must reject it.
            runner.compare(root,runner.Oracle(expected),1,output,False)
            with self.assertRaisesRegex(ValueError,'exact CDC'):
                runner.compare(root,runner.Oracle(expected),1,output,True)

    def test_no_fail_fast_collects_every_selected_case(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            manifest=root/'manifest.json'
            binary=root/'binary'
            manifest.write_text('{}')
            binary.write_text('test')
            argv=['runner',str(manifest),str(binary),str(root/'results'),'--no-fail-fast']
            output=io.StringIO()
            with patch.object(sys,'argv',argv), patch.object(runner,'run',side_effect=ValueError('probe')) as run:
                with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as status:
                    runner.main()
            self.assertEqual(status.exception.code,1)
            self.assertEqual(run.call_count,8)
            result=json.loads(output.getvalue())
            self.assertEqual(result['status'],'fail')
            self.assertEqual(len(result['cases']),8)
            self.assertEqual(len({(r['backend'],r['protocol'],r['batch_rows'])
                                  for r in result['cases']}),8)
            # Existing invocations retain their first-failure behavior.
            with patch.object(sys,'argv',argv[:-1]), patch.object(runner,'run',side_effect=ValueError('probe')) as run:
                with self.assertRaisesRegex(ValueError,'probe'):
                    runner.main()
            self.assertEqual(run.call_count,1)

    def expected(self,strict=True):
        rows=[dict(k='g',a=1,b=10),dict(k='g',a=2,b=20),dict(k='g',a=3,b=30)]
        return dict(key_fields=['k'],strict_prefix_rows=strict,
                    prefix_states=[[r] for r in rows],checkpoint=[rows[0]],final=[rows[-1]])

    def test_mixed_prefix_fields_rejected(self):
        mixed=dict(k='g',a=1,b=20)
        with self.assertRaisesRegex(ValueError,'complete row'):
            runner.Oracle(self.expected()).check(mixed)
        runner.Oracle(self.expected(False)).check(mixed)

    def test_reverse_prefix_rejected_and_exact_before_required(self):
        expected=self.expected()
        rows=[state[0] for state in expected['prefix_states']]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'output.jsonl'
            events=[dict(before=None,after=rows[1],op='c'),
                    dict(before=rows[1],after=rows[0],op='u')]
            path.write_text(''.join(json.dumps(e)+'\n' for e in events))
            with self.assertRaisesRegex(ValueError,'forward'):
                runner.Oracle(expected).reduce(path)
            events[1]=dict(before=rows[0],after=rows[2],op='u')
            path.write_text(''.join(json.dumps(e)+'\n' for e in events))
            with self.assertRaisesRegex(ValueError,'continuity'):
                runner.Oracle(expected).reduce(path)


if __name__=='__main__':
    unittest.main()
