#!/usr/bin/env python3
"""Independent oracle and append adapter checks; no actual SQL execution."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPTS=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SCRIPTS))
import native_window_selectors as fixture

spec=importlib.util.spec_from_file_location('result_manifest',SCRIPTS/'test-native-result-manifest.py')
runner=importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class WindowSelectors(unittest.TestCase):
    def test_final_observable_cases_in_every_window_lifecycle(self):
        for kind in fixture.WINDOWS:
            final=fixture.states(fixture.events(),kind)[-1]
            grouped={}
            for row in final:
                grouped.setdefault(row['canonical_id'],[]).append(row)
            self.assertEqual(set(grouped),{'seed','raw','ties','ordinal','nullable','panes','merge','reorder','blank'})
            for row in grouped['raw']:
                self.assertEqual((row['earliest_value'],row['latest_value']),('B','A'))
                self.assertEqual(row['fixed_first'],[999,1000])
                self.assertEqual(row['fixed_last'],[0,1])
            for row in grouped['ties']:
                self.assertEqual((row['latest_value'],row['descending_value']),('C','A'))
                self.assertEqual(row['midpoint'],1.0)
                self.assertEqual(row['spread'],0.0)
            for row in grouped['ordinal']:
                self.assertEqual((row['ordinal_first'],row['ordinal_last']),('A','C'))
            for row in grouped['nullable']:
                self.assertIsNone(row['ordinal_last'])
                self.assertEqual(row['latest_nonblank'],'A')
                self.assertEqual(row['fixed_last'],[0,1])
                self.assertIsNone(row['fixed_nullable_last'])
                self.assertEqual(row['fixed_nonnull_last'],[0,1])
                self.assertEqual(row['spread'],0.0)
                self.assertIsNone(row['filtered_spread'])
            for row in grouped['reorder']:
                self.assertEqual((row['earliest_value'],row['ordinal_last']),('earlier','C'))
            for row in grouped['blank']:
                self.assertEqual(row['ordinal_last'],'')
                self.assertIsNone(row['latest_nonblank'])
                self.assertEqual(row['fixed_first'],[None,None])
                self.assertEqual(row['fixed_last'],[None,None])
                self.assertEqual(row['fixed_nonnull_last'],[None,None])
                self.assertIsNone(row['midpoint'])
                self.assertIsNone(row['spread'])

    def test_checkpoint_and_final_counts(self):
        for kind,checkpoint,final in (('tumble',1,9),('hop',2,20),('session',1,9)):
            states=fixture.states(fixture.events(),kind)
            self.assertEqual(len(states[fixture.CHECKPOINT-1]),checkpoint)
            self.assertTrue(all(row['canonical_id']=='seed' for row in states[fixture.CHECKPOINT-1]))
            self.assertEqual(len(states[-1]),final)
            self.assertTrue(all(not state or all(row['canonical_id']=='seed' for row in state)
                                for state in states[:-1]))

    def test_hop_neighbor_panes_and_shared_arrival_identity(self):
        source=fixture.events()
        windows=fixture.members(source,'hop')
        full=windows['panes',120,140]
        self.assertEqual([fixture.second(row) for row,_ in full],[122,130,130])
        self.assertEqual(fixture.output('panes',120,140,full)['ordinal_last'],'B')
        self.assertEqual(fixture.output('panes',120,140,full)['midpoint'],0.0)
        tied=windows['panes',130,150]
        self.assertEqual([(row['row_id'],ordinal) for row,ordinal in tied],
                         [(row['row_id'],ordinal) for row,ordinal in full if fixture.second(row)==130])
        self.assertEqual(fixture.output('panes',130,150,tied)['ordinal_last'],'B')

    def test_session_merge_and_reordered_reader_keep_original_ordinals(self):
        source=fixture.events()
        windows=fixture.members(source,'session')
        merged=windows['merge',120,146]
        self.assertEqual([ordinal for _,ordinal in merged],[13,15,14,24])
        row=fixture.output('merge',120,146,merged)
        self.assertEqual((row['n'],row['earliest_value'],row['ordinal_last']),(4,'A','C'))
        reordered=windows['reorder',120,138]
        self.assertEqual([ordinal for _,ordinal in reordered],[17,16,18,25])
        self.assertEqual(fixture.output('reorder',120,138,list(reversed(reordered)))['ordinal_last'],'C')

    def test_append_adapter_rejects_duplicates_and_missing_final_rows(self):
        final=fixture.states(fixture.events(),'tumble')[-1]
        checkpoint=[row for row in final if row['canonical_id']=='seed']
        expected=dict(key_fields=['canonical_id','start_epoch','end_epoch'],row_format='append_json',
            strict_prefix_rows=True,prefix_states=[checkpoint,final],checkpoint=checkpoint,final=final)
        oracle=runner.Oracle(expected)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            output=root/'output.jsonl'
            initial=output.with_suffix('.initial.jsonl')
            ordered=checkpoint+[row for row in final if row not in checkpoint]
            def write(path,rows):
                path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
            write(initial,ordered)
            write(output,ordered)
            def log(count):
                (root/'capture.log').write_text(
                    f'CAPTURE_MATCH_INITIAL_CHECKPOINT input_rows=19 output_rows=1\n'
                    f'CAPTURE_RESULT phase=initial rows=9 path={initial}\n'
                    f'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=19 '
                    f'committed_rows=1 rows={count} bytes=1 path={output} job=test\n'
                    'test result: ok. 1 passed; 0 failed;\n')
            log(9)
            runner.compare(root,oracle,19,output,True)
            write(output,ordered+ordered[-1:])
            with self.assertRaisesRegex(ValueError,'duplicate append'):
                oracle.reduce(output)
            write(output,ordered[:-1])
            log(8)
            with self.assertRaisesRegex(ValueError,'complete values differ'):
                runner.compare(root,oracle,19,output,True)

    def test_retained_ordered_window_driver_uses_declared_time_primary(self):
        spec=importlib.util.spec_from_file_location('ordered_window',SCRIPTS/'test-native-window-ordered.py')
        ordered=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ordered)
        rows=ordered.expected('hop')
        selected=next(row for row in rows if row['segment']=='a' and row['start']=='2023-10-09T17:13:20')
        self.assertEqual((selected['earliest'],selected['latest']),(-16,54))
        selected=next(row for row in rows if row['segment']=='b' and row['start']=='2023-10-09T17:13:20')
        self.assertEqual((selected['latest'],selected['latest_selected']),(55,53))

    def test_prepare_all_window_manifests(self):
        with tempfile.TemporaryDirectory() as directory:
            for kind in fixture.WINDOWS:
                path=Path(directory)/kind
                result=fixture.prepare(path,kind)
                self.assertEqual(result['source_rows'],26)
                runner.Oracle(json.loads((path/'expected.json').read_text()))
                manifest=json.loads((path/'manifest.json').read_text())
                self.assertTrue(manifest['native_windows'])
                self.assertTrue(manifest['require_matching_cdc'])
                query=(path/'query.sql').read_text()
                self.assertNotIn('{{WINDOW}}',query)
                self.assertIn("WATERMARK FOR event_time AS event_time - INTERVAL '60' SECOND",query)
                self.assertIn('STDDEV(CAST(1 AS DOUBLE))',query)
                self.assertIn('MEDIAN(CAST(priority AS DOUBLE))',query)
                self.assertIn("FIRST_VALUE(arrow_cast(make_array(priority, priority + 1), 'FixedSizeList(2, Int64)'))",query)
                self.assertIn('CAST(fixed_first AS BIGINT[]) AS fixed_first',query)
                self.assertIn('CAST(fixed_nonnull_last AS BIGINT[]) AS fixed_nonnull_last',query)
                self.assertIn("format='json',type='sink'",query)


if __name__=='__main__':
    unittest.main()
