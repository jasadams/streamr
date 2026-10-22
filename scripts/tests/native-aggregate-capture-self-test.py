#!/usr/bin/env python3
"""Check logical CDC oracles independently of runtime flush scheduling."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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

    def test_manifest_frame_limits_are_explicit_bounded_and_coherent(self):
        module=load('test-native-result-manifest')
        self.assertEqual(module.test_limit_environment({}),({},dict(requested={},derived=None)))
        for limits in ({'FRAME_BYTES':0},{'FRAME_BYTES':True},{'FRAME_BYTES':32767},
                       {'FRAME_BYTES':1048577},{'UNKNOWN':1},{'MAX_SNAPSHOTS':1<<100},
                       {'FRAME_BYTES':1048576,'SCAN_PAGE_BYTES':2097152},
                       {'FRAME_BYTES':1048576,'QUEUED_WRITE_BYTES':1048576}):
            with self.assertRaises(ValueError):module.test_limit_environment(limits)
        env,evidence=module.test_limit_environment({'FRAME_BYTES':1048576})
        self.assertEqual(env,{'STREAMR_TEST_FRAME_BYTES':'1048576'})
        self.assertEqual(evidence['derived']['page_bytes'],2097152)
        self.assertEqual(evidence['derived']['minimum_scan_page_bytes'],16777216)
        self.assertEqual(evidence['derived']['max_pending_output_bytes'],25165824)
        self.assertEqual(evidence['derived']['required_decoded_value_bytes'],148897792)
        self.assertEqual(evidence['derived']['aggregate_write_bytes'],67108864)
        self.assertEqual(evidence['derived']['window_write_bytes'],8388608)
        self.assertEqual(evidence['derived']['overlay_bytes'],67108864)

    def test_frame_receipt_after_libtest_progress_prefix(self):
        module=load('test-native-result-manifest')
        _,evidence=module.test_limit_environment({'FRAME_BYTES':1048576})
        progress='running 1 test\ntest smoke_tests::external_sql_checkpoint_capture ... '
        # Reservation metadata is reported by the Rust API, not inferred from
        # a guessed ABI size in this Python receipt verifier.
        reservation=5*67108864+24576
        receipt=('CAPTURE_FRAME_LIMITS frame_bytes=1048576 page_bytes=2097152 '
                 'aggregate_write_bytes=67108864 window_write_bytes=8388608 overlay_bytes=67108864 '
                 'max_pending_output_bytes=25165824 scan_page_bytes=16777216 '
                 f'queued_write_bytes={2*reservation} write_reservation_bytes={reservation} decoded_value_bytes=153272318 checkpoint_writer_decoded_bytes=4374526\n')
        # The helper's authoritative final-config writer allowance is reported,
        # not reconstructed in Python. Exactly A+C is sufficient; A alone fails.
        for old in (148897792,153272317):
            with self.assertRaisesRegex(ValueError,'did not honor'):
                module.verify_frame_limits(progress+'\n'+receipt.replace('decoded_value_bytes=153272318',f'decoded_value_bytes={old}'),evidence)
        for wrong in (0,sys.maxsize,sys.maxsize+1):
            with self.assertRaisesRegex(ValueError,'did not honor'):
                module.verify_frame_limits(progress+'\n'+receipt.replace('checkpoint_writer_decoded_bytes=4374526',f'checkpoint_writer_decoded_bytes={wrong}'),evidence)
        # The first diagnostic must start its own line after libtest's pending
        # progress text. The parser retains strict full-line receipts.
        module.verify_frame_limits(progress+'\n'+receipt,evidence)
        with self.assertRaisesRegex(ValueError,'not acknowledged'):
            module.verify_frame_limits(progress+receipt,evidence)
        with self.assertRaisesRegex(ValueError,'not acknowledged'):
            module.verify_frame_limits(progress+'\n'+receipt+receipt,evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('page_bytes=2097152','page_bytes=32768'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('decoded_value_bytes=153272318','decoded_value_bytes=16777216'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('decoded_value_bytes=153272318','decoded_value_bytes=145752064'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('max_pending_output_bytes=25165824','max_pending_output_bytes=524288'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('aggregate_write_bytes=67108864','aggregate_write_bytes=8388608'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('window_write_bytes=8388608','window_write_bytes=67108864'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace('overlay_bytes=67108864','overlay_bytes=8388608'),evidence)
        with self.assertRaisesRegex(ValueError,'did not honor'):
            module.verify_frame_limits(progress+'\n'+receipt.replace(f'queued_write_bytes={2*reservation}',f'queued_write_bytes={2*reservation-1}'),evidence)

    def test_matching_batch_boundaries_and_durable_offset_receipts(self):
        module=load('test-native-result-manifest')
        self.assertEqual(module.matching_boundaries(34,19,8),[8,16,19,27])
        self.assertEqual(module.matching_boundaries(25,19,1),list(range(1,25)))
        lines=[]
        for phase,rows in [('initial',[(1,8),(2,16),(3,19),(4,27)]),
                           ('checkpoint',[(1,8),(2,16),(3,19)]),('recovered',[(4,27)])]:
            lines.extend(f'CAPTURE_BATCH_BOUNDARY phase={phase} epoch={epoch} input_rows={row} saved_rows={row}\n'
                         for epoch,row in rows)
        log=''.join(lines)
        module.verify_batch_boundaries(log,34,19,8)
        for wrong in (log.replace('saved_rows=19','saved_rows=20'),log.replace('input_rows=27','input_rows=24'),
                      ''.join(lines[:-1]),''.join(reversed(lines)),log+lines[-1]):
            with self.assertRaisesRegex(ValueError,'durable source offsets'):
                module.verify_batch_boundaries(wrong,34,19,8)
        for args in ((19,19,8),(20,0,8),(20,19,0),(20,19,True)):
            with self.assertRaises(ValueError):module.matching_boundaries(*args)

    def test_manifest_limit_transport_clears_defaults_and_records_receipt(self):
        module=load('test-native-result-manifest')
        fixture=load('native_grouped_selectors')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            prepared=root/'fixture'
            fixture.prepare(prepared)
            manifest_path=prepared/'manifest.json'
            manifest=json.loads(manifest_path.read_text())
            binary=root/'binary'
            binary.write_text('test-only child is mocked to fail before SQL')
            for requested,name in (({},'default'),({'FRAME_BYTES':1048576},'explicit')):
                manifest['test_limits']=requested
                manifest_path.write_text(json.dumps(manifest))
                with patch.dict(os.environ,{'STREAMR_TEST_FRAME_BYTES':'999999','STREAMR_TEST_UNKNOWN':'stale'}):
                    with patch.object(module.capacity,'child_with_usage',return_value=(1,0)):
                        with self.assertRaisesRegex(RuntimeError,'capture failed'):
                            module.run(manifest_path,binary,root/name,'memory','controller',8,None)
                output=root/name/'memory-controller-batch8'
                env=json.loads((output/'capture-env.json').read_text())
                self.assertNotIn('STREAMR_TEST_UNKNOWN',env)
                self.assertEqual(env['STREAMR_CAPTURE_CHECKPOINT_EPOCH'],'3')
                if requested:self.assertEqual(env['STREAMR_TEST_FRAME_BYTES'],'1048576')
                else:self.assertNotIn('STREAMR_TEST_FRAME_BYTES',env)
                evidence=json.loads((output/'capture-limits.json').read_text())
                self.assertEqual(evidence['requested'],requested)
                inventory=json.loads((output/'inventory.json').read_text())
                self.assertEqual(inventory['test_limits']['sha256'],module.digest(output/'capture-limits.json'))
            # A child that ignores an explicit knob cannot produce a passing
            # receipt even if it claims a successful process exit.
            def ignores_limit(command,env,log,timeout):
                log.write_text('test-only mocked child omitted the frame acknowledgement\n')
                return 0,0
            with patch.object(module.capacity,'child_with_usage',side_effect=ignores_limit):
                with self.assertRaisesRegex(ValueError,'not acknowledged'):
                    module.run(manifest_path,binary,root/'ignored','memory','controller',8,None)


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
        # A later equal-sort peer is not interchangeable with the preserved
        # no-FOR first-write winner (9 versus the final peer's NULL).
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'equal-order.jsonl'
            wrong=json.loads(json.dumps(states[-1]))
            wrong['b']['latest']=None
            path.write_text(''.join(json.dumps(dict(before=None,after=row,op='c'))+'\n'
                                    for row in wrong.values()))
            with self.assertRaises(AssertionError):
                module.strict_reduce(path,states[-1],False)

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
        aggregates = load('test-native-aggregates')
        unordered = load('test-native-unordered-first-last')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'output.jsonl'
            for first, final, reduce in (
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
        unordered = load('test-native-unordered-first-last')
        aggregates = load('test-native-aggregates')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'checkpoint.jsonl'
            for checkpoint, future, reduce in (
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
