#!/usr/bin/env python3
"""Pure Python tests for exact streaming row comparison; no runtime qualification."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

import bounded_row_oracle as oracle


class ExactOracle(unittest.TestCase):
    def compare(self, expected, actual, passes=True, prefix=None, ordered=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wanted, got = root/'expected.json', root/'actual.jsonl'
            wanted.write_text(expected if isinstance(expected, str) else json.dumps(expected))
            got.write_text(actual if isinstance(actual, str) else ''.join(json.dumps(row)+'\n' for row in actual))
            rows = oracle.ArrayRows(wanted)
            if passes:
                oracle.exact_rows(got, rows, prefix, ordered)
            else:
                with self.assertRaises((AssertionError, ValueError)):
                    oracle.exact_rows(got, rows, prefix, ordered)
            self.assertEqual(list(root.glob('exact-rows-owned-*')), [])

    def test_types_null_nested_order_and_nanoseconds(self):
        row = {'x': 1, 'nested': [None, True, 1.0, {'z': 'é', 'a': 2}],
               'timestamp': '2023-10-09T17:13:20.000000001'}
        self.compare([row], [dict(reversed(list(row.items())))])
        for value in (True, 1.0, None, '1'):
            self.compare([row], [dict(row, x=value)], False)
        self.compare([row], [dict(row, nested=list(reversed(row['nested'])))], False)
        self.compare([row], [dict(row, timestamp='2023-10-09T17:13:20.000000002')], False)
        self.compare([{'x': None}], [{}], False)
        for value in (True, 1.0):
            self.compare([{'x': [1]}], [{'x': [value]}], False)

    def test_duplicates_last_field_wins_and_prefix(self):
        self.compare([{'x': 1}]*2, [{'x': 1}]*2)
        self.compare([{'x': 1}]*2, [{'x': 1}], False)
        self.compare([{'x': 1}], [{'x': 1}]*2, False)
        self.compare('[{"x":0,"x":1}]', '{"x":8,"x":1}\n')
        self.compare([{'x': 1}], '{"x":1}\nmalformed suffix is outside prefix\n', prefix=1)
        self.compare([], 'malformed suffix\n', prefix=0)
        self.compare([{'x': 1}], [], False, prefix=1)

    def test_ordered_stream_preserves_order_and_length(self):
        rows = [{'x': 1}, {'x': 2}]
        self.compare(rows, rows, ordered=True)
        self.compare(rows, list(reversed(rows)), False, ordered=True)
        self.compare(rows, rows[:1], False, ordered=True)
        self.compare(rows[:1], rows, False, ordered=True)
        self.compare(rows, list(reversed(rows)))

    def test_strict_array_grammar_at_chunk_boundaries(self):
        valid = ' \n [ {"x":"é\\\"\\\\x","a":[1,null,true]}, {"x":{}} ] \t'
        with patch.object(oracle, 'CHUNK_CHARS', 1):
            self.compare(valid, [{'x': 'é"\\x', 'a': [1, None, True]}, {'x': {}}])
            for bad in ('', '[', '[{"x":1},]', '[{"x":1}', '{"x":1}', '[] garbage',
                        '[1]', '[{} {}]', '[{},', '[{"x":NaN}]', '[{"x":Infinity}]'):
                with self.subTest(bad=bad), tempfile.TemporaryDirectory() as temporary:
                    path = Path(temporary)/'bad.json'
                    path.write_text(bad)
                    with self.assertRaises(ValueError):
                        oracle.ArrayRows(path)
        self.compare([], [])
        self.compare([{'x': 1}], '{"x":NaN}\n', False)
        self.compare([{'x': 1}], '\n', False)

    def test_sqlite_settings_and_cleanup_after_database_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            got = root/'actual.jsonl'
            got.write_text('{"x":1}\n')
            real_connect = sqlite3.connect
            statements = []
            closed = []

            class BrokenConnection:
                def __init__(self, *args, **kwargs):
                    self.connection = real_connect(*args, **kwargs)

                def execute(self, sql, *args):
                    statements.append(sql)
                    if sql.startswith('INSERT'):
                        raise sqlite3.OperationalError('injected disk error')
                    return self.connection.execute(sql, *args)

                def close(self):
                    self.connection.close()
                    closed.append(True)

            with patch.object(oracle.sqlite3, 'connect', BrokenConnection):
                with self.assertRaises(sqlite3.OperationalError):
                    oracle.exact_rows(got, [{'x': 1}])
            self.assertEqual(closed, [True])
            self.assertIn('PRAGMA cache_size=-8192', statements)
            self.assertIn('PRAGMA temp_store=FILE', statements)
            self.assertIn('PRAGMA mmap_size=0', statements)
            self.assertEqual(list(root.glob('exact-rows-owned-*')), [])

    def test_array_jsonl_and_large_dataset_do_not_retain_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wanted, got = root/'expected.json', root/'actual.jsonl'
            # 16 MiB of payload; write rows individually and trace only comparison.
            with wanted.open('w') as expected, got.open('w') as actual:
                expected.write('[')
                for index in range(2048):
                    row = json.dumps({'id': index, 'payload': 'é'*4096}, ensure_ascii=False)
                    expected.write((',' if index else '')+row)
                    actual.write(row+'\n')
                expected.write(']')
            tracemalloc.start()
            try:
                with patch.object(Path, 'read_text', side_effect=AssertionError('whole-file read forbidden')):
                    lazy = oracle.ArrayRows(wanted)
                    self.assertEqual(len(lazy), 2048)
                    oracle.exact_rows(got, lazy)
                    oracle.exact_rows(got, oracle.expected_rows(got), ordered=True)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertLess(peak, 2*1024*1024, 'Python memory must not retain the 16 MiB dataset')
            # Change only the final payload character: whole values must matter.
            with got.open('a') as stream:
                stream.write(json.dumps({'id': 2047, 'payload': 'é'*4095+'z'})+'\n')
            with self.assertRaises(AssertionError):
                oracle.exact_rows(got, lazy)
            self.assertEqual(list(root.glob('exact-rows-owned-*')), [])

    def test_bundle_copies_and_hashes_helper_and_runs_without_scripts_mount(self):
        scripts = Path(__file__).parent
        spec = importlib.util.spec_from_file_location('bundle', scripts/'sql-capture-bundle.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary, loader = root/'test-elf', root/'ld-linux-test.so'
            binary.write_bytes(b'\x7fELF fake trusted test input')
            loader.write_bytes(b'fake loader; never executed')
            fixture = root/'manifest.json'
            manifest = dict(query='query.sql', input='input.jsonl', expected='expected.json',
                            expected_checkpoint='checkpoint.json', checkpoint_input_rows=1)
            fixture.write_text(json.dumps(manifest))
            (root/'query.sql').write_text("SELECT '{{INPUT}}', '{{OUTPUT}}'")
            (root/'input.jsonl').write_text('{}\n{}\n')
            (root/'expected.json').write_text('[]')
            (root/'checkpoint.json').write_text('[]')
            provenance = root/'provenance.json'
            provenance.write_text('{}')
            bundle = root/'bundle'
            args = SimpleNamespace(binary=binary, fixture=fixture, directory=bundle,
                                   provenance=provenance, route='native-windows')
            fake_ldd = SimpleNamespace(stdout=f'{loader} (0x0000)\n')
            with patch.object(module.subprocess, 'run', return_value=fake_ldd):
                module.package(args)
            captured = json.loads((bundle/'bundle.json').read_text())
            self.assertEqual(captured['files']['bounded_row_oracle.py'],
                             module.sha256(scripts/'bounded_row_oracle.py'))
            result = subprocess.run([sys.executable, '-B', str(bundle/'run.py'), '--help'],
                                    cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_public_adapters_use_shared_comparison(self):
        scripts = Path(__file__).parent
        for filename, function in [('test-native-session-manifest.py', 'exact_rows'),
                                   ('sql-capture-bundle.py', 'exact')]:
            spec = importlib.util.spec_from_file_location(filename, scripts/filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                wanted, got = root/'expected.json', root/'actual.jsonl'
                wanted.write_text('[{"x":1},{"x":1}]')
                got.write_text('{"x":1}\n')
                with self.assertRaises(AssertionError):
                    if function == 'exact':
                        module.exact(got, wanted, False)
                    else:
                        module.exact_rows(got, oracle.ArrayRows(wanted))


if __name__ == '__main__':
    unittest.main()
