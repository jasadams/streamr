"""Exact row comparison with streaming input and an owned disk-backed multiset.

Memory scales with the largest row, the parser chunk and the SQLite cache, not
with dataset cardinality. No row-size admission limit is imposed. Canonical JSON
preserves the existing oracle's types, nulls, array order and duplicate-field
last-key-wins behavior; object field order is ignored.
"""
import hashlib
import itertools
import json
from pathlib import Path
import sqlite3
import tempfile

CHUNK_CHARS = 65536
CACHE_KIB = 8192


def token(row):
    if not isinstance(row, dict):
        raise ValueError('output oracle rows must be complete JSON objects')
    return json.dumps(row, sort_keys=True, separators=(',', ':'), allow_nan=False)


def iter_array(path):
    """Incremental strict array grammar with current/next-row overlap bounded by row size."""
    decoder = json.JSONDecoder()  # Same duplicate-field behavior as json.loads.
    with Path(path).open(encoding='utf-8') as stream:
        buffer, position, eof = '', 0, False
        def refill():
            nonlocal buffer, position, eof
            buffer = buffer[position:] + stream.read(CHUNK_CHARS)
            position = 0
            if not buffer:
                eof = True
        def character():
            nonlocal position
            while True:
                while position < len(buffer) and buffer[position] in ' \t\r\n':
                    position += 1
                if position < len(buffer):
                    return buffer[position]
                if eof:
                    return None
                refill()
        if character() != '[':
            raise ValueError('expected must be a JSON array of complete rows')
        position += 1
        if character() == ']':
            position += 1
        else:
            while True:
                character()
                while True:
                    try:
                        row, end = decoder.raw_decode(buffer, position)
                        break
                    except json.JSONDecodeError:
                        if eof:
                            raise ValueError('invalid/truncated JSON oracle row')
                        tail = buffer[position:]
                        added = stream.read(CHUNK_CHARS)
                        buffer, position = tail + added, 0
                        if not added:
                            eof = True
                # raw_decode can succeed before a scalar delimiter arrives;
                # complete rows are objects and require their closing brace.
                token(row)  # Reject non-object/nonfinite values before SQL launch.
                position = end
                yield row
                delimiter = character()
                if delimiter == ']':
                    position += 1
                    break
                if delimiter != ',':
                    raise ValueError('JSON array requires a comma or closing bracket')
                position += 1
                if character() in (']', None):
                    raise ValueError('JSON array has trailing comma or missing row')
        if character() is not None:
            raise ValueError('trailing data after JSON oracle array')


class ArrayRows:
    """Reiterable array oracle retaining only its path and validated row count."""
    def __init__(self, path):
        self.path = Path(path)
        self.count = sum(1 for _ in iter_array(self.path))

    def __len__(self):
        return self.count

    def __iter__(self):
        return iter_array(self.path)


def iter_jsonl(path, prefix_rows=None):
    with Path(path).open(encoding='utf-8') as stream:
        # islice stops before reading the next row of a committed prefix.
        lines = stream if prefix_rows is None else itertools.islice(stream, prefix_rows)
        for line in lines:
            row = json.loads(line)
            token(row)
            yield row


def expected_rows(path):
    path = Path(path)
    yield from iter_jsonl(path) if path.suffix == '.jsonl' else iter_array(path)


def exact_rows(path, expected, prefix_rows=None, ordered=False):
    """Check complete rows, including duplicate multiplicity, without row history.

    The comparison owns its SQLite files beneath the output directory and closes
    its connection before removing them on success or on any exception. Ordered
    comparisons stream both sides without creating a database.
    """
    path = Path(path)
    actual_rows = iter_jsonl(path, prefix_rows)
    wanted_rows = iter(expected)
    actual = (token(row) for row in actual_rows)
    wanted = (token(row) for row in wanted_rows)
    try:
        if ordered:
            for index, (left, right) in enumerate(itertools.zip_longest(actual, wanted)):
                if left != right:
                    raise AssertionError(f'{path}: wrong/missing/extra row at {index}')
            return
        with tempfile.TemporaryDirectory(prefix='exact-rows-owned-', dir=path.parent) as temporary:
            connection = sqlite3.connect(Path(temporary) / 'counts.sqlite')
            try:
                connection.execute(f'PRAGMA cache_size=-{CACHE_KIB}')
                connection.execute('PRAGMA temp_store=FILE')
                connection.execute('PRAGMA mmap_size=0')
                connection.execute('PRAGMA journal_mode=DELETE')
                connection.execute('CREATE TABLE counts (value TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID')
                for value in wanted:
                    connection.execute('INSERT INTO counts VALUES (?,1) ON CONFLICT(value) DO UPDATE SET n=n+1', (value,))
                connection.commit()
                for value in actual:
                    changed = connection.execute('UPDATE counts SET n=n-1 WHERE value=? AND n>0', (value,)).rowcount
                    if changed != 1:
                        digest = hashlib.sha256(value.encode()).hexdigest()
                        raise AssertionError(f'{path}: unexpected/duplicate/wrong complete row sha256={digest}')
                if connection.execute('SELECT 1 FROM counts WHERE n!=0 LIMIT 1').fetchone():
                    raise AssertionError(f'{path}: missing complete expected row or multiplicity')
            finally:
                connection.close()
    finally:
        actual.close()
        wanted.close()
        actual_rows.close()
        close_expected = getattr(wanted_rows, 'close', None)
        if close_expected is not None:
            close_expected()
