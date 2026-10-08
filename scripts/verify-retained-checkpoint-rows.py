#!/usr/bin/env python3
"""Read-only checkpoint measurement: exact caller-projected retained IPC rows.

Writes a fresh SQLite/result directory; never modifies checkpoint/oracle inputs.
No SQL, worker recovery, live-RSS, or application lifecycle qualification follows.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

sys.dont_write_bytecode = True
MAX_FILE = 2 * 1024 * 1024
MAX_ROWS = 4096
MAX_IPC = 32 * 1024
MAX_LINE = 128 * 1024
MAX_METADATA = 3 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate JSON field')
        value[key] = item
    return value


def token(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def load_json(path):
    require(Path(path).stat().st_size <= MAX_METADATA, 'declaration exceeds metadata cap')
    return json.loads(Path(path).read_text(), object_pairs_hook=unique_object)


def raw_key(encoded, namespace):
    require(type(encoded) is bytes and encoded.startswith(namespace),
            'checkpoint key has wrong namespace')
    suffix = encoded[len(namespace):]
    raw = bytearray()
    index = 0
    while index < len(suffix):
        byte = suffix[index]
        index += 1
        if byte:
            raw.append(byte)
        else:
            require(index < len(suffix), 'unterminated key escape')
            escape = suffix[index]
            index += 1
            if escape == 0:
                require(index == len(suffix), 'partition-local key has trailing bytes')
                return bytes(raw)
            require(escape == 255, 'unsupported key escape')
            raw.append(0)
    raise ValueError('partition-local key has no terminator')


def declarations(spec, pa):
    fields = spec['fields']
    require(type(fields) is list and 0 < len(fields) <= 32, 'field declaration limit')
    result = {}
    integers = {f'{kind}{bits}': (getattr(pa, f'{kind}{bits}')(), bits, kind == 'int')
                for kind in ('int', 'uint') for bits in (8, 16, 32, 64)}
    for field in fields:
        require(type(field) is dict and set(field) <= {'name', 'type', 'nullable', 'timezone'},
                'unsupported field declaration')
        name, kind = field['name'], field['type']
        require(type(name) is str and name and name not in result and len(name) <= 256,
                'invalid/duplicate field name')
        require(type(field['nullable']) is bool, 'nullable must be explicitly boolean')
        require(kind in integers or kind in ('utf8', 'large_utf8', 'bool', 'timestamp_ns'),
                'unsupported projected type')
        require(kind == 'timestamp_ns' or 'timezone' not in field, 'timezone only applies to timestamp_ns')
        if kind in integers:
            arrow_type = integers[kind][0]
        elif kind == 'timestamp_ns':
            timezone = field.get('timezone')
            require(timezone is None or type(timezone) is str, 'invalid timezone declaration')
            arrow_type = pa.timestamp('ns', tz=timezone)
        else:
            require('timezone' not in field, 'timezone only applies to timestamp_ns')
            arrow_type = {'utf8': pa.string(), 'large_utf8': pa.large_string(),
                          'bool': pa.bool_()}[kind]
        result[name] = (field, arrow_type)
    identities, payloads = spec['identity_columns'], spec['utf8_payload_columns']
    for values in (identities, payloads):
        require(type(values) is list and len(values) == len(set(values)), 'duplicate column selection')
        require(all(type(name) is str and name in result for name in values), 'undeclared selected column')
    require(bool(identities), 'unique identity columns required')
    require(bool(payloads) and all(result[name][0]['type'] in ('utf8', 'large_utf8')
                                  for name in payloads), 'payload columns must be UTF8')
    for name in identities:
        require(not result[name][0]['nullable'], 'identity fields must be declared nonnullable')
    for name in ('expected_rows', 'minimum_utf8_bytes'):
        require(type(spec[name]) is int and spec[name] > 0, f'{name} must be positive integer')
    return result, integers


def typed_row(row, fields, integers):
    require(type(row) is dict and set(row) == set(fields), 'oracle projection differs from declared fields')
    for name, (field, _) in fields.items():
        value, kind = row[name], field['type']
        if value is None:
            require(field['nullable'], 'null in nonnullable projected field')
            continue
        if kind in ('utf8', 'large_utf8'):
            require(type(value) is str, 'UTF8 expected value is not a string')
            value.encode('utf-8', 'strict')
        elif kind == 'bool':
            require(type(value) is bool, 'boolean expected value has wrong type')
        else:
            require(type(value) is int, 'integer/timestamp_ns expected value has wrong type')
            bits, signed = (64, True) if kind == 'timestamp_ns' else integers[kind][1:]
            low, high = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if signed else (0, (1 << bits) - 1)
            require(low <= value <= high, 'integer/timestamp_ns value out of range')
    return row


def identity(row, spec):
    return token([row[name] for name in spec['identity_columns']])


def payload_bytes(row, spec):
    return sum(len(row[name].encode('utf-8')) for name in spec['utf8_payload_columns']
               if row[name] is not None)


def measure(args, directory):
    import pyarrow as pa
    import pyarrow.parquet as pq
    require(pa.__version__ == '19.0.1', 'requires pinned PyArrow19.0.1')
    dependency_files = {str(Path(module.__file__).resolve()): digest(Path(module.__file__))
                        for module in (pa, pa.lib)}
    inventory, spec = load_json(args.inventory), load_json(args.projection)
    inputs = {'inventory': digest(args.inventory), 'projection': digest(args.projection),
              'expected': digest(args.expected), 'inventory_helper': digest(args.inventory_helper),
              'measurement_helper': digest(Path(__file__))}
    require(inventory['status'] == 'passed' and inventory['format_version'] == 2,
            'reviewed format-2 inventory must have passed')
    require(inputs['inventory_helper'] == inventory['helper_sha256'], 'inventory helper hash differs')
    module_spec = importlib.util.spec_from_file_location('checkpoint_inventory', args.inventory_helper)
    helper = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(helper)
    storage = args.storage_root.resolve(strict=True)
    def physical(reference):
        require(type(reference) is str, 'reference must be string')
        parts = reference.split('/')
        require(all(part and part not in ('.', '..') and '\x00' not in part for part in parts),
                'invalid object reference')
        path = (storage / Path(*(helper.object_component(part) for part in parts))).resolve(strict=True)
        require(path.is_relative_to(storage) and path.is_file(), 'object escapes storage or is not a file')
        return path
    selected = physical(inventory['selected_path'])
    require(digest(selected) == inventory['selected_sha256'], 'selected checkpoint metadata changed')
    table = inventory['table'].encode('utf-8')
    namespace = bytes.fromhex(inventory['namespace_hex'])
    require(namespace == b'\x01\x00' + bytes(4) + (1).to_bytes(4, 'big')
            + len(table).to_bytes(4, 'big') + table, 'requires exact singleton partition-local namespace')
    prefix = bytes.fromhex(args.row_key_prefix_hex)
    require(0 < len(prefix) <= 512, 'nonempty opaque logical key prefix required')
    fields, integers = declarations(spec, pa)
    db = sqlite3.connect(directory / 'bookkeeping.sqlite')
    db.execute('PRAGMA cache_size=-8192')
    db.execute('PRAGMA mmap_size=0')
    db.execute('PRAGMA temp_store=FILE')
    db.execute('PRAGMA journal_mode=DELETE')
    db.execute('PRAGMA synchronous=NORMAL')
    db.execute('CREATE TABLE expected (identity TEXT PRIMARY KEY, row TEXT NOT NULL, seen INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID')
    db.execute('CREATE TABLE state_keys (key BLOB PRIMARY KEY) WITHOUT ROWID')
    expected_count = expected_bytes = 0
    with args.expected.open('rb') as stream:
        while line := stream.readline(MAX_LINE + 1):
            require(len(line) <= MAX_LINE, 'oracle row exceeds bounded line limit')
            row = typed_row(json.loads(line, object_pairs_hook=unique_object), fields, integers)
            db.execute('INSERT INTO expected(identity,row) VALUES (?,?)', (identity(row, spec), token(row)))
            expected_count += 1
            expected_bytes += payload_bytes(row, spec)
            if expected_count % 1024 == 0:
                db.commit()
    db.commit()
    require(expected_count == spec['expected_rows'], 'declared expected row count differs')
    require(expected_bytes >= spec['minimum_utf8_bytes'], 'expected raw UTF8 premise below declared minimum')
    files = inventory['files']
    require(type(files) is list and len(files) == inventory['file_count'] <= 65536,
            'file inventory count differs/exceeds bound')
    transport_rows = retained_rows = retained_bytes = 0
    schema = None
    refs = set()
    for record in files:
        require(record['path'] not in refs, 'duplicate file reference')
        refs.add(record['path'])
        path = physical(record['path'])
        require(type(record['bytes']) is int and 0 < record['bytes'] <= MAX_FILE
                and path.stat().st_size == record['bytes'], 'Parquet file size differs/exceeds2MiB')
        require(type(record['rows']) is int and 0 < record['rows'] <= MAX_ROWS,
                'Parquet declared row count exceeds4096')
        require(digest(path) == record['sha256'], 'Parquet checksum differs')
        parquet = pq.ParquetFile(path, memory_map=False, pre_buffer=False)
        require(parquet.schema_arrow == pa.schema([pa.field('key', pa.binary(), nullable=False),
                                                  pa.field('value', pa.binary(), nullable=False)]),
                'unexpected checkpoint transport schema')
        require(parquet.metadata.num_rows == record['rows'], 'actual Parquet row count differs')
        for index in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(index)
            require(sum(group.column(i).total_uncompressed_size for i in range(group.num_columns)) <= MAX_FILE,
                    'decoded Parquet row group exceeds2MiB measurement bound')
        file_rows = 0
        for batch in parquet.iter_batches(batch_size=32, columns=['key', 'value'], use_threads=False):
            require(batch.nbytes <= MAX_FILE, 'decoded Parquet batch exceeds2MiB')
            for index in range(batch.num_rows):
                require(batch.column(0)[index].is_valid and batch.column(1)[index].is_valid,
                        'null transport key/value')
                encoded = batch.column(0)[index].as_py()
                key = raw_key(encoded, namespace)
                db.execute('INSERT INTO state_keys VALUES (?)', (encoded,))
                file_rows += 1
                transport_rows += 1
                if not key.startswith(prefix):
                    continue
                value = batch.column(1)[index].as_py()
                require(0 < len(value) <= MAX_IPC, 'selected IPC row exceeds32KiB')
                buffer = pa.BufferReader(value)
                reader = pa.ipc.open_stream(buffer)
                if schema is None:
                    schema = reader.schema
                    for name, (field, arrow_type) in fields.items():
                        matches = reader.schema.get_all_field_indices(name)
                        require(len(matches) == 1, 'projected field absent/ambiguous')
                        actual = reader.schema.field(matches[0])
                        require(actual.type == arrow_type and actual.nullable == field['nullable'],
                                'projected Arrow type/nullability differs')
                require(reader.schema.equals(schema, check_metadata=True), 'retained IPC schema changed')
                row_batch = reader.read_next_batch()
                require(row_batch.num_rows == 1 and row_batch.nbytes <= MAX_IPC,
                        'retained IPC must contain one bounded row')
                try:
                    reader.read_next_batch()
                except StopIteration:
                    pass
                else:
                    raise ValueError('retained IPC contains an extra batch')
                require(buffer.tell() == len(value), 'retained IPC has trailing bytes')
                row = {}
                for name, (field, _) in fields.items():
                    scalar = row_batch.column(schema.get_field_index(name))[0]
                    row[name] = (None if not scalar.is_valid else scalar.value
                                 if field['type'] == 'timestamp_ns' else scalar.as_py())
                typed_row(row, fields, integers)
                key_id = identity(row, spec)
                expected = db.execute('SELECT row,seen FROM expected WHERE identity=?', (key_id,)).fetchone()
                require(expected is not None and expected[1] == 0, 'unexpected/duplicate retained identity')
                require(expected[0] == token(row), 'complete retained projected row differs')
                db.execute('UPDATE expected SET seen=1 WHERE identity=?', (key_id,))
                retained_rows += 1
                retained_bytes += payload_bytes(row, spec)
                if transport_rows % 1024 == 0:
                    db.commit()
        require(file_rows == record['rows'], 'decoded file row count differs')
        parquet.close()
        require(digest(path) == record['sha256'], 'Parquet changed during decoding')
        db.commit()
    require(transport_rows == inventory['declared_snapshot_encoded_rows'], 'actual transport total differs')
    require(retained_rows == expected_count and db.execute('SELECT COUNT(*) FROM expected WHERE seen=0').fetchone()[0] == 0,
            'missing retained expected rows')
    require(retained_bytes == expected_bytes and retained_bytes >= spec['minimum_utf8_bytes'],
            'actual retained UTF8 floor differs/is insufficient')
    db.close()
    for name, path in [('inventory', args.inventory), ('projection', args.projection), ('expected', args.expected),
                       ('inventory_helper', args.inventory_helper), ('measurement_helper', Path(__file__))]:
        require(digest(path) == inputs[name], 'input/helper changed during measurement')
    require(digest(selected) == inventory['selected_sha256'], 'selected metadata changed during measurement')
    for path, expected in dependency_files.items():
        require(digest(Path(path)) == expected, 'PyArrow dependency changed during measurement')
    return dict(status='passed', inputs_sha256=inputs, pyarrow_version=pa.__version__,
                dependency_files_sha256=dependency_files, namespace_hex=namespace.hex(), row_key_prefix_hex=prefix.hex(),
                transport_rows=transport_rows, retained_rows=retained_rows,
                retained_utf8_bytes=retained_bytes, declared_minimum_utf8_bytes=spec['minimum_utf8_bytes'],
                projected_fields=spec['fields'], identity_columns=spec['identity_columns'],
                counted_utf8_columns=spec['utf8_payload_columns'],
                ipc_schema_sha256=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
                sqlite_cache_limit_bytes=8*1024*1024,
                scope='Exact projected retained checkpoint rows and actual selected UTF8 bytes only; excludes schema/IPC/key/index overhead and copies; not live-state/RSS/recovery/lifecycle qualification')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('inventory', 'inventory-helper', 'storage-root', 'projection', 'expected', 'directory'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--row-key-prefix-hex', required=True)
    args = parser.parse_args()
    require(not args.directory.exists(), 'preserve existing proof directory')
    args.directory.mkdir(parents=True)
    try:
        result = measure(args, args.directory)
    except Exception as error:
        result = dict(status='failed', error_type=type(error).__name__, error=str(error),
                      scope='No retained-row qualification; evidence preserved')
        (args.directory/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        raise
    (args.directory/'result.json').write_text(json.dumps(result, indent=2, sort_keys=True)+'\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
