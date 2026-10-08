"""Read-only native format-2 checkpoint object inventory, independent of SQL oracles."""
import argparse
import hashlib
import importlib
import json
from pathlib import Path
import re
import sys


# Cargo.lock object_store 0.12.3: src/path/parts.rs INVALID, then
# Path::from(str). LocalFileSystem URL conversion preserves these escaped
# components as literal filesystem names; ordinary URL quoting is different.
OBJECT_KEY_INVALID = set(b'/\\{}^%`[]"<>#|~*?')
METADATA_BYTES = 3 * 1024 * 1024
MAX_FILES = 65536
PARQUET_MAX_BYTES = 2 * 1024 * 1024
PARQUET_MAX_ROWS = 4096


def object_component(value):
    raw = value.encode('utf-8')
    if raw in (b'.', b'..'):
        return '%2E' * len(raw)
    return ''.join(f'%{byte:02X}' if byte < 32 or byte >= 127
                   or byte in OBJECT_KEY_INVALID else chr(byte) for byte in raw)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def component(value):
    require(bool(value) and value not in ('.', '..')
            and '/' not in value and '\\' not in value
            and all(ord(c) >= 32 and ord(c) != 127 for c in value),
            'identity must be a single non-control path component')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-log', type=Path, required=True)
    parser.add_argument('--storage-root', type=Path, required=True)
    parser.add_argument('--bindings', type=Path, required=True)
    parser.add_argument('--operator', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--epoch', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'preserve existing inventory')
    require(0 < args.epoch <= 0xffffffff, 'epoch must fit positive u32')
    component(args.operator)
    component(args.table)
    sys.path.insert(0, str(args.bindings.resolve(strict=True)))
    rpc = importlib.import_module('rpc_pb2')
    storage = args.storage_root.resolve(strict=True)

    def physical(relative):
        parts = relative.split('/')
        require(parts and all(part and part not in ('.', '..') for part in parts)
                and '\0' not in relative, 'checkpoint reference must be canonical and relative')
        encoded = Path(*(object_component(part) for part in parts))
        result = (storage / encoded).resolve()
        require(result.is_relative_to(storage), 'checkpoint reference escapes storage')
        return result

    def stored(relative):
        result = physical(relative)
        require(result.is_file(), 'checkpoint reference is not a file')
        return result

    def metadata_bytes(path):
        require(0 < path.stat().st_size <= METADATA_BYTES, 'metadata exceeds unchanged three-MiB cap or is empty')
        return path.read_bytes()

    matches = []
    with args.capture_log.open() as log:
        for line in log:
            match = re.match(r'^CAPTURE_CHECKPOINT path=(.*?) metadata=', line)
            if match:
                matches.append(match.group(1))
    require(len(matches) == 1, 'expected exactly one selected checkpoint')
    selected_ref = matches[0]
    selected = stored(selected_ref)
    selected_bytes = metadata_bytes(selected)
    manifest = None
    checkpoint = None
    if selected_ref.endswith('/checkpoint-manifest.pb'):
        manifest = rpc.CheckpointManifest.FromString(selected_bytes)
        require(manifest.epoch == args.epoch, 'manifest epoch differs')
        pipeline = component(manifest.pipeline_id)
        job = component(manifest.job_id)
        generation = manifest.generation
        base = f'{pipeline}/{job}/generations/{generation}/checkpoints/checkpoint-{args.epoch:07}'
        require(selected_ref == base + '/checkpoint-manifest.pb', 'manifest identity differs from selected path')
        candidates = [op for op in manifest.operators
                      if op.operator_metadata.operator_id == args.operator]
        require(len(candidates) == 1, 'selected manifest owner missing or duplicated')
        op = candidates[0]
    else:
        require(selected_ref.endswith('/metadata'), 'unknown selected metadata filename')
        checkpoint = rpc.CheckpointMetadata.FromString(selected_bytes)
        job = component(checkpoint.job_id)
        generation = 0
        base = f'{job}/checkpoints/checkpoint-{args.epoch:07}'
        require(checkpoint.epoch == args.epoch and selected_ref == base + '/metadata',
                'controller metadata identity differs from selected path/epoch')
        require(list(checkpoint.operator_ids).count(args.operator) == 1,
                'selected controller checkpoint does not uniquely reference operator')
        metadata = stored(base + '/operator-' + args.operator + '/metadata')
        op = rpc.OperatorCheckpointMetadata.FromString(metadata_bytes(metadata))
    owner = op.operator_metadata
    require(owner.operator_id == args.operator and owner.epoch == args.epoch
            and owner.parallelism == 1 and owner.job_id == job,
            'operator ownership/epoch/fixed parallelism differs')
    require(op.ByteSize() <= METADATA_BYTES, 'operator metadata exceeds cap')
    require(args.table in op.table_configs and args.table in op.table_checkpoint_metadata,
            'selected table configuration or metadata missing')
    config = op.table_configs[args.table]
    table = op.table_checkpoint_metadata[args.table]
    require(config.table_type == rpc.DiskKeyedMap and table.table_type == rpc.DiskKeyedMap,
            'inventory requires native disk-keyed transport')
    declared = rpc.DiskKeyedTableConfig.FromString(config.config)
    require(declared.table_name == args.table and declared.encoding_version == 1
            and 0 < len(declared.schema_identity) <= 1024,
            'table schema/encoding contract missing or exceeds construction limit')
    disk = rpc.DiskKeyedTableTaskCheckpointMetadata.FromString(table.data)
    require(disk.format_version == 2 and set(disk.subtasks) == {0},
            'expected format-2 singleton owner')
    sub = disk.subtasks[0]
    table_name = args.table.encode('utf-8')
    namespace = b'\x01\x00' + (0).to_bytes(4, 'big') + (1).to_bytes(4, 'big')
    namespace += len(table_name).to_bytes(4, 'big') + table_name
    require(len(namespace) <= 512, 'namespace exceeds unchanged construction limit')
    require(sub.format_version == 2 and sub.subtask_index == 0
            and sub.encoding_version == declared.encoding_version
            and sub.schema_identity == declared.schema_identity
            and sub.epoch == args.epoch and sub.namespace == namespace
            and sub.generation == generation,
            'subtask identity/encoding/namespace/epoch/generation differs')
    require(sub.ByteSize() <= METADATA_BYTES and len(sub.files) <= MAX_FILES,
            'subtask checkpoint exceeds unchanged metadata/file-count limits')
    require(sub.empty == (len(sub.files) == 0), 'empty discriminator differs from file inventory')
    table_ref = base + '/operator-' + args.operator + '/table-' + args.table + '-000'
    table_dir = physical(table_ref)
    refs = set()
    object_paths = set()
    files = []
    for record in sub.files:
        parent, _, filename = record.path.rpartition('/')
        require(parent == table_ref and filename.startswith('disk-') and filename.endswith('.parquet'),
                'object reference has another owner or format')
        path = stored(record.path)
        require(path.parent == table_dir, 'physical object reference has another owner')
        require(record.path not in refs and path not in object_paths, 'duplicate object reference')
        refs.add(record.path)
        object_paths.add(path)
        require(0 < record.size_bytes <= PARQUET_MAX_BYTES
                and 0 < record.row_count <= PARQUET_MAX_ROWS
                and len(record.checksum) == 32, 'invalid format-2 object size/rows/checksum')
        require(path.stat().st_size == record.size_bytes, 'object size differs')
        checksum = digest(path)
        require(checksum == record.checksum.hex(), 'object checksum differs')
        files.append(dict(path=record.path, bytes=record.size_bytes,
                          rows=record.row_count, sha256=checksum))
    actual = set()
    if table_dir.exists():
        require(table_dir.is_dir(), 'checkpoint table path is not a directory')
        for path in table_dir.iterdir():
            require(path.is_file(), 'unexpected child directory in checkpoint table')
            actual.add(path.resolve(strict=True))
    require(object_paths == actual, 'actual checkpoint table objects differ from references')
    result = dict(status='passed', selected_path=selected_ref, selected_sha256=hashlib.sha256(selected_bytes).hexdigest(),
                  operator=args.operator, table=args.table, epoch=args.epoch,
                  job_id=owner.job_id, generation=sub.generation,
                  namespace_hex=sub.namespace.hex(), schema_identity_hex=sub.schema_identity.hex(),
                  format_version=2, selected_metadata_bytes=len(selected_bytes),
                  operator_metadata_bytes=op.ByteSize(), table_metadata_bytes=len(table.data),
                  file_count=len(files), object_bytes=sum(f['bytes'] for f in files),
                  declared_snapshot_encoded_rows=sum(f['rows'] for f in files), files=files,
                  capture_log_sha256=digest(args.capture_log),
                  bindings_sha256=digest(Path(rpc.__file__)), helper_sha256=digest(Path(__file__)),
                  scope='Selected owner lineage, exact object inventory, sizes and checksums; row counts are declared metadata only, not decoded Parquet, logical payload, live state, SQL values, or production recovery')
    with args.output.open('x') as output:
        output.write(json.dumps(result, indent=2, sort_keys=True)+'\n')
    print(json.dumps({key:value for key,value in result.items() if key != 'files'}, sort_keys=True))


if __name__ == '__main__':
    main()
