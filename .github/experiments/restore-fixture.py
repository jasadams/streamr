#!/usr/bin/env python3
"""Create and prewarm synthetic bounded local checkpoint objects."""
import hashlib
import json
import os
from pathlib import Path
import sys

root = Path(sys.argv[2])
source = root / 'source'
manifest = root / 'manifest.json'
if sys.argv[1] == 'create':
    sizes = json.loads(Path('.github/experiments/restore-sizes.json').read_text())
    assert len(sizes) == 194 and sum(sizes) == 503318140
    source.mkdir(parents=True, exist_ok=False)
    entries = []
    for i, size in enumerate(sizes):
        name = f'f{i:04}.sst'
        chunk = bytes([i % 251]) * 65536
        h = hashlib.sha256()
        with (source / name).open('xb') as output:
            remaining = size
            while remaining:
                part = chunk[:min(remaining, len(chunk))]
                output.write(part)
                h.update(part)
                remaining -= len(part)
        entries.append([name, size, list(h.digest())])
    manifest.write_text(json.dumps(entries))
elif sys.argv[1] == 'warm':
    entries = json.loads(manifest.read_text())
    total = 0
    for name, size, checksum in entries:
        h = hashlib.sha256()
        with (source / name).open('rb') as stream:
            while block := stream.read(1024 * 1024):
                h.update(block)
                total += len(block)
        assert list(h.digest()) == checksum
    assert total == 503318140
    print(f'prewarmed and verified {len(entries)} files, {total} bytes', flush=True)
else:
    raise ValueError('expected create or warm')
