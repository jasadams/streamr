#!/usr/bin/env python3
import json
import sys

matches = []
with open(sys.argv[1]) as source:
    for line in source:
        if (line.startswith('RocksDB native mode: prebuilt;')
                or line.startswith('streamr-rocksdb: invalidated unwrapped build script ')):
            continue
        record = json.loads(line)
        if (record.get('reason') == 'compiler-artifact'
                and record.get('target', {}).get('name') == 'arroyo_state'
                and record.get('profile', {}).get('test')
                and record.get('executable')):
            matches.append(record['executable'])
assert len(matches) == 1, matches
print(matches[0])
