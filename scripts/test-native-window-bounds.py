#!/usr/bin/env python3
"""Prepare/run many-pane and hot-pane fixed-window fresh-worker captures.

Uses the existing generic finite-source capture harness and its exact full-row
oracle. Slow collector/cancellation ownership is covered by worker unit tests;
this driver checks persisted open panes after fresh-worker checkpoint restore.
"""
import argparse
import importlib.util
from pathlib import Path
from datetime import timedelta
import json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--backend', choices=('memory', 'rocksdb'), action='append')
    parser.add_argument('--protocol', choices=('controller', 'leader'), action='append')
    parser.add_argument('--batch', choices=(1, 8), type=int, action='append')
    parser.add_argument('--panes', type=int, default=48)
    parser.add_argument('--hot-rows', type=int, default=128)
    args = parser.parse_args()
    if args.panes < 4 or args.hot_rows < 8 or args.hot_rows % 8:
        parser.error('--panes must be >=4 and --hot-rows must be a positive multiple of8')
    specification = importlib.util.spec_from_file_location(
        'native_windows', Path(__file__).with_name('test-native-windows.py'))
    windows = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(windows)
    windows.EVENTS = [('hot', 0, value) for value in range(args.hot_rows)]
    # Eight same-time rows per later pane keep checkpoint watermark/prefix
    # deterministic for both supported source batch targets. Half the panes
    # have already closed, and the last pane remains open at the checkpoint.
    windows.EVENTS.extend((f'group-{pane % 3}', pane * 2, pane + offset)
                          for pane in range(1, args.panes) for offset in range(8))
    checkpoint_pane = args.panes // 2
    checkpoint_input = args.hot_rows + checkpoint_pane * 8
    checkpoint_watermark = windows.BASE + timedelta(seconds=checkpoint_pane * 2)
    root = args.directory.resolve()
    for kind in ('tumble', 'hop'):
        prefix = [row for row in windows.expected(kind)
                  if row['end'] <= checkpoint_watermark.isoformat()]
        if not args.binary:
            windows.write_fixture(root / kind, kind)
            (root / kind / 'expected.checkpoint.json').write_text(
                json.dumps(prefix, indent=2) + '\n')
            (root / kind / 'checkpoint-input-rows.txt').write_text(str(checkpoint_input) + '\n')
            continue
        for backend in args.backend or ('memory', 'rocksdb'):
            for protocol in args.protocol or ('controller', 'leader'):
                for batch in args.batch or (1, 8):
                    windows.run_case(
                        args.binary.resolve(strict=True),
                        root / f'{kind}-{backend}-{protocol}-{batch}',
                        kind, backend, batch, protocol, True, checkpoint_input, prefix)


if __name__ == '__main__':
    main()
