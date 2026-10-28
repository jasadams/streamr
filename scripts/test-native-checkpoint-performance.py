#!/usr/bin/env python3
"""One preregistered ABBA native SQL checkpoint comparison; no automatic rerun.

Both pinned SQL-testing executables need the identical measurement-only Rust
driver. Baseline adaptation must be reviewed separately; this runner never builds
or edits product sources. Prepare/freeze before execution, then run the same root.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location('native_checkpoints', Path(__file__).with_name('test-native-incremental-checkpoints.py'))
native = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(native)
KEYS, SIZE = 49152, 8192
SPARSE, CHURN = 492, 4915
ARM_SECONDS = 144


def prepare(args):
    root = args.directory.resolve()
    native.require(not root.exists(), 'campaign directory must be new')
    root.mkdir(parents=True)
    baseline, candidate = args.baseline.resolve(strict=True), args.candidate.resolve(strict=True)
    receipts = [args.baseline_receipt.resolve(strict=True), args.candidate_receipt.resolve(strict=True)]
    for executable, path in zip([baseline, candidate], receipts):
        receipt = json.loads(path.read_text())
        native.require(receipt.get('executable_sha256') == native.digest(executable), 'receipt executable hash differs')
        native.require(receipt.get('profile') == 'release', 'optimized comparison requires explicit release receipts')
        native.require(receipt.get('native_archive_mode') == 'matching-prebuilt', 'receipt must declare matching prebuilt native archive')
        native.require(receipt.get('source_manifest_sha256') and receipt.get('source_commit'), 'source-bound receipt missing')
        native.require(receipt.get('reviewed_measurement_overlay') is True, 'matching driver overlay needs independent review')
    cpus = sorted({int(value) for value in args.cpus.split(',')})
    native.require(cpus and set(cpus) <= os.sched_getaffinity(0), 'requested CPU set unavailable')
    native.require(args.checkpoint_ms == 5000, 'reviewed cadence is 5000ms; obtain review before changing it')
    native.require(args.rss_limit_mib == 512, 'reviewed process RSS envelope is 512MiB')
    # Raw full-export snapshots can be much larger than compressed expected data.
    free = shutil.disk_usage(root).free
    native.require(free >= 40 * 1024**3, 'need 40GiB free before bounded ABBA full-snapshot fixtures')
    state = {}
    seq = 0
    with (root / 'input.jsonl').open('w') as stream, (root / 'expected.jsonl').open('w') as oracle:
        def emit(key, mode, version):
            nonlocal seq
            seq += 1
            native.emit_event(stream, oracle, state, seq, key, mode, version, SIZE)
        for key in range(KEYS):
            emit(key, 'put', 1)
        seed = seq
        # Deterministic separated keys, rather than a single favorable hot block.
        for item in range(SPARSE):
            emit((item * 97) % KEYS, 'put', 2)
        sparse = seq
        for key in range(CHURN):
            emit(key, 'delete', 0)
        for key in range(CHURN):
            emit(key, 'put', 3)
        churn = seq
        # Do not release EOF during a processing arm. This last oracle probe is
        # deliberately beyond the final target and never counted as completed.
        emit(KEYS - 1, 'probe', 0)
    files = native.frozen_files([baseline, candidate, *receipts, Path(__file__),
                                  Path(native.__file__), root / 'input.jsonl', root / 'expected.jsonl'])
    arms = []
    for index, role in enumerate(['baseline', 'candidate', 'candidate', 'baseline']):
        case = root / f'{index + 1}-{role}'
        case.mkdir()
        (case / 'checkpoints').mkdir()
        (case / 'query.sql').write_text(native.query(root / 'input.jsonl', case / 'output.jsonl'))
        env = native.clean_environment(case, 'rocksdb', args.protocol, 174, max_databases=1)
        env.update(STREAMR_NATIVE_PHASE_TARGETS=f'{seed},{sparse},{churn}',
                   STREAMR_NATIVE_INPUT_PATH=str(root / 'input.jsonl'),
                   STREAMR_NATIVE_CHECKPOINT_MS=str(args.checkpoint_ms),
                   STREAMR_NATIVE_CHECKPOINT_RECEIPTS='1')
        controls = {key: value for key, value in env.items() if key.startswith(('STREAMR_', 'CHECKPOINT_URL'))}
        native.write(case / 'controls.json', controls)
        files.update(native.frozen_files([case / 'query.sql', case / 'controls.json']))
        arms.append(dict(role=role, directory=str(case), executable=str(baseline if role == 'baseline' else candidate),
                         controls=controls))
    manifest = dict(status='PREPARED; NOT MEASURED', arms=arms, files=files, protocol=args.protocol,
                    cpu_affinity=cpus, checkpoint_ms=args.checkpoint_ms,
                    child_rss_limit_bytes=args.rss_limit_mib * 1048576,
                    phase_boundaries_seconds=[0, 60, 88, 116, 144], phase_targets=[seed, sparse, churn],
                    seed_keys=KEYS, payload_bytes=SIZE, seed_logical_payload_bytes=KEYS * SIZE,
                    backend_pool_bytes=32 * 1048576, max_processing_seconds=576,
                    checkpoints_required_per_phase=2, startup_cleanup_outer_timeout_seconds=184,
                    host_initial_disk_free_bytes=free,
                    comparison_limits=['Single generic MERGE/lookup fixture; not previous Flink comparison.',
                     'File-source event-to-checkpoint publication latency is not Kafka read_committed visibility.',
                     'Opt-in native boundary receipts add candidate-only property/timing work; no overhead subtraction.',
                     'Raw cfstats compaction counter deltas require independent audit; no inference from SST size.'])
    native.write(root / 'manifest.json', manifest)
    return manifest


def audit_arm(case, manifest, arm):
    result = json.loads((case / 'arm.json').read_text())
    native.require(result['phase_boundaries_seconds'] == manifest['phase_boundaries_seconds'], 'compiled driver phase boundaries differ from frozen controls')
    native.require(result['processing_seconds'] <= ARM_SECONDS + 0.25, 'processing cap exceeded')
    rows = result['completed_rows']
    native.require(0 <= rows <= manifest['phase_targets'][-1], 'completed row denominator differs')
    native.compare_jsonl(case / 'output.processing.jsonl', case.parent / 'expected.jsonl', rows)
    by_phase = {'idle': [], 'sparse': [], 'churn': []}
    intervals = [('idle', 60, 88), ('sparse', 88, 116), ('churn', 116, 144)]
    inventories = []
    prior = set()
    for checkpoint in result['checkpoints']:
        inventory = native.native_inventory(case / 'checkpoints', result['job_id'], manifest['protocol'],
                                            checkpoint['epoch'], verify_objects=False)
        native.require(bool(inventory) == (arm['role'] == 'candidate'), 'native representation differs from comparison arm')
        files = [item for descriptor in inventory for item in descriptor['files']]
        current = {item['path'] for item in files}
        native_bytes = sum(item['size_bytes'] for item in files)
        reused_bytes = sum(item['size_bytes'] for item in files if item['path'] in prior)
        inventories.append(dict(epoch=checkpoint['epoch'], full_native_file_bytes=native_bytes,
                                reused_from_previous_manifest_bytes=reused_bytes,
                                newly_referenced_native_file_bytes=native_bytes - reused_bytes,
                                descriptors=inventory))
        prior = current
        for phase, start, stop in intervals:
            if start <= checkpoint['started_seconds'] < checkpoint['published_seconds'] <= stop:
                by_phase[phase].append(checkpoint)
    log = (case / 'performance.log').read_text()
    receipts = [json.loads(line.removeprefix('NATIVE_CHECKPOINT_RECEIPT '))
                for line in log.splitlines() if line.startswith('NATIVE_CHECKPOINT_RECEIPT ')]
    for receipt in receipts:
        native.require(receipt.get('job_id') == result['job_id'], 'native receipt job differs')
        raw_stats = receipt.get('cfstats_no_file_histogram')
        native.require(raw_stats is None or (isinstance(raw_stats, str) and len(raw_stats.encode()) <= 65536),
                       'native cfstats receipt exceeds declared limit')
    starts = [line for line in log.splitlines() if line.startswith('NATIVE_PERF_START unix_micros=')]
    native.require(len(starts) == 1, 'missing unique processing start')
    start_unix = int(starts[0].split('=')[1]) / 1e6
    samples = [json.loads(line) for line in (case / 'performance.samples.jsonl').read_text().splitlines()]
    processing_samples = [row for row in samples if start_unix <= row['unix'] <= start_unix + result['processing_seconds']]
    native.require(len(processing_samples) >= 2, 'missing processing CPU/RSS history')
    phase_metrics = {}
    for phase, start, stop in [('seed', 0, 60), *intervals]:
        selected = [row for row in processing_samples if start <= row['unix'] - start_unix <= stop]
        phase_metrics[phase] = dict(samples=len(selected),
            cpu_seconds=(selected[-1]['cpu_seconds'] - selected[0]['cpu_seconds']) if len(selected) >= 2 else None,
            peak_sampled_rss_bytes=max((row['rss_bytes'] for row in selected), default=0),
            read_bytes=(selected[-1]['read_bytes'] - selected[0]['read_bytes']) if len(selected) >= 2 else None,
            write_bytes=(selected[-1]['write_bytes'] - selected[0]['write_bytes']) if len(selected) >= 2 else None)
    gaps = []
    if not result['seed_complete']:
        gaps.append('seed incomplete at fixed60s cutoff')
    for index, phase in enumerate(['seed', 'sparse', 'churn']):
        if not result['phase_complete'][index]:
            gaps.append(phase + ' exact input frontier incomplete')
    for phase, checkpoints in by_phase.items():
        if len(checkpoints) < manifest['checkpoints_required_per_phase']:
            gaps.append(phase + ' lacks two wholly-contained completed checkpoints')
    if arm['role'] == 'candidate':
        by_epoch = {}
        for receipt in receipts:
            by_epoch.setdefault(receipt['epoch'], []).append(receipt)
        for checkpoint, inventory in zip(result['checkpoints'], inventories):
            matching = by_epoch.get(checkpoint['epoch'], [])
            native.require(len(matching) == 1, 'missing/duplicate native boundary receipt')
            receipt = matching[0]
            files = [item for descriptor in inventory['descriptors'] for item in descriptor['files']]
            new_bytes = sum(item['size_bytes'] for item in files if item['birth_epoch'] == checkpoint['epoch'])
            reused_bytes = sum(item['size_bytes'] for item in files if item['birth_epoch'] != checkpoint['epoch'])
            native.require(receipt['uploaded_bytes'] == new_bytes and receipt['reused_bytes'] == reused_bytes,
                           'native boundary byte counts differ from complete published inventory')
            native.require(receipt['sync_capture_seconds'] >= 0 and receipt['async_export_seconds'] >= 0,
                           'invalid native duration receipt')
        # Preserve actual property output for the independent reviewer. Parsing
        # a guessed column or seeing churn is not evidence of a compaction.
        gaps.append('independent raw cfstats completed-compaction/rewrite delta audit required')
    frontier_latencies = []
    for checkpoint in result['checkpoints']:
        frontier = next((row for row in processing_samples
                         if row.get('observed_completed_rows', 0) >= checkpoint['released_rows']), None)
        frontier_latencies.append(dict(epoch=checkpoint['epoch'],
            sampled_frontier_to_publication_seconds=(start_unix + checkpoint['published_seconds'] - frontier['unix'])
                if frontier else None))
    audited = dict(status='INCONCLUSIVE' if gaps else 'PASS', acceptance_gaps=gaps,
                   completed_rows=rows, completed_rows_per_processing_second=rows / result['processing_seconds'],
                   phase_checkpoint_counts={key: len(value) for key, value in by_phase.items()},
                   phase_process_metrics=phase_metrics, native_inventories=inventories,
                   native_boundary_receipts=receipts, sampled_frontier_publication_latencies=frontier_latencies,
                   raw_arm=result, sampling_limits='100ms requested interval; inspect actual gaps. RSS sampled, not exact peak; frontier observations may lag completion.')
    native.write(case / 'audit.json', audited)
    return audited


def execute(root, manifest):
    native.verify_frozen(manifest['files'])
    native.require(not (root / 'campaign.started.json').exists(), 'campaign already attempted; no automatic repeat')
    native.write(root / 'campaign.started.json', dict(unix=time.time(), manifest_sha256=native.digest(root / 'manifest.json')))
    os.sched_setaffinity(0, manifest['cpu_affinity'])
    results = []
    processing_total = 0.0
    attempted, receipted = 0, 0
    failure = None
    try:
        for arm in manifest['arms']:
            native.verify_frozen(manifest['files'])
            case = Path(arm['directory'])
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(('STREAMR_TEST_', 'STREAMR_CAPTURE_', 'STREAMR_NATIVE_', 'ARROYO__'))
                   and key != 'CHECKPOINT_URL'}
            env.update(arm['controls'])
            attempted += 1
            try:
                native.run_child(Path(arm['executable']), 'native_checkpoint_performance_arm', case, env,
                                 manifest['startup_cleanup_outer_timeout_seconds'], manifest['child_rss_limit_bytes'], 'performance')
            finally:
                if (case / 'arm.json').is_file():
                    processing_total += json.loads((case / 'arm.json').read_text())['processing_seconds']
                    receipted += 1
            audited = audit_arm(case, manifest, arm)
            results.append(audited)
            native.require(processing_total <= manifest['max_processing_seconds'] + 0.25, 'campaign processing cap exceeded')
            native.verify_frozen(manifest['files'])
            if not audited['raw_arm']['seed_complete']:
                break  # Do not consume remaining arms after a failed seed.
    except BaseException as error:
        failure = f'{type(error).__name__}: {error}'
        raise
    finally:
        native.write(root / 'result.json', dict(status='FAIL' if failure else 'INCONCLUSIVE',
            failure=failure, attempted_arms=attempted, audited_arms=len(results),
            unreceipted_attempts=attempted - receipted,
            processing_seconds_from_arm_receipts=processing_total,
            arms=results, conclusion='No KEEP or speedup verdict; independent receipt/raw evidence audit remains required.',
            cleanup='test process joined; immutable runtime/checkpoint evidence preserved, no automatic removal'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--baseline-receipt', type=Path)
    parser.add_argument('--candidate-receipt', type=Path)
    parser.add_argument('--cpus', help='Explicit identical CPU affinity, e.g. 0,1')
    parser.add_argument('--protocol', choices=('controller', 'leader'), default='controller')
    parser.add_argument('--checkpoint-ms', type=int, default=5000)
    parser.add_argument('--rss-limit-mib', type=int, default=512)
    args = parser.parse_args()
    if args.prepare:
        native.require(all([args.baseline, args.candidate, args.baseline_receipt, args.candidate_receipt, args.cpus]),
                       'preparation requires both executables, source-bound receipts and CPU allocation')
        prepare(args)
        print('PREPARED; no benchmark has run. Freeze/review manifest before invoking without --prepare.')
    else:
        root = args.directory.resolve(strict=True)
        execute(root, json.loads((root / 'manifest.json').read_text()))


if __name__ == '__main__':
    main()
