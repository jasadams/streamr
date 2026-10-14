# STR-113 native SESSION drain investigation

The native SESSION deadline lookup repeatedly restarted at the beginning of its
index. This revisited millions of deleted/internal entries while closing only
thousands of sessions. Retaining an inclusive logical minimum removes that
repeated work without changing session output or recovery formats.

## Source and observed production behavior

Fetched base: `6793409cb53eb7b13f5a591ce1717bcf1e846de8`; worktree
`/home/jason/repos/streamr-worktrees/str-113-session-drain-20261014`.
Instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, current
STR-113, milestone status/work plan. STR-103 evidence remains read-only.

The interrupted STR-103 run had received all 1.2M logical events, then continued
writing SESSION output at approximately 30 rows/s. Its final outputs were not
validated. Its original production executable was `ad7c90dca2c46775ef17fe572856c4ddc2315d65b976544af5297e0015d18b02`.

Saved worker metrics for three late approximately 60-second intervals accumulated
57.248, 57.188 and 56.531 seconds in native scans, versus 0.463, 0.498 and 0.643
seconds in writes. These shared worker operation wall-time sums can overlap;
they are neither CPU percentages nor direct per-operator attribution.
`original-drain-metrics.json` binds the raw metrics by SHA256. The later isolated
fixture establishes which SESSION stage causes the repeated scan work.

`session_store.rs::first_due` originally created a fresh snapshot, scanned the
D prefix with no start bound, and returned one session. The operator repeated
this after each closure. Earlier deadlines removed while merging input and
retired session deadlines can leave internal deletion/version entries. Prefix
upper bounds from PR #45 exclude unrelated families, but do not avoid revisiting
earlier entries inside D. In pre-native ancestor
`4ffac3ff90a94bedb179dd38256e0f7a512bdc96`, the existing SESSION implementation
used `BTreeMap::pop_first()` in `results_at_watermark`. The repeated native seeks
are an adapter cost, not evidence of a RocksDB defect.

## Focused measurements

Evidence root: `/home/jason/qa-evidence/str113-session-drain-20261014/`.
Required immutable Bookworm image:
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Commands used the shared `scripts/rust-build` reservation and one existing target.
Pins and receipts are outside Cargo caches. This is the unoptimized Rust test
profile and prebuilt native O0 RocksDB, **not production throughput**.

The fixture uses opaque 8-byte group keys, two rows/session at times 0 and 9,
values 3 and 5, gap 10, and COUNT/SUM/ordered ARRAY_AGG. Each emitted batch is
checked for exact count, sum, ordered values, start/end and output timestamp;
selected group order is asserted. The manual closure loop splits `first_due`,
actual DataFusion `emit`, closing marker and retirement. The collector validates
then discards output; it excludes physical file-sink cost, `is_closing`, normal
acknowledgment orchestration and process recovery. Validation occurs inline and
is included in drain wall time; cleanup follows it.

Window limits match STR-103: key 512, partial 32768, page 131072/64 entries,
write 524288/64 operations. Shared pool: block cache 8MiB, memtable 4MiB,
queued writes 32MiB, decoded 16MiB, scan 2MiB; blocking 2, snapshots 4, databases 4,
disk reserve 64MiB. No pool increase, backend tuning or compaction override.

The initial 20,000-key case ingested 40,000 rows in 36.859s and completed all
20,000 output assertions in 319.883s drain. First/middle/last 1,000-session
intervals took 8.541/12.062/27.519s (117.08/82.90/36.34 sessions/s).
Deadline selection consumed 245.457s total, including 23.826s of the last interval;
emit 31.847s, marker 8.167s and retirement 34.183s. The recorded curve is uneven;
compaction was not independently isolated or measured. The harness then **failed cleanup** because a local
LiveTable handle remained; this is not a passing test. The corrected harness
explicitly drops that handle before closing the backend.

Successful matched 5,000-key cases use the same temporary counter probe on both
sides: RocksDB `PerfContext` is reset/read on the blocking thread around actual
D-prefix Seek/Next. The production repair alone differs between cases.

| Measurement | Baseline replication | Repair |
| --- | ---: | ---: |
| Ingestion, 10,000 rows |8.659s|8.947s|
| Drain, 5,000 sessions |37.858s|17.844s|
| Whole test including setup/cleanup |46.64s|26.93s|
| Deadline selection total |22.337s|2.701s|
| Emit: aggregate/setup/row reads/IPC/collector |6.475s|6.268s|
| Closing markers |1.705s|1.615s|
| Retirement |7.287s|7.206s|
| Deleted entries skipped by deadline scans |12,570,036|8,662|
| Internal keys skipped by deadline scans |16,682,419|15,983|
| First 1,000 sessions/s |123.18|239.58|
| Middle 1,000 sessions/s |173.70|302.06|
| Last 1,000 sessions/s |99.53|293.31|

The intervention cuts deadline selection 8.27× and full drain 2.12× in this pair;
last-interval deadline selection falls 6.975→0.521s. Deleted/internal-key traversal
falls approximately 1,451×/1,044×. An earlier successful baseline replicate took
39.297s, with 12,561,394 deleted skips. Both baseline runs and the candidate
validated every output and completed cleanup.

Raw `/proc/self/{stat,status,io}` is recorded every 1,000 closures. The pinned
baseline's final interval used 9.75 CPU-seconds across 10.047s wall; candidate 3.08
across 3.409s. Both recorded zero physical `read_bytes` throughout those samples.
Final sampled RSS was 134.79/134.98MiB, respectively; these sparse samples are not
continuous memory qualification. Full raw rate/stage/counter histories are in
`benchmark-summary.json`, with source logs and SHA256s.

Baseline pin SHA256: `fd199cbe655c0e5c18ef8a4968d4d0a729430557930629987eb75da441ab2346`.
Candidate diagnostic pin SHA256: `38ac970853ab4116e77b988d06d95dec1f8c8ef632b15c3555d595f45c04a84a`.
`baseline-probe-pin/receipt.json` and `candidate-pin/receipt.json` bind the exact
source patches and exit status. To reproduce, create a disposable worktree at
the base, apply `probe-baseline-pinned.patch` or `probe-candidate.patch`, then run:

```sh
scripts/cargo-dev test --locked -p arroyo-worker --lib str113_focused_drain -- --ignored --nocapture --test-threads=1
```

Do not apply diagnostic patches to a production worktree. Temporary counter
instrumentation and the stage fixture are absent from the published change.
Original failed/aborted diagnostic attempts remain preserved and are not passes.

## Repair and correctness

The optional admitted deadline cache now retains one inclusive logical minimum.
Every lookup still obtains a fresh native snapshot. Repeated lookup before
retirement therefore returns the same session; cancellation cannot skip it.
Insert/merge clears the minimum before awaiting a write, including ambiguous
failure. The minimum is not serialized and starts cold after restore. Equal
watermark behavior, EOF maximum watermark, output acknowledgment/retirement and
checkpoint formats remain unchanged.

Cache admission includes the retained key and request copy using one checked
charge shared with group-cache headroom accounting. Valid tight decoded pools
retain the uncached fallback. No cross-snapshot ScanCursor is reused.

Focused regressions cover repeated reads, equal-deadline ordering, earlier/equal
watermark queries, earlier deadline/group insertion, cancelled/failed insertion
after a nonempty minimum, closing state and cold store reconstruction. Existing
SESSION cancellation, paged retirement and full checkpoint/fresh-backend recovery
checks remain required.

Clean-source validation: locked worker check, strict all-target Clippy,
all 22 `arrow::session` library tests and workspace formatting passed. The
checkpoint test includes actual export/restore into a fresh memory backend;
this is not a native process-loss qualification. Exact commands/exits/durations
are in `gates.json` and `final-gates.json`. The strengthened checkpoint test
populates the minimum before export and verifies a cold minimum after restore.
Final test pin SHA256 is recorded in `final-worker-tests.sha256`.
Independent source/evidence review approved the repair; the report was corrected
to avoid attributing uneven rates to unmeasured compaction. Final review receipt:
`independent-source-evidence-review.json` (with its final recheck).

The scan investigation and focused repair checks are complete; publication/CI
remain separate delivery gates.
STR-103 remains unqualified; no 400,000-session production drain or full 1.2M
rerun was performed here, and no production speedup is extrapolated from O0 tests.
EOF replay correctness remains with its unchanged full acceptance.
