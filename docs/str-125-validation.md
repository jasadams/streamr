# STR-125 typed-table enumeration and pagination

The measured public enumeration path already advances a stable snapshot cursor
without restarting at the table minimum. Retain the implementation. This increment
documents correct continuation and reservation ownership and adds exact-value,
resource/cancellation and fresh-backend recovery fixtures. No production code,
API, configured limit, checkpoint format or SQL behavior changes.

This conclusion is limited to the finite settings below. It establishes bounded
returned work and proportional measured native traversal for those settings; it
does not establish optimal IPC decoding, production throughput, arbitrary-history
bounds or integrated capacity qualification. No runtime scan caller was found;
none was invented. STR-103/126 retain integrated qualification.

## Source and established contract

- Fetched base `11a18a4281335690431fc52afd60e5dfadfd19fb`, branch
  `jason/str-125-pagination-20261015`; invoking checkout preserved.
- Read current STR-125 and `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
  `docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md` and
  `/home/jason/.claude/agents/feature-implementation-engineer.md`.
- `TableSnapshot` originated in fork commit
  `ad90cee99a99bdb78fcd4352eefe6c9e47d13645`. It is a typed adapter over the
  established live-state snapshot contract, rather than an upstream SQL operator.
  Upstream-style table/checkpoint machinery remains in `src/tables`; no runtime
  integration or replacement was added. The previous source audit is
  `/home/jason/qa-evidence/str113-session-drain-20261014/rocksdb-scan-audit.md`.
- `TableSnapshot::scan` forwards one namespace-wide request per caller page.
  Explicit ordered enumeration discovers unknown primary keys; a known key uses
  `get`. There are no additional production scans in this increment.
- Forward native positioning seeks at the greatest of encoded namespace/start,
  prefix and returned cursor key. This table request has no logical prefix/start/
  end, so the first seek is the encoded namespace; continuation seeks the previous
  encoded key and skips it exclusively. This is a forward seek lower bound, not
  an installed `iterate_lower_bound`. Native `iterate_upper_bound` is the exclusive
  successor of the encoded namespace. It excludes other namespaces before native
  traversal. Cursor validation checks both snapshot identity and exact range.
- One native seek occurs per page. The existing iterator examines a lookahead to
  determine continuation/exhaustion; a byte cutoff may return fewer rows than the
  entry limit. `None` starts enumeration and returned `None` means stop; supplying
  `None` again restarts. Later insertions/deletions do not alter the held view.
  Recovery starts a new view with `None`; old cursors are invalid.
- Native snapshots retain their database owner and snapshot permit until the last
  reader drops. Backend scan reservations follow blocking work through
  cancellation, then encoded pages through decoding. The typed page holds its
  existing `2 * page_bytes` reservation; rows share the page's existing
  `3 * decoded_bytes * returned_rows` permit. Retaining one row retains that shared
  decoded charge. No new cache/index or retained iterator was introduced.

## Finite actual-API measurements

Evidence root: `/home/jason/qa-evidence/str125-pagination-20261015/`.
`settings.json`, per-mode source manifests/patches, immutable executable hashes,
image inspection/native receipts, commands and raw logs are preserved there.
All builds and measurements use maintained `scripts/cargo-dev` inside
`scripts/rust-build` reservations and the shared `target/milestone2-runtime`.
Pins are copied outside regenerable Cargo caches before use within the same
reservation. Queue startup cache eviction and waiting are excluded from segment
measurements.

The purpose is focused correctness and traversal diagnosis. Actual profile is
normal unoptimized dev/test with matching prebuilt RocksDB, not release.
Compatible image:
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Dev native archive:
`c1c76537d9d655fca93b9cc5e8f50f82406ef5da584294aee013605f69d5bd3e`.
No native source compilation or unsupported profile override was used.

Nine scenarios per arm cross retained rows 192/768/3072 with:

| Payload bytes | Entry limit | Encoded page byte limit | Observed rows/page |
| --- | --- | --- | --- |
| 16 | 1 | 32768 | 1 |
| 16 | 16 | 32768 | 16 |
| 256 | 16 | 1900 | 1 (byte cutoff) |

All other fixture settings stay fixed: key/row/decoded limits 1024/8192/8192,
write scope 32768 bytes/16 operations (eight mutations per transaction), block
cache 1 MiB, memtable pool 512 KiB, queued/decoded pools 2 MiB each, scan pool
1 MiB, two blocking operations, four snapshots, one database, zero disk reserve.
Each fresh database initially receives twice the retained count, pins that initial
view, then performs three rounds deleting even keys and updating odd keys. A
second stable view enumerates the surviving odd keys in exact primary-key order,
including exact nullable Int64 values and both text fields. Default native
compaction remains enabled; the initial snapshot retains the original values,
but the experiment does not assume every intermediate version survives compaction.
Each arm completes exactly 12,096 returned rows across all nine scenarios.

Each scan is awaited, its exact typed output checked and continuation advanced;
consumed pages drop before the next call. Early/middle/late markers count returned
rows separately from backend visits. `/proc/self/stat`, `/proc/self/io` and
`/proc/self/status` record process CPU ticks (100 Hz), physical I/O bytes and RSS.
Initialization/churn is excluded from enumeration segments. Segment elapsed time
includes exact-value assertions and sampling; `scan_us` separately totals awaited
API calls. This is a public-API fixture, not source receipt or engine sink output.

Two diagnostic repeats use temporary test-only `PerfContext` inside the actual
RocksDB scan blocking closure. Native seek/internal/deleted/recent traversal,
child/memtable seeks, block reads/bytes and seek/next times are recorded per page
in `native/native-1.log` and `native/native-2.log`; `measurements.json` aggregates
per completed third. The temporary probe patch is archived in `native-probe.patch`
and removed completely from delivery. Native instrumentation/printing timings
are separate from the clean timing campaign.

| Setting | Retained rows | Seeks, both repeats | Internal skips, repeat range | Deleted skips, both repeats |
| --- | --- | --- | --- | --- |
| 16-byte payload, page 1 | 192 / 768 / 3072 | 192 / 768 / 3072 | 2122 / 5404 / 22828 | 383 / 1535 / 6143 |
| 16-byte payload, page 16 | 192 / 768 / 3072 | 12 / 48 / 192 | 1125 / 2869 / 12125 | 203 / 815 / 3263 |
| 256-byte payload, byte cutoff | 192 / 768 / 3072 | 192 / 768 / 3072 | 1612 / 5075–5099 / 21036–21180 | 383 / 1535 / 6143 |

At 3072 retained rows, the page-1 early/middle/late thirds each complete 1024
rows with 1024 seeks and 2048/2048/2047 deleted skips. Page-16 thirds each use
64 seeks and 1088/1088/1087 deleted skips. The byte-cutoff setting has the page-1
seek/deletion counts. This is proportional enumeration plus page-boundary overlap,
not repeated traversal of the already traversed deleted table prefix. Internal-version
and block-read costs vary with actual compaction/cache layout; all counters are
preserved rather than inferred from returned rows.

At 3072 retained rows, child seeks total 15,360 / 960 / 15,360 and
memtable seeks 3072 / 192 / 3072 for the three settings above in both repeats.
Recent-version skips are zero. Native block-read counts vary 18,448–20,131 /
2849–3850 / 3421–21,391, with block-read bytes 10,088,614–14,455,044 /
1,829,340–2,430,163 / 2,135,638–13,756,620 respectively. This variation is retained;
no fixed physical-I/O cost or throughput gain is inferred from the native probe.

The clean executable has no probe. Four arms run in ABBA label order
`baseline-1`, `documented-1`, `documented-2`, `baseline-2`; **all four use the same
immutable executable and implementation**. These are repeatability observations
for an unchanged path, not before/after optimization comparisons. Each passed
with all nine scenarios and 12,096 exact returned rows. Clean executable SHA256:
`317d56df68595aaf39e2b1441dbe4fab86b569383f069af2a9cd457c1af311b0`.
Native diagnostic SHA256:
`b0a9e3fa029e28f162543168b44a197aaa929129db610bf461380271654cdca8`.

At 3072 retained rows, each third completes 1024 rows. The table shows completed
rows/second ranges across all four clean repeats; full settings and every third
at all three retained sizes remain in `measurements.json` and clean raw logs.

| Setting | Early rows/s | Middle rows/s | Late rows/s |
| --- | --- | --- | --- |
| 16-byte payload, page 1 | 1873–2425 | 2023–2383 | 2132–2360 |
| 16-byte payload, page 16 | 6984–8060 | 7006–7865 | 7109–7572 |
| 256-byte payload, byte cutoff | 1899–2196 | 1904–2234 | 1972–2181 |

Across the 108 clean segments, process CPU deltas total 1476 user / 52 system
ticks; sampled RSS ranges 41,783,296–47,439,872 bytes. Kernel physical read deltas
are zero and write deltas total 1,998,848 bytes. These are process-wide observations
including existing native background work, not isolated reader CPU/I/O or a claim
that RocksDB issued no reads. They do not establish disk throughput or engine RSS
qualification. Native and clean timings are deliberately not compared.

The measured seek/deletion totals scale with rows and configured page count;
late continuation does not revisit the complete deleted prefix. Decode/schema
integrity validation still runs for every returned row. There is no demonstrated
contract-preserving implementation defect in these settings that warrants a new
cache, index, API or bypass of validation. Retain the bounded implementation;
no speedup, absolute optimum or arbitrary-history traversal bound is claimed.
The final affected-state gates passed on this same Rust source, as recorded below.

## Exactness, resources and recovery

- Existing typed-table conformance covers memory, a third forwarding adapter and
  RocksDB. Existing malformed IPC/schema/key/value and nested-admission tests
  remain intact.
- The new parity fixture crosses memory/RocksDB with one-entry, sixteen-entry and
  byte-cutoff pages. It checks empty/exhausted views, no missing/duplicate rows,
  exact nullable values/order, foreign-table isolation, wrong-snapshot/foreign-table cursor
  errors and visibility while returned rows are deleted and earlier/later keys
  inserted. Oversized first entries fail with `ReadLimitExceeded`.
- Retaining a row after dropping its page retains decoded admission while encoded
  scan admission becomes available. Dropping the row restores decoded capacity.
  The RocksDB fixture polls a scan while all native operation slots are occupied,
  cancels it, verifies page capacity returns, then releases/awaits the blockers.
  Backend close and full-pool reacquisition check final cleanup. Existing backend
  resource tests separately cover already-running native work retaining capacity
  through cancellation.
- The recovery fixture writes real typed values, holds pre-churn history, exports
  the established checkpoint format and restores into fresh memory and RocksDB
  backends. It rejects the pre-recovery cursor and enumerates all 24 exact restored
  rows with a new cursor chain. Existing full/changed/empty cross-backend checkpoint
  tests remain part of the final affected-state gate. This is backend checkpoint
  recovery, not a worker/OS-process fault campaign.
- Restore fixtures use the established full-page restore adapter's 8 MiB queued/
  scan pools identically for both targets; production caps and all matched
  enumeration settings remain unchanged.

Initial typed inventory: exit 0, 10 passed / 0 failed / 1 ignored diagnostic;
`initial-typed-tests.log`. Both explicit native diagnostics: exit 0, 1 passed
per invocation, no failed scenarios. No engine or fixture failure required repair.
The first default-image fmt attempt was refused before execution (exit 2): its
old image pin did not match the fetched selector. Supported `STREAMR_DEV_IMAGE`
selected the already-existing compatible image; no shared image was rebuilt.
The subsequent maintained fmt completed exit 0. Refusal and exact settings are
preserved in `initial-image-refusal.txt` and `settings.json`.

A summary-printing analysis one-liner initially exited 1 with an unmatched
parenthesis. Corrected `print-summary.py` exited 0 against the original raw
measurements; no workload was rerun or result replaced (`analysis-refusal.txt`).

## Final current-source gates and delivery boundary

All commands below completed in the clean build/pin/measurement reservation.
`clean/commands.json` records exact argv, exits and durations; corresponding logs
and immutable source/executable/native receipts are alongside it. Relevant final
Rust file hashes equal the clean source manifest, and `live/rocks.rs` is identical
to the fetched base.

| Check | Exit | Result |
| --- | --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-state` | 0 | Current affected state code |
| `scripts/preflight-clippy.sh -p arroyo-state` | 0 | Required locked strict all-feature/all-target gate |
| `scripts/cargo-dev fmt --all -- --check` | 0 | Workspace formatting |
| `scripts/cargo-dev test --locked -p arroyo-state --lib --no-fail-fast -- --test-threads=1` | 0 | 82 passed, 0 failed, 4 ignored |
| Explicit STR-125 diagnostic, two native and four clean invocations | 0 each | 1 passed per invocation; 72,576 exact completed rows total |

The four ignored library entries are this opt-in STR-125 diagnostic and the
existing STR-114 native/admitted and STR-116 cleanup diagnostics. STR-125 was
explicitly executed six times; the other three were not rerun or counted passed.
The 82 ordinary passes include actual backend checkpoint/restore, cursor bounds,
resource/admission/cancellation, cleanup and typed-table conformance. There were
no Rust build, lint, correctness or measurement failures. The complete nonzero
inventory is the initial image-selector refusal (2) and corrected summary-print
syntax error (1), both preserved above; neither was an engine/test failure.

Independent written source/evidence review, coordinator publication and
current-head PR CI remain delivery steps. No user semantics/architecture decision
is pending. Full engine throughput/RSS, arbitrary-history/large-state and actual
worker/OS-process fault qualification remain outside this focused ticket and with
STR-103/126; they are not claimed by these fixtures.

## Delivery integration

Independent final source/evidence review approved with zero findings. Delivery
rebased onto fetched main `704b30c2cf7d617527ad495c3636369cd80f07a9`
(merged STR-123/124). Only the ledger conflicted; all three ticket blocks were
preserved. State sources, Cargo inputs and AGENTS/build/team guidance are identical
to the validated source. Static diff/whitespace checks pass; no executable check
was repeated for this documentation-only integration. Independent integration
review and current-head PR CI remain delivery gates.
