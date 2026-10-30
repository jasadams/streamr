# STR-163: known-owner calendar dirty delivery

Calendar watermark dispatch commits one known owner, then used to discover that
owner again through a fresh global dirty-index scan. With retained deletion
history, each discovery can traverse the dirty family even when only one marker
is live. The repair borrows the existing admitted H continuation's opaque key,
checks its D marker by point lookup, and uses the ordinary shared G/D delta,
suppression, retirement, commit and collection path. Initial dirty discovery and
the full-scan fallback remain unchanged.

No SQL, clock, state format, checkpoint/transaction protocol, configured resource
limit or public interface changes. Dispatch and delivery remain separate commits;
G.last_emitted/D deletion still commits before collection. No snapshot survives
collection. The borrowed owner adds no carried allocation. A missing optional
continuation retains the old path, including tight-resource behavior.

## Scan and ownership accounting

Only calendar per-owner D discovery changes. The original D prefix bounds,
exclusive progress and chunk admission remain for initial/fallback drains and
all other callers. H remains ordered and bounded through the current watermark;
B/calendar scans are unchanged. No new RocksDB scan or larger page is added.
Known D/G reads use the same stable scope, output reservation and mutation code.

The existing H progress reservation covers the retained key and temporary D/G
keys; the marker value is dropped before the G read. The original decoded peak,
write/overlay/output checks and permit lifetimes are retained. Independent
resource review checked the key overlap and the fallback without optional
progress. Tests cover both backends, same-deadline order, quiet suppression,
FALSE/NULL filters, stale generations, a pending earlier dirty key, slow
collection, cancellation on both sides of the delivery commit, and actual
checkpoint export into a fresh owner.

## Mechanism evidence

The completed diagnostic baseline07 used the normal dev/test profile and existing
opt-in per-scan hook. It processed 18,433 inputs, verified 1,024 quiet owners and
exactly 2,046 ordered CDC rows. Every one of 1,024 dispatch D-puts was independently
joined to a fresh cold D scope and a same-owner D-delete. Each cold page skipped
1,024 deletions. D contributed 98.2804922% of measured rollover
FindNextUserEntryTime; all D pages skipped 1,573,377 deleted and 14,172,673 internal
entries. These counters establish the isolated mechanism, not a speedup estimate.

The production profile was scan-heavy but its asynchronous caller boundary hid
the exact prefix. Source event-time observations correlate the stall with day
rollover; this does not prove every production sample came from this D path.
Native checkpoint export was not established as the bottleneck.

## Correctness gates

Combined correctness01+02 passed 38 selected runtime tests: 35 calendar, two dirty
output and one slow-collector test. Three manual diagnostics remained ignored.
Current formatting, locked worker check and strict all-feature/all-target Clippy
passed through maintained scripts and a matching dev prebuilt RocksDB archive.

Correctness01 remains partial on two diagnostic-helper Clippy lints. Equivalent
helper-only repairs were independently reviewed;02 rebuilt/pinned the current
source and reran calendar/format/Clippy, explicitly carrying01 successful
check/dirty/slow evidence. No product or focused-test changes were carried silently.
All included container/session cleanup receipts passed.

Explicit named cargo-dev owners now retain their containers until actual CID
observation and owned cleanup, including short commands. Ordinary unnamed calls
still use --rm. The maintained wrapper's 22 stub tests passed. This fixes the
measurement orchestration race without changing normal build behavior.

## Optimized probe-off comparison

One sequential A1/B1/B2/A2 comparison used fresh processes and databases, the same
18,433-input/1,024-owner/four-horizon fixture and all exact value/order/clock
oracles. Optional H progress was admitted under unchanged controls. Both products
used the same Rust opt3 profile and prebuilt release native archive. No per-scan
probe, calibration arm, forced compaction, state import or automatic retry ran.
Each whole-fixture future had 149s allowance (596s total); cleanup remained 30s.

| Arm | Whole fixture, s | Rollover wall, s | Rollover CPU, s | Whole-process HWM, KiB |
| --- | ---: | ---: | ---: | ---: |
| A1 | 12.720015 | 4.251383 | 4.01 | 76,832 |
| B1 | 8.789003 | 0.824684 | 0.84 | 77,332 |
| B2 | 9.301203 | 0.843064 | 0.87 | 76,948 |
| A2 | 11.800134 | 3.819018 | 3.60 | 77,020 |

Mean rollover wall improved 4.839102x (4.035200→0.833874s); CPU improved 4.450292x
(3.805→0.855s). Both candidates beat both baselines, including conservative
summed-CPU uncertainty of ±2 clock ticks; the all-arm CPU lower-bound ratio is
4.022472x. Whole-fixture wall improved 1.355438x (12.260075→9.045103s).
Seed preparation means 8.123547s versus 8.107488s were closely matched.

All four exact oracles and all six build/arm ownership cleanups passed. Affinity
and container resources matched. Filesystem observations were complete and
metadata-only: eight files/five directories, 57,344 allocated bytes throughout;
logical bytes differed by at most two. They do not prove identical LSM history.
Baseline order drift is visible; two observations per product provide limited
uncertainty evidence. CPU includes background work and boundary overhead; HWM
includes preparation, not only rollover. All raw samples and zero-progress gaps
are preserved.

**KEEP the scoped efficiency repair. The 10x/20x target was not met.** These are
isolated rollover results, not production throughput, startup margin, remaining
backlog, application parity, native-format recovery or process-loss claims. The
checkpoint tests establish same-process logical fresh-owner recovery; the
historical Rocks recovery fixture uses synchronous WAL. Production diagnosis,
rollout and acceptance remain with the coordinator; no deployment occurred here.

## Source, native and artifact binding

Fixed baseline commit 2b521b0200ae0d7e433149da123c9043a3f1dad5. A/B each froze 1,356
compiled inputs and differed only in incremental_aggregator.rs after identical
test registrations/temporary measurement overlay. Registration inverses recover
baseline and reviewed candidate product bytes. Candidate product file SHA256:
19cc1517ad4b9925d362d71b1f9397bdc497a4486975dd3cd9641df6fd06440e.

- A ELF: bd227feb8ae924754bd4fa872475cabefa7ea118af606b05acb7321fb931949b.
- B ELF: c1ad5ff8a90b4d109f29351030bfe0402fc48273283117185bf2147a2df337fb.
- Runtime native archive: cd8300d81598fb5e0e6b5dd73b55e1355fc004b7f4ca8b27341cf759ef927d54,
  actual release/opt3/debugtrue, with no unaccounted native mapping. All selected
  custom-build fingerprints were checked; no host RocksDB use was observed in
  the declared traversal. No CLI/OpenAPI host proof is implied.
- Immutable image: bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e.
- Staged runner: 56a681d43e9b2174280c3120e21ddcf5ac4d589a5b41eb4604865bb13ead685f.
- Controls: 18b9ab3bcf989c81c4b60a9530f83db92bf06731ac33fac8dc680fd808b3223f.

An initial uncertain whole-campaign growth forecast was mistakenly made an
admission requirement. This was corrected before execution: the maintained
100 GiB reserve, actual copy-size checks and existing periodic owned-work guard
were enforced; forecasts became advisory. Runtime/cache/write/decoded limits did
not change. No further deletion or reserve bypass was used. The temporary test
harness overlay was restored by checked CAS after all owners stopped.

Full evidence is preserved outside Cargo caches:
/home/jason/qa-evidence/str163-calendar-optimized-abba-20261030-01/ contains both
source/native/ELF proofs, all four raw arms, source-after/restoration and result.
The independent measurement report is optimized01-independent-measurement-result-review.md
there; source/resource audit and prior diagnostic/correctness receipts are under
/home/jason/qa-evidence/str163-frontier-20261030/ and
/home/jason/qa-evidence/str163-candidate-correctness-20261030-{01,02}/.
Earlier preparation/compilation/log-cap aborts remain partial or inconclusive;
none is rewritten passed or used as the optimized BEFORE. The earlier unsafe
binary-listener HTTP probe/panic disclosure is also preserved separately.
