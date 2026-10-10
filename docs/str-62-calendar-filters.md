# STR-62 maintained calendar FILTERs

Working branch: `jason/str-62-calendar-2147`, initial base `67c242f5`;
current executable qualification rebased onto optimized main `48ceb3fe`.
Start: 2026-10-09 21:47 UTC; resumed on explicit user direction.
The [current ticket](https://trakkt.app/issues/STR-62) owns acceptance.
Automatic elapsed-time limits are disabled. The earlier queued retry was stopped
at 22:46:54 UTC (exit 143), before acquiring a build slot; that historical stop
is not an ongoing deadline. The reviewed implementation is delivered through
[PR #23](https://github.com/jasadams/streamr/pull/23), awaiting current-head CI and merge.
Final main integration is tooling/docs only; validated engine/harness hashes are preserved.

## Contract and ownership

COUNT/SUM calendar FILTERs use inclusive UTC dates `[D-(N-1), D]`.
The contribution DATE expression is independent of the declared FOR column.
Event triggers use their own raw FOR reference date, including backwards dates;
static eligibility remains attached to each contribution. Ordinary lifetime
aggregates retain their own state. Unsupported clock-dependent predicates fail
instead of becoming arrival-only filters.

The native keyed owner stores shared typed daily accumulator buckets and exact
original CDC contribution metadata through the configured aggregate store.
Compatible horizons share argument/filter/date families. Registered generation
state includes reference dates and forward/reverse due-boundary indexes.
STR-29 owns quiet-key traversal and output scheduling; its maintenance callback
supplies the real watermark UTC day, distinct from event-row clock evaluation.

All keys remain in the existing registered `native-aggregate-v1` owner table.
The checkpoint identity includes the calendar layout and physical descriptors;
reconstruction checks descriptors against the actual argument and static filter.

| Prefix | Stored state |
| --- | --- |
| G | Typed aggregate state, generation and last emitted CDC row; lifetime state remains here |
| B | Group/generation/compatible family/UTC day typed accumulator |
| J | Group/generation/family/source row identity original eligibility, day and typed arguments |
| Q | Group/generation/output aggregate current raw reference day |
| H | Ordered next UTC boundary/group with generation |
| U | Group/generation reverse pointer to H |
| D / C | Existing dirty-output marker and bounded generation-retirement cursor |
| W / V | Finite real pruning progress and resumable bounded bucket-cleanup cursor |

A family requires identical function kind, argument, static gate and contribution
date expression. Immutable append inputs require no per-event J ledger. Distinct
CDC versions are not deduplicated by row identity; committed replay uses the
existing source-position/checkpoint protocol. STR-29 must compare H's generation
with the live G owner before invoking `recalculate_calendar_group` in its bounded
owner scope. The callback updates membership, due metadata and the dirty marker;
STR-29 then drains output through the established path.

## Watermark finality and retention

The user directed acceptance to be reconciled with the discussed single-control
Arroyo-style design. The existing `WATERMARK FOR ... AS ...` expression controls
progress. Common source admission drops a source change whose declared trigger
is older than previously emitted finite progress; equality is admitted. All
functions receive that same admitted stream. No separate allowed-lateness period
or calendar-only historical-error policy is introduced. Optional post-finality
corrections are tracked separately in [STR-69](https://trakkt.app/issues/STR-69).

A source-only envelope ordinal keeps CDC before/after images atomic, including
when before carries an old date or the grouping key changes. Both signed images
use the current change's internal trigger timestamp; original business payload,
eligibility and contribution dates remain unchanged. The ordinal is consumed at
the watermark boundary and is absent from downstream SQL schemas. Common
admission persists signed optional emitted progress in the existing global state
and advances progress only after admitted data is forwarded.

Calendar W metadata records finite real progress. For progress UTC day F, each
compatible family's largest horizon N retains buckets from F-(N-1) onward,
including the boundary and future buckets. V stores the cleanup frontier,
continuation key and completed marker. Each watermark/start/flush performs at
most one admitted page of deletes plus its cursor update; a completed frontier
avoids rescanning. New UTC progress resets the cursor consistently. Lifetime G
and original-contribution J survive recent payload pruning; expired original
retractions update lifetime without recreating old buckets.

Idle, complete silence and terminal EOF never manufacture a history cutoff.
Internal recalculation checks coverage before mutation. Generated calendar clock
plans require one triggering source, one watermark owner and idling disabled;
relaxing those constraints requires renewed admission/progress alignment checks.
STR-29 retains ownership of quiet-key traversal and output scheduling.

## Evidence ledger

| Check | Result |
| --- | --- |
| Pickup and pre-review in-flight guards | Passed, explicit STR-62 branch exclusion before review |
| Source formatting and staged diff whitespace | Passed |
| Oracle self-test and fixture generation | Passed; engine execution not implied |
| Historical core affected planner/worker check | Exit 0; `repaired-affected-check.log` |
| Historical core strict Clippy | Exit 0; `repaired-clippy.log` |
| Historical core planner tests | 13/13 passed; `repaired-planner-event-clock-tests.log` |
| Historical core worker tests | 6/6 passed; `resumed-worker-calendar-tests.log` |
| Historical core SQL executable | SHA-256 `5a0a52d17c6f887ad0c81657b3a330eb687b2455a7276e2c5d792feceec937f9`; `sql-source.diff` / `sql-testing.sha256` |
| Previous core SQL matrix | 32/32 passed on pre-revision source; `calendar-matrix-final-audit.json`. This does not qualify the new admission/pruning source |
| Revised admission/pruning source gates | Check, formatting and strict Clippy passed; planner 162/162, calendar worker 12/12 and admission 7/7 passed. Retry3 exact patch audit proves the intervening Rust change was test-only |
| Current actual SQL matrices | 40/40 passed: default 16, generic expression shapes 16, admission/pruning 8; memory/RocksDB, batches 1/8 and controller/leader recovery |
| Current SQL executable | SHA-256 `b3d80edbf416b8b903e74838c0b14e2155d95ceab2f084b83d2745583931e080`; immutable image `a95d0ca27fc1`; `optimized-pruning-retry3/final-matrix-audit.json` |
| Native window SQL investigation | Ten actual probes completed: four append closed-window maps/recovery passed, ordinary FILTER oracle/recovery passed, three precise planner rejections, two CDC window oracle failures |
| Quiet retained-key output with STR-29 | Not run; scheduler integration pending |
| Pruning/cursor/CDC recovery | 12 native tests and 8/8 actual SQL configurations passed on memory/RocksDB, batches 1/8, controller/leader recovery |
| Committed replay injection and full capacity/fault batch | Shared qualification remains STR-32 |
| Independent source review | Written independent source/evidence review approved; no confirmed unresolved source findings |
| Publication / PR CI | PR #23; current-head active Lint/tooling checks required before handoff. Full GitHub CI is disabled manually; no full-CI qualification claim |

The independent SQL oracle is `scripts/test-native-calendar-filters.py`.
It recomputes current source rows using ordinary date arithmetic, checks typed
CDC before/after continuity and covers 25 generic horizon outputs plus lifetime.
Its separate `--catalog-shapes` mode covers 18 calendar and seven lifetime
outputs, with enum, OR, IS DISTINCT FROM, NOT IN and COALESCE expression shapes.
These generic shape tests are distinct from externally supplied exact catalog
SQL and application parity. Both 16-configuration matrices passed on current source. The original catalog-shape planning
failure is preserved; explicit Boolean grouping repaired the fixture without
changing its independent expected values.
The 14-day expression in the default generic matrix is additional implementation
coverage, not an extra catalog output.
Fixture preparation and parser tests are distinct from native worker execution.
Full milestone capacity/resource/fault qualification remains STR-32.

Local logs and source receipts are preserved outside regenerable Cargo caches in
`/home/jason/qa-evidence/streamr-str62-2147/`. Review logs are in the canonical
checkout's `docs/review-logs/2026-10-10.md`. No calendar acceptance was moved to a new ticket:
unfinished scope remains STR-62. The tested core increment is published with final written review.
The user-authorized admission/pruning completion passed current-source verification.
`optimized-pruning-retry3/final-matrix-audit.json` records all 40 SQL passes,
40 unique fresh-worker recovery receipts and 136 live observation files.
`current-source-audit.json`, `validated-source-files.json` and
`test-only-difference-audit.json` link the executable, engine and harness receipts
and explain why retry2 planner/calendar passes remain applicable after the
admission fixture-only repair. Failed attempts retain exact source and logs;
none counts as a pass. The final rebase changes only unrelated main tooling/docs,
with reviewed executable file hashes checked again before publication.


## Source investigation retained as historical evidence

The initial investigation used the pre-revision source, pinned in the original
QA receipts. It established the admission gap that the user-directed common
watermark implementation now repairs. Do not repeat settled SQL probes without
changed source or contradictory evidence. Agent review signing is retired;
written findings/verdict and current-source checks remain required.

### Existing Arroyo machinery traced

At the original source revision, independent implementation and reviewer
traces found no source-wide late-input admission rule. `watermark_generator.rs::process_batch`
forwards the record before evaluating AS. `window_native.rs::process_batch`
rejects contribution panes below the watermark's slide bin, while its watermark
handler emits completed `[next-width,next)` intervals and retires panes before
`next+slide-width`. The original nonwindowed updating aggregate did not have that gate.
Upstream Arroyo preserves operator-local window admission.

The current calendar component already uses the existing keyed owner, registered
LiveTable, admitted bounded scopes/cursors, typed DataFusion update/retract/merge
state and checkpoint/source-position protocol. A fixed-window pane store is not
a missing backend adapter: it stores unique partial rows for closed intervals,
while this component point-updates shared daily state and signed original CDC
contributions. No independent review found a justified reason to replace the
owner/checkpoint machinery with a separate HOP pipeline.

An aligned daily HOP can calculate the same inclusive date-set for a completed
day. It does not by itself publish the current raw-reference result at 00:00:03
while that day remains open, nor reopen an older raw reference. Actual SQL probes now confirm the completed append-window maps and fresh-worker
recovery. They do not establish current raw-reference equivalence. The user subsequently instructed acceptance revision and consistent common
watermark finality; the current implementation applies that admission before
all downstream functions, rather than special-casing calendar aggregates.

### Actual native SQL probe results

The ten probes are preserved in
`/home/jason/qa-evidence/streamr-str62-2147/native-window-attempts/`, with each
`result.json`, exact capture log, independent expected values and source/hash
receipts. Original DATE sink transport failures were archived separately in
`native-window-attempts-date-sink-failure/`; corrected TEXT transport retains
internal DATE arithmetic. Those initial DDL failures are not capability evidence.

- Raw-reference-clock and contribution-clock TUMBLE/HOP append probes: four
  independent closed-window value maps and fresh-worker restores passed. Changing
  the window clock selects a different column; completion still waits for progress.
- Ordinary arrival-only FILTER: every typed CDC image, checkpoint prefix and
  fresh restore passed its own oracle. Final 1day/7day/lifetime counts are `1/4/6`,
  whereas maintained backwards-reference recomputation requires `1/3/6`.
- HOP plus lifetime join, retained daily aggregate plus trigger join, and direct
  TUMBLE-to-HOP rewindowing: rejected with the precise predicted planner errors,
  rather than another parser or transport failure.
- Contribution-clock CDC TUMBLE and HOP: capture completed but both independent
  current-row oracles failed. Existing window partial aggregation treats
  before/after images as unsigned input. That source was unchanged by STR-62;
  a separate base executable was not run, so this is source-attributed evidence,
  not a base-versus-candidate runtime comparison. The separate existing-window
  deficiency is tracked in [STR-68](https://trakkt.app/issues/STR-68); direct
  calendar CDC acceptance remains with STR-62.

Pinned upstream Arroyo commit
`5031092d3dea39710d44ea5fdabfe153426179e0` has the same relevant join-input and
nested-window restrictions; its downloaded source and receipt accompany the
probes. The broader upstream SQL documentation does not prove every relational
pattern exhausted. These completed investigations should be reused, not repeated
without changed source or contradictory evidence. No new syntax, historical cutoff
or public admission policy follows from these probe results.


Planner execution found a descriptor schema-mapping regression: DATE expressions
retained qualifiers, but serialization used an unqualified Arrow-derived schema.
The repair uses the qualified logical input with wire ordinal/name/type checks.
The SMALLINT numeric fixture also used an unsupported connector source type;
BIGINT plus explicit casts now tests physical coercion. Independent source review
approved this repair; repaired affected checks and Clippy passed, and all 13
planner tests passed. Default calendar and repaired catalog-shape matrices both passed 16/16,
for 32/32 completed configurations.


The first catalog-shape execution failed type coercion before calendar rewriting:
`Utf8 AND Boolean`. The generated static gate left `IS DISTINCT FROM` ungrouped;
the pinned Arroyo SQL parser reads its right side as a full expression, including
following AND. The fixture now groups each Boolean conjunct explicitly, preserving
IS DISTINCT FROM null behavior, both OR branches and NOT IN exclusions. Its failed
capture remains in `catalog-shapes-sql-repaired-schedule/`; rerun uses a fresh
directory. This fixture repair neither validates all unparenthesized SQL nor
requires repeating the completed default matrix.
