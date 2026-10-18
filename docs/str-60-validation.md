# STR-60 grouped event-time selectors

## Controlling scope clarification — resumed 18 October

The user explicitly approved one shared grouped FIRST/LAST implementation for
ordinary GROUP BY and TUMBLE/HOP/SESSION on both memory and RocksDB. Analytic
OVER is unchanged; no-FOR retains explicit ordering without a guessed clock.
Independent architecture review approved one descriptor/candidate/identity/codec
and selection implementation, with retractable candidate retention in ordinary
CDC groups and mergeable winner retention for append-only lifecycle paths.
Window panes allocate persistent arrival identity before bin sorting, HOP retains
that identity across overlap, and SESSION exposes its existing stored row ordinal.
Existing per-lifecycle state/resource limits are required, with precise missing
configuration errors; no new knobs, budgets or alternative selector dispatch.
Window changelog support remains governed by the existing operator eligibility
contract; this change does not add a window CDC/recovery protocol.

The shared implementation and final SQL matrix pass the selected current-source
checks below. Earlier failed runs remain historical evidence; the discussion hold
is resolved.

Implementation base: `14353046e5085325b66563981be61c96ecfed7b7`.
Instructions: `AGENTS.md`, `.claude/build-test.md`, current Trakkt STR-60,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`.
Evidence: `/home/jason/qa-evidence/str60-selectors-20261018/`.

## Final current-source validation — phase18 and runtime4

On base `eb5127dba7ac90ec0bea44249b0ec0ca27db46c8`, phase18 completed in one
maintained shared reservation, normal dev/test profile with matching precompiled
RocksDB and immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Prep fmt, fmt check, locked state/worker/SQL-testing check and strict Clippy all
exited0. State checkpoint tests18passed, aggregate-store5passed,
incremental90passed/0failed/5existing diagnostic ignores, capture5passed.
The full incremental suite includes inherited quiet-calendar regressions, exact
CDC identity/retraction/recovery, bounded streamed replay, and
`selector_real_flush_preserves_checkpoint_writer_headroom_and_reloads_both_backends`.
The latter holds the actual exporter-sized lease during real drains on both
backends, checks refusal at142F and success at exact A+C, fresh-owner continuation,
optional-cursor refusal, boundary/overflow and permit release. Actual exporter
checkpoint/restore is established separately by runtime4.

Runtime4 then built/pinned and executed inside the maintained image in the SAME
reservation: **64/64 supported output cases PASS; 8/8 expected existing RocksDB
analytic eligibility rejections PASS**. Output cases comprise ordinary CDC8,
append8, memory analytic8, TUMBLE/HOP/SESSION24, no-FOR typed8 and finite mixed
companions8. Of these,56 require and pass exact per-key uninterrupted/recovered
CDC comparison with durable source-offset boundary receipts; typed8 instead
validate their established typed output/recovery contract and do not claim exact
CDC comparison. The eight analytic RocksDB refusals are negative eligibility
checks, not successful analytic outputs. No-FOR unordered changelog input still
requires explicit ordering; analytic semantics and window changelog eligibility
are unchanged.

All source maps, external input maps and pinned executable hashes match before
and after. Pin SHA256:
`e6e11675183fdcb46055492b755874e6d79c54911e55d71f81812780e62dad9a`.
Owned containers remaining0. Finite capture sampled process RSS spans
177668096..189911040B, diagnostic rather than a peak/capacity bound.
Native traversal counts, CPU and I/O remain unmeasured; no throughput,
larger-than-RAM, process-kill or whole-graph resource guarantee is claimed.
Explicit finite FRAME1MiB receives authoritative exporter C4374526B and final
D153272318B; core profiles/defaults remain unchanged. Independent source review
approved the shared kernel, bounded adapters and exporter overlap repair; final
evidence review and publication/PR CI are separate delivery steps.

Exact evidence: `repair-gates-18.json`, `repair-18-compiled-source.json`,
`repair-18-format-paths.json`, per-suite repair-18 logs, and
`runtime-4/{commands,source-and-binary,full-case-inventory,finite-resource-receipts,cleanup}.json`.
Unchanged planner23/window15/session5/store31 checks passed phase13; dependency
full-library15 and strict lint passed phase15. These were not repeated for the
later backend-helper-only repair. Phase18 covers the affected state helper,
ordinary store/runtime and capture harness. Calendar namespace filter0 in phase16
is not separate coverage; its inherited five tests execute in incremental90.

## Historical verification inventories — superseded by final results above

All pending/not-run statements below describe their named earlier source/run;
they do not describe final acceptance. Failed logs and superseded budget profiles
are preserved to distinguish diagnosis from successful current-source evidence.

### Earlier selected verification and runtime repair status

Current base is `eb5127dba7ac90ec0bea44249b0ec0ca27db46c8` after integrating
STR29 quiet-calendar dispatch. The task was preserved in a stash and external
hash/diff snapshot; three-way application had no conflicts. AGENTS/build guidance
is unchanged. `eb5127-integration.json` identifies exact changed task paths.
Phase16 maintained fmt, locked planner/worker/sql-testing check and strict Clippy
all exited0. Full incremental88passed/0failed/5ignored (including five inherited
quiet-calendar regressions), harness4passed. The calendar_native filter selected
0 tests/263filtered and provides no separate coverage. Exact receipts/logs:
integration-gates-16.json, incremental-integration-16.log and
harness-integration-16.log. Unchanged planner/window/session/store sources and
verified vendor tests were not repeated solely for the integration.

Phase13 on8f937aa9: maintained fmt, locked planner/worker/sql-testing check and
strict Clippy all0; planner19+3+1, incremental83, window15, session5,
windowstore8, sessionstore23 and harness4 pass. Diagnostic ignores remain5/1/1/1/2
for incremental/window/session/windowstore/sessionstore. Isolated session/store
subprocesses also pass. The vendor regression initially could not run because
Cargo excludes nonmember package dev-dependencies. Workspace membership and its
existing Rand0.9.1 lock edge fix that; no version/source upgrade. Phase14 strict
vendor lint found two upstream test-only redundant into_iter calls; phase15 fmt,
strict vendor Clippy and full common library15passed/0failed. Production patch
uses state_mut for emitted/removed group states, preserving allocation accounting
and unemitted groups. Exact pinned source/license/provenance live in
vendor/datafusion-functions-aggregate-common; only terminal-state semantics,
regression, two test lint cleanups and rustfmt differ from upstream.

Runtime1 completed all70 cases with source-before==after and unchanged pinned
executable:36passes/34failures. TUMBLE/HOP/SESSION24 and no-FOR typed8 passed;
analytic memorybatch1 four passed. Failures:16 ordinary fixtures used unsupported
JSON RETURNING BIGINT;4 memory analytic batch8 checkpoints closed no window;
8 analytic RocksDB cases hit existing unsupported WindowFunction eligibility;
2 finite ordinary constructors exceeded the wide query's admitted indexed-event
bound;2 finite TUMBLE cases exposed upstream immutable BytesView DISTINCT state;
2 finite SESSION checkpoints closed no window. All logs/receipts retained under
runtime-1, including full-case-inventory.json. No hand-input result substitutes
for SQL execution.

Repairs retain current contracts: DOUBLE PRECISION JSON extraction plus BIGINT
cast; filtered controls align checkpoints with complete source batches; exact
upstream terminal consumptive state API; split companion queries preserve all37
independent oracle fields. Finite FRAME derives aggregatewrite/overlay64F,
windowwrite8F, page2F, pending24F, decoded139F and queue twice the actual
AdmittedWriteBatch reservation. Only explicit test flag changes those budgets;
core defaults stay pending512KiB/decoded16MiB. Current runtime2 plan is64 supported
output cases plus8 separately asserted existing analytic RocksDB rejections.
Final source runtime2 is NOT RUN. No capacity/performance qualification is claimed.

Exact gate receipts: reserved-gates-shared-13.json, vendor-gates-14.json and
vendor-gates-15.json. Earlier phase12 bounded replay test proved logical payload
and encoded members>F, the actual single-materialization budget, with unchanged
8MiB backend and fresh owner exact HLL1; the old>3F attempt hit capacity and stays
a recorded failure, not a larger-than-RAM claim.

## Runtime2 complete inventory and bounded repair

On eb5127db, runtime2 built/pinned normal dev/test and used the full immutable
maintained image with matching precompiled RocksDB. All72 attempts completed;
source-before/after, external-input maps and executable hashes match. Linkage,
visible pin and native receipt checks passed. No task-owned container remains.
64 output cases:52pass/12fail. Exact passes: CDC4, append6, analytic memory8,
TUMBLE/HOP/SESSION24, typed8, finite companion TUMBLE2. Failures: six ordinary
initial/recovery CDC sequence comparisons (startup timer first-tick/input race),
four finite ordinary runs exhausted the explicit139F decoded pool, two finite
SESSION rows differ at singleton CORR (Single0.0 versus partial-merge oracleNULL).
Eight unchanged analytic RocksDB refusals occurred; the strict negative checker
accepted four controller logs and falsely rejected four differently formatted
leader errors. These four checker failures remain recorded as failures.
Evidence: runtime-2/full-case-inventory.json, commands.json, cleanup.json and
per-case comparison/failure/capture artifacts. No failed output case is counted
passed merely because its final scalar state or capture process was correct.

The resource failure has exact simultaneous ownership: output3pending72F plus
scopeoverlay64F/read3F consumes139F, then selector evaluation independently
reserves up to3F. A conditional selector constructor floor and explicit finite
profile142F preserve existing pools/defaults/permit lifetimes; tight nonselector
configurations retain their old139F floor. A direct simultaneous-permit negative
139F/positive142F/release/overflow regression passed in phase17.
This checks resource ownership/guard behavior; only a subsequent actual SQL flush
will establish integration success. Nonselector constructor admission is unchanged. Phase17 maintained prep fmt/fmtcheck, locked worker/SQL-testing check and strict
Clippy exited0; full incremental89pass/0fail/5ignored and capture5pass.
The matching-CDC opt-in now checkpoints each real source batch plus the forced
prefix, reads each saved f offset via TableManager and verifies monotone epochs
and identical initial/recovery boundaries before exact CDC comparison. Source
linger is pinned only for this test mode. The pure boundary test preserves8/16/19/27
for a forced19-row prefix with8-row batches and accounts for the automatic
post-checkpoint row. No production timer/control semantics changed.
Finite SESSION uses two seed rows (same18-row source/CP16) to keep nondegenerate
CORR expectations. Ordinary scalar CORR0 for singleton follows pinned upstream
evaluate behavior; every input/retraction and37-field split oracle is preserved.
Negative checks accept only the exact pre-execution refusal in the two existing
protocol error forms. Actual runtime3 validation remains pending.
Receipts: repair-gates-17.json, repair-17-compiled-source.json and per-suite logs.

## Runtime3 inventory and checkpoint-writer overlap repair

Runtime3 completed all72 attempts on one current pin:63/64 output cases passed,
eight/eight unchanged analytic RocksDB eligibility checks passed. The sole failure
was finite ordinaryB on memory, decoded pool148,897,792B exhausted; the identical
RocksDB case passed. Every core ordinary/append/window/session/analytic-memory
and no-FOR typed case passed exact output and checkpoint/CDC checks. Source and
external-input maps match, pin unchanged, no owned containers remain. See
runtime-3/full-case-inventory.json and cleanup.json. Passing finite-test process
RSS177,405,952..188,530,688B is recorded in finite-resource-receipts.json;
CPU/I/O/native seek/internal/deleted-key traversal are unmeasured. These small
correctness cases do not establish capacity, throughput or larger-than-RAM bounds.

Independent source analysis identified an omitted existing owner: asynchronous
checkpoint export retains C4,374,526B while operator callbacks resume. Output72F
and scope67F plus C exceed142F even before candidate decoding. The historical
failing acquisition is inferred (its old log omitted stage), not a sampled stack.
New stage/request contexts and a forced-writer-permit real-drain regression make
that overlap explicit. A selector owner's foreground A includes output3pending,
scopeoverlay+3value, and max(candidate3value, one actual encoded-key Rocks read).
It must coexist with ONE own exporter C(D). Capture semaphore1 and serial table
export establish one writer per owner, not max_open_databases writers. Other
owners share the worker pool and need their own configured headroom; this is no
whole-graph guarantee or graph admission layer.

The existing backend Parquet calculation is exposed as a pure ResourceConfig
helper and reused unchanged by export/restore. Selector startup checks A+C(D)
before backend construction; nonselector admission remains unchanged. Optional
selector cursor reservations preserve the same mandatory allowance or decline.
The explicit test FRAME profile recomputes C using final scan/queue/database
settings to a checked fixed point and emits C with actual D. For the finite
1MiB profile A142F+C requires153,272,318B. No default/pool/protocol/timer change.
Boundary-1/exact, overflow/release and real drain/new-owner continuation on both
backends subsequently passed phase18; runtime4 actual checkpoint/recovery also
passed. See the final current-source section above.

## Newbase selected validation — phases 10 and 11

On integrated base8f937aa9, phase10 fmt/locked3cratecheck/strict Clippy all exit0.
Planner19+3+1, nativewindow15, nativesession5, windowstore8, sessionstore23 and
harness4 tests passed (session/store isolated subprocesses each add one pass).
Their explicit diagnostics remain ignored as recorded in suite logs. Incremental
coverage had81pass/2fail/5ignored; both HLL fixture stores lacked7KiB of scan
request/cursor headroom before processing input. The exact required1,055,584B
versus old1,048,576B is retained in the logs. Only the cfg(test) store resource
helper changed to max(existing1MiB,8*page), matching finiteFRAME's16F scan pool;
production constructor admission remains unchanged.

Selective phase11 fmt/lockedworkercheck/strictworkerClippy exit0. Full incremental
suite82pass/1fail/5ignored: HLL payload-scratch corruption, conservative maximum,
nonlargest/largest deletion and empty reset pass. The remaining finite history
test hits the fixture's8MiB memory-backend resident cap during insertion
(required8,389,280B). Its requested encoded history target was>3F, whereas the
actual single-replay materialization admission is F (`working<=value_bytes`).
Resolution of this test setup remains pending; no memory capacity or larger-than-
RAM claim is made. Other eight unchanged phase10 suites were not repeated.
Exact receipts/logs: `reserved-gates-shared-10.json`,
`focused-tests-shared-6-inventory.json`, `reserved-gates-shared-11.json`,
`focused-tests-shared-7-inventory.json`. ActualSQL70 NOT RUN.

## Integrated base and reserved phase 9

Fetched main advanced to `8f937aa9b64e3dafd8af3c0c9bd7d733a208900e`
(Kafka checkpoint integration). The task branch fast-forwarded without a WIP
commit; its stash reapplied with only the ledger conflict, resolved by preserving
both STR146 and STR60 entries. Task compiled-source hashes match phase9 except
that ledger. Integration receipt `main-integration.json`, preserved patch and
stash reference remain outside caches. Current AGENTS/build guidance and ledger
were reread; native RocksDB pin files remain unchanged. Main's vendor/Kafka
files are baseline, not STR60 changes.

Phase9 completed on the initial base: fmt/check/strict Clippy exit0; planner
selector19, capability3 and computed-projection1 passed; incremental81pass/
2fail/5ignored; nativewindow14pass/1fail/1ignored; nativesession5pass/1ignored;
windowstore8pass/1ignored; sessionstore23pass/2ignored; harness4pass. Session
and sessionstore isolation subprocesses each additionally passed one test.
`reserved-gates-shared-9.json`, `focused-tests-shared-5-inventory.json` and source
receipts contain exact commands/counts. Two earlier phase9 submissions were
canceled while queued (exit130), before any gate started, to consolidate the
maintained evidence mount and canonical nested-metadata identity repair.

The three remaining failures exposed incomplete test configuration or stage
assertions: fixed HLL replay needs the full retained bound under value_bytes and
24 times that working bound under pending-output bytes; the physical window
stage precedes SELECT aliases. Current repairs keep the existing conservative
materialization factor and derive frame/pending/decoded bounds from existing
configuration. For explicit test FRAME=F, overlay=8F, pending=24F and the existing
constructor inequality requires decoded>=8F+3*24F+3F=83F. This applies only to
explicit finite fixtures; absent FRAME core64 limits stay unchanged. Runtime
acknowledgements must expose the actual limits. No product default/pool change.
The raw-FOR/AS fixture starts progress at11 seconds after epoch to keep its
progress-minus5s clock nonnegative while remaining distinct from raw2026 time.
Newbase gates and actual SQL70 remain NOT RUN.

## Reserved phase 8 — complete selected inventory

Formatting, locked planner/worker/SQL-testing check and strict Clippy all exit0.
One shared reservation and normal dev/prebuilt RocksDB profile as above. Source
receipts `shared-8-compiled-source.json`/`shared-8-source.diff`; exact commands,
exits/durations `reserved-gates-shared-8.json`; complete suite counts/failure
names `focused-tests-shared-4-inventory.json`.

| Selected suite | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| planner selector | 18 | 0 | 0 |
| planner capability | 3 | 0 | 0 |
| planner computed-variable projection | 1 | 0 | 0 |
| worker incremental aggregates | 76 | 7 | 5 |
| worker native windows | 14 | 1 | 1 |
| worker native sessions | 2 | 3 | 1 |
| window store | 8 | 0 | 1 |
| session store | 23 | 0 | 2 |
| harness capture | 4 | 0 | 0 |

Session native/store subprocess isolation additionally passed one test each.
Eleven failures were collected before repairs: three calendar restores exposed
ordinary-SUM versus sliding-SUM descriptor layout mismatch; two HLL tests had
insufficient constructor allowance; two older assertion strings referred to a
later rejection than the now-earlier typed/identity checks; one new planned
window test lacked queued-write batch headroom; three session tests exposed
mandatory retained-state headroom and optional proof-cache contention. Current
repairs retain the small-limit negative cases and derive positive allowances
from the actual typed/batch bounds. The receipt marker also needs a leading
newline because libtest can prefix its first stdout line. These repairs remain
unvalidated. Actual SQL70 is NOT RUN.

## Current validation gate — reserved phase 7

Normal dev/test, immutable matching-precompiled-RocksDB image `bad108ab1443`.
Purpose: affected planner/worker/SQL-testing correctness gate, not performance.
One outer shared reservation held formatting, locked check and strict Clippy;
current unchanged api.proto timestamp forced prost regeneration under ownership.
Queue guard reclaimed 16.72 GiB of regenerable invoking-checkout target cache;
103.9 GiB remained. Source receipts: `shared-7-compiled-source.json` and
`shared-7-source.diff` outside caches.

- Formatting exit0, 4.08s; locked three-crate check exit0, 266.41s.
- Strict Clippy exit1, 43.92s: two planner test lint errors, one worker
  collapsible-if lint, nine worker test compile errors (one missing L32 field,
  eight stale test-only member-key call signatures). Complete log retained as
  `clippy-shared-7.log`; exact command exits in `reserved-gates-shared-7.json`.
- All nine selected test suites NOT RUN, blocked by the required lint gate.
  Mechanical fixes are now applied but not revalidated. No actual SQL has run.
- Finite six-case mixed-builtin fixtures are prepared externally. Their verified
  constructor floors exceed the core 32KiB test frame limit; a scoped test-only
  limit transport is being added. Core64 configuration stays unchanged.

The current versioned member totals are 32 bytes (count, encoded sum, scalar
sum, maximum typed current-member workspace). Fixed-state replay reserves its
verified accumulator plus one member, and streams bounded M pages without
collecting history. Linear-state companions reserve the complete admitted
relation. The maximum includes typed decoding, temporary encoded copies and
verified payload scratch; each restored member is checked before decoding.
It remains conservatively high after deleting the largest row and resets only
when the relation becomes empty. Old/malformed totals are rejected; no migration
or checkpoint protocol replacement is introduced. New codec/corruption/reset/
large-history tests are prepared and remain NOT RUN.

## Contract and source trace

This is Streamr's non-ANSI grouped FIRST_VALUE/LAST_VALUE extension. With an
unambiguous declared WATERMARK FOR lineage, raw FOR time is the ascending
primary key, user ORDER BY expressions retain their direction/null placement as
secondary keys, and write identity breaks remaining ties. FIRST keeps the
oldest accepted write; LAST chooses the newest. FILTER and IGNORE/RESPECT NULLS
retain SQL meaning. No-FOR explicit ordering and analytic OVER retain their
existing behavior. The planner exposes the effective ordering in Explain.

Upstream source inspected: local pinned Arroyo DataFusion `916b45f`,
`datafusion/functions-aggregate/src/first_last.rs`: LAST update_batch and
merge_batch use strict `is_lt`, retaining the earlier equal-key winner across
batches. Streamr's indexed native path already reverses LAST SQL keys but
previously appended every insertion ordinal ascending. Its args-tuple retraction
lookup could remove an equal-valued peer rather than the intended row identity.

The planner rewrites grouped FIRST/LAST to internal shared wrappers carrying an
explicit descriptor and lifecycle arrival ordinal; analytic registrations remain
unchanged. Ordinary CDC stores all candidates in M and exact row identity in R;
append-only ordinary/windows retain one mergeable winning candidate. Both call
the same descriptor, eligibility, ordering, candidate codec and winner operation.
Debezium IDs are MultiHash(primary_keys), produced in physical.rs; updates emit
before then after. Duplicate live IDs are rejected, replacements receive a new
ordinal, and exact retraction restores the remaining winner's original ordinal.
Existing group generation, native checkpoint and source-position protocols are
reused. TUMBLE/HOP assign persistent ordinals before bin sorting; SESSION exposes
its retained row-key ordinal. No legacy grouped selector dispatch remains.

## Resource and scan account

No new RocksDB scan primitive or cache is introduced. The existing winner read
uses native member prefix M(group length, group, generation, aggregate), with an
inclusive prefix lower bound and its exclusive native prefix successor upper
bound. FIRST keys sort forward; LAST SQL keys and final ordinal sort in reverse.
For retractable CDC this enumeration discovers the remaining winner after
insertion/deletion; known-key reads cannot identify the next candidate. One
winner lookup occurs per selected CDC aggregate while draining a dirty group.
Append-only selectors retain one winner at the exact M prefix and use point
gets, with no winner scan. The CDC scan returns the first live member, subject to configured page_entries/page_bytes; backend traversal can
exceed returned rows because deleted internal keys and the scope overlay remain.

CDC selector retractions replace the args-peer prefix scan with two known-key
reads R(id) then M(stored key), followed by two deletes. This selector comparison
does not describe the new companion replay/prepasses accounted separately below. Each scan snapshot and bounded overlay/decoded
reservation belongs to its AggregateScope and ends at scope drop/commit. Scan
pages are released before fetching another page; no cursor/snapshot survives an
input batch or recovery. Insertions invalidate optional duplicate progress;
selector retractions use exact keys and do not create duplicate progress.
All nonwinning candidates remain on the configured backend. Candidate key/value,
page, write, overlay and output limits remain configured; no admission mechanism
removed by the user is reintroduced.

Compared with fetched main, the CDC M-prefix bounds, traversal implementation
and lookup frequency are unchanged; exact retraction removes peer enumeration.
Append-only selection still introduces no scan. Finite actual-SQL workload and
RSS receipts are recorded in runtime-4/finite-resource-receipts.json. Native seek/internal-key/deleted-key traversal
counts, CPU and I/O are unmeasured; returned rows are not native traversal work.
No performance improvement claim is made.

### Companion replay scans introduced by preserving mixed SQL

Verified replay companions reuse ordinary M/R/L member retention and
DataFusion accumulation. L statistics cap the combined members, encoded args,
decoded slots and verified linear accumulator bytes before replay. A dirty-group
flush uses one existing visit_prefix over M(exact group,generation,aggregate),
merging its bounded overlay and one page at a time. Its inclusive prefix lower
bound/exclusive prefix-successor upper bound cannot traverse other groups.
Before each decode/update, live counts/bytes must remain within admitted L totals;
the complete replay must match those totals. This enumerates the finite admitted
relation (or stream fixed-state companions one row at a time); shared FIRST/LAST still read only their winner. CDC retractions
of replay companions with IDs use exact R(id)->M point reads.

Window verified-companion final-state preflight reuses the existing collection preflight
snapshot/pass, counting stored List child cardinalities for all admitted linear
companions before invoking DataFusion. It traverses only the selected group's
pane range, with logical after-key progress and existing page/partial limits.
Partial preflight runs before take/DataFusion, charges shared typed fixed and
linear per-group floors, and caps chunks under the existing partial allowance.
Focused refusal, cancellation, cache-yield and retry tests passed in phases13/16.

SESSION verified-companion admission adds a conservative raw-row/payload counting
pass before DataFusion,
without evaluating SQL arguments/FILTER twice. It uses the same StateSnapshot as
the subsequent producer. Existing next_row scans R(group,time,ordinal), lower
row_key(group,start,0), exclusive upper row_key(group,end,u64::MAX)+00, and after
progress max(lower,previous_full_key+00). Each call requests one entry and the
configured page_bytes; an empty lookup ends the pass. The count holds only one
decoded row and the last logical key, then reserves the combined verified companion
state allowance from existing decoded-state resources before materialization.
The producer performs the second necessary pass over that same range/view.
Snapshots/pages/row permits and retained-state permits end on completion,
rejection or cancellation; no backend cursor is persisted across recovery.
These are new scans/charges. Focused refusal/cancellation/retry tests passed;
actual core SESSION8 cases passed in runtime2. The
expanded finite companion SESSION oracle remains pending its corrected fixture.
Native internal traversal counters remain unmeasured; finite RSS/page evidence is
pending. No performance or broad qualification claim is made.

## Historical validation inventories (superseded by current status above)

Purpose: correctness and checkpoint recovery, normal dev/test profile, matching
precompiled RocksDB through maintained scripts and shared target/queue. Initial
disk110GiB free; latest106GiB. Default image failed before Cargo exit2; supported
STREAMR_DEV_IMAGE=bad108ab1443 selects the installed matching archive. No native
source build or release build selected. Evidence is outside regenerable caches.

Initial prototype evidence (before unified implementation): check-initial.log
exit0; clippy-initial.log exit1 (test helper Box mismatch, repaired);
worker-tests-initial.log exit0,81passed/0failed/5ignored;
planner-tests-initial.log exit101,3passed/2failed (FILTER/IGNORE parser fixture
position and analytic input lacking finalized window, repaired); fmt-initial.log0.
These results do not qualify the current shared source.

First shared-source inventory, completed before consolidated repair:

| Log | Exit | Passed | Failed | Ignored |
| --- | --- | --- | --- | --- |
| check-shared-1 | 0 | affected planner/worker/sql-testing check | 0 | — |
| planner-selector-tests-shared-1 | 0 | 11 | 0 | 0 |
| worker-incremental-tests-shared-1 | 101 | 61 | 16 | 5 |
| worker-window-native-tests-shared-1 | 0 | 11 | 0 | 1 |
| worker-session-native-tests-shared-1 | 0 | 4 | 0 | 1 |
| worker-window-store-tests-shared-1 | 101 | 7 | 1 | 1 |
| worker-session-store-tests-shared-1 | 0 | 23 | 0 | 2 |
| harness-capture-tests-shared-1 | 0 | 3 | 0 | 0 |

Session suites additionally spawn one successful isolated child test each; the
parent result counts above avoid double counting. Exact command exits/durations
are in focused-tests-shared-1.json; individual logs retain every failure.

Post-repair maintained fmt-shared-2 and affected locked check-shared-2 passed
exit0 (planner/worker/sql-testing); gates-shared-2.json records exact durations.
This precedes the final IPC integrity/companion adapter changes and does not
substitute for final strict Clippy, focused tests or actual SQL.

Consolidated repairs in progress: physical augmented schema vs raw graph input;
scope-owned resources while the operator temporarily takes its store; strict
pre-builder ordinal/raw-FOR descriptor validation; bounded logical one-row value
and ordering encoding including FixedSizeList and compact dictionary slices.
Three calendar fixture failures arose from unused selector positions combined
with intentionally repeating calendar contribution IDs; those unused positions
now use COUNT, preserving all calendar identity/recovery assertions. Production
duplicate selector IDs remain rejected. Window arrival recovery fixture now drops
its restored store before closing its backend. Repairs are not yet validated.

Reserved shared-source phase6: fmt-shared-6 exit0; check-shared-6 exit0;
clippy-shared-6 exit1 only unused UnionMode test import. The required lint gate
blocked all8 selected focused suites; they are NOT RUN on this source. One
outer scripts/rust-build reservation prevents other worktrees from replacing
shared generated RPC artifacts between maintained commands. Earlier check3/5
failed against stale generated emit_per_event absent from current proto;
timestamp-only regeneration restored check4. rpc-cache-invalidation.json keeps
source/baseline and stale/regenerated hashes; no protocol content was changed.
The normal queue guard reclaimed7.08GiB cache before phase6, with114.1GiB free.

Consolidated next source batch includes the lint import, typed fixed-companion
partial/final workspace, verified nonretractable fixed companions and audited
TDigest/COUNT DISTINCT state descriptors. Approximate numerical acceptance must
respect upstream approximation guarantees, without claiming bitwise equivalence
across different compression schedules. Opaque UDAFs are not blanket admitted.
Final strict Clippy, focused runtime suites, all64 actual SQL cases and finite
scan/resource evidence remain pending. Actual SQL covers memory/RocksDB,
batches1/8, controller/leader recovery, ordinary CDC exact before-image identity
and per-key emitted CDC equality, append-only ordinary/TUMBLE/HOP/SESSION,
FixedSizeList values, coherent JSON/nullable output and analytic/no-FOR behavior.
Prepared fixtures and direct operator tests are not SQL runtime passes.
Independent implementation review requests changes; architecture direction approved.

## Consolidated test ownership

The seven retired tests used the removed grouped-selector fallback/checkpoint
layout. Their current replacement checks are explicit below; no replacement is
counted passed on the shared implementation until it has actually run.

| Retired test | Current replacement coverage (see current source-bound results above) |
| --- | --- |
| `native_ordered_values_across_batches_and_retractions` | `native_owner_reloads_ordered_and_filtered_state_after_retraction`; mandatory SQL keys `raw`, `ties`, `replacement` |
| `native_ordered_repeated_values_keep_distinct_sequences` | `event_time_selectors_keep_exact_write_identity_after_reload`; mandatory SQL keys `identity`, `replacement` |
| `native_ordered_equal_keys_retain_remaining_duplicate` | `event_time_selectors_keep_exact_write_identity_after_reload`; SQL `identity` has X/Y/X then retracts the later X, requiring Y |
| `native_filter_null_values_and_row_updates` | `native_ordered_ignore_nulls_skips_only_null_members`; SQL `nullable`, `blank`, `email-priority`, `replacement`, including JSON_EXISTS eligibility, explicit nulls, false, zero and updates |
| `native_ordered_checkpoint_rows_reload_into_fresh_operator` | `native_owner_reloads_ordered_and_filtered_state_after_retraction`; actual fresh-worker controller/leader checkpoint suffix for each ordinary/window matrix |
| `native_ordered_legacy_checkpoint_rows_reject_before_input` | Obsolete selector serialized format retired; current candidate codec/schema/checksum rejection is tested by `codec_schema_corruption_and_stale_secondary_version_fail_precisely` |
| `native_filtered_legacy_checkpoint_rejects_before_input` | Obsolete selector serialized format retired; current stale-version rejection is tested by `shared_selector_rejects_stale_before_version_without_losing_candidate` |

The existing append-only winner/reload/null tests now construct the shared
wrapper and read its persisted candidate. The 100-row check still requires
constant five selector winner records. Ordinary CDC retains nonwinning indexed
candidates so exact retractions can expose the next winner. Both retention
strategies call the same candidate encoding, ordering, eligibility and winner
logic. No old grouped FIRST/LAST accumulator or fallback index remains an
alternate implementation.
