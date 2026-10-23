# Native finalized-result composition (STR-150)

The existing accepted native `MERGE` result route now admits one to four
aggregate branches, including at most one native finalized SESSION producer
followed by an updating aggregate. Ordinary updating JOIN behavior is unchanged.
Applications still own their SQL and schema; no application is recognized by the
engine.

A single source admission root supplies the original-event responses. Direct
aggregate branches retain their copied source grouping proof. The finalized
branch consumes each admitted response through the existing native SESSION
operator and forwards an empty acknowledgement. An empty/filtered input never
creates a session result. SESSION closes only through its existing watermark
callback, emitting a bounded native append row with its native closure clock.
Those rows do not acquire original-event identity or an unrelated trigger clock.

The post-SESSION native aggregate consumes one finalized append row at a time,
commits its native aggregate update and emits one complete per-key replacement.
Existing terminal native materialization consumes that replacement. It can
interleave with complete native calendar replacements without crossing the
original-event response cut. Existing input parking, awaited collectors and
checkpoint barriers remain responsible for ordering; there is no new state,
checkpoint, admission pool, cache or timer protocol.

Raw branches still require one physical source definition and identical copied
source grouping keys. Finalized branches expose their complete ordered native
group tuple; they may derive its values through native aggregates such as
FIRST_VALUE. Their empty raw acknowledgements carry no invented touched key.
Calendar expressions after finalized SESSION remain unsupported because a window
closure is not the original raw event clock. Multiple finalized producers and
foreign source ancestry are rejected.

## Regression and integration acceptance

Planner tests cover valid four-branch composition, foreign-source rejection and
multiple-finalized rejection. Runtime tests cover empty and filtered responses,
duplicate/skipped sequences, exact before/after replacement clocks, ordinary
native SESSION finalization, memory/RocksDB exports/restores with fresh attempt
context, and four-input maintenance/barrier alignment.

External application integration uses caller-owned SQL, fixtures and literal
expected results. The captured ordinary JOIN c/d/c sequence remains failed
baseline evidence; it is not filtered into a passing result. The application
uses native MERGE RETURNING snapshots with SQL-produced native action, rather
than describing ordinary differential CDC as a completed materialized result.
Actual Kafka worker-SIGKILL recovery must establish committed-prefix continuity,
fresh recovered outputs and restored native state-table lookups. Compilation and
source review do not substitute for this execution.

## RocksDB scan accounting

This extension adds no new scan implementation or index. Each finalized append
callback invokes the existing generic dirty drain after the keyed native update.
For the SUM/COUNT average shape, the update uses exact group-key reads/writes.
Dirty enumeration uses logical prefix `D`, with markers
`D || u32(group_key_length) || group_key`, then exact `G` state reads. One dirty
group with no backlog requires two fresh drain scopes: find/delete its marker
and publish its replacement, then establish exhaustion.

Each query requests at most `min(page_entries, 1 + has_after)` entries and
`page_bytes` bytes. The exclusive logical continuation survives committed drain
iterations. RocksDB seeks at the greatest encoded namespace/prefix/start/cursor
bound, with an exclusive native upper bound at the encoded D-prefix successor;
there is no native iterate-lower-bound, so positioning uses Seek(lower).
Snapshots and decoded/write scope ownership end at commit, before awaited output
delivery; output workspace remains reserved through collector acknowledgement.

Dirty enumeration is reused to preserve the existing native delta/commit/delivery
kernel. The callback knows its one group, so enumeration is not logically
necessary: a dedicated known-key publication path could avoid it. This repair
introduces neither that refactor nor an unbounded cache/index. Exact internal
seek/next/deleted-key traversal can be observed with the existing opt-in
`STREAMR_LIVE_SCAN_DIAGNOSTICS=1`; returned rows alone do not measure backend
work. No throughput, production capacity or performance improvement is claimed.

## Evidence

Fresh fetched base: `a5d28ad868f8e64cd3d2daa158b12e11b52e3c03`.
Maintained dev/test profile and matching precompiled RocksDB; immutable build
image `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Exact source/image/commands/results and preserved executables live under
`/home/jason/qa-evidence/str150-composed-publication-20261023/`.
Locked planner/worker check, 17 maintained MERGE planner cases, two finalized
routing-proof cases, two finalized aggregate cases, seven response-ordering cases,
and SESSION lifecycle recovery on memory/RocksDB pass. Strict worker/planner
Clippy passes. Independent written source review approves the intended capability,
no concealed deficiency and no unauthorized architectural deviation.

First CLI pin `dev-pin` has binary SHA256
`564d24d097ebf6227d65fee13d0e8ed66f60cd1789cd0a24b4f2d9d6f27baa24`;
compiled-source audit passes. Its real Kafka graph (`graph-1`) fails before input:
response SESSION retained a Shuffle physical edge and the unchanged worker guard
rejects repartitioning. The planner now emits Forward only for the proven response
SESSION route. Ordinary SESSION remains Shuffle; both physical layouts have
regression assertions. Fresh independent review approves this repair. Maintained
regression gates and a new pin/Kafka run are in `attempt-2`; their pending outcomes
are not represented as passes. Arcstream's 30 native Python tests and 10 API tests
pass; these do not establish real Kafka application recovery or production readiness.
