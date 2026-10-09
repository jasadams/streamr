# Milestone 3 — feature execution plan

Use existing Trakkt feature tickets; do not create audit, decision or QA children.
Current descriptions own the scope and acceptance. This plan replaces the old
single active queue and twelve planning groups; Git history retains that text.

## Parallel implementation

| Ticket | Before / after | Scope boundary |
| --- | --- | --- |
| STR-16 | Retained queue/checkpoint buffers must preserve byte ownership through backpressure/cancel | Shared resources and queue permits; no read-view redesign |
| STR-17 | Ordered ARRAY_AGG followed by slice must select finite K without materializing the full member array | Aggregate ranking/CDC; all necessary member counters may remain on disk |
| STR-19 | Closing panes/collections must drain within declared limits or fail precisely | Existing TUMBLE/HOP semantics; no quiet-key policy change |
| STR-20 | Many-key/hot-session closure must stay paged and recover remaining open state | Existing SESSION semantics; no application timer emulation |
| STR-26 | Health signals must describe actual logical/disk/execution/checkpoint pressure | Existing generic lifecycle hooks and accurate operations/support docs |
| STR-42 | Recovery must cover nonempty, changed/deleted/reinserted and completely empty epochs | Conformance harness; no new snapshot or compatibility format |
| STR-29 | Quiet retained keys receive zero rolling counts when watermarks pass expiry | Approved event-time composition; ordinary windows unchanged |

| STR-43 | Remove legacy state SQL, processor paths and associated artifacts completely | Preserve current native infrastructure; no legacy compatibility or migration layer |

Each agent claims one feature, uses a separate worktree, owns its declared files,
adds focused regressions, obtains independent review and delivers a PR. If the
criteria already hold, cite exact evidence rather than inventing code changes.
Only a demonstrated contract-preserving defect justifies a repair. Use current
main; the root checkout may be an older STR-1 branch.

File coordination: STR-16 owns shared resource API and graph/network queues;
STR-17 owns aggregate kernels/ranking planner and aggregate capture helpers;
STR-19 owns fixed-window stores; STR-20 owns session stores; STR-26 coordinates
metric hooks with those owners. STR-42 owns multi-epoch table fixtures/capture
scenario; coordinate shared smoke_tests.rs sections with STR-17. A shared file
is a coordination requirement, not a blanket milestone-wide dependency.

Configured worker time budgets remain; stop/report safely if exceeded. Parallel
implementation is authorized. Full compilation and capacity runs still share
one machine queue and preserved pinned executable; no competing heavy runs.
Fast delivery uses formatting/static lint and independent review. Do not claim
an unexecuted regression as passed. Shared runtime checks belong to STR-32.

## Approved feature contracts

STR-29 is now ready: the user approved watermark-driven expiry on 2026-10-09.
For retained lifetime/key rows, recent count becomes zero after expiry;
removing the retaining key removes its composed result. Complete input silence
without watermark progress does not advance event time. Ordinary window
outputs remain unchanged. No new public timer callback API is preapproved.

STR-43 is ready: remove the obsolete scalar SQL surface and its implementation, serialized
types, tests, fixtures, examples, scripts and documentation completely. Streamr
is pre-release; old plans/checkpoints need no compatibility or migration layer.
Preserve reusable infrastructure required by the current native engine.
Coordinate shared files with other owners; their future features do not block
removal. No decision blockers remain for the eight feature assignments.
New public semantics/interface changes require discussion; ordinary repairs
preserving existing contracts proceed autonomously. STR-44 remains post-MVP.

## One integrated acceptance batch

STR-32 owns the finite native aggregate/window/session/state-table matrices and
all final combined qualification. Its checklist includes both backends and
checkpoint protocols, exact typed/CDC/prefix oracles, multi-epoch empty/deleted
state, actual retained state at least10x declared state-memory budget, whole
process RSS, hot keys, slow consumers/cancellation, representative backfill,
production worker/controller/broker/storage faults, retention/cleanup,
source/sink/offset consistency and actual24-hour live/fault operation.

Choose exact commands/manifests, resource limits and pinned source/binaries in
the explicitly requested shared batch. Preserve held scenarios and source-bound
historical successes/failures. Do not rebuild per feature or substitute fixture
size for retained state. Failed cases return to the owning existing feature;
there is no second operator QA ticket layer.

Engine completion is independent of external application parity. Broader
operators, rescaling, incremental checkpoints and full-engine qualification
remain milestone4. No release, deployment, cutover or license change follows.
