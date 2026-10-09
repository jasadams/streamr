# Milestone 3 — agent backlog

Updated 2026-10-09 at the user's request. Read the current Trakkt ticket description
before claiming work. Historical comments and validation logs are evidence, not
current dispatch instructions. No implementation worker is claimed by this reset.

## Available features

All eight tickets are Todo, agent-ready and unblocked. Independent agents may work
in parallel in separate worktrees with the file ownership in each ticket.

| Ticket | Deliverable | Primary ownership |
| --- | --- | --- |
| [STR-16](https://trakkt.app/issues/STR-16) | Bounded queue/transfer ownership through slow consumers and cancellation | Shared resources, graph/network queues, checkpoint permits |
| [STR-17](https://trakkt.app/issues/STR-17) | Bounded native SQL top-K, exact CDC and recovery | Aggregate kernels/planner/ranked collections and aggregate capture assertions |
| [STR-19](https://trakkt.app/issues/STR-19) | Bounded TUMBLE/HOP closure and collection output | Fixed-window stores/operators/fixtures |
| [STR-20](https://trakkt.app/issues/STR-20) | Bounded many-key/hot-key SESSION closure and recovery | Session stores/operators/fixtures |
| [STR-26](https://trakkt.app/issues/STR-26) | Native state health metrics and usable operating limits | Collectors/metric hooks and operations/support docs |
| [STR-42](https://trakkt.app/issues/STR-42) | Three-epoch nonempty/changed/empty state-table recovery | Conformance driver/fixtures and multi-epoch capture scenario |
| [STR-29](https://trakkt.app/issues/STR-29) | Watermark-driven zero counts for quiet retained keys | Result composition/expiry, coordinated with STR-19 |

| [STR-43](https://trakkt.app/issues/STR-43) | Complete removal of legacy state SQL and processor code | Legacy catalog/planner/runtime/protocol paths and their fixtures/docs |

One feature per worker. Claim with worker/session, start, acceptance and owned
files; only then set In Progress. Coordinate shared sections before editing.
STR-17 owns aggregate capture assertions in smoke_tests.rs; STR-42 owns the
multi-epoch scenario. Compiler/capacity jobs remain serialized through cargo-dev.

## STR-26 current increment

2026-10-09 worker `str-26-native-health`, started 01:22 UTC, deadline 02:22 UTC:
implementation complete and staged: logical accounting, weak cached SST/free/
stall observations, execution refusal/reservation classes, monotonic publication/
initialization/local readiness timings, and operating/support docs. Current-source
repaired-Bookworm/shared-target gates passed: narrowed state/worker check,
state health tests 2/2, worker execution/lifecycle metrics tests 7/7, required
all-feature/all-target preflight Clippy, and workspace formatting. Independent
source review found no issues; exact next action is final reviewed receipt and PR
delivery. Evidence: `/home/jason/qa-evidence/streamr-str26-20261009/` (base SHA,
staged patch/hash, image ID, commands, exit statuses and logs). Actual load/restart
scrapes and production sizing remain STR-32; total allocated directory bytes,
cumulative stall duration, per-resource reservation attribution and end-to-end
outage time are explicitly unsupported. No SQL, admission or checkpoint semantics
change; no user decision or local verification blocker remains.

## Decisions and final acceptance

- STR-29's contract was approved on 2026-10-09: watermark expiry emits zero
  rolling counts for retained lifetime/key rows; complete input silence does
  not advance event time. Ordinary window behavior remains unchanged.
- STR-43 is approved and ready: remove the legacy implementation completely.
  Streamr is pre-release; no compatibility shims or migration tooling are required.
- STR-32 holds one shared operator/combined capacity, resource, backfill,
  process-loss/storage-fault and actual 24-hour acceptance checklist. Heavy runs
  require an explicitly requested shared batch; held/cancelled cases stay held.
- STR-44 optional per-group emission remains post-MVP. Read-view lifecycle
  redesign remains unapproved. Application schemas/parity stay external.

STR-28 is folded into STR-26. STR-49/50 into STR-17; STR-51 into STR-29;
STR-52–55 into STR-32. These are Cancelled duplicates, not another queue.
STR-38–41 implementation is Done on its recorded own-scope acceptance;
STR-45/47/48 repairs are also Done. Earlier STR-48-only dispatch is superseded.

## Evidence

Reuse source-specific results without presenting them as fresh qualification.
See milestone-3-validation.md, batch-qa-2026-10-08.md and
state-table-all-key-capacity.md. Basic native operators, merged state tables,
strong window/hot-session and selected all-key capacity results already exist.
Later cancelled reruns do not erase earlier passes or authorize another run.
Full current-candidate qualification remains STR-32; no test was run by this
backlog rewrite. Git history preserves the previous working ledger.
