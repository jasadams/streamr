# Milestone 3 remaining work plan

Planning baseline: the 2026-10-08 handoff in `milestone-3-validation.md` and
`target/milestone-3-pr6-publication/repaired-candidate.json`. This is a task
decomposition, not new qualification evidence or approval of semantic changes.
Full acceptance remains STR-16/17/20/26/28/29/32 plus STR-43 prerequisites.

The candidate includes two independently reviewed contract-preserving repairs.
The Python oracle's eight tests passed twice. Fresh v15 Bookworm formatting,
workspace all-target check, strict Clippy and library tests passed (25 suites,
708 passed, zero failed, six ignored), including both startup delivery and
cancellation regressions. The all-target build also passed (229.81 seconds),
completing all five gates. Cache eviction and an assumed old Cargo artifact
filename interrupted smoke preparation before any case. The all-target rebuild
passed (1190.44 seconds); the actual `default` + `integration-tests` SQL ELF was
pinned and all 12 small SESSION/operations public-helper integrations passed
under continuation session `66569`. Complete typed initial/recovered rows and
committed prefixes were independently checked; peak child RSS was
184,414,208–195,432,448 bytes, below 512 MiB. Source/helper hashes are unchanged;
original gate receipts and distinct rebuilt-artifact provenance remain in
`target/native-m3-reviewed-repairs-v15-build-evidence/` and
`target/native-m3-reviewed-repairs-v15-runtime-smokes/integration-evidence.json`.
The reviewed increment is published as `65827b2622f4c8acb6c9e1de31fcf58a1c64ead7`
on draft PR #6. Pull-request CI passed completely (709 library tests and 12
integration tests); duplicate push CI failed a Kafka metadata test timeout.
The independently reviewed fixture-only repair passed formatting, affected
all-target/all-feature check, strict Clippy, test build and 11 separate-process
parallel Kafka tests with no retries. Both unchanged baselines also passed; the
CI timeout was not reproduced. Repair `77ca1517` is published; both fresh CI
workflows passed, each with 709 library tests, six skipped and 12 integration
tests. All seven checks are green for that head. The subsequent large hot
SESSION case failed early with default queue8192 under a 16 MiB execution pool;
a separate existing queue32 configuration exceeded the unchanged900-second
capture deadline. Both failures are preserved. A reviewed private, constant-size
expiry proof in session_store.rs passed fresh v16 formatting, workspace check,
strict Clippy, 25 library suites (714 passed, zero failed, six ignored), full
all-target build and all 12 small recovery integrations. Fresh SQL/production
executables are pinned with Cargo profile and source provenance. Independent
actual-evidence review precedes publication and fresh exact-head CI; old
published-head CI does not qualify this Rust change.
These are small integration cases: large SESSION preparation and actual retained
capacity qualification remain separate. Frozen v14b gates do not qualify the
changed `network_manager.rs`; existing finite native and partial external parity
receipts retain their original scope.

The rows below are dependency groups, not whole-ticket agent assignments.
Dispatch one failing query/test, one resource-ownership repair, one metrics
family, or one declared qualification case at a time, with named files and an
exact acceptance command. Split broad groups before implementation; retain
failures as well as successes.
“Ready” means no new semantic approval is identified, not that tests have passed.

| Task | Parent ticket | Concrete outcome | Required acceptance evidence | Dependency / decision |
| --- | --- | --- | --- | --- |
| 1. Validate and publish repaired candidate | STR-16/32 | Verify current staged/unstaged source against preserved patch, obtain fresh container gates, and publish a reviewable candidate revision. | Source/patch and executable hashes; exact commands and logs for formatting, workspace all-target check, strict Clippy, library tests including startup full-channel delivery/cancellation, full all-target build, Python oracle receipt; CI tied to published SHA. | Ready; first prove container access. Preserve old receipts. Publication does not close M3. |
| 2. Finish execution accounting | STR-16 | Close retained-state, output, checkpoint, cancellation and slow-consumer accounting gaps through existing shared contracts. | Reproducible memory/RocksDB cases showing accounting before/after ownership transfer, bounded queues/output, cancellation release and checkpoint/recovery; declared pool limits, peak usage and no leaked reservations. | Ready for contract-preserving coverage/repairs after 1. Any ownership/visibility contract change requires discussion. |
| 3. Qualify native aggregation and ranking | STR-17/29 | Current-source high-cardinality and hot-key native aggregation/recovery; rankings remain bounded while produced. | Exact values/order/ties and recovered values; measured retained payload versus declared pool, RSS and accounting; memory/RocksDB appropriate matrices and controller/leader recovery. Prove ranking does not first materialize the full array. | After 1–2; reuse prepared fixtures/evidence. New ranking/public-interface semantics need approval. |
| 4. Run prepared large SESSION qualification | STR-20 | Execute retained hot-key and many-key SESSION fixtures, then close native out-of-order/open-session recovery coverage. | Existing preparation hashes plus current binary hashes; runtime manifests, exact session values, restored open-session continuation, declared time/order cases, checkpoints and resource peaks; >10× retained-state proof rather than fixture size alone. | Ready after 1; serialized capacity queue. Full external lifecycle parity belongs to 8. |
| 5. Close native composition decision and implement agreed work | STR-29 | Resolve retained rolling-zero/expiry and updating-input JOIN gaps using existing SQL/runtime where possible; implement only agreed contract-preserving repairs or approved contracts. | Reuse quiet-key diagnosis and exact planner rejection; decision packet stating existing behavior, remaining semantics, upstream/native evidence, smallest proposed change and tradeoffs. Then runnable typed SQL with values/CDC, autonomous idle expiry/zero, clocks and fresh-worker recovery. | Needs user discussion before observable semantic/architectural change; unchanged quiet-key query is known failing. Do not reopen completed encoding investigation. |
| 6. Decide RocksDB read-view contract separately | STR-16/17/29 | Review existing read-view lifetime proposal and implement only an explicitly accepted generic ownership/visibility contract if required. | Concrete missing capability and existing behavior; user decision; after approval, backend-equivalent visibility/lifetime tests, cancellation/retention limits and checkpoint recovery evidence. | Decision packet can proceed independently; architectural/lifetime changes remain unapproved. Needed only where retained operations demonstrate the gap. |
| 7. Qualify operational configuration | STR-26 | Document and verify backend selection, supported limits/defaults, metrics, operational recovery and rollback using existing machinery. | Runnable configuration examples, invalid-config outcomes, measured defaults under representative loads, metric observations and fault/recovery/rollback receipts with compatibility limits. | After relevant 2–6 changes. Private instrumentation proposals are not implemented evidence; new semantics require discussion. |
| 8. Close external semantic and field parity | STR-28/29/32; STR-20 | Application-owned native identity, complete 33-field profile and 12-session lifecycle/oracle comparisons; classify remaining defects versus policy differences. | External SQL/fixture/oracle revisions and hashes; field-by-field typed value, lifecycle, ordering/time, out-of-order and restored-state expectations; exact initial/recovered comparisons across required backends/batches/protocols. Include rolling expiry and composition gates, not only existing 14-field/core probes. | After 3–5 where necessary. Independent application preparation stays external; Streamr harness accepts supplied fixtures/expectations without customer schemas. Policy differences need user agreement. |
| 9. Close native replacement and compatibility prerequisites | STR-43; STR-38–42 | Produce explicit prerequisite ledger for legacy callers: native replacement qualification plus old-plan/checkpoint handling. | Caller/catalog/example inventory; externally owned caller migration receipts; native state-table/MERGE retained ownership, current-row visibility, output/recovery proof; explicit old-plan/checkpoint compatibility test matrix and unresolved decisions. | After relevant 3–8. Reuse existing 40 parity/four export-fault cases within their scope. Compatibility behavior changes need discussion before implementation. |
| 10. Remove legacy state_* paths | STR-43 | Remove active legacy catalog/runtime/examples only after prerequisite ledger passes and compatibility policy is agreed. | Inventory-to-removal review; rejected/handled old plans as agreed; migration/recovery/rollback receipts; current native SQL regressions and fresh gates proving no replacement depends on removed paths. | Blocked on 9 and user agreement for observable removal/compatibility semantics. Removing names alone is insufficient. |
| 11. Run combined capacity and fault qualification | STR-32; STR-16/17/20/26 | Complete fixed-parallelism ≥10× combined workload with hot keys, backfill and slow consumers on the final candidate. | Actual retained-state floor ≥10× declared pool sum, fixed topology/pool/config manifest, exact external oracle values, bounded resource/output accounting, throughput/latency/backpressure observations and worker/controller fault recovery on required protocols. | Long serialized qualification after 2–10. Component-only capacity and older-source passes cannot substitute; failures return to scoped repair packets. |
| 12. Run actual 24-hour qualification and final acceptance audit | STR-32; all M3 parents | Complete 24 hours of the required live/fault workload on final qualified source and audit every parent acceptance gate. | Start/end wall-clock receipts covering actual 24 hours, continuous workload/captures/metrics, scheduled fault/recovery records, exact external oracle checks, source/config hashes and ticket-by-ticket evidence ledger with no open required gates. | Long serialized run after 11 and all functional gates; broker uptime or finite replay is insufficient. A source change invalidates affected qualification and needs a scoped rerun. |

The Kafka repair increment in task1 is published and green. The task4 large
SESSION failure now has a bounded internal expiry-check repair with passing v16
gates, fresh artifact pins and small integrations; finish independent evidence
review, publish and obtain fresh CI before the unchanged large queue32 case. Reuse the
prepared fixtures and preserve both prior failures and all original receipts. Task 2 coverage planning and tasks 5/6 decision
packets can be delegated independently without competing for the build queue.

Next bounded qualification order after the hot SESSION rerun: task4's many-key
SESSION case and remaining checkpoint-protocol coverage, then task3's existing
eight-key aggregate smoke before the declared 100,000-key capacity case in
`native-aggregate-capacity.md`. Bind each run to fresh executables and source
receipts; an individual hot/leader pass does not cover those additional cases.

Task5 decision preparation must distinguish working result composition from
missing expiry: the recorded quiet-key result is lifetime3/recent1 instead of
lifetime3/recent0. Existing UNION/current-result composition passes its selected
value/recovery cases, but retains the most recent nonempty HOP count. Supporting
updating-input joins would enable the proposed standard SQL plan; it would not
alone settle sparse/all-idle clock advancement. Discuss the required clock,
zero-versus-delete behavior, retained keys and fan-out before selecting a change.

Use one Debian Bookworm development-container target,
`target/milestone2-runtime`, with incremental compilation disabled; serialize
builds and capacity runs. Check disk and active process/container references
before large runs or cleanup. Preserve uncommitted user work and all receipts.
Delegate one bounded subtask or review partition per agent; the coordinator owns
shared-file integration, queue scheduling and evidence reconciliation.

STR-44 immediate-first/per-group delayed emission remains optional post-MVP;
existing periodic flushing is accepted. That deferral does not reduce field,
lifecycle, expiry, recovery, resource, parity or duration gates above. No new
SQL/public primitive, application-specific engine code or checkpoint redesign
is authorized by this plan.
