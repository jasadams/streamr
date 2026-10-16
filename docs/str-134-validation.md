# STR-134 — reuse calendar progress within input scopes

Status: reviewed implementation and dev gates passed; one bounded release measurement completed without a demonstrated throughput gain. No publication or merge.

User approved plan `66810a31f9807da1c03a5246a0879ea43adce045ef93bb942edfb74239ac780a` after three independent pre-code reviews. Fetched base `761b2575b2cf4f0c6c2acc77239667698c923a5c` includes merged STR-133. Current AGENTS.md, .claude/build-test.md, .claude/team.md, milestone-3 status/work plan read. The invoking dirty checkout is preserved. Evidence root: `/home/jason/qa-evidence/str134-calendar-progress-20261016/`; pre-code plan/reviews remain in STR133 evidence.

The measured calendar path performs approximately 5.333 native reads/input, including 2.333 reads of the same 13-byte W watermark/pruning boundary. W is immutable during each existing serial snapshot-backed input chunk. The proposed change lazily decodes and reuses that value only in that scope, preserving initialized absence, validation and cold boundaries. Upfront batch validation and watermark/quiet callbacks retain separate reads. Fixed retained ownership uses existing optional decoded admission; insufficient headroom falls back to the old reads. Preparation failure may retry the uncommitted chunk cold, never retry commit. No new generic cache, native memory option, SQL, persisted format, admission architecture or wider chunk is authorized.

Expected fresh-fixture reads are approximately 3.667/input when optional reuse is admitted, with zero Q mutations, five writes/input and approximately one commit per three inputs unchanged. This operation-count prediction is not a CPU or throughput promise. Calendar checkpoint dirty work remains substantial and is outside this experiment.

## Scan report

No RocksDB scan is added or changed by the approved plan. Known-key W reuse does not alter existing pruning B/V, due H/U, dirty D/G, collection J or expiry/generation scan bounds, cursors, pages, snapshot lifetimes or progress under mutation. Final source review must confirm this; no unmeasured scan benefit is claimed.

## Validation and experiment

Use normal dev/test with matching precompiled RocksDB via maintained scripts for worker check, strict Clippy, formatting and focused lifecycle/store tests. Focus includes actual multirow production chunks, None/Some/invalid W, between-scope watermark advancement, stale reference rejection before mutation, relevant/retraction/correction paths, optional fallback, loaded-context pressure/cancellation/cold retry and fresh-owner continuation. Pending tests do not establish a pass or OS-loss qualification.

After independent source review, use a release pin with matching precompiled native archives and exact saved probes, preserving every unrelated compiled input. Reuse saved STR133 exclusive AFTER as BEFORE: 450,560 minimum completed four-owner handler inputs / 599.501952664 seconds =751.557185090/s; CPU/progress3,880.30 microseconds, average2.918 cores, sampled peak338.707 MiB. One fresh AFTER targets599.5 seconds and must stop within600 processing seconds, with no extended drain. Reserve exclusive canonical queue gate through build/pin/run/cleanup. Keep same fixture/query/source rate/batch, p1/blocking2, shared8MiB cache including4MiB WBM across4DBs,768MiB sampled owned RSS stop, CP120s and explicit source-relative UTC-day oracle reconciliation.

Record completed handler denominator, actual duration, per-owner/sink/checkpoint states, exact observed-prefix oracle, per-second slowdown history, process CPU/cores,100ms RSS and sampling gaps, normalized CP-free read/write/commit ratios, optional mechanism/fallback evidence, native/source/package receipts and owned cleanup. Inclusive elapsed counters are nonadditive and not CPU. Different retained-state/checkpoint phases constrain saved nonalternating comparisons. Full1.2M/all-output/SESSION completion or OS-loss recovery requires explicit evidence; no such claim follows from a partial prefix. Preserve failed gates/runs. Stop for user discussion before another hypothesis.

## Gate inventory

Initial formatter selection refused the stale default image before running; explicitly selecting the already verified matching image bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e passed. No native source build or image rebuild was needed.

Independent code review v1 approved the frozen patch. Gate1: fmt/check/strictClippy/store pass; lifecycle77 passed,2 failed,5 ignored. Both new-test failures came from a fixture capacity assumption: six Batch accumulators and calendar allowance8 reserve22 operations/input, so1MiB overlay admitted2 rows/chunk, not3. The fixture-only repair increases its write/overlay capacity to1280KiB, asserts actual computed capacity>=3 and retains exact onechunk/onecommit/W-read assertions. Production chunking and benchmark settings are unchanged. The original failure logs are preserved. Fresh v2 review approved this test-only repair. Gate2 passed: fmt/check/strictClippy0, lifecycle79 passed/0 failed/5 historical diagnostics ignored, store4 passed. Exact source/log/ignored inventory is in dev-gates-2/inventory.json.

## Bounded release result

Product source `9e211e9a`, measurement HEAD `05dd9f740b9af9a28c4891bf38cfb436092b05b6`, binary SHA256 `d31b7ddb0ff3c379adef343f120d31a2343563ba8504bae93a770c71980ed6ff`. Only the two independently reviewed candidate compiled inputs differ; all596 other saved inputs/probes are byte-equal. Maintained release pin/native/package/control guards passed. The same exclusive canonical gate covered build/pin/run/cleanup, and the runner exited0.

AFTER:385,024 minimum completed four-owner handler inputs /599.571028293s =**642.165784922 inputs/s**, versus saved STR133 **751.557185090/s**: **−14.56% observed rate**. Matched owned CPU1,476.36s /599.164s =2.464cores; CPU/progress**3,834.46µs**, **−1.18%** versus3,880.30µs. Sampled100ms group RSS peak**345.980MiB**, versus338.707MiB before, within unchanged768MiB criterion. Prefix oracle and engine/client/container cleanup passed.

Two checkpoints were Ready; the first took117.720s versus88.551s before, second89.149s versus84.591s before. Final table prefix388,936 rows, aggregate/calendar352,256 each; SESSION output0. This is not full1.2M completion, complete SESSION-output or OS-loss recovery qualification. The baseline crossed400k handler inputs while the candidate did not; retained/update/checkpoint phases differ. Whole owned CPU and stage inclusive elapsed are different measures; no summing nested elapsed counters or universal causal-regression claim.

A very slow filesystem-bound final link preceded runtime. Its mapped output was1.782GB, while task write accounting exceeded12GB with Btrfs allocation waits and host I/O pressure. These build observations are retained in link-progress.jsonl/link-io-audit.json; they do not alone identify the cause of runtime throughput. No file attributes, filesystem settings, priorities, profile or native options were changed, and compliant build clients were excluded from the owned campaign by the exclusive gate. Host runtime pressure evidence remains available for independent final review.

The observed result does not satisfy the approved keep gate. Preserve the local candidate, tests, failed gate1, repaired gate2, pin and both comparison inputs as negative evidence; no PR, merge or automatic repeat. STR133 remains the merged performance increment. Final mechanism/pressure audits and ticket decision are recorded in the evidence directory and ticket.

## Mechanism audit

Independent completed-work audit confirms reuse was exercised: a checkpoint-free first-pass window has183,833 native reads /50,135 calendar event children =3.66676/input, and33,428 W reads /50,135 =0.66676/input, versus saved5.33328 and2.33333. G/D/H/U/P mutations each50,135, exactly5/input;16,714 commits preserve approximately1/3input; Q0. Full2,146,891 native reads and2,665,566 mutations reconcile by family. Local calendar read await decreased531.46→423.04µs/input and read body133.54→101.79; write body252.83→184.54. These are inclusive elapsed measures, not additive or exclusive CPU. The mechanism worked, but whole-pipeline throughput did not improve. No production admission/fallback counter quantifies fallback incidence; focused both-backend tests cover fallback/cold retry, and the measured read ratio proves substantial admitted reuse. Completed dirty131.012s and SESSION writebody234.082s plus active work remain substantial; no new fix or causal runtime-I/O attribution is made.

## Final decision

Independent final source/pin/package/native/control/oracle/CPU/cleanup audit recommends **REJECT as a throughput improvement**, no PR and no automatic repeat. Runtime host I/O pressure differed: PSI I/O-some33.20s versus15.95s, I/O-full26.55s versus11.97s, and host I/O-wait186.49s versus81.80s. These are host observations, not exclusive per-event attribution; no subtraction, adjusted throughput or proof of a code-caused slowdown is justified. Build-link pressure is separate evidence. The approved whole-pipeline keep gate was not met despite successful read-count reduction.
