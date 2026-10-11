# STR-32 qualification follow-ups

2026-10-11: the user explicitly split the remaining capacity and production
recovery qualification into independent agent-ready tickets. These are related
follow-ups, not blockers for closing STR-32 as the implemented engine increment.
This does not establish production qualification or authorize deployment.

| Ticket | Scope | Status |
|---|---|---|
| STR-103 — Qualify combined native state capacity and resource bounds | Existing four-owner pilot followed by retained logical state >=10x the assigned budget, sampled RSS, exact outputs and recovery evidence | Todo; agent-ready |
| STR-104 — Qualify production worker and controller process recovery | Existing six process-loss cases, exact source/sink/checkpoint recovery, resource measurements and independent audit | Todo; agent-ready |

Both tickets contain the source/ELF/image hashes, prepared runner and review
paths, acceptance checks, cleanup ownership and scope exclusions. The frozen
candidate is `75e430b4e2473c15a5fa752c5196be7b5e249ef1`; its reviewed engine
changes are in PR36 (merged as `00563591`) and dependent PR37 (pending at
this delivery review).

Evidence and runners:
`/home/jason/qa-evidence/str32-final-qualification-20261011`.
Capacity entry point: `combined-capacity/run-impulse.py` (pilot first).
Recovery entry point: `production-recovery-rerun.py`, results in
`process-recovery-attempt-2` and log `production-recovery-rerun.log`.
The recovery run was already launched by the STR-32 worker. Check its current
status and coordinate ownership before starting anything; preserve and reuse
completed results. Do not start a duplicate run or change the source under an
active source-guarded caller.

Broader fault/operator qualification remains STR-27. Application backfill,
live processing, dashboard verification and application rollback remain ARC-17.
Those are outside STR-32's new completion boundary. No new admission policies,
SQL semantics, checkpoint protocols, 24-hour run or application cutover is
authorized by these tickets. Existing repository build/review instructions apply.
