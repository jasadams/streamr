# Project instructions

## Pre-release compatibility policy

Streamr is pre-release. Backward compatibility is not required for code, SQL,
APIs, configuration, saved plans, checkpoints or state formats.

- Remove obsolete implementations completely, including registrations,
  dispatch, serialized types, tests, fixtures, examples, scripts and
  documentation. Do not retain aliases, compatibility shims, deprecated paths,
  migration tooling or special handling for removed features.
- Do not spend implementation time preserving or analyzing legacy
  compatibility unless the user explicitly requests it for a particular task.
  Backward incompatibility alone is not a reason to ask for approval or block
  authorized work.
- Keep reusable infrastructure needed by the current native engine and verify
  current behavior. Follow normal protocol/schema integrity rules without
  retaining legacy implementations.
- This policy does not authorize deleting user data, rewriting Git history,
  introducing unrelated semantics or redesigning architecture. Current
  correctness, resource bounds and recovery remain required.

## Engine and application boundary

Streamr is a general-purpose streaming engine. Applications depend on Streamr;
Streamr must not depend on, recognize, or implement a particular application.
Arcstream is an external consumer, not part of this engine.

- Keep application schemas, business rules, event classifications, identity
  policies, profile/session lifecycle rules, and application-specific SQL/UDFs
  in the application repository.
- Do not add application-specific RPC messages, planner markers, operator enum
  variants, runtime dispatch, configuration, or dependencies to Streamr. Moving
  those definitions into another crate inside Streamr does not fix the boundary.
- Streamr capabilities must have generic contracts: caller-defined schemas,
  opaque keys and values, explicit ownership, configurable limits and clocks,
  and application-independent checkpoint/recovery behavior. Renaming an
  application algorithm does not make it generic.
- When an application needs a capability missing from Streamr's public interface,
  identify the gap and discuss the proposed contract/interface change with the
  user before implementing it. Do not work around the gap by embedding the
  application into the engine.
- Compatibility evidence may mention external applications in documentation.
  Reusable test harnesses must accept externally supplied fixtures and declared
  expectations without hard-coded customer schemas, cardinalities, business
  invariants, or required application checkouts. Application-specific preparation
  and oracle comparison belong in the application repository.

## Scope of engine changes

Work is limited to configurable live-state backends (including state larger than
RAM), state tables for in-stream lookups, and demonstrated deficiencies in the
existing Arroyo engine. A backend addition does not by itself justify replacing
checkpoint formats, writers, coordination or recovery protocols. Reuse existing
Arroyo machinery through bounded backend adapters; establish a concrete missing
capability before proposing a replacement.

## Native SQL and backend design

- Start with existing native relational operators, updating aggregates and
  TUMBLE/HOP/SESSION windows. Do not manually recreate supported SQL behavior in
  opaque state blobs or application UDFs. Demonstrate a specific missing
  capability with planned SQL and value assertions before proposing a primitive.
- The bar for adding SQL or public-interface extensions is high. Exhaust existing
  native SQL patterns and investigate upstream Arroyo capabilities before
  proposing new syntax or primitives. A rejection in the current fork alone
  does not establish that an extension is needed. Record runnable attempts,
  exact remaining semantics and evidence, then discuss the smallest necessary
  contract with the user before implementation.
- Distinguish SQL expressiveness from planner/runtime support. Repair engine
  support for existing SQL before proposing language changes; an implementation
  limitation alone does not justify new syntax.
- SQL/operator semantics depend on generic state interfaces. Select the live
  backend through configuration and construction/lifecycle adapters; do not
  duplicate SQL execution for memory versus RocksDB. Future adapters must satisfy
  the same ownership, visibility, limits and checkpoint contract.
- Distinguish a runnable native query from a proposal and distinguish operator
  recovery from a standalone SQL/state simulation. Preserve application output
  and time semantics or discuss explicit differences before changing them.

## Work and verification

- Before fixing a regression, trace the existing behavior and relevant upstream
  implementation. Distinguish a bug introduced by Streamr changes from a missing
  capability; do not turn an implementation bug into a request for new semantics.
- Ask the user before changing observable semantics or introducing architectural
  redesign, including changes presented as bug fixes. Explain the existing
  behavior, evidence, proposed change and tradeoffs in plain language, and wait
  for agreement before implementing the change. Do not stop asking merely to
  avoid interrupting progress. Repairs that preserve the established contract
  can proceed within the authorized task.
  Removing obsolete behavior already authorized by the user follows the
  pre-release policy above and does not require a separate compatibility review.

Preserve unrelated user changes. Follow [.claude/build-test.md](.claude/build-test.md)
for the required development container and checks. Record exact validation
evidence; pending tests and partial milestones do not establish completion.

Reuse one container build target and serialize builds and capacity runs. Check
disk space before large runs and remove unused build targets when safe. Audit
worktree changes, PR dependencies and active process/container references before
removing obsolete worktrees; preserve uncommitted work and validation evidence.

## Delegation

Proactively delegate suitable independent subtasks when doing so improves speed,
coverage, or review quality. Give each agent a clear scope and expected result,
coordinate shared files, and avoid overlapping edits. Keep short or dependent
steps local. The coordinating agent owns integration and verification and waits
for required delegated work before ending the turn.

## Continuity and delivery

- Each implementation worker owns one ticket at a time. The user authorized
  independent milestone 3 features to run in parallel on 2026-10-09. Claim
  separate tickets/worktrees and coordinate declared file ownership; delegates
  work within their assigned feature. Record start, acceptance and the one-hour
  deadline before implementation begins.
- If the ticket remains unfinished after one hour, stop its work safely, pause
  and explain the elapsed time, completed work, blocker and remaining steps.
  Wait for user direction; do not switch tickets or restart automatically.
- Report concrete ticket outcomes and remaining acceptance gaps, rather than
  only narrating builds/tests. Discussion takes precedence over automation.
- Read the current Trakkt ticket description and compact status ledger before
  implementation or after context compaction. For milestone 3, use
  `docs/milestone-3-status.md` and its feature plan. Current ticket scope and the
  user's parallel-work direction supersede old single-ticket dispatch comments;
  historical validation logs are supporting evidence, not worker assignments.
- Keep the ledger short: active task, acceptance check, exact next action,
  decisions awaiting the user, source-bound evidence and known blockers. Update
  it when those facts change, not with repeated status narration.
- Delegate narrow deliverables with explicit file ownership and acceptance
  checks. Use a separate reviewer; reuse agents for repairs. The coordinator
  owns integration, serialized builds and the final evidence audit.
- Do not repeat a settled investigation unless source changes, contradictory
  evidence or user steering provides a concrete reason. Preserve and link the
  previous conclusion rather than reconstructing it from conversation summaries.
- Keep executable pinning and dependent validation within the same shared queue
  reservation: scheduled cache eviction can remove binaries between commands.
  Preserve pinned executables and source/hash receipts outside regenerable Cargo
  caches.
- Deliver reviewed, tested increments before expanding the unpublished batch.
  Distinguish implementation, review, current-source validation, publication and
  milestone qualification; none substitutes for the others.
- A conversational question or request to discuss takes precedence over
  automatic goal continuation. Stay with that discussion until user steering
  supports returning to implementation.
