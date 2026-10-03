# Project instructions

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

## Work and verification

Preserve unrelated user changes. Follow [.claude/build-test.md](.claude/build-test.md)
for the required development container and checks. Record exact validation
evidence; pending tests and partial milestones do not establish completion.

## Delegation

Proactively delegate suitable independent subtasks when doing so improves speed,
coverage, or review quality. Give each agent a clear scope and expected result,
coordinate shared files, and avoid overlapping edits. Keep short or dependent
steps local. The coordinating agent owns integration and verification and waits
for required delegated work before ending the turn.
