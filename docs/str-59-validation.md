# STR-59 limited standard LIKE_REGEX

Implementation base: `34623ba2518bcf86cb13e994c46157cc9bed506a`.
Workspace: `/home/jason/repos/streamr-worktrees/str-59-like-regex-20261011`.
Instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, and
`/home/jason/.claude/agents/feature-implementation-engineer.md`.
`CLAUDE.md` and `docs/CODING_STANDARDS.md` are absent.
Evidence: `/home/jason/repos/streamr-validation/str-59-like-regex-20261011/`.

## Contract and boundary

The maintained sqlparser fork represents `value LIKE_REGEX pattern` and
`value NOT LIKE_REGEX pattern` as predicates with normal Boolean precedence.
The registered concrete kernel supports nullable UTF8 inputs and non-null literal
patterns: anchors, literal and escaped dots, alternatives, capturing groups,
character classes and substring matching. Matching is case sensitive; SQL
`LOWER` supplies normalization. SQL NULL propagates through the predicate and
NOT. Explicit `FLAG`, dynamic/NULL patterns, engine-specific escapes, inline
flags, lookaround, backreferences, repetition and class set operators are
rejected clearly. A public REGEXP_LIKE spelling is not substituted for syntax.

Logical binding compiles each literal expression once. Its physical codec carries
the pattern source and reconstructs a compiled expression once on decode.
Evaluation uses that compiled instance. The decoder bounds serialized metadata
before allocating the owned source string and verifies the physical arguments,
literal/source agreement, Boolean return type and nullability. Ordinary plans,
native MERGE clauses, fused projections and finite local plans use the concrete
checks; owner admission qualifies the implementation type. User UDFs cannot
shadow `like_regex` in planner or worker loading paths.

The stateless kernel reuses the existing SQL/JSON scalar limits: 4096 source
bytes, 64 nesting levels and 16 MiB per input. RegexBuilder limits compiled
program and DFA-cache size to 16 MiB. Encoded JSON metadata is at most
`6 * 4096 + 2` bytes before deserialization. Exceeding a limit produces an error,
never a nonmatch. These are per-expression limits; they do not establish a
whole-operator or process memory budget. The user retired execution-memory
admission and queue-byte admission; this change adds neither reservations nor a
replacement pool. The ticket's older execution-reservation acceptance language
therefore does not apply. Existing owner/backend limits and rollback remain.

This change adds no retained streaming state, backend, checkpoint format or
application policy. Composed login-country grouped selection still depends on
STR-60 and is not claimed by the core VARCHAR fixtures. STR-58 already implements LOWER/TRIM in finite local execution. Direct native
owner normalization is outside these core VARCHAR fixtures; no composed
owner qualification is claimed. Ordinary paid-medium execution is independently
covered without widening owner admission here.

## Parser evidence

Both maintained patches pin `02d020901b65d048e97feae48083fcac38d343ca`
from fork branch `str-59-like-regex`, based on `bc7f08176c80f8fae443e69add9489b7627cbacb`.
Engine and DataFusion use that same 0.55 instance. The existing registry 0.56
instance belongs to `deltalake-core 0.27.0`; it is unrelated to engine AST binding.

- Focused LIKE_REGEX parser tests: 2/2 passed, including Arroyo, ANSI and Generic
  dialects, nullable/nonliteral syntax, precedence, grouping and escaped dots.
- Maintained visitor/library and common suites: 102 + 375 tests passed; formatting
  passed. `parser-maintained-tests-refreshed.log` records actual exit 0.
- Full parser tests fail to compile the existing DuckDB fixture because its
  CreateTable initializer lacks `arroyo_partitions`. Strict all-target visitor
  Clippy reports existing 51 library / 54 library-test warnings. The exact gates
  reproduce on clean `bc7f081` with identical 58 error headlines; these gates
  are not passed. Logs: `parser-gates.log`, `parser-baseline-gates.log`.
- Initial parser attempts exposed a duplicate existing keyword, then test dialect
  import/constructor errors; both were repaired before review/publication.
  Switching the same `/fork` mount to baseline caused Cargo to reuse stale source
  artifacts. Refreshing changed source mtimes rebuilt the current AST without
  deleting caches. The failed attempt remains in `parser-maintained-tests.log`.

## Engine checks

All engine runs use immutable `arroyo-dev` image
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`
and the shared `target/milestone2-runtime`, serialized by `scripts/rust-build`.
The final inventory keeps compilation, executable copying/hashing, export and
decoded execution in one queue reservation. `inventory4/source-receipt.json`,
`source.patch`, `binaries.json`, `commands.json` and `fixtures-sha256.json`
record the exact inputs and artifacts outside regenerable Cargo caches.

- `scripts/cargo-dev check --locked -p arroyo-planner -p arroyo-worker`: exit 0
  (`check-final.log`, 8.02s); `scripts/cargo-dev fmt --all -- --check`: exit 0
  (`fmt-final.log`).
- `scripts/preflight-clippy.sh -p arroyo-planner -p arroyo-worker`: exit 0,
  strict all-feature/all-target locked gate (`clippy-final.log`, 14.71s).
- `inventory4`: exit 0. Locked test compilation uses `--no-fail-fast` before
  harness arguments. Planner core 4 passed; worker core 2 passed; complete SQL
  export 1 passed; decoded worker fixture execution 1 passed. Initially ignored
  export/execution tests were then explicitly run. Exact agents/channels/paid
  and NULL oracles execute through physical decode, including source batches
  1 and 8. The real MERGE owner tests cover memory and RocksDB, matching,
  nonmatching and NULL guards, updates, an oversized-input error before commit,
  rollback of pending mutation/output, and a subsequent successful event.
- Existing neighbors: SQL/JSON 61, JSON_TABLE 8, planner local 12, worker local
  3 and worker UDF guard 2 passed. The unrelated ignored SQL/JSON and JSON_TABLE
  fixture exporters were not run by these neighbor filters.

Earlier receipts are preserved, not counted as passes. `inventory1` failed test
compilation because the new corruption helper used an unavailable protobuf
field; it now traverses pinned `l`/`r`. `inventory2` exposed an unserializable
optimized upstream MemorySource and an incorrect assumption that a fused view
must use a projection operation. The exporter now uses the existing
ArroyoMemExec adapter before serialization, and the corruption test traverses
the actual fused value plan. No new serializer or execution capability was added.

The owner fixture originally declared a 128 MiB working-event limit while its
private decoded pool was 16 MiB. Its corrected private pool is 137,505,536 bytes,
derived from existing simultaneous owner/table reservations. `inventory3`
then exposed the fixture's hardcoded internal chunk of 8, which bypassed
production construction. Diagnostic tracing records the failed RocksDB read
request of 65,706 bytes with only 4,943 available: working 134,217,728 + scope
986,880 + eight captured-event permits 2,097,152 + decoded read 196,608 + key
workspace 2,225. Small output envelopes do not release captured-event permits.
The fixture now uses the complete existing production chunk calculation, which
selects 3 from mutation/scope and pending-capture bounds. Source input batches
remain 1 and 8; the two-event error pair shares one scope. No further pool
increase or production policy change was needed. The 16 MiB + 1 input error
and rollback assertions remain. `owner-resource-diagnostic.log` records the
failed diagnosis; temporary instrumentation was removed completely.

These tests establish this feature's scalar/decode and owner mutation behavior.
They do not claim full server, capacity, checkpoint/recovery or STR-60 composed
qualification.
