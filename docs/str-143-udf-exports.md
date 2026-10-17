# STR-143 Rust 2024 UDF exports

The existing public Rust UDF interface rejected even a scalar increment function.
The official compiler generates Rust edition 2024 crates, but the public `udf`
macro emitted bare `#[no_mangle]` attributes. Rust rejected expansion with
`unsafe attribute used without unsafe`. The same upstream emission was inspected;
this is an implementation compatibility repair, not a new UDF or state contract.

The macro now emits `#[unsafe(no_mangle)]`. Exported symbols, function signatures,
FFI, synchronous/asynchronous wrapper bodies and the nonexported `local_udf`
test path are unchanged. No state/backend/resource/checkpoint or RocksDB scan is
added or changed. Application functions remain outside Streamr.

## Source and actual validation

- Fetched main/base: `ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062`.
- Reviewed implementation/test commit: `a86297dc049c0c3a5a85f965c782cc571ae0d2d2`.
- Isolated worktree: `/home/jason/repos/streamr-worktrees/arc17-udf-unsafe-export-20261017`;
  original dirty invoking checkout preserved. Fresh AGENTS, build/test/team,
  milestone ledger and work plan read. STR-143 pickup guard CLEAR.
- Maintained `scripts/cargo-dev` fmt/check/tests and `scripts/preflight-clippy.sh`
  passed for `arroyo-udf-macros` and `arroyo-udf-plugin`. Normal dev/test profile,
  immutable Bookworm image
  `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`,
  canonical shared target/queue and matching prebuilt native selection retained.
- Two new integration tests compile the actual public synchronous/asynchronous
  macros under the plugin crate's edition 2024. Existing selected crate unit/doc
  suites contain no further tests. The statically linked `local_udf` path does
  not emit dynamic symbols and could not catch the original failure.

Actual public API/compiler/native execution ran under the maintained queue using
the already-qualified CLI. Its production source remains
`e2ff41e7c991b3e0f0c68bd7d2f874631a00501b`, binary SHA-256
`0e6078d2fe6f2a2c4279b4de3d7a8d6b5486b369b47a23a5c26dcd2bee363a36`.
The CLI was not rebuilt or relabelled as the plugin repair. Supported
`compiler.use-local-udf-crate` selected the exact patched plugin source at the
binary's baked `/app` dependency path. Its generated Cargo manifest/lock, all
plugin/common/macro source hashes and compiled dylib hash are retained separately.
The official compiler's existing `cargo check/build --release` behavior was used;
this is no optimized engine measurement.

The repaired run (`smoke3`) passed official `/udfs/validate`, query validation and
pipeline creation with an inline Rust UDF. A native state-table MERGE invoked
that function and produced four exact independently declared results. The actual
worker was SIGKILLed after source prefix 2/checkpoint epoch 2. Recovery used a
fresh generation for the same job/pipeline, restored the exact logged epoch,
preserved the committed 76 output bytes and produced one record absent from the
frozen preloss physical output. All four final records matched the independent
oracle. Typed checkpoint descriptors/checksums/objects and source/sink offsets
were inspected and retained; owned cleanup completed.

## Reproduction and retained failures

Evidence root:
`/home/jason/qa-evidence/arc17-native-mvp-20261017/profile-runtime/`.

- `str143-gates.sh` and `str143-{fmt,check,tests,clippy}.log`: exact maintained gates.
- `run-smoke3.sh`, `smoke3/{supervisor.py,config.yaml,pipeline-request.json,query.sql}`:
  exact application-independent scalar smoke preparation/commands.
- `smoke3/{result.json,fault.json,selected-checkpoint-metadata,retained-checkpoint-objects}`:
  source/artifact bindings and raw checkpoint/process-loss evidence.
- `smoke1/`: original real compiler failure, retained unchanged.
- `smoke2/`: public compiler/runtime outputs were correct, but the first caller
  rejected an ANSI-encoded restore log. An independently reviewed caller repair
  strips terminal escapes and strengthens paused-worker/physical-record/source
  assertions. It does not change the engine or oracle. Fresh `smoke3` supplies
  the passing complete proof.
- `str143-build-purpose.json`: first gate attempt stopped before compilation on
  a stale mutable image tag; explicit selection of the existing qualified image
  repaired the environment without a native-source override or image rebuild.

Use a fresh case directory and the recorded maintained shared queue. These finite
scalar/plugin checks do not establish full application parity, live Kafka
recovery, capacity, throughput, production deployment or cutover. Independent
source review approved the repair; current-head CI/publication remain separate.
