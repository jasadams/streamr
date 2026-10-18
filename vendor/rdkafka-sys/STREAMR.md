# Streamr compact transaction recovery extension

Base: crates.io `rdkafka-sys 4.8.0+2.3.0`, including its bundled librdkafka 2.3.0.
Original archive SHA-256:
`ced38182dc436b3d9df0c77976f37a67134df26b050df1f0006688e46fc4c8be`.
Download: https://static.crates.io/crates/rdkafka-sys/rdkafka-sys-4.8.0+2.3.0.crate
Original upstream licenses remain alongside their source.

The root Cargo patch pins this exact source. The maintained Cargo/dev build
compiles this native dependency from source because upstream 2.3.0 does not expose
prepared transaction attachment. This is separate from RocksDB's precompiled
archive selection. Dynamic system librdkafka linking is rejected explicitly.

Changes to upstream files:

- `librdkafka/src/rdkafka.h`, `src/bindings.rs`: two documented source-level APIs.
- `librdkafka/src/rdkafka_txnmgr.c`: prepare seals output and flushes delivery,
  exports PID/epoch, and requires a registered transaction without failed
  deliveries. Resume installs supplied identity on the native main thread and
  resolves the coordinator without InitProducerId. It uses native EndTxn commit
  coordination/retries and forces the nonempty-transaction path. No serialized
  payload is needed for recovery. Fresh recovered commits wait for the logical
  coordinator connection/API negotiation through its existing UP monitor before
  constructing EndTxn. The wait starts only on explicit commit, uses the existing
  API timeout/resumable background semantics and never initializes a producer ID.
- `librdkafka/src/rdkafka_op.h`: explicit typed identity fields for the native op.
- `librdkafka/src/rdkafka_int.h`: commit-only handle flag and bounded coordinator-wait flag. State transitions clear
  the wait when leaving BEGIN_COMMIT and never
  permit enqueue for such a handle, including transient states, and beginning
  another transaction is rejected after completion. Aborting a recovered handle
  is also rejected after abortable errors, preventing an abort/epoch-bump escape.
- `build.rs`: reject dynamic linking to an unpatched system library.

Both APIs require externally serialized application use, as do the upstream
transaction APIs. Resume accepts only an unused transactional client. The caller
must supply the original transactional.id and a checkpointed identity produced
by prepare after successfully delivering all records, including the commit
marker. It must not initialize/fence that identity first. Broker expiry, fencing,
missing markers or lost broker data cannot be repaired without payload replay;
Streamr fails closed unless an exact committed marker proves completion.

No private layout offsets, reflection, assumed Rust ABI or runtime mutation of
opaque native memory are used. This is a native source extension tied to its
source version. Tests and real-process recovery evidence are recorded in
`docs/str-146-kafka-journal.md`; this file itself claims no test result.
