# Pinned DataFusion aggregate adapter repair

Source: ArroyoSystems/arrow-datafusion, commit
`916b45f5c28d94765ae4a6393c5e126b2ea55e1c`, package
`datafusion-functions-aggregate-common`48.0.1. Apache-2.0 license and NOTICE
are preserved. Cargo workspace inheritance expands to the same package,
lint and dependency settings; DataFusion dependencies retain their existing
locked git source. No dependency upgrade is intended.

The adapter removes emitted groups before serializing terminal state.
Use the consumptive `state_mut` API there, as DataFusion's ordinary partial
finalizer already does. BytesView COUNT DISTINCT deliberately does not implement
immutable `state`. Allocation accounting is freed before consumption, and
un-emitted groups remain live. A multiple-group regression covers First then
All emission, continued updates and complete accounting release.
Production source changes are two lines in groups_accumulator.rs. Workspace
formatting normalizes the copied sources. Two existing test-only iter_to_array
calls omit redundant into_iter() for the maintained Rust 1.96 strict Clippy gate.
The crate is a workspace member so its existing Rand 0.9.1 tests and terminal
regression run through the root locked graph; no dependency version changed.

The upstream Criterion benchmark source is retained for provenance, but its
benchmark target/dev dependency is disabled in this runtime patch to avoid
introducing an unrelated benchmarking dependency graph. Existing Rand unit-test
dependency is retained. No benchmark/performance qualification is claimed.
