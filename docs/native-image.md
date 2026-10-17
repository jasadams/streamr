# Native engine image

The manually dispatched **Publish native Streamr image** workflow builds main
for Linux amd64 using the maintained `docker/Dockerfile` `streamr-native` target.
It pins the Rust 1.96.0 Bookworm image by registry digest and labels the output
with the exact source revision. It publishes
`ghcr.io/jasadams/streamr:sha-<full-source-revision>` only after the built image
passes a public API Rust UDF check and save. Saving performs an actual release
cdylib build and link. This is packaging qualification, not application recovery
or resource qualification. The workflow preserves its result and engine logs.

The image retains the builder's compiler tools and source workspace because the
API embeds the local UDF plugin path at compile time. This deliberately trades
image size for the existing, coherent build and UDF compilation path. It does
not use the separately published crates.io plugin. Application SQL and UDFs are
supplied by consumers and do not belong in this image.

The entrypoint is `/app/arroyo`, default command `cluster`, with HTTP port 5115.
For a single-pod process-scheduler installation, pass `--config-dir /config
cluster` and persist `database.sqlite.path`, `checkpoint-url`,
`compiler.artifact-url`, `compiler.build-dir` and the configured RocksDB live
state directory. Persisting only checkpoints does not preserve the catalog or
compiled UDFs. Use an immutable published digest in deployment configuration.

Dispatch requires GitHub repository package-write permission and can take a full
native release build on a cold runner. It does not run on every application PR,
create a version tag, deploy an image, or switch application traffic.
