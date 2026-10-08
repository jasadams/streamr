# Native state checkpoint Parquet adapter

The milestone-2 live-state exporter wrote one `STRDS001` binary object per
snapshot scan page. That coupled the memory-admission page size to the remote
file count. A 65,000-row HOP leader checkpoint failed before publication when
its file descriptors exceeded the existing 3 MiB metadata message limit.
This was introduced by Streamr's exporter; it was not established as an
upstream Arroyo defect.

The correction adapts stable backend snapshots to Arroyo's existing keyed-state
Parquet representation: non-null Binary `key` and `value` columns, the shared
`GLOBAL_KEY_VALUE_SCHEMA`, and `ArrowWriter`. It reads bounded snapshot pages
and feeds their Arrow batches into a size-limited file. A file can contain many
scan pages. Only one bounded file is buffered for exclusive upload through
`StorageProvider::put_if_not_exists`; the complete state is never collected into
one map, batch or file buffer. Opaque keys include the existing namespace and
ownership encoding, and values keep their existing encoding.

The adapter retains the existing barrier capture, TableManager dispatch,
controller/leader checkpoint publication, selected-generation recovery and
exclusive-file garbage collection. The existing disk/typed metadata wrappers
carry schema, namespace, ownership, checksums and complete file references.
They distinguish the new Parquet file format (version 2) from existing binary
files (version 1); logical key encoding stays at version 1. Version-1 binary
reading is retained for selected old checkpoints. New exports write Parquet.
Task and subtask versions must agree, and selected file paths must belong to
the selected job, operator, table, epoch and generation.

Files have a byte target independent of snapshot page size, with explicit
row-group, footer and file bounds. The configured worker resource pools admit
writer buffers and restore buffers. Checksums are computed from the admitted
file buffer during export. Restore verifies each file before applying its rows,
then uses the existing Parquet batch reader to validate schema, namespace,
strict key ordering and row counts while writing bounded batches into the
configured live backend. Empty snapshots are explicit; failed partial restore
cannot become a running state, and retries require fresh storage.

The exporter keeps the former scan-page limit. It reserves bounded writer
memory from the configured shared decoded-value pool before scanning; it fails
explicitly if the reservation is unavailable. Restore admits decoded memory
before parsing the Parquet footer. A regression test covers two concurrent
300,000-byte-row exports and exact restoration under a 4 MiB decoded pool.
Independent source review approved these admission rules.

The first runtime matrix exposed a separate writer-admission failure for
sixteen 8 KiB window values. Small scan pages could leave a partial Parquet row
group retaining earlier pages, although the reservation covered only two page
copies. The correction flushes after each scan page and reserves the three
page-sized representations that can coexist during encoding. Flushing a row
group does not close the file: multiple scan pages still share a bounded file.
An exact export/restore regression uses the window resource configuration;
configured pool sizes and file/metadata limits remain unchanged.

Source commit `4fbd740a0861dddf1bbdd9e264f2e6fd11742413` passed all five
Bookworm gates: formatting, workspace/all-target check, strict Clippy, library
tests (645 passed, four ignored) and workspace/all-target build. The SQL capture
executable SHA-256 is
`1c57540e44ff31ba736a9ac037212fad8628729adaa6841e5e0dd1c954f980ac`.
The source inventory and gate logs are retained in
`target/native-hop-capacity-parquet-v3-65000/build-evidence/`.

Exact initial/checkpoint/fresh-worker comparisons passed 60 captures: 24 generic
updating timestamp/count/MAX cases, eight typed arrays, eight existing-SQL
top-five arrays, eight typed state-table MERGE/lookup cases and twelve
TUMBLE/HOP/SESSION cases. The matrices cover memory/RocksDB and controller/leader;
the aggregate and state-table matrices also cover source batches 1/8. Four
RocksDB checkpoint fault tests passed across both checkpoint protocols, covering
retained checkpoints after worker recreation and upload failure recovering from
the last published checkpoint.

Both 65,000-key RocksDB HOP captures passed. Each checkpoint retained 64,999
real input rows, a conservative 532,471,808-byte payload floor above ten times
the declared 50 MiB pool sum. Each initial and fresh-worker recovered output
matched all 130,000 item/window pairs and complete 8,192-byte payloads.

| Protocol | Window files | Operator metadata bytes | Peak whole-child RSS bytes |
| --- | ---: | ---: | ---: |
| Controller | 1,476 | 304,062 | 356,876,288 |
| Leader | 1,476 | 339,486 | 352,108,544 |

Both RSS measurements are below the unchanged 512 MiB cap. The leader's complete
published manifest is 340,747 bytes. Both inventories contain 194,997 logical
rows; every actual file matches the published reference, size and SHA-256.
The largest observed Parquet file is 236,730 bytes. The previous completed
controller artifact had 14,015 binary page files and 2,840,928-byte operator
metadata; the previous leader failed before publication. That historical failure
is retained in [the investigation](native-hop-leader-checkpoint-limit.md).

Exact runtime and checkpoint measurements are at
`target/native-hop-capacity-parquet-v3-65000/{measurements.json,checkpoint-measurements.json}`.
The combined pipeline exited zero and confirmed all frozen Rust source hashes
were unchanged. All seven CI checks also passed on engine commit `4fbd740a`.
This qualifies the selected HOP capacity shape in both protocols, not complete
milestone 3 or all window, timing and resource cases.

The adapter is a bounded full-snapshot transport. Files target 512 KiB and have
a 2 MiB hard ceiling; the configuration may derive a lower admitted allowance.
A file has at most 4,096 rows and 256 row groups. The native asynchronous reader
loads one row group at a time; the pinned Arrow fork does not combine groups
into a larger decoded batch. Whole-file checksum verification adds a streaming
read before Parquet range reads. Metadata still enumerates complete owned file
references under the existing 3 MiB limit, so this is not an unlimited-state
capacity claim. Higher-volume shapes and other resource/backpressure gates
remain under the existing milestone-3 tickets.
