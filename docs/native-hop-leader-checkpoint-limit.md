# Native HOP leader checkpoint metadata limit

The 65,000-item RocksDB HOP capacity probe emitted 130,000 initial
item/window rows (capture count only; full-payload comparison was not reached), then failed while exporting the epoch-1 **leader** checkpoint.
The worker reported `disk checkpoint file metadata exceeds 3 MiB RPC limit`
for table `w` before checkpoint publication or restore. The failure is recorded
in `target/native-hop-capacity-timestamp-fixed-65000-leader/hop-rocksdb-leader/runtime.log`.
It is not a completed leader recovery result.

The comparable **controller** probe completed its checkpoint and fresh-worker
recovery, validating all 130,000 rows. Its window table has **14,015 actual
immutable page files** under job
`external-sql-capture-1124693-1791127024750423110`, and its window operator
metadata file is **2,840,928 bytes**. The files and metadata are retained in
`target/native-result-checkpoints/`. The failed leader export cleaned its
partial upload; its exact attempted page count and metadata size were not
measured.

Both protocols use the same [page exporter](../crates/arroyo-state/src/live/checkpoint.rs):
each bounded scan page becomes one `STRDS001` object, and the
`DiskKeyedTableSubtaskCheckpointMetadata.files` field contains its full path,
size, checksum and row count. The exporter incrementally rejects file-list
metadata above **3,145,728 bytes** (3 MiB). The
[table manager](../crates/arroyo-state/src/tables/table_manager.rs) also checks
the complete subtask envelope against that limit. The protocol layout adds
`pipe-test/` and `/generations/0` to each leader path compared with the
controller layout: **24 additional bytes per file**. At the controller's
observed 14,015 files, that alone projects **336,360 additional bytes**, or
**3,177,288 bytes** for the leader window operator metadata, **31,560 bytes
above the limit before its outer envelope**. This is a projection from the
completed controller artifact, not a measurement of the failed leader's
serialized message.

The existing compact hexadecimal filename test covers 14,015 synthetic files,
but its test directory is five bytes shorter than this HOP leader directory
and it checks the table metadata rather than the full subtask envelope. It
therefore does not establish that the actual leader checkpoint fits. A still
shorter reversible encoding of the same per-export filename components could
save enough repeated bytes for this particular case without changing page
contents, file ownership or restore order. That would only move the threshold:
metadata remains proportional to the number of pages. It should not be treated
as milestone capacity closure without an actual leader checkpoint and recovery
run.

The leader's top-level `CheckpointManifest` is assembled **after** workers
complete their subtask checkpoints, so it cannot bypass this worker-side file
list limit. Existing checkpoint compaction likewise does not reduce the list
before this export check.

A scalable repair would version the disk/typed subtask metadata so one
validated owner prefix is stored once and each page carries a relative suffix,
while keeping old full-path metadata readable. Restore, controller and leader
ownership checks, checksum and ordering validation, and remote garbage
collection would need to reconstruct and validate the complete owned path.
This is an **unimplemented checkpoint-format proposal** requiring contract
review; it is not new SQL or a change to application behavior. An alternative
page-packing design would need separate resource-admission and concurrent
export/restore deadlock proof before use.
