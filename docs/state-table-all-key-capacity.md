# Native state-table all-key checkpoint capacity fixture

`scripts/test-state-table-capacity.py` runs generic SQL `MERGE RETURNING` and
same-event current lookup through the existing full-checkpoint/fresh-worker
capture. The strengthened fixture inserts independent deterministic payloads for
every opaque integer key, then probes **every key**, including every restored
checkpoint-prefix row. No application schemas or policies are embedded.

The checkpoint defaults to the proper insertion prefix `rows - 1`.
`--checkpoint-rows` declares another positive prefix strictly smaller than
`--rows`. The resumed source inserts the remaining keys before all-key probes;
this separates restored prefix values from suffix inserts. Inserts expect
`action=insert`, empty OLD and present NEW/current lookup. Probes expect
`action=none` and unchanged OLD/NEW/current lookup. Each comparison covers the
complete independently generated payload through SQL string equality, quantity,
key, event ID, field names, exact JSON types and per-event order. The oracle is
prepared before execution and never derived from captured output.

The default 65,000-key × 8,192-byte configuration retains 532,480,000 payload
bytes at full insertion and **532,471,808 bytes at the 64,999-row checkpoint**.
Both exceed ten times the declared 48 MiB pool sum (503,316,480 bytes). The
configured pool sum is executor 16, block cache 8, memtable 4, queued write 2,
decoded values 16 and scan 2 MiB. This conservatively includes the memtable even
though backend accounting also charges it against the shared cache resources.
The raw-payload floor is justified by the INSERT-only prefix: one complete
independent text value per key, no deletes, updates, TTL or filtering before the
checkpoint. It is not an estimate of compressed RocksDB bytes or host RAM.

`--require-10x` rejects configurations before creating files unless both the
checkpoint and full retained-payload floors reach this threshold. Small fixtures
remain explicit smoke checks and report both capacity flags as false.

Run serially in the required Bookworm container using the coordinating agent's
fresh SQL test executable (replace `BINARY` with its absolute path):

```sh
python3 scripts/test-state-table-capacity.py BINARY --directory /app/target/state-table-all-key-small-new --rows 8 --payload-bytes 64
ARROYO__WORKER__QUEUE_SIZE=32 python3 scripts/test-state-table-capacity.py BINARY --directory /app/target/state-table-all-key-65000-new --rows 65000 --payload-bytes 8192 --checkpoint-rows 64999 --require-10x --rss-limit-mib 512 --timeout-seconds 1800
```

Both controller and leader protocols run by default; `--protocol` selects one
explicitly. These are finite foreground commands. The default timeout is 900
seconds; the large example allows 1,800 because it doubles source events relative
to the old 64-probe fixture. Python blocks until the SQL child terminates, records
Linux `wait4` peak RSS across all child threads, and kills/reaps the child on
timeout. The declared 512 MiB limit applies to the SQL-test process, including
worker/state/checkpoint tasks. Generator/verifier Python memory is outside that
child. Preparation, hashing and comparisons stream files instead of retaining
the complete input or oracle. `--prepare-only` allows preparation without a
binary. Check disk before the large run: input alone contains each >500 MiB
payload set twice, before RocksDB and checkpoint storage.

The output directory must be new or empty. Existing fixture/evidence directories
are rejected before any write; choose a fresh destination for each validation
attempt. Existing historical capacity evidence is preserved. Preparation writes
`input.jsonl`, an independent `expected.jsonl`, `fixture.json` and per-protocol
SQL. Passing modes append measurements immediately, so a later protocol failure
does not erase a prior successful mode's evidence.

Both initial and recovered outputs compare every event. The recovered sink file
preserves checkpoint output before appending resumed output. The driver verifies
`CAPTURE_RESULT committed_rows` equals the declared prefix, then independently
compares those first committed rows. It records exact committed-prefix coverage
and the number of restored-prefix keys probed. Artifacts include source revision,
tracked source diff hash (including staged changes), input/oracle/query/driver/
shared child runner/capture harness/binary hashes, final output/log hashes,
configured pool limits and measured RSS. The binary hash identifies the executable
used; it does not prove that executable was built from the source revision, so
retain the coordinating build evidence as well. Source revision plus diff hash
identifies tracked source state; it excludes unrelated untracked files.

This driver qualifies only the selected singleton RocksDB/native state-table
capacity and epoch-1 checkpoint shape when actual current-source runs pass.
It does not establish memory parity, multiple checkpoint epochs, deleted/empty
state, composite/multiple-table ownership, backend switching, hot-value/output
limits, remote/process restart, stale publication rejection, sink commits or
full live/backfill/fault readiness. The harness recreates workers within the same
OS process. Other generic drivers and real backend tests retain those separate
acceptance gates. Preparing fixtures and passing Python comparator checks are
not SQL/operator qualification or milestone completion.

## Measured frozen-v10 qualification with queue32

Both large cases passed under
`target/native-m3-state-table-queue32-diagnostic-v10/`; `result.json` reports
`status=pass` and `final_provenance_stable=true`. These are actual native
singleton RocksDB SQL/operator captures, not fixture-only simulation.

| Checkpoint mode | Peak SQL-child RSS | Enclosing driver duration | Retained checkpoint objects / bytes |
| --- | ---: | ---: | ---: |
| controller | 216,408,064 bytes (206.38 MiB) | 528.67 seconds | 1,455 / 283,159,385 |
| leader | 218,284,032 bytes (208.17 MiB) | 537.48 seconds | 1,454 / 283,195,508 |

Each case retained the exact 65,000 × 8,192-byte workload, 130,000 input
records, 64,999-row proper-prefix epoch-1 checkpoint, 48 MiB pool sum,
16 MiB execution pool, 512 MiB RSS ceiling and 1,800-second SQL deadline.
The only resource configuration difference from the preceding failed large
attempt was the existing `ARROYO__WORKER__QUEUE_SIZE=32` setting; the default
is 8,192 rows. The earlier controller source-queue allocation requested
257.8 KB with only 90.2 KB free, and the large leader case was not reached.
That failure and both successful eight-key sanity cases remain untouched in
`target/native-m3-state-table-capacity-admission-v10-v2/`. Queue32 is explicitly
supplied and bound through the pinned config loader; no independent numeric
scrape of the effective queue setting was recorded. This pass does not qualify
the default queue at the same execution budget.

The independent post-run audit compared every row of both initial and recovered
outputs (130,000 each, per protocol) against the separately specified event
oracle, including exact JSON types and per-event order. The SQL assertions
compare complete independently generated payloads, not hashes or sampled
substrings. Each capture reported exactly 64,999 committed rows, and all
65,000 probe rows include the 64,999 restored prefix keys. Initial/recovered
output SHA-256 was `2ef6d47d…` in both modes. Both enclosing drivers exited 0
with `cleanup_complete=true`.

The payload floors are 532,480,000 bytes at full insertion and 532,471,808 at
the checkpoint, exceeding the 503,316,480-byte ten-times threshold. Neither
floor counts the repeated probe inputs as additional retained state. Complete
retained-object inventories in each case record paths, sizes and SHA-256;
independent inspection matched every artifact and the exact file set. Each
mode has 1,445 Parquet objects totaling 282,758,172 bytes. Compression means
physical artifact bytes differ from the logical payload floor. No additional
raw-IPC content decoder was run for this state-table evidence; exact recovery
and full-payload lookup comparisons establish the values.

Frozen evidence is copied alongside the results: `source.json` SHA-256
`4d1782df0e2efac315e0d8a7c3895c9d21eb5c698e55ed6bef83d64939920f21`,
`build-evidence.json`
`481a00ed8f850ae87f1633aebd419ea1ffdeda40763d44a4bae98441288f5d05`,
and SQL ELF
`0a8c8f52ff5344e2639a26f820cfd5e32501129490d0a5cfb1baf96a84a594c1`.
The frozen source/build records include the compiled file-set/content and all
five successful v10 formatting/checking/strict-Clippy/unit/full-build gates.
The launcher SHA-256 is `c258d45a…`, driver `24580ca9…`; per-case measurements
also pin query, input, expected rows, capture harness, shared child runner,
outputs and logs. Final source/helper/binary checks remained stable during the
wave. Later integrated Rust repairs require their own build/runtime evidence.

The measured RSS covers the SQL process and its worker/checkpoint threads;
Python generation, streaming comparison and launcher memory are outside that
ceiling. This is state over ten times the declared pool sum, not state greater
than host RAM. It covers only the same-process fresh-worker restore of this
singleton insert-only checkpoint under the two protocols. Memory parity,
multiple epochs, process loss, deletes/empty state, multiple tables, composite
ownership and 24-hour live/fault readiness remain separate qualifications.
