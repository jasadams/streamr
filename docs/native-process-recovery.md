# Native production process recovery

`scripts/test-native-process-recovery.py` prepares and supervises a finite,
caller-pinned top-level `arroyo run` experiment. It uses existing Impulse input,
a raw file sink and a native scalar updating aggregate with a Debezium file sink,
fixed parallelism one and the existing process scheduler. It changes no engine,
SQL, clock or recovery semantics. The harness is prepared infrastructure; no
production startup or recovery result is claimed here.

The caller supplies the real production ELF, read-only source-free artifact root,
nonempty provenance JSON, new writable case directory, backend and production
checkpoint mode (`controller` or `worker`, called leader in SQL captures).
Preparation writes JSON-as-YAML configuration, SQL and independent complete raw
and final aggregate oracles before startup. It pins the executable, packaged
files, harness, fixture and supplied provenance hashes. Caller provenance must
identify source revision and dirty diff, successful gates, executable/dependency
hashes and the compatible runtime image digest. A caller-supplied provenance file
is retained evidence; the harness cannot itself establish its build claims.

The default fixture emits 20,000 counters at 100/second, with periodic five-second
checkpoints. The supervisor requires a completed checkpoint during a proper
source prefix and checks committed metadata identity, singleton ownership, the
Impulse source, its existing watermark owner, both file sinks and
`native-aggregate-v1`. It reads every
referenced object and verifies native disk-object checksums. It independently
decodes the existing global Parquet key/value state and Bincode 2 standard
encoding: source counter/start time, sink paths and committed byte offsets. The
committed raw output must equal counters `0..counter-1`; the committed aggregate
CDC must have exact prefix arithmetic, continuous before-images and final
`n` exactly equal to the decoded source counter. These
objects and committed output prefixes are retained before fault injection, so
normal checkpoint retention cannot erase the selected proof.

The default `--fault-mode worker` targets one directly observed owned worker. PID start time, actual ELF,
process-scheduler flag and pipeline/job/generation environment identify it;
SIGKILL follows completed checkpoint validation while the top-level process
remains alive. The harness requires a different PID/start identity and greater
generation for the same job/pipeline, then job `Finished`, all raw records in
exact order and exact final aggregate values. All aggregate CDC actions and
before/after images are checked; variable source batching may skip intermediate
prefix sizes, but no value, type, order, duplicate or final state is weakened.
The default final state is `n=20000,total=199990000,lo=0,hi=19999`.

Linux subreaper mode and a new owned process session contain the run. Cleanup
continuously discovers that session, including replacements spawned during
shutdown, signals recorded PID/start identities through pidfd, and reaps adopted
worker zombies. It waits for the top-level child and escalates after a finite
timeout. A cleanup failure makes the terminal result fail and is retained.
No broad process-name kill or detached runtime is used. The full job API timeline,
process identities, output files, retained checkpoint objects, fault/replacement
records and terminal result remain in the case directory on pass or failure.
RSS is the sampled sum of top-level and current descendants (250 ms nominal
poll interval plus API/validation work); it is not a kernel-enforced bound or a
continuous peak. The declared default combined envelope is 1 GiB, distinct from
individual SQL capture envelopes. API failures, job failures, constructor limits,
missing checkpoint artifacts and unsupported output envelopes fail visibly.

## Source-free execution

Root packaging is separate from this helper. Use one pinned compatible Bookworm
runtime with Python 3.11+, PyArrow and the ELF system interpreter. Root can supply the verified PyArrow
19.0.1 CPython3.11 wheel under `/bundle/python` and set `PYTHONPATH` accordingly. Copy the real
`arroyo` ELF, its required shared libraries, this script and provenance into the
artifact. Invoke the ELF normally with packaged libraries in `LD_LIBRARY_PATH`;
the process scheduler spawns `current_exe()`, so invoking a loader explicitly is
not equivalent. No repository or application checkout is mounted at execution.

After root has packaged and reviewed the actual artifact, run one foreground
case at a time. Replace the package/results paths and image with pinned values:

```sh
podman run --rm --network none --security-opt label=disable \
  -v PACKAGE:/bundle:ro -v NEW_RESULTS:/results:rw \
  -e LD_LIBRARY_PATH=/bundle/lib -e PYTHONPATH=/bundle/python PINNED_BOOKWORM_IMAGE \
  python3 /bundle/test-native-process-recovery.py \
  --binary /bundle/bin/arroyo --source-free-root /bundle \
  --provenance /bundle/provenance.json \
  --case /results/rocks-controller-v4 --backend rocksdb --protocol controller
```

Repeat with unique case directories for memory/controller, memory/worker and
RocksDB/worker. The API, controller and worker bind loopback inside the container, and the
existing global `hostname` is explicitly `127.0.0.1`; external network is
unnecessary. `arroyo_rpc::local_address` honors this existing hostname override
or loopback worker bind instead of discovering a non-loopback interface. The script cleans inherited `ARROYO__`, `STREAMR_TEST_` and
`STREAMR_CAPTURE_` variables and uses actual production config fields, not test
flags. Generated operator/resource budgets are explicit small-fixture admission
values, not recommended production defaults. A 16 MiB execution startup refusal
is a finding, not an automatic reason to enlarge it. Both backends declare shared live resource budgets required by the native
aggregate. Memory uses its resident cap; RocksDB additionally declares disk
configuration. The case must be new and
outside the read-only package. `--prepare-only` creates reviewable assets without
starting any process. Default overall timeout is 600 seconds; chosen row/rate
must allow at least 60 seconds of real source activity and leave 90 seconds for
recovery inside the timeout. Initial startup must reach `Running` within the
separate `--startup-timeout` (default90 seconds); Scheduling/API startup failures
retain the last job state and a bounded runtime-log tail in the terminal result.
This is a test deadline, not a change to engine scheduling or recovery semantics.

## Controller/catalog process loss

`--fault-mode controller --protocol controller` optionally kills the actual top-level
controller parent after completed checkpoint validation, tears down all still-live
owned workers and reaps the whole original session before relaunching the identical
ELF argument vector. It preserves the same query/config/checkpoint directory and
embedded `state.sqlite` catalog, including post-loss physical WAL/SHM evidence.
Read-only SQLite connections make consistent NEW evidence backups; no catalog
row is edited or reset by the harness. Both phase logs and complete pre-loss outputs
are retained. Default worker behavior remains available without this option.

Initial fault admission uses the highest ready checkpoint with nonnull finish_time.
The post-loss catalog determines the highest ready/committing checkpoint for the
same internal job, including any commit that raced the fault. Existing committing
rows have null finish_time and remain eligible for recovery; there is no fallback
to an older ready checkpoint after loss. Its exact metadata,
source counter/start_time, sink offsets and committed raw/aggregate prefixes are
independently checked and retained again. Recovery must log that selected epoch
and the exact restored Impulse counter/start_time; committing state additionally
requires the existing commit replay log. The supervisor waits within its restart
startup deadline for these actual restoration messages instead of assuming that
Running alone proves restoration. INFO logging is forced for this mode.

The restarted catalog must preserve cluster id, pipeline identity, query and
serialized program hash, job id and status public id, ownership and parallelism
configuration. The new parent/worker have new PID/start identities and the same
ELF/argv/job/pipeline, with increased catalog run_id and worker generation.
Final raw/CDC oracles remain exact and both sinks must preserve the selected
committed bytes. A wrong empty state directory creating a new job, orphan workers
writing concurrently, a stale restored state or reset source/sink cannot pass.

Use a fresh case and reviewed artifact, for example add:

```sh
--case /results/controller-restart-rocks-controller \
--backend rocksdb --protocol controller --fault-mode controller \
--rows 6000 --rate 100 --timeout 600 --startup-timeout 90
```

This is the existing local catalog/recovery path; no Postgres or replacement catalog
architecture is needed for this case. Controller loss with `--protocol worker` is
explicitly rejected before creating a case until its leader/catalog path is separately
qualified. Remote catalog publication, surviving-worker reconnection, catalog data
loss, power loss, rolling binary upgrades and 24-hour faults remain distinct gates.
Normal ELF invocation and read-only source-free bundle requirements apply to both
phases. Avoid `:z`/`:Z` relabeling when dependencies share hardlinks with earlier
artifacts; the isolated runtime example disables SELinux labeling instead.

## Limits and remaining gates

The default worker mode is process loss after a completed checkpoint, with real production
source counter and sink offset consistency. It does not deterministically kill
inside export/publication, establish which of several rapidly completed epochs
the controller selected, or compare native accumulator bytes to an external
business oracle. Complete final output and committed-prefix values are the
independent recovery assertions. The optional controller mode below selects and asserts the exact persisted recovery epoch; it remains unverified until actually run.

Startup, automatic restart generation handling, API availability and both
production protocols still require actual runs on the rebuilt candidate. The
strict source/sink codec reader follows the current checkpoint format (Parquet task/subtask version2, encoding1, exact schema/namespace/
epoch/generation/config discriminators, numeric protobuf map keyzero defaults
and declared object sizes/checksums); unfamiliar formats fail rather than being guessed. PyArrow is a runtime preparation
prerequisite. No arbitrary broker, TLS, Kafka duplicate/upsert policy, resource
capacity, hot key, slow sink, 24-hour live/fault or full external profile/session
acceptance follows from this small run. Those retain the requirements in STR29,
STR32 and the milestone capability records.

The first source-free attempt is preserved under
`target/native-process-recovery-admission-v5/rocks-controller/`: the network-none
runtime lacked a non-loopback IPv4 address, and the default worker bind triggered
`LocalIpAddressNotFound` while the controller stayed Scheduling. Root interrupted
that attempt; its failed result records cleanup complete and top-level exit0.
The explicit existing loopback configuration repairs the harness environment.
It does not establish that the next packaged attempt starts or recovers. Use a
new artifact/case and retain the original failure logs.

The second attempt (`rocks-controller-v2`) failed during worker construction,
before Running, output, checkpoint or injected loss. Its existing configuration
validator derives a checkpoint page of
`min(2097152/(8*(2+1)),33554432/8,16777216/4,1048576)-32768 = 54613`
bytes. The prior32KiB row cap required `2*32768+1024 = 66560` bytes and was
therefore correctly rejected. The scalar fixture now declares a16KiB disk row
cap, requiring33792 bytes within that bound. Shared pool budgets, engine
validation and all input/output/prefix oracles are unchanged. This is a small
fixture configuration correction, not capacity evidence; a larger application
row contract must receive independently validated sizing. Preserve v1/v2
artifacts and package the reviewed helper into a new v3 artifact/case before
retrying on the same pinned production ELF.
