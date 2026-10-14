# STR-109 native scan bounds repair

The RocksDB adapter checked namespace/prefix/end only after native seek/next.
An empty calendar prefix could therefore traverse unrelated deleted dirty-state
keys before Rust rejected the first visible out-of-range key. Passing the same
exclusive ceiling to RocksDB repairs that misuse without changing SQL, snapshots,
cursors, checkpoints or resource limits.

Source: `74ff105641eea659d80f6a584f2f4c2fdd829e91`, based on main
`453b33c73cd11c5cc277a08639bbda2c7fe08ae1`. Only `rocks.rs` differs among the
605 compiled inputs from the telemetry-only baseline. The unpublished first-entry
scan change and PR #44 serialization work are excluded.

## Local checks

Locked `arroyo-state` check, strict preflight Clippy, formatting and all 14
`live::rocks::tests` passed in the required Bookworm container. Tests cover encoded
zero/FF prefixes, namespace isolation, range intersections, pagination, snapshots,
read limits, admission and checkpoint behavior. The direct native regression
reproduces exactly 1,000 unrelated deletion skips without the bound and zero with
the production helper; no wall-clock performance assertion is used.

Independent source/local validation approved. Production CLI build passed using
the unchanged release/O3 native recipe (OpenAPI host dependency remains O0).
Binary SHA256: `ad7c90dca2c46775ef17fe572856c4ddc2315d65b976544af5297e0015d18b02`.
Runtime image: `e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Native archives and executable are preserved outside Cargo caches.

## Full-engine growth observation

Same fresh four-owner Impulse graph, 400,000 event keys, 2,048-byte payload,
1,000 rows/s source target, 62 MiB shared state pool, 512 MiB sampled process-group
RSS limit and 120-second checkpoint cadence. Metrics sampled every five seconds;
reported rates use actual monotonic intervals. This is one sequential growth pair.

The telemetry-only baseline collapsed to about 7–8 emitted rows/s after checkpoint
three near 199,000 events; native scans averaged approximately 14 ms. The repaired
run continued through the planned 460,000-event update observation:

| Completed checkpoint preceding non-checkpoint intervals | Emitted rows/s | Average native scan ms | Logical value range, decimal GB |
| --- | ---: | ---: | ---: |
| 3 | 411.14 | 0.07275 | 3.592–4.219 |
| 4 | 403.02 | 0.07479 | 4.737–5.377 |
| 5 | 423.33 | 0.06135 | 5.955–6.611 |
| 6 | 319.43 | 0.06959 | 6.973–7.123 |

At the common pre-checkpoint-three state range near 3.03–3.14 GB, baseline and
candidate rates were 563 and 504 rows/s respectively. There is no universal
speedup claim. The baseline has no non-checkpoint observation at the candidate's
larger post-checkpoint-three sizes, so the late rates are evidence that the cliff
did not recur at larger state, not a strictly matched-state speedup ratio.

The run emitted 460,500 events in 1,455.228 seconds: 316.45 rows/s including
startup and checkpoint costs. All four owners received at least 460,178 rows;
all crossed 400,000 by 1,174.414 seconds. Six checkpoints completed. Final logical
values were 7,122,769,959 bytes across 7,775,064 backend entries. Backend entries
are not distinct event keys. Peak sampled owned process-group RSS was
378,798,080 bytes (361.25 MiB), below 512 MiB; sampling is not an exact maximum.
Diagnostic runner exited zero, sampler joined, provenance matched and owned container
and native processes were absent after cleanup.

Source emitted and owner received counters increment before queueing/processing;
they are flow observations, not durable completion or output correctness proofs.
Checkpoint-related costs and slow intervals remain. This repair does not establish the
1,200 rows/s target or complete STR-103's 1.2-million-event correctness/recovery
qualification.

## Evidence

Root: `/home/jason/qa-evidence/str109-throughput-20261013/`.

- `scan-investigation-20261014/{summary.json,actual-review.json,bounds-source-review.json}`
- `scan-bound-validation-20261014/{results.json,final-review.json}` and exact command logs
- `bounds-production-build/{production-source-manifest.json,actual-pin-review.json,native-cache-preserved.json}`
- `growth-20261014/{early-late-summary.json,growth-result-review.json}` (baseline)
- `growth-bounds-20261014/{preparation.json,review.json,matched-state-summary.json,case/result.json,launch-result.json}`

Raw metrics, interval indices, checkpoint exclusions and hashes are retained in
the growth evidence. Independent final growth review and PR CI are recorded in
the ticket; pending work must not be inferred as passed from this document.
