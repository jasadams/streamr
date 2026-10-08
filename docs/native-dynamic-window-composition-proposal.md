# Dynamic keys and latest closed-window counts: SQL proposal

This is an unqualified standard-SQL proposal, not an implemented capability or
milestone 3 completion claim. The diagnostic against the frozen v14b candidate
finished with a planner rejection: `Error during planning: can't handle updating
left side of join`. The SQL child exited 101; cleanup completed and provenance
remained stable. This matches the restriction found in current and pinned
upstream Arroyo source.

Given a caller-defined, watermarked event source `events` with opaque key `k`
and a caller-defined sink `result`, the proposed relation is:

```sql
SET updating_ttl = NULL;

CREATE VIEW lifetime AS
SELECT k, COUNT(*) AS lifetime_count FROM events GROUP BY k;

CREATE VIEW rolling AS
SELECT k,
       HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
       COUNT(*) AS recent_count
FROM events
GROUP BY k, window;

CREATE VIEW closed AS
SELECT k, window.end AS window_end, recent_count FROM rolling;

CREATE VIEW lifetime_keys AS
SELECT k, lifetime_count, CAST(1 AS BIGINT) AS clock_tag FROM lifetime;

CREATE VIEW current_clock AS
SELECT CAST(1 AS BIGINT) AS clock_tag, MAX(window_end) AS current_end
FROM closed;

CREATE VIEW current_counts AS
SELECT l.k, l.lifetime_count,
       COALESCE(r.recent_count, CAST(0 AS BIGINT)) AS recent_count
FROM lifetime_keys l
INNER JOIN current_clock c ON l.clock_tag = c.clock_tag
LEFT JOIN closed r ON l.k = r.k AND r.window_end = c.current_end;

INSERT INTO result SELECT k, lifetime_count, recent_count FROM current_counts;
```

The constant equality tag broadcasts one clock row to dynamic lifetime keys; it
does not enumerate a fixed set of keys. When the globally latest closed window
has no row for a retained key, the left join would provide zero for that key.
The diagnostic applies the original caller-selected key filter only at the final
output so its existing input and independent oracle remain unchanged. It requires
lifetime/recent counts of 3/2 at the selected checkpoint and 3/0 during the two
live holds and at EOF. Those are required values, not observed passing results.

## Current engine restrictions

Projecting `window.end` without the full window field drops full window scope in
`crates/arroyo-planner/src/plan/mod.rs`. The lifetime and clock aggregates retain
their updating metadata. `plan/join.rs` rejects updating join inputs and rejects
unwindowed non-inner joins. The [pinned upstream implementation](https://github.com/ArroyoSystems/arroyo/blob/1546c10edbcc5e00f229c7e819480ce5e8acc940/crates/arroyo-planner/src/plan/join.rs)
has these restrictions too. An updating join output does not imply support for
updating inputs.

Relaxing those checks alone would be insufficient. The existing
`join_with_expiration.rs` runtime retains key/time batches, concatenates matching
batches, constructs a separate execution environment and substitutes a 24-hour
TTL for zero. It does not establish the indefinite lifetime visibility, configured
live backend, bounded working memory, updating-input retractions and checkpoint
lineage required here. No guard bypass, new operator or interface is approved by
this proposal.

## Semantic and resource limits

This clock is the latest **nonempty closed window**, not wall time or an empty
window generator. It cannot advance when no source produces another closed
window, and sparse input can make it lag a required current/calendar boundary.
The query does not specify initial partial-window output or atomic intermediate
results across independently scheduled branches. Retaining lifetime keys and
closed history indefinitely can grow state; bounded join retention and fan-out
must be established before this could qualify the application contract.

The private diagnostic manifest is
`49c31365064d1a93eafc6a214890e08e4841c6d36d9281701437b7f8460e85c0`.
Its original input, value oracle, 120-second deadline, 512 MiB SQL-process RSS
limit and 16 MiB execution limit are unchanged. The completed evidence is under
`target/native-dynamic-latest-window-join-admission-v14b/`: `capture.log` records
the exact planner error and `result.json` records failure and completed cleanup.
The capture log SHA-256 is
`8d3ffa184d639d65ef0f4cb8505ae8bee9e5ec4827d14908430238ad45feb161`.
The rejection establishes a planner limitation, not dynamic-key runtime parity,
bounded state, recovery or all-idle behavior. No engine change is approved.
