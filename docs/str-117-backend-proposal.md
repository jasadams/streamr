# STR-117 bounded ordered-neighbor proposal

The user approved this concrete public backend contract on 2026-10-15 after
reviewing the walkthrough. Implementation and source-bound measurements resumed
on the same fetched base. The API and SESSION integration now implement this
contract; [current validation and exact remaining gates](str-117-validation.md)
record the authoritative outcome. The original queued-only cancellation remains
below as historical evidence. Base is fetched `origin/main`
`aaab7727200defe3c5b00e13975ef61a379084e1` in
`/home/jason/repos/streamr-worktrees/str-117-metadata-20261015`.

## Concrete gap

`SessionStore::insert` enumerates metadata from the group minimum to locate an
unknown overlapping interval. `next_session` also rereads the minimum for
Empty/Sole proofs before successor discovery. The proposed local repair can
remove redundant proof/minimum scans and deletion restarts, but it still examines
O(n) earlier intervals when appending to a group with n retained disjoint sessions.
The corrected source-pinned baseline has completed. Current candidate gates and
matched measurements are recorded in the validation report; this proposal is
not itself performance evidence.

Seeking forward at the event timestamp alone is incorrect. With an existing
interval `[0,100]`, gap 10 and event at 90, the key is stored at start 0; a forward
start bound at 90 omits that interval. Even starting at `event - gap` misses a
long interval. Because established intervals are ordered and separated by more
than the gap, only the greatest start at or before the event can be its earlier
overlapping neighbor. Subsequent possible neighbors are available through the
existing bounded forward scan. No interval-end index or application algorithm
is necessary.

## Existing source contracts

- [SnapshotReader and scan types](../crates/arroyo-state/src/live/mod.rs) expose
  snapshot `get`/`try_get`, forward `scan`/`try_scan`, namespace/prefix filtering,
  inclusive logical `start`, exclusive logical `end`, and encoded-key/value page
  byte limits. There is no predecessor or greatest-entry operation. Cursors
  belong to one exact snapshot/range; `ScanPage` owns its reservation until drop.
- [LiveTable](../crates/arroyo-state/src/live/table.rs) constructs opaque keys and
  preserves namespace/ownership. Its point reads need a fully known key; it has
  no interval or SESSION knowledge. The first caller can use its existing
  namespace with a `StateSnapshot`; no table-manager lifecycle change is needed.
- [Encoding](../crates/arroyo-state/src/live/encoding.rs) places the complete
  namespace before escaped logical key bytes, then a key terminator, then an
  optional routed hash. Escaping preserves logical byte order. Full encoded
  order therefore compares logical keys first, then routing hashes for equal
  keys. A greatest entry for a routed namespace must choose the greatest hash
  at the greatest eligible logical key, not silently collapse hash variants.
- [MemorySnapshot](../crates/arroyo-state/src/live/memory.rs) owns a stable cloned
  BTreeMap. Existing scans begin at the encoded lower bound and walk forward.
  [RocksSnapshot](../crates/arroyo-state/src/live/rocks.rs) retains a stable native
  read view, admits page/request/container memory before native work, uses escaped
  unterminated logical bounds and an exclusive native upper bound, and returns
  admitted pages. The blocking closure owns the DB view and page permit.
- [run_blocking](../crates/arroyo-state/src/live/resources.rs) retains its operation
  permit in the native closure even if the awaiting future is cancelled. The
  existing `try_scan` fails fast for scan-page admission; native completion still
  awaits the existing blocking-operation budget. A new `try` operation must not
  imply stronger fail-fast behavior than this established contract.

## Smallest proposed interface

Add stable-snapshot `last_in_range` and `try_last_in_range` operations to
`SnapshotReader` and `StateSnapshot`. A small `LastEntryRequest` contains only
an existing `ScanRange` and `max_bytes`, with the same combined encoded-key/value
meaning as `ScanRequest::max_bytes`. Return an existing `ScanPage` containing
zero or one entry and `next_cursor = None`. The name describes a generic ordered
range operation; the backend never sees times, intervals, or SESSION metadata.

The contract is the greatest full encoded entry whose namespace matches exactly,
whose logical key has the requested prefix, and whose logical key is in
`[start,end)`. It returns empty if the intersection is empty, including equal
bounds. Reversed bounds and zero byte budget are rejected as in existing scans.
An oversized eligible greatest entry errors; it must not skip that entry and
return a smaller one. Malformed eligible key/value encoding errors. No cursor
is accepted: this is one bounded neighbor lookup, not reverse pagination.

`ScanPage` retains the same reservation ownership as forward scanning. Memory
accounting covers the bounded request and encoded bound copies, the one decoded
key/value and entry container, and native iterator ownership. Existing worker
limits remain unchanged; failure to admit the request is an error rather than
an uncharged fallback/index. If an implementation cannot retain the returned
entry within this budget, it must fail before allocating it.

For SESSION, request prefix `S || group-length || group` and an exclusive logical
end equal to `session_key(group,event)` followed by byte `0x00`. Under ordinary
lexicographic byte-string ordering, appending zero is the immediate strict
successor of that complete key; it includes exactly the event start without
`event + 1` overflow at `i64::MAX`. SESSION keys have a fixed-width timestamp
suffix. The returned predecessor is validated by existing metadata decoding and
checked for inclusive gap connectivity. Existing forward scans then start at the greater of
that original start and the event key (excluding equality at the caller).
The greatest-predecessor proof establishes that no later live start is at or
before the event, so this bound skips obsolete earlier starts. Later neighbors
retain their original starts; an absent predecessor begins at the event key. No earlier interval can overlap if the nearest predecessor
does not. Connectivity/end expansion remains the existing insertion algorithm.

Take one stable snapshot for predecessor and its associated forward discovery.
Under the existing serial operator owner, newly inserted metadata cannot race
these reads. Retain only bounded pages and local scalar metadata; release each
page before the next awaited read/delete. Advance past an original examined
start after successful deletion. Cancellation drops local traversal state; a
retry discovers live metadata in a fresh snapshot. Existing group-proof
invalidation before ambiguous metadata writes, strict expiry, duplicate arrival
ordinals, row histories and checkpoint/recovery machinery remain unchanged.

## Native and memory realization to verify after agreement

Derive native lower as the maximum of namespace start, escaped prefix and escaped
inclusive logical start. Derive exclusive upper as the minimum of namespace
successor, prefix successor and escaped exclusive logical end. Check empty
intersection before constructing the iterator. Both native lower and upper
bounds must be installed before reverse positioning, so an empty group cannot
search preceding unrelated deleted keys. The request/admission must account for
both owned native bound copies; do not assume today's forward-request accounting
already proves the reverse allocation bound.

The pinned local Rust RocksDB 0.24.0 source exposes `seek_to_last`, `seek_for_prev`,
`set_iterate_lower_bound` and `set_iterate_upper_bound`. Its bundled RocksDB
`include/rocksdb/options.h` explicitly documents that `SeekToLast` with an upper
bound selects the first key smaller than that bound, and that the lower bound
limits backward iteration inclusively. Exact pinned-source hashes and line
excerpts are preserved in
`/home/jason/qa-evidence/str117-metadata-20261015/proposal-source-receipt.json`
and `proposal-native-api-excerpts.txt`. Therefore bounded `seek_to_last` is a
simpler implementation candidate than manually managing an exclusive target for
`seek_for_prev`. Validate native iterator status and logical bounds before decode.
Do not position an unbounded iterator and filter only after native traversal.

For memory, use the same encoded-bound intersection and BTreeMap bounded range
`next_back`, checking exactly the same returned-byte/error/ownership contracts.
The memory backend already clones its stable snapshot; this proposal does not
alter that lifecycle or introduce another live index. Current memory scan pages
have no additional scan-pool permit; preserve backend parity of values/errors
and the existing bounded-memory ownership model rather than claiming identical
internal resource mechanisms without checking it.

## Alternatives and tradeoffs

| Option | Benefit | Cost or remaining gap |
| --- | --- | --- |
| Proposed last-in-range plus existing forward scan | One bounded predecessor lookup; existing successor path and ranges; no reverse cursor or index | New generic public read operation and two backend implementations; native reverse seek may still traverse old internal versions, so measure it |
| Direction field and reverse cursors on ScanRequest | General reverse enumeration | Broader contract, every request literal/cursor validation and paging caller affected; unnecessary for a single predecessor |
| Strict predecessor plus existing point read at event | Avoids appended-zero inclusive-end idiom | Two read operations and explicit exact-key route ownership handling |
| Local redundant-proof/deletion-restart repair only | Small worker-only contract-preserving increment | Leaves O(n) hot-group discovery; does not satisfy timestamp-neighbor seeking |
| Extra end index or many-group interval cache | Could avoid some discovery reads | New persistent consistency/recovery surface or growing retained cache; unnecessary and outside the bounded proposal |

The only concrete caller proposed now is SESSION metadata insertion. Other
ordered aggregate/join/calendar/expiry callers remain unchanged: some require
complete enumeration and cannot be replaced by a last-entry lookup. There is no
new general profiling framework, cap increase or checkpoint redesign.

The user has agreed to this exact generic contract. Implementation must
verify empty ranges/prefixes, escaped bytes, routed duplicate logical keys,
inclusive/exclusive boundaries and integer extrema; both backend values/error
parity; oversized rows/admission refusal/cancellation/snapshot release; and
SESSION exact intervals/timestamps/values/duplicates/out-of-order bridges with
tight-cache fallback and the existing actual checkpoint/fresh-backend regression.
Reuse the finite diagnostic fixture for matched alternating baseline/candidate
measurements with source/pin receipts and actual internal/deleted-key traversal.
A single-result API bounds returned memory, not physical RocksDB traversal;
performance acceptance remains measurement, not a promised constant-time cost.

## Historical suspended work

The initial baseline command was queued only, log
`/home/jason/qa-evidence/str117-metadata-20261015/baseline-build-run.log`.
The user discussion choice stopped owned exec session 84345 with Ctrl-C; actual
exit was 130. It never acquired the build slot. Process search and container
mount search found no STR-117-owned process/container afterward. At that suspension there was no baseline/candidate run, compiled pin, check,
test, Clippy pass or speedup result. Subsequent approved implementation and
validation are recorded separately in the validation report.

The initial finite ignored fixture and temporary S-prefix PerfContext probe
were preserved during suspension; neither was then a production change. Diagnostic
`SeekOnMemtableCount` means memtable-child seeks, not total native/issued seeks.
Per-scan output overhead is diagnostic-only. After approval, the reused probe
gained error-safe opt-in profiling reset; all temporary production probes and
environment checks were then removed before final source gates.
The original forward-only transformation was not applied. The approved API,
SESSION integration and focused regression scripts were applied after agreement. The later explicit user approval authorizes the minimal contract above;
all current passes and measurements must still be recorded separately.
