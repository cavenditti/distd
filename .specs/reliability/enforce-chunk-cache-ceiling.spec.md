---
id: TASK:reliability/enforce-chunk-cache-ceiling
type: task
status: accepted
summary: >
  Enforce the configured client hot-chunk cache ceiling under real sync
  ownership patterns rather than treating it as a best-effort target.
owners: [carlo]
progress: done
refines: [REQ:reliability/durable-state-contract#c-cache]
assignee: carlo
eta:
blocked_by: []
---

# Enforce chunk cache ceiling

## Plan

Correct [HotChunkCache admission and eviction](spec:src:core/src/chunk_storage/fs_storage.rs)
so the limit is a hard bound on payload allocations retained by the cache.
Owned `Vec` entries are charged by capacity while the cache holds its `Arc`;
eviction releases that reference and its charge regardless of external clones.
Slices of one `ReceivedChunk` backing retain and charge the backing's complete
allocation exactly once while at least one view remains cached; external views
do not affect cache accounting. Process RSS, caller and transport buffers,
activation staging, the tree cache, immutable file descriptors, OS page cache,
and changing the configured default are non-goals.

Replace duplicate-token recency with strict entry LRU: one recency node per
live key, hits and replacements move that node to MRU in bounded time, and
eviction always removes the LRU cache entry even when another owner retains the
payload. A zero ceiling and an empty payload retain nothing. A candidate whose
standalone retained charge exceeds the limit bypasses admission without
changing an existing same-key entry. For an admissible candidate, compute its
incremental charge after same-key replacement and shared-backing deduplication,
evict before publishing the new entry, then install it without any transient
over-ceiling state. Shared-backing siblings may require multiple entry
evictions before the final view releases their charge.

Use checked accounting and cached-view counts. `total_bytes` must exactly equal
the sum of owned capacities plus unique shared-backing allocations after every
operation; impossible underflow, overflow, missing backing usage, or receive
backing-identity exhaustion fails rather than saturating or wrapping. Expose a
read-only accounting snapshot for integration proof: configured limit, current
and peak charged bytes, entry and recency counts, unique shared backings, and
cached shared views. Production behavior does not depend on this observer.

Factor the blocking storage-commit half of client sync so production and the
integration test use the same `spawn_blocking -> receive_sync_item_shared`
route. The integration fixture uses real bulk `ReceivedChunk` slices with a
one-byte cache ceiling, completes initial and changed activation with local
reuse, and verifies the final artifact, restart, transfer cardinality, and
accounting. No network protocol or QUIC behavior changes in this task.

On the disclosed local reference system, an ignored release-mode cache
benchmark performs one million warm hits in at most 100 ms without recency
growth. Same-key replacement remains constant-time, with median latency at
most 10 microseconds and p95 at most 100 microseconds. These are local
regression gates, not portable performance guarantees.

## Acceptance

- After every insertion, replacement, hit, and eviction, cache-owned payload
  bytes are exactly recomputable and at most the configured ceiling; zero
  retains no entries or empty payloads and an oversized candidate bypasses
  retention without disturbing an existing same-key entry.
- Held external `Arc` clones cannot pin entries in the cache or strand
  charged bytes above the ceiling. Recency contains each live key exactly once,
  remains equal in length to the entry map during repeated hits, and implements
  strict LRU on hits, insertion, and replacement.
- Owned entries charge `Vec::capacity`; shared receive views charge the full
  original backing allocation once. Cached-view counts, backing counts, current
  charge, and peak charge remain exact across duplicate hashes, same-backing
  views, replacements, and multi-entry eviction.
- Randomized model tests cover duplicate hashes, variable sizes, external
  owners, shared slices, zero/small/maximum ceilings, at-limit and over-limit
  candidates, and synthetic near-overflow accounting. The independent model is
  compared after every operation, including exact entries, MRU order, shared
  view counts, current charge, peak charge, and invariant recomputation.
- Regressions cover 100,000 repeated hits without recency growth; normal reads
  returning held owned `Arc` values followed by cache pressure; every shared
  view being evicted before its backing is uncharged; rejected oversized
  replacement preserving the previous value; and fail-stop receive-backing
  identity exhaustion.
- A client sync integration test with a one-byte ceiling passes real shared
  bulk receive slices through the production blocking storage-commit helper.
  Initial and changed revisions activate and restart with exact bytes while
  cache current and peak charge never exceed one byte, the oversized bulk
  backing retains no cached views, local-basis chunks are reused, and recency
  and backing metadata remain bounded.
- The ignored release-mode reference benchmark reports aggregate elapsed time
  for one million hits plus replacement median and p95, and meets those local
  latency gates without growing recency metadata.
- Existing cache/storage tests, strict configured Clippy, spec lint, and the
  full documented workspace validation suite pass.
