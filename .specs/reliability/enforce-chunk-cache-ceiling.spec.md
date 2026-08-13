---
id: TASK:reliability/enforce-chunk-cache-ceiling
type: task
status: accepted
summary: >
  Enforce the configured client hot-chunk cache ceiling under real sync
  ownership patterns rather than treating it as a best-effort target.
owners: [carlo]
progress: pending
refines: [REQ:reliability/durable-state-contract#c-cache]
assignee: carlo
eta:
blocked_by: []
---

# Enforce chunk cache ceiling

## Plan

Correct [HotChunkCache admission and eviction](spec:src:core/src/chunk_storage/fs_storage.rs)
so cache ownership is released to meet `max_chunk_bytes` even while staging or
callers hold cloned `Arc` values. Keep recency bookkeeping bounded under hits
and duplicate inserts, expose test-only/accounting observability, and ensure a
chunk larger than the limit is never retained. Streaming transfer buffers,
tree-cache policy, OS page cache, and changing the configured default are
non-goals.

## Acceptance

- After every insertion, replacement, hit, and eviction, cache-owned payload
  bytes are at most the configured ceiling; zero retains no entries and an
  oversized chunk bypasses retention.
- Held external `Arc` clones cannot pin entries in the cache or strand
  `total_bytes` above the ceiling, and recency metadata remains proportional to
  live entries during repeated hits.
- Randomized model tests cover duplicate hashes, variable sizes, external
  owners, zero/small ceilings, and overflow-safe accounting.
- A client sync integration test with a deliberately tiny ceiling demonstrates
  correct activation while cache accounting never exceeds the limit; existing
  cache/storage tests and the full documented validation suite pass.
