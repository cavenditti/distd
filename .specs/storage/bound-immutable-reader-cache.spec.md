---
id: TASK:storage/bound-immutable-reader-cache
type: task
status: accepted
summary: >
  Keep immutable active-revision reader recency exact and proportional to the
  bounded live file cache under repeated reads.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-immutable-reader-cache
assignee: carlo
eta:
blocked_by: []
aspects: [activation, cache, memory, lru, evidence]
---

# Bound immutable reader recency

## Plan

Keep the existing 32-file immutable reader ceiling and make its recency queue
an exact permutation of the live cache keys. A hit removes the path's previous
position and appends it once as most-recently used. Admission appends one new
path, and pressure evicts the single front path. The cache stays protected by
its existing mutex, so each operation publishes one complete recency state.

The fixed 32-entry ceiling bounds the move-to-MRU scan independently of read
count. No active handle or caller-owned file reference is invalidated by cache
eviction; eviction releases only the cache's file reference.

## Acceptance

- Reading more than 32 immutable files keeps both live entries and recency
  records at or below 32 with unique, matching keys.
- One hundred thousand hits on a cached path leave exactly 32 recency records;
  admitting a new path retains the hot path and evicts the true oldest path.
- A release reference preserves 20,000 raw hit samples, keeps entry and recency
  counts equal, and gates p95 at ten microseconds.
- Focused/full tests, strict Clippy, formatting, exact-baseline spec lint, and
  requirement coverage pass.
