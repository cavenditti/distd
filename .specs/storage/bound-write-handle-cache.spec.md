---
id: TASK:storage/bound-write-handle-cache
type: task
status: accepted
summary: >
  Replace eager unbounded ordinary-file write handles with a lazy 32-entry
  cache whose aggregate userspace buffers stay within eight MiB.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-write-handle-cache
assignee: carlo
eta:
blocked_by: []
aspects: [storage, file-descriptors, memory, restart, evidence]
---

# Bound ordinary write handles

## Plan

Retain at most 32 ordinary-file write handles, each with one 256 KiB buffer.
Open handles only when a mutable path is preallocated; persisted populated
backings reopen transiently for reads instead of being eagerly loaded at
construction. Cache hits move one path to most-recently used. Admission at the
ceiling flushes the true oldest handle before releasing the cache's reference.

When every chunk for a path is populated, flush and remove its cached handle.
Explicit item removal and activation reconciliation flush before removing any
matching handle. External `Arc` owners do not count against cache ownership and
do not prevent eviction.

## Acceptance

- Pressure over a two-entry fixture flushes and removes the true oldest path,
  preserves a caller-held handle, and keeps exact recency.
- Completing an ordinary item leaves no cached write handle. Restart opens no
  write handles, while indexed reads still return the exact persisted bytes.
- The production 32-entry count times the 256 KiB buffer is at most eight MiB,
  independent of stored item count.
- A release reference preserves 20,000 raw hot-lookup samples and gates p95 at
  ten microseconds with one entry and one recency record.
- Focused/full tests, strict Clippy, formatting, exact-baseline spec lint, and
  requirement coverage pass.
