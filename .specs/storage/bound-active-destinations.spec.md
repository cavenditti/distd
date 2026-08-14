---
id: TASK:storage/bound-active-destinations
type: task
status: accepted
summary: >
  Enforce the startup active-record ceiling during concurrent runtime
  activation so successful state always remains restartable.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-activation-count
assignee: carlo
eta:
blocked_by: []
aspects: [activation, restart, concurrency, accounting, evidence]
---

# Bound active destinations

## Plan

Track the number of active selectors plus in-flight first-destination
reservations with one checked atomic counter. Startup initializes it from the
fully validated active-reference set. Under the existing per-destination lock,
a first activation reserves one slot before namespace, transaction, revision,
or selector mutation. Ordinary pre-selector failure releases the reservation;
successful selector replacement commits it. An indeterminate selector write
continues to poison activation until restart, where authoritative active files
rebuild the exact count.

An update whose selector already exists consumes no new slot and therefore
remains valid at the ceiling. Atomic compare-and-update admission prevents two
different destinations from both claiming the final slot. The existing
65,536-record startup bound remains the single source of truth.

## Acceptance

- With the count at 65,535, two concurrent new-destination reservations admit
  exactly one; the rejected reservation performs no mutation and releasing the
  admitted uncommitted reservation restores 65,535.
- A real new destination is rejected at 65,536 before its compatibility path or
  selector appears. An existing destination can still advance, and restart
  reconstructs the exact physical selector count rather than retaining a
  synthetic runtime value.
- Pre-selector ordinary and simulated-crash fault suites continue to prove
  reservation rollback or restart reconstruction, while same/different
  destination concurrency remains monotonic and independent.
- A release reference preserves 20,000 raw reserve/drop samples, asserts zero
  final reservations, and gates p95 at one microsecond. Focused/full tests,
  strict Clippy, formatting, exact-baseline spec lint, and coverage pass.
