---
id: TASK:storage/bound-activation-lock-registry
type: task
status: accepted
summary: >
  Retain filesystem activation serialization slots only while destination
  operations actively reference them.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-activation-locks
assignee: carlo
eta:
blocked_by: []
aspects: [activation, concurrency, cancellation, accounting, evidence]
---

# Bound activation lock registry

## Plan

Replace the process-lifetime destination-identity mutex map with a
participant-counted registry. Creating or joining one destination slot
increments its participant count before locking the destination mutex. Every
return path owns a lease, so success, rejection, a missing active revision, or
an abandoned waiter decrements the count; the final lease removes the exact
slot. Registry mutation remains a short synchronous critical section and never
contains filesystem work or waits on the per-destination mutex.

Slot removal is identity checked so delayed cleanup cannot remove a replacement
slot. Checked participant arithmetic treats overflow or underflow as an
invariant failure. Existing same-destination monotonic serialization and
different-destination independence remain authoritative.

## Acceptance

- At least 10,000 distinct missing-destination active-revision lookups leave
  zero registry entries instead of retaining one slot per caller-selected path.
- Exact test accounting proves one slot per distinct participating identity,
  participants equal active plus waiting leases, abandoning one waiter does not
  remove an active slot, and dropping the final lease empties the registry.
- Existing same-destination reverse-order and different-destination activation
  concurrency regressions continue to pass unchanged.
- A release reference preserves 20,000 raw distinct acquire/lock/drop samples,
  asserts an empty final registry, and gates p95 at 10 microseconds. Focused and
  full tests, strict Clippy, formatting, spec lint, and coverage pass.
