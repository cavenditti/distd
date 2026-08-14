---
id: TASK:reliability/bound-publication-lock-registry
type: task
status: accepted
summary: >
  Retain publication serialization slots only while active or waiting
  coordinators reference their artifact IDs.
owners: [carlo]
progress: done
refines:
  - REQ:trust/publisher-authenticity#c-publication-locks
assignee: carlo
eta:
blocked_by: []
aspects: [publication, concurrency, cancellation, accounting, evidence]
---

# Bound publication lock registry

## Plan

Replace the process-lifetime artifact-to-lock map with a participant-counted
registry. Creating or joining one artifact slot increments its participant
count before awaiting the artifact mutex. Cancellation while waiting and guard
drop after success or failure decrement the count; the last participant removes
that exact slot. Registry mutation remains a short synchronous critical section
with no filesystem, cryptography, network, or asynchronous wait inside it.

The participant lease is acquired before the asynchronous mutex wait and owns
cleanup independently of whether the wait completes. Slot removal is identity
checked so a delayed cleanup cannot remove a replacement slot. Counter overflow
and registry poisoning fail closed before publication storage mutation.

## Acceptance

- Sequential success and authorization failure across at least 10,000 distinct
  artifact IDs leave zero registry entries after every completed request.
- Cancelling a same-artifact waiter removes only that waiter; the active guard
  retains the slot until release, after which the registry is empty. Two
  same-artifact coordinators remain serialized, while different artifacts may
  still overlap.
- Test-only accounting reports exact slot and participant counts. At every
  checkpoint, slots are no greater than distinct participating artifact IDs,
  participants equal active plus waiting acquisition leases, and zero
  participants implies zero slots.
- A release reference preserves raw samples for 20,000 distinct acquire/drop
  cycles, asserts an empty final registry, and gates p95 at 10 microseconds.
  Focused/full tests, strict Clippy, formatting, spec lint, and coverage pass.
