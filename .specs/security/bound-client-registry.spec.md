---
id: TASK:security/bound-client-registry
type: task
status: accepted
summary: >
  Bound retained client enrollment identities and reject excess registrations
  atomically without breaking exact UUID reuse.
owners: [carlo]
progress: done
refines:
  - REQ:security/bounded-quic-service#c-registry
assignee: carlo
eta:
blocked_by: []
aspects: [configuration, enrollment, accounting, concurrency, evidence]
---

# Bound client registry

## Plan

Add `DISTD_MAX_REGISTERED_CLIENTS` with a 4,096 default and strict
1..=65,536 range, parsed before secrets, state, listeners, or storage. Carry the
validated limit into every server backend. Reject empty names and names above
256 UTF-8 bytes before logging, UUID derivation, or registry locking.

Under the one registry write lock, return and refresh an exact existing UUID,
then check capacity before inserting a distinct derived UUID. This keeps
idempotent reconnects available at capacity and makes competing final-slot
registrations linearizable. Enrollment-token, TLS, UUID derivation, and wire
formats remain unchanged.

## Acceptance

- Configuration absent/1/4,096/65,536 produces exact limits; zero, 65,537,
  empty, signed, fractional, leading-zero, nonnumeric, non-UTF-8, and overflow
  fail before server construction.
- Names of one and 256 UTF-8 bytes register; empty and 257-byte names fail with
  zero registry mutation. Diagnostics do not include the rejected name,
  enrollment token, or UUID material.
- A full registry still accepts its exact existing UUID without increasing the
  count. A distinct registration fails, and two registrations competing for
  one final slot produce exactly one success and never exceed the limit.
- A release reference fills the default registry, preserves raw samples, and
  gates existing-UUID and full-registry rejection p95 at 100 microseconds.
  Focused/full tests, strict Clippy, formatting, spec lint, and coverage pass.
