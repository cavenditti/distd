---
id: TASK:reliability/durable-state-envelopes
type: task
status: accepted
summary: >
  Add the minimal canonical durable envelope and atomic migration primitive,
  then apply it to filesystem-storage state as the first consumer.
owners: [carlo]
progress: pending
refines:
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-canonical
  - REQ:reliability/durable-state-contract#c-migration
aspects: [envelope, canonicalization, migration]
assignee: carlo
eta:
blocked_by: []
---

# Durable state envelopes

## Plan

Implement a bounded kind/version/length/integrity envelope and same-directory
durable atomic replacement primitive, then apply it to `FsStorageState` in
[filesystem storage](spec:src:core/src/chunk_storage/fs_storage.rs) as the first
consumer. Define canonical serialization by sorting every map and set before
encoding rather than relying on hash iteration order. Migrate only the exact
legacy filesystem-storage formats declared supported, preserving their source
until the replacement and parent directory are durable. Server metadata adopts
the primitive in durable publication, activation transaction state in
crash-consistent activation, and client trust state in atomic TOFU; those
consumers do not block this primitive. This task neither introduces a QUIC
version preface nor depends on the wire-compatibility task.

## Acceptance

- The shared decoder and filesystem-storage kind reject bad magic, wrong kind,
  unsupported version, checked length overflow, truncation, integrity failure,
  trailing bytes, and payloads above the documented bound before codec
  allocation.
- Repeated encoding of logically equal current state with different map/set
  insertion orders produces identical bytes. Golden current fixtures are
  byte-stable; legacy fixtures are compared semantically before migration.
- Fault injection after create, encode, write, file sync, rename, and directory
  sync leaves either the untouched legacy/current source or one complete new
  envelope; repeated restart recovery is idempotent.
- No test or implementation path requires QUIC negotiation to open or migrate
  local state. Focused primitive, filesystem fixture, corruption, and restart
  tests pass, followed by the full documented validation suite.
