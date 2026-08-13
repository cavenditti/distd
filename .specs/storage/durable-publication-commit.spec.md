---
id: TASK:storage/durable-publication-commit
type: task
status: accepted
summary: >
  Publish server metadata only after revision storage and its durable metadata
  snapshot commit successfully as one recoverable transition.
owners: [carlo]
progress: pending
refines:
  - REQ:storage/durable-artifact-state#c-publication-commit
  - REQ:storage/durable-artifact-state#c-durability
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-canonical
  - REQ:reliability/durable-state-contract#c-migration
aspects: [publication, durability, envelope, canonicalization, migration]
assignee: carlo
eta:
blocked_by: [TASK:storage/version-isolated-revisions]
---

# Durable publication commit

## Plan

Refactor [single-file and directory publication](spec:src:server/src/server.rs)
to build an isolated candidate, durably commit its version-isolated storage and
metadata snapshot, and only then swap the in-memory state visible to fetch and
sync readers. The metadata writer must use unique same-directory staging,
the shared canonical durable envelope, file/directory sync, and explicit
recovery rather than mutating the shared `RwLock` before a fallible write.
Migrate only declared-supported legacy metadata while preserving the original
until the replacement is durable. Unreferenced chunks from an aborted candidate
may remain for later garbage collection; multi-process writers and retention
policy are non-goals.

## Acceptance

- Injected serialization, write, sync, rename, and directory-sync failures
  return publication failure while latest, exact-version, REST metadata, and
  on-disk metadata all retain the previous revision.
- Crash injection at every storage/metadata/visibility boundary restarts into
  either the complete previous revision or complete new revision; metadata
  never references missing or mismatched bytes.
- Concurrent fetch/sync readers cannot observe the candidate until durable
  commit succeeds, and successful publication remains idempotent and version
  monotonic under the existing publication lock.
- Logically equal metadata assembled with different map/set insertion orders
  produces identical envelope bytes; corrupt or unsupported legacy metadata is
  preserved and fails explicitly rather than becoming an empty server.
- Single-file and directory tests exercise both filesystem and redb/default
  server backends where supported, followed by the full validation suite.
